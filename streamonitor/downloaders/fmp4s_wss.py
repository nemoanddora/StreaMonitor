import json
import os
import shutil
import subprocess
import struct
import time
import socket
from threading import Thread
from websocket import (
    create_connection,
    WebSocketConnectionClosedException,
    WebSocketException,
    WebSocketTimeoutException,
    ABNF,
)
from contextlib import closing
from ffmpy import FFmpeg, FFRuntimeError
import re
from datetime import datetime

from parameters import DEBUG, CONTAINER, SEGMENT_TIME, FFMPEG_PATH


_OUTPUT_MODE_ALIASES = {
    "segments": "segments",
    "segment": "segments",
    "hls": "segments",
    "hls_segments": "segments",
    "mpegts": "mpegts",
    "single": "mpegts",
    "single_file": "mpegts",
    "singlefile": "mpegts",
    "ts": "mpegts",
    "disabled": "disabled",
    "disable": "disabled",
    "off": "disabled",
    "none": "disabled",
}


def _normalize_output_mode(value):
    try:
        key = str(value).strip().lower()
    except Exception:
        key = "segments"
    return _OUTPUT_MODE_ALIASES.get(key, "segments")


DEFAULT_LIVE_OUTPUT_MODE = _normalize_output_mode(
    os.environ.get("STRMNTR_LIVE_OUTPUT_MODE", "singlefile")
)


def postprocess_and_move(
    basefilename, suffix, filename, CONTAINER, SEGMENT_TIME, tmp_mp4, DEBUG, FFMPEG_PATH
):
    try:
        # -------- Post-processing --------
        stdout = (
            open(filename + ".postprocess_stdout.log", "w+")
            if DEBUG
            else subprocess.DEVNULL
        )
        stderr = (
            open(filename + ".postprocess_stderr.log", "w+")
            if DEBUG
            else subprocess.DEVNULL
        )
        output_str = "-c:a copy -c:v copy"
        if SEGMENT_TIME is not None:
            output_str += (
                f" -f segment -reset_timestamps 1 -segment_time {str(SEGMENT_TIME)}"
            )
            outname = basefilename + "_%03d" + suffix + "." + CONTAINER
        else:
            outname = filename
        ff = FFmpeg(
            executable=FFMPEG_PATH,
            inputs={tmp_mp4: "-ignore_editlist 1"},
            outputs={outname: output_str},
        )
        ff.run(stdout=stdout, stderr=stderr)
        os.remove(tmp_mp4)
    except FFRuntimeError as e:
        if e.exit_code and e.exit_code != 255:
            return False

    try:
        # -------- Move processed file(s) --------
        import shutil
        import glob
        import re

        today_str = datetime.now().strftime("%Y%m%d")
        target_dir = f"/Volumes/T7 2TB/opnemen/klaar/{today_str}"
        os.makedirs(target_dir, exist_ok=True)
        if SEGMENT_TIME is not None:
            segment_pattern = basefilename + "_*" + suffix + "." + CONTAINER
            for segfile in glob.glob(segment_pattern):
                shutil.move(
                    segfile, os.path.join(target_dir, os.path.basename(segfile))
                )
        else:
            cleanName = re.sub(r"_LR_F\d+(?=\.(?:mkv|ts)$)", "", filename)
            pattern = re.compile(
                r"^(.*?)-(\d{4})(\d{2})(\d{2})-(\d{2})(\d{2})(\d{2})(?=\.(?:mkv|ts)$)"
            )
            match = pattern.match(cleanName)
            if match:
                prefix, year, month, day, hour, minute, second = match.groups()
                newFilename = f"{prefix}_{year}-{month}-{day}_{hour}-{minute}-{second}_DreamCam.mkv"
                shutil.move(
                    filename, os.path.join(target_dir, os.path.basename(newFilename))
                )
            else:
                print("Filename does not match expected pattern.")
                shutil.move(
                    filename, os.path.join(target_dir, os.path.basename(filename))
                )
        return True
    except Exception as e:
        print(f"Error during post-processing or moving: {type(e).__name__}: {e!r}")
        return True


def getVideoWSSVR(self, url, filename):
    """
    Records fMP4-over-WebSocket with robust reconnects and live muxing.

    - Writes init (ftyp+moov) once, then appends moof/mdat across reconnects.
    - Streams bytes to ffmpeg via stdin; ffmpeg produces either HLS segments or a single MPEG-TS file.
    - Auto-stops on idle / too many consecutive failures / optional deadline.
    - If live muxing is disabled, falls back to previous behavior: write tmp fMP4 and post-process after.

    Outputs when live muxing is enabled:
      segments mode: <base><suffix>.m3u8 + <base>_00001.ts, <base>_00002.ts, ...
      mpegts mode:   <base><suffix>.ts
    """

    # -------- Switches --------
    raw_output_mode = getattr(self, "live_output_mode", DEFAULT_LIVE_OUTPUT_MODE)
    try:
        raw_output_mode_key = str(raw_output_mode).strip().lower()
    except Exception:
        raw_output_mode_key = None
    invalid_output_mode = raw_output_mode_key not in _OUTPUT_MODE_ALIASES
    output_mode = _normalize_output_mode(raw_output_mode)
    LIVE_HLS = output_mode == "segments"
    USE_FFMPEG_PIPE = output_mode in ("segments", "mpegts")
    # HLS tuning
    HLS_TIME = 4  # seconds per segment
    HLS_WINDOW = 12  # number of segments kept in the playlist
    HLS_DELETE_OLD = True  # delete old segments
    HLS_START_NUMBER = 1  # starting segment index
    # --------------------------

    self.stopDownloadFlag = False
    error = False
    url = url.replace("fmp4s://", "wss://")

    basefilename = (
        self.outputFolder
        + "/"
        + datetime.now().strftime("%Y%m%d_%H%M%S")
        + "/"
        + self.username
        + "_"
        + datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        + "_Dreamcam"
    )

    target_dir = os.path.dirname(basefilename)
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    suffix = getattr(self, "filename_extra_suffix", "")

    # Paths
    tmp_mp4 = basefilename + ".tmp.mp4"
    hls_m3u8 = basefilename + suffix + ".m3u8"
    hls_seg_tpl = basefilename + "_%05d.ts"
    ts_output_path = basefilename + ".ts"

    # Reconnect + stop thresholds
    RECONNECT_DELAYS = [0, 0.5, 1, 2, 5, 10]
    TAIL_RETRY_DELAY = 0.5
    IDLE_AFTER_SEC = 1
    MAX_CONSEC_FAILS = 15
    GIVE_UP_AFTER_SEC = None
    SOCKET_TIMEOUT = getattr(self, "wss_socket_timeout", IDLE_AFTER_SEC)

    MEDIA_TIMEOUT_THRESHOLD = getattr(self, "wss_media_timeout_limit", 1)
    FFMPEG_MAX_RESTARTS = getattr(self, "ffmpeg_restart_limit", 2)
    FFMPEG_RESTART_BACKOFF = getattr(self, "ffmpeg_restart_backoff", [0, 2, 5, 10])

    def debug_(message):
        try:
            msg = str(message)
        except Exception:
            msg = repr(message)
        self.debug(msg, filename + ".log")

    if invalid_output_mode:
        debug_(
            f"Unknown live_output_mode value {raw_output_mode!r}; defaulting to 'segments'."
        )
    debug_(f"Live output mode selected: {output_mode}")

    # ----------------- fMP4 helpers -----------------
    def _iter_boxes(buf):
        i, n = 0, len(buf)
        while i + 8 <= n:
            size = struct.unpack(">I", buf[i : i + 4])[0]
            typ = buf[i + 4 : i + 8]
            if size == 0 or size == 1 or size < 8:
                break
            if i + size > n:
                break
            yield i, size, typ
            i += size

    def _split_keep_tail(buf):
        init = bytearray()
        frags = bytearray()
        last = 0
        for off, size, typ in _iter_boxes(buf):
            box = buf[off : off + size]
            t = typ.decode("ascii", "ignore")
            if t in ("ftyp", "moov"):
                init.extend(box)
            else:
                frags.extend(box)
            last = off + size
        tail = buf[last:] if last < len(buf) else b""
        return bytes(init), bytes(frags), tail

    # ------------------------------------------------

    def execute():
        nonlocal error
        wrote_init = False
        tail = b""
        start_ts = time.time()
        last_fragment_ts = None
        consec_failures = 0
        fragments_written = 0

        # Optional live muxing: start ffmpeg and keep its stdin open
        ff_proc = None
        ff_stdin = None
        ff_stderr_handle = None
        ffmpeg_restart_attempts = 0
        init_cache = b""
        needs_init_replay = False

        class _FFmpegFatal(RuntimeError):
            """Raised when ffmpeg cannot be recovered."""

            pass

        def _stop_ffmpeg():
            nonlocal ff_proc, ff_stdin, ff_stderr_handle
            if ff_stdin:
                try:
                    ff_stdin.flush()
                except Exception:
                    pass
                try:
                    ff_stdin.close()
                except Exception:
                    pass
                ff_stdin = None
            if ff_proc:
                try:
                    ff_proc.terminate()
                except Exception:
                    pass
                try:
                    ff_proc.wait(timeout=5)
                except Exception:
                    pass
                ff_proc = None
            if ff_stderr_handle:
                try:
                    ff_stderr_handle.close()
                except Exception:
                    pass
                ff_stderr_handle = None

        def _launch_ffmpeg(reason="initial"):
            nonlocal ff_proc, ff_stdin, ff_stderr_handle, ffmpeg_restart_attempts, wrote_init, needs_init_replay
            if not USE_FFMPEG_PIPE:
                return
            if reason != "initial":
                debug_(
                    f"Restarting ffmpeg ({reason}); attempt {ffmpeg_restart_attempts}/{FFMPEG_MAX_RESTARTS}"
                )
            log_level = "warning" if not DEBUG else "info"
            if LIVE_HLS:
                ff_cmd = [
                    FFMPEG_PATH,
                    "-hide_banner",
                    "-loglevel",
                    log_level,
                    "-fflags",
                    "+genpts+discardcorrupt",
                    "-i",
                    "pipe:0",
                    "-c:v",
                    "copy",
                    "-c:a",
                    "copy",
                    "-mpegts_flags",
                    "+resend_headers",
                    "-f",
                    "hls",
                    "-hls_time",
                    str(HLS_TIME),
                    "-start_number",
                    str(HLS_START_NUMBER),
                    "-hls_segment_filename",
                    hls_seg_tpl,
                    hls_m3u8,
                ]
            else:
                ff_cmd = [
                    FFMPEG_PATH,
                    "-hide_banner",
                    "-loglevel",
                    log_level,
                    "-fflags",
                    "+genpts+discardcorrupt",
                    "-i",
                    "pipe:0",
                    "-c:v",
                    "copy",
                    "-c:a",
                    "copy",
                    "-mpegts_flags",
                    "+resend_headers",
                    "-f",
                    "mpegts",
                    ts_output_path,
                ]
            mode_tag = "hls" if LIVE_HLS else "mpegts"
            try:
                if DEBUG:
                    ff_stderr_handle = open(
                        f"{filename}.ffmpeg_{mode_tag}_stderr.log", "a+", buffering=1
                    )
                ff_proc = subprocess.Popen(
                    ff_cmd,
                    stdin=subprocess.PIPE,
                    stdout=None,
                    stderr=ff_stderr_handle,
                    bufsize=0,
                )
                ff_stdin = ff_proc.stdin
                wrote_init = False
                needs_init_replay = bool(init_cache)
                if needs_init_replay:
                    debug_("Queued init segment for ffmpeg replay after restart.")
            except Exception as launch_err:
                debug_(
                    f"Failed to launch ffmpeg ({type(launch_err).__name__}: {launch_err!r})"
                )
                raise _FFmpegFatal() from launch_err

        def _apply_restart_backoff(attempt):
            backoff = 0
            if isinstance(FFMPEG_RESTART_BACKOFF, (list, tuple)):
                if attempt - 1 < len(FFMPEG_RESTART_BACKOFF):
                    backoff = FFMPEG_RESTART_BACKOFF[attempt - 1]
                else:
                    backoff = FFMPEG_RESTART_BACKOFF[-1]
            elif isinstance(FFMPEG_RESTART_BACKOFF, (int, float)):
                backoff = FFMPEG_RESTART_BACKOFF
            if backoff > 0:
                time.sleep(backoff)

        def _attempt_ffmpeg_restart(reason):
            nonlocal ffmpeg_restart_attempts
            ffmpeg_restart_attempts += 1
            if ffmpeg_restart_attempts > max(1, FFMPEG_MAX_RESTARTS):
                debug_("ffmpeg restart limit reached; giving up.")
                return False
            debug_(f"ffmpeg issue detected ({reason}); restarting muxer.")
            _stop_ffmpeg()
            _apply_restart_backoff(ffmpeg_restart_attempts)
            try:
                _launch_ffmpeg("restart")
                return True
            except _FFmpegFatal:
                return False

        def _ensure_ffmpeg_alive():
            if not USE_FFMPEG_PIPE:
                return True
            if ff_proc is None or ff_stdin is None:
                try:
                    _launch_ffmpeg("initial")
                    return True
                except _FFmpegFatal:
                    return False
            if ff_proc.poll() is None:
                return True
            if _attempt_ffmpeg_restart("process exited"):
                return True
            return False

        def _write_to_ffmpeg(payload):
            if not USE_FFMPEG_PIPE:
                return True
            attempts = 0
            while True:
                if not _ensure_ffmpeg_alive():
                    return False
                try:
                    ff_stdin.write(payload)
                    return True
                except (BrokenPipeError, OSError) as pipe_err:
                    attempts += 1
                    reason = f"pipe error ({type(pipe_err).__name__}: {pipe_err!r})"
                    if not _attempt_ffmpeg_restart(reason):
                        debug_(f"ffmpeg restart failed after error: {reason}")
                        return False
                    if attempts >= max(1, FFMPEG_MAX_RESTARTS):
                        return False

        def _write_output(data, *, is_init=False):
            nonlocal wrote_init, init_cache, needs_init_replay, error
            if not data:
                return
            if ff_proc and ff_proc.poll() is not None:
                if not _attempt_ffmpeg_restart("process exited before write"):
                    error = True
                    raise _FFmpegFatal()
            if USE_FFMPEG_PIPE:
                if is_init:
                    init_cache = data
                    needs_init_replay = False
                if needs_init_replay and init_cache:
                    if not _write_to_ffmpeg(init_cache):
                        error = True
                        raise _FFmpegFatal()
                    wrote_init = True
                    needs_init_replay = False
                if is_init:
                    if not _write_to_ffmpeg(data):
                        error = True
                        raise _FFmpegFatal()
                    wrote_init = True
                    return
                if not wrote_init and init_cache:
                    # Replay init automatically before first fragment.
                    if not _write_to_ffmpeg(init_cache):
                        error = True
                        raise _FFmpegFatal()
                    wrote_init = True
                    needs_init_replay = False
                if not _write_to_ffmpeg(data):
                    error = True
                    raise _FFmpegFatal()
            elif outfile:
                try:
                    outfile.write(data)
                except OSError as file_err:
                    error = True
                    debug_(
                        f"Failed to write temp MP4 data: {type(file_err).__name__}: {file_err!r}"
                    )
                    raise _FFmpegFatal() from file_err

        # Fallback file when not doing live muxing
        outfile = open(tmp_mp4, "ab") if not USE_FFMPEG_PIPE else None

        if USE_FFMPEG_PIPE and not _ensure_ffmpeg_alive():
            raise _FFmpegFatal()

        try:
            while not self.stopDownloadFlag:
                # Stop conditions
                now = time.time()
                if (
                    GIVE_UP_AFTER_SEC is not None
                    and (now - start_ts) >= GIVE_UP_AFTER_SEC
                ):
                    debug_(
                        f"Stopping: reached overall deadline ({GIVE_UP_AFTER_SEC}s)."
                    )
                    self.stopDownloadFlag = True
                    break
                if (
                    last_fragment_ts is not None
                    and (now - last_fragment_ts) >= IDLE_AFTER_SEC
                ):
                    idle_for = int(now - last_fragment_ts)
                    debug_(f"Idle for {idle_for}s; cycling connection.")
                    last_fragment_ts = None
                    tail = b""
                    continue
                if consec_failures >= MAX_CONSEC_FAILS:
                    why = (
                        "after writing media"
                        if fragments_written > 0
                        else "before any media"
                    )
                    debug_(f"Stopping: {consec_failures} consecutive reconnects {why}.")
                    self.stopDownloadFlag = True
                    break

                # connect with quick retry/backoff
                conn = None
                for delay in RECONNECT_DELAYS:
                    if self.stopDownloadFlag:
                        break
                    if delay:
                        time.sleep(delay)
                    try:
                        conn = create_connection(
                            url,
                            timeout=20,
                            ping_interval=20,
                            ping_timeout=10,
                            enable_multithread=False,
                            sockopt=((socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),),
                        )
                        debug_("Connected to WebSocket")
                        break
                    except Exception as e:
                        consec_failures += 1
                        debug_(
                            f"Connect failed (attempt {consec_failures}): {type(e).__name__}: {e!r}"
                        )
                        conn = None
                        if consec_failures % len(RECONNECT_DELAYS) == 0:
                            time.sleep(TAIL_RETRY_DELAY)

                if conn is None:
                    continue

                try:
                    with closing(conn):
                        # handshake
                        try:
                            conn.settimeout(max(1, SOCKET_TIMEOUT))
                        except Exception:
                            pass
                        handshake_timeouts = 0
                        conn.send('{"url":"stream/hello","version":"0.0.1"}')

                        # Wait for 'stream/qual' or media
                        while not self.stopDownloadFlag:
                            try:
                                fr = conn.recv_frame()
                            except (
                                WebSocketTimeoutException,
                                socket.timeout,
                            ) as timeout_err:
                                handshake_timeouts += 1
                                debug_(
                                    f"Handshake timeout ({handshake_timeouts}); reconnecting."
                                )
                                raise WebSocketConnectionClosedException(
                                    "timeout during handshake"
                                ) from timeout_err
                            if fr is None:
                                raise WebSocketConnectionClosedException(
                                    "no frame during handshake"
                                )

                            if fr.opcode in (
                                ABNF.OPCODE_PING,
                                ABNF.OPCODE_PONG,
                                ABNF.OPCODE_CLOSE,
                            ):
                                if fr.opcode == ABNF.OPCODE_CLOSE:
                                    raise WebSocketConnectionClosedException(
                                        "close during handshake"
                                    )
                                continue

                            if fr.opcode == ABNF.OPCODE_TEXT:
                                try:
                                    tj = json.loads(fr.data)
                                except Exception:
                                    debug_(
                                        "Non-JSON text during handshake; reconnecting."
                                    )
                                    raise WebSocketConnectionClosedException(
                                        "bad handshake text"
                                    )
                                if tj.get("url") == "stream/qual":
                                    conn.send(
                                        '{"quality":"test","url":"stream/play","version":"0.0.1"}'
                                    )
                                    debug_("Connection opened (play).")
                                    consec_failures = 0
                                    break
                                if tj.get("message") == "ping":
                                    debug_(
                                        "Server ping (not ready / changed); cycling connection."
                                    )
                                    raise WebSocketConnectionClosedException(
                                        "server pinged; cycle"
                                    )
                                continue

                                if fr.opcode == ABNF.OPCODE_BINARY:
                                    payload = tail + fr.data
                                    init, frags, tail = _split_keep_tail(payload)

                                    # write init once
                                    if init and not wrote_init:
                                        _write_output(init, is_init=True)

                                    if frags:
                                        _write_output(frags)
                                        fragments_written += 1
                                        last_fragment_ts = time.time()
                                        consec_failures = 0
                                    break

                        # media loop
                        media_timeouts = 0
                        while not self.stopDownloadFlag:
                            now = time.time()
                            if (
                                GIVE_UP_AFTER_SEC is not None
                                and (now - start_ts) >= GIVE_UP_AFTER_SEC
                            ):
                                debug_("Stopping in media: overall deadline reached.")
                                self.stopDownloadFlag = True
                                raise WebSocketConnectionClosedException(
                                    "stop deadline"
                                )
                            if (
                                last_fragment_ts is not None
                                and (now - last_fragment_ts) >= IDLE_AFTER_SEC
                            ):
                                idle_for = int(now - last_fragment_ts)
                                debug_(
                                    f"Idle in media loop for {idle_for}s; cycling connection."
                                )
                                tail = b""
                                raise WebSocketConnectionClosedException("stop idle")

                            try:
                                fr = conn.recv_frame()
                            except (
                                WebSocketTimeoutException,
                                socket.timeout,
                            ) as timeout_err:
                                media_timeouts += 1
                                debug_(
                                    f"Media timeout ({media_timeouts}/{MEDIA_TIMEOUT_THRESHOLD}); waiting for data."
                                )
                                if media_timeouts >= max(1, MEDIA_TIMEOUT_THRESHOLD):
                                    raise WebSocketConnectionClosedException(
                                        "timeout during media receive"
                                    ) from timeout_err
                                continue
                            if fr is None:
                                raise WebSocketConnectionClosedException("no frame")
                            if fr.opcode in (ABNF.OPCODE_PING, ABNF.OPCODE_PONG):
                                continue
                            if fr.opcode == ABNF.OPCODE_CLOSE:
                                raise WebSocketConnectionClosedException("close frame")
                            if fr.opcode == ABNF.OPCODE_TEXT:
                                continue
                            if fr.opcode == ABNF.OPCODE_BINARY:
                                payload = tail + fr.data
                                init, frags, tail = _split_keep_tail(payload)

                                if init and not wrote_init:
                                    _write_output(init, is_init=True)

                                if frags:
                                    _write_output(frags)
                                    fragments_written += 1
                                    last_fragment_ts = time.time()
                                    consec_failures = 0
                                    media_timeouts = 0
                                elif not frags:
                                    media_timeouts = 0

                except _FFmpegFatal:
                    debug_("Stopping download: ffmpeg fatal error.")
                    error = True
                    self.stopDownloadFlag = True
                    break
                except (WebSocketConnectionClosedException, OSError) as e:
                    msg = ""
                    if isinstance(e, WebSocketConnectionClosedException) and e.args:
                        msg = str(e.args[0])
                    else:
                        msg = str(e)
                    benign = msg in (
                        "stop idle",
                        "timeout during media receive",
                        "timeout during handshake",
                    )
                    if benign:
                        consec_failures = 0
                    else:
                        consec_failures += 1
                    debug_(
                        f"WebSocket closed: {type(e).__name__}: {e!r} — will reconnect"
                    )
                    continue
                except WebSocketException as wex:
                    consec_failures += 1
                    debug_("WebSocket error when downloading")
                    debug_(f"{type(wex).__name__}: {wex!r}")
                    continue
                except Exception as ex:
                    consec_failures += 1
                    debug_(f"Unexpected error: {type(ex).__name__}: {ex!r}")
                    continue

        except Exception as ex:
            error = True
            debug_(f"Fatal in execute: {type(ex).__name__}: {ex!r}")
        finally:
            # close output resources
            tail = b""
            if outfile:
                try:
                    outfile.flush()
                except:
                    pass
                try:
                    outfile.close()
                except:
                    pass
            if USE_FFMPEG_PIPE:
                _stop_ffmpeg()

            if not USE_FFMPEG_PIPE:
                script_path = "/Users/larswissink/Documents/GitHub/StreaMonitor/streamonitor/downloaders/make_mp4.sh"
                target_dir = os.path.dirname(filename)

                if not os.path.isdir(target_dir):
                    print(
                        f"⚠️ Target directory not found; skipping make_mp4: {target_dir}"
                    )
                else:
                    video_exts = {".ts", ".mkv", ".mp4"}
                    has_video_files = False

                    for entry in os.scandir(target_dir):
                        if not entry.is_file():
                            continue
                        _, ext = os.path.splitext(entry.name)
                        if ext.lower() in video_exts and entry.stat().st_size > 0:
                            has_video_files = True
                            break

                    if not has_video_files:
                        print(
                            f"⚠️ No video files found; removing capture folder {target_dir} and skipping make_mp4."
                        )
                        try:
                            shutil.rmtree(target_dir)
                            print(f"🧹 Removed empty capture folder: {target_dir}")
                        except Exception as cleanup_err:
                            print(
                                f"Failed to remove capture folder {target_dir}: {cleanup_err!r}"
                            )
                    else:

                        logfile = os.path.join(
                            os.path.dirname(basefilename), "make_mp4.log"
                        )

                        make_mp4_args = ["bash", script_path]
                        # if CONTAINER.lower() == "mkv":
                        #     make_mp4_args.append("--mkv")
                        # elif CONTAINER.lower() == "mp4":
                        #     make_mp4_args.append("--mp4")
                        make_mp4_args.append(os.path.dirname(basefilename))

                        with open(logfile, "w") as log_handle:
                            subprocess.Popen(
                                make_mp4_args,
                                stdout=log_handle,
                                stderr=subprocess.STDOUT,
                                stdin=subprocess.DEVNULL,
                                close_fds=True,
                            )

                print(f"🎬 Script launched, logs are being written to {logfile}")
            else:
                script_path = "/Users/larswissink/Documents/GitHub/StreaMonitor/streamonitor/downloaders/make_mp4.sh"
                target_dir = os.path.dirname(basefilename)
                if not os.path.isdir(target_dir):
                    print(
                        f"⚠️ Target directory not found; skipping make_mp4: {target_dir}"
                    )
                else:
                    logfile = os.path.join(target_dir, "make_mp4.log")

                    with open(logfile, "w") as log_handle:
                        # write complete recording to log and close file again
                        log_handle.write(
                            f"FFmpeg live muxing completed, output at {ts_output_path}\n"
                        )
                        log_handle.flush()
                        log_handle.close()

        self.stopDownloadFlag = True

    def terminate():
        self.stopDownloadFlag = True
        debug_(f"Post-Killing {filename}")

    process = Thread(target=execute, daemon=True)
    process.start()
    self.stopDownload = terminate
    process.join()
    self.stopDownload = None

    if error:
        debug_("Download failed ")
        # return False

    # -------- Post-processing --------
    if USE_FFMPEG_PIPE:
        return True

    return True
