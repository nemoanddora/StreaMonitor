import requests

from streamonitor.bot import Bot
from streamonitor.enums import Status


class DreamCam(Bot):
    site = "DreamCam"
    siteslug = "DC"

    _stream_type = "video2D"
    status_timeout = 10

    def getVideoUrl(self):
        for stream in self.lastInfo["streams"]:
            if stream["streamType"] == self._stream_type:
                if stream["status"] == "online":
                    if stream["resolutionInfo"]["height"] > 2029:
                        return stream["url"]
        return None

    def getStatus(self):
        try:
            r = self.session.get(
                "https://bss.dreamcamtrue.com/api/clients/v1/broadcasts/models/"
                + self.username,
                headers=self.headers,
                timeout=self.status_timeout,
            )
        except requests.exceptions.RequestException as exc:
            self.logger.warning(f"DreamCam status request failed: {exc}")
            return Status.UNKNOWN

        if r.status_code != 200:
            return Status.UNKNOWN

        self.lastInfo = r.json()

        if self.lastInfo["broadcastStatus"] in ["public"]:
            return Status.PUBLIC
        if self.lastInfo["broadcastStatus"] in ["private"]:
            return Status.PRIVATE
        if self.lastInfo["broadcastStatus"] in ["away", "offline"]:
            return Status.OFFLINE
        self.logger.warn(f'Got unknown status: {self.lastInfo["broadcastStatus"]}')
        return Status.UNKNOWN
