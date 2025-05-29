import os
import requests
import logging
from .media_player import MediaPlayer, MediaPlayerType
from ..rewriter import Rewriter
from typing import Set, List
from collections import OrderedDict
from datetime import timedelta, datetime
from functools import cached_property

class Jellyfin(MediaPlayer):
    def __init__(self, now: datetime, rewriter: Rewriter, url: str, api_key: str, libraries: List[str] = [], users: List[str] = []):
        self.now: datetime = now
        self.rewriter: Rewriter = rewriter
        self.url: str = url.rstrip('/')
        self.api_key: str = api_key
        self.libraries: Set[str] = set(libraries)
        self.users: Set[str] = set(users)

    def _headers(self):
        return {
            "X-Emby-Token": self.api_key,
            "Accept": "application/json"
        }

    def _get(self, endpoint, params=None):
        response = requests.get(f"{self.url}/emby{endpoint}", headers=self._headers(), params=params)
        response.raise_for_status()
        return response.json()

    def _get_library_ids(self):
        libraries = self._get("/Users/Me/Views").get("Items", [])
        return [lib["Id"] for lib in libraries if not self.libraries or lib["Name"] in self.libraries]

    def _get_items(self, filters):
        items = []
        for library_id in self._get_library_ids():
            params = filters.copy()
            params["ParentId"] = library_id
            result = self._get("/Items", params)
            items.extend(result.get("Items", []))
        return items

    @cached_property
    def not_watched_media(self) -> Set[str]:
        not_watched = set()
        items = self._get_items({"Filters": "IsUnplayed"})
        for item in items:
            media_sources = item.get("MediaSources", [])
            for media in media_sources:
                file_path = media.get("Path")
                if not file_path:
                    continue
                path = self.rewriter.on_source(file_path)
                if os.path.exists(path):
                    logging.debug("Unwatched %s: %s (%s)", item.get("Type"), item.get("Name"), path)
                    not_watched.add(path)

        logging.info("Found %d not-watched files in the Jellyfin library", len(not_watched))
        return not_watched

    def get_sort_key(self, path: str) -> int:
        return 1 if path in self.not_watched_media else 0

    def is_active(self, file: str) -> bool:
        sessions = self._get("/Sessions")
        for session in sessions:
            now_playing = session.get("NowPlayingItem")
            if now_playing:
                media_sources = now_playing.get("MediaSources", [])
                for media in media_sources:
                    path = self.rewriter.on_source(media.get("Path", ""))
                    if os.path.exists(path) and os.path.samefile(path, file):
                        return True
        return False

    @cached_property
    def continue_watching(self) -> List[str]:
        result = OrderedDict()
        cutoff = self.now - timedelta(weeks=1)
        items = self._get_items({"Filters": "IsResumable", "SortBy": "DatePlayed", "SortOrder": "Descending"})

        for item in items:
            last_played = item.get("DatePlayed")
            if not last_played:
                continue

            try:
                last_played_dt = datetime.fromisoformat(last_played.replace("Z", "+00:00"))
                if last_played_dt < cutoff:
                    continue
            except Exception:
                continue

            media_sources = item.get("MediaSources", [])
            for media in media_sources:
                path = self.rewriter.rewrite(media.get("Path", ""))
                destination_path = self.rewriter.on_destination(media.get("Path", ""))
                if not os.path.exists(path) and os.path.exists(destination_path):
                    result[destination_path] = None

        logging.info("Detected %d watching files not currently available on source drives in Jellyfin library", len(result))
        return list(result.keys())

    @property
    def type(self):
        return MediaPlayerType.JELLYFIN

    def __str__(self):
        return self.url

    def __repr__(self):
        return self.__str__()