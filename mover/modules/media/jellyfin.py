import os
import requests
import logging
from .media_player import MediaPlayer, MediaPlayerType
from ..rewriter import Rewriter, RealRewriter, NoopRewriter
from typing import Dict
from collections import OrderedDict
from datetime import timedelta, datetime
from functools import cached_property

class Jellyfin(MediaPlayer):
    def __init__(self, now, source: str, url: str, token: str, libraries: list[str] = [], users: list[str] = [], rewrite: Dict[str, str] = {}):
        self.now = now
        self.url = url.rstrip('/')
        self.token = token
        self.libraries = set(libraries)
        self.users = set(users)
        if rewrite and "from" in rewrite and "to" in rewrite:
            src, dst = rewrite["from"], rewrite["to"]
            self.rewriter: Rewriter = RealRewriter(source, src, dst)
        else:
            self.rewriter: Rewriter = NoopRewriter()

    def _headers(self):
        return {
            "X-Emby-Token": self.token,
            "Accept": "application/json"
        }

    def _get(self, endpoint, params=None):
        response = requests.get(f"{self.url}/emby{endpoint}", headers=self._headers(), params=params)
        response.raise_for_status()
        return response.json()

    def _get_users(self):
        all_users = self._get("/Users")
        return [u for u in all_users if not self.users or u["Name"] in self.users]

    def _get_library_ids(self, user_id):
        libraries = self._get(f"/Users/{user_id}/Views").get("Items", [])
        return set([lib["Id"] for lib in libraries if not self.libraries or lib["Name"] in self.libraries])

    def _get_items(self, filters):
        items = []
        for user in self._get_users():
            user_id = user["Id"]
            for library_id in self._get_library_ids(user_id):
                params = filters.copy()
                params["ParentId"] = library_id
                params["UserId"] = user_id
                params["IsMissing"] = False
                result = self._get(f"/Items", params)
                items += result.get("Items", [])
        return items

    @cached_property
    def not_watched_media(self) -> set[str]:
        not_watched = set()
        items = self._get_items({"Filters": "IsUnplayed"})
        for item in items:
            media_sources = item.get("MediaSources", [])
            for media in media_sources:
                file_path = media.get("Path")
                if not file_path:
                    continue
                path = self.rewriter.rewrite(file_path)
                if os.path.exists(path):
                    logging.debug("Unwatched %s: %s (%s)", item.get("Type"), item.get("Name"), path)
                    not_watched.add(path)

        logging.info("Found %d not-watched files in the Jellyfin library", len(not_watched))
        return not_watched

    def is_not_watched(self, file: str) -> bool:
        return file in self.not_watched_media

    def is_active(self, file: str) -> bool:
        for session in self._get("/Sessions"):
            for item in [i for i in [session.get("NowPlayingItem"), session.get("NowViewingItem")] if i]:
                media_sources = item.get("MediaSources", [])
                for media in media_sources:
                    if "Path" not in media:
                        continue
    
                    path = self.rewriter.rewrite(media["Path"])
                    if os.path.exists(path) and os.path.samefile(path, file):
                        return True
        return False

    @cached_property
    def continue_watching(self) -> list[str]:
        result = OrderedDict()
        cutoff = (self.now - timedelta(weeks=1)).isoformat()

        for user in self._get_users():
            user_id = user["Id"]
            allowed_library_ids = self._get_library_ids(user_id)

            # Get NextUp shows for this user
            params = {
                "userId": user_id,
                "limit": 20,
                "nextUpDateCutoff": cutoff,
                "enableUserData": "true",
                "enableResumable": "true",
                "disableFirstEpisode": "false",
                "fields": "MediaSources",
            }

            nextup_items = self._get("/Shows/NextUp", params).get("Items", [])
            
            def get_episodes(season: int, index: int):
                episode_params = {
                    "userId": user_id,
                    "enableUserData": "true",
                    "startIndex": index,
                    "season": season,
                    "fields": "MediaSources",
                    "sortBy": "IndexNumber",
                    "sortOrder": "Ascending",
                }

                return self._get(f"/Shows/{series_id}/Episodes", episode_params).get("Items", [])

            for item in nextup_items:
                remaining = 25
                series_id = item.get("SeriesId")
                
                if not series_id:
                    continue
                
                parent_id = item.get("ParentId")
                if parent_id not in allowed_library_ids:
                    continue

                start_index = item.get("IndexNumber", 1) - 1
                season = item.get("SeasonNumber", 1)
                episodes = get_episodes(season, start_index)

                while remaining and episodes:
                    for ep in episodes:
                        user_data = ep.get("UserData", {})
                        if user_data.get("Played", False):
                            continue
                        
                        media_sources = ep.get("MediaSources", [])
                        for media in [m for m in media_sources if "Path" in m]:
                            path = self.rewriter.rewrite(media["Path"])
                            if not os.path.exists(path):
                                result[path] = None
                        
                        remaining -= 1
                        
                    season += 1
                    start_index = 0
                    episodes = get_episodes(season, start_index)
                    

        logging.info(
            "Detected %d watching files not currently available on source drives in Jellyfin library",
            len(result),
        )
        
        return list(result.keys())

    @property
    def type(self):
        return MediaPlayerType.JELLYFIN

    def __str__(self):
        return self.url

    def __repr__(self):
        return self.__str__()
