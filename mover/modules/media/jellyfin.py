import os
import httpx
import asyncio
import logging
import threading
from typing import Set, List, Tuple
from datetime import timedelta, datetime, timezone
from .media_player import MediaPlayer, MediaPlayerType
from ..rewriter import Rewriter
from functools import cached_property
from asyncstdlib import cached_property as acached_property
from asyncstdlib.builtins import list as alist, map as amap, set as aset

class Jellyfin(MediaPlayer):
    def __init__(self, now: datetime, rewriter: Rewriter, url: str, api_key: str, libraries: List[str] = [], users: List[str] = []):
        self.now = now.astimezone(timezone.utc)
        self.rewriter = rewriter
        self.url = url.rstrip('/')
        self.api_key = api_key
        self.libraries = set(libraries)
        self.users = set(users)
        self._lock = threading.Lock()
        self.logger = logging.getLogger(__name__)

    @cached_property
    def _client(self) -> httpx.AsyncClient:
        with self._lock:
            logging.getLogger("httpx").setLevel(logging.WARNING)
            
            return httpx.AsyncClient(
                base_url=self.url, 
                headers={
                    "Authorization": f'MediaBrowser Token="{self.api_key}"',
                    "Accept": "application/json",
                },
                timeout=httpx.Timeout(
                    connect=10.0,
                    read=120.0,
                    write=10.0,
                    pool=30.0
                ),
                limits=httpx.Limits(max_keepalive_connections=20, max_connections=100),
            )

    async def _get(self, endpoint: str, params=None):
        response = await self._client.get(endpoint, params=params)
        response.raise_for_status()
        return response.json()

    @acached_property
    async def _get_users(self):
        users = await self._get("/Users")
        return {u["Id"] for u in users if not self.users or u["Name"] in self.users}

    @acached_property
    async def _get_library_ids(self):
        user_ids = await self._get_users
        
        async def get_views(user_id):
            views = await self._get(f"/Users/{user_id}/Views")
            return user_id, {
                v["Id"]
                for v in views.get("Items", [])
                if not self.libraries or v["Name"] in self.libraries
            }

        return dict(await alist(amap(get_views, user_ids)))
        
    @acached_property
    async def not_watched_media(self) -> Set[str]:
        async def get_for_user_id(user_id: str, allowed_ids: Set[str]) -> List[str]:
            async def get_for_library(library_id: str) -> List[str]:
                found: Set[str] = set()
                params = {
                    "Filters": "IsUnplayed",
                    "IncludeItemTypes": ["Episode", "Movie", "Video"],
                    "ParentId": library_id,
                    "UserId": user_id,
                    "IsMissing": False,
                    "Fields": "MediaSources",
                    "Recursive": True,
                }
                result = await self._get("/Items", params)
                for item in result.get("Items", []):
                    for media in item.get("MediaSources", []):
                        path = media.get("Path")
                        if path:
                            local_path = self.rewriter.on_source(path)
                            if os.path.exists(local_path):
                                self.logger.debug("Unwatched %s: %s (%s)", item.get("Type"), item.get("Name"), local_path)
                                found.add(local_path)
                return found

            await aset(get_for_library(lib_id) for lib_id in allowed_ids)

        not_watched = await aset(
            get_for_user_id(user, lib_ids) 
            for user, lib_ids in (await self._get_library_ids).items()
        )
        self.logger.info("[%s] Found %d not-watched files in the Jellyfin library", self, len(not_watched))
        return not_watched

    async def is_active(self, file: str) -> bool:
        sessions = await self._get("/Sessions")
        for session in sessions:
            for item in filter(None, [session.get("NowPlayingItem")]):
                for media in item.get("MediaSources", []):
                    path = media.get("Path")
                    if path:
                        resolved = self.rewriter.on_source(path)
                        if os.path.exists(resolved) and os.path.samefile(resolved, file):
                            return True
        return False
    
    async def get_sort_key(self, path: str) -> int:
        not_watched, continue_watching = await asyncio.gather(
            self.not_watched_media,
            self.__continue_watching_on_source
        )
        
        return (1 if path in not_watched else 0) + (1 if path in continue_watching else 0)
    
    async def continue_watching(self, pq: asyncio.Queue[Tuple[float, int, str]]) -> None:
        total: int = 0
        max_count: int = 25
        on_source = await self.__continue_watching_on_source
        
        for key, bucket in await self.__continue_watching:
            remaining = max_count
            for item in bucket:
                if not remaining:
                    break
                
                remaining -= 1
                
                for index, path in enumerate(item):
                    source_path = self.rewriter.on_source(path)
                    if source_path in on_source:
                        continue
                    
                    detination_path = self.rewriter.on_destination(path)
                    if not os.path.exists(detination_path):
                        continue
                    await pq.put((key, index, detination_path))
                    total += 1
                
        self.logger.info("[%s] Detected %d watching files not currently available on source drives in Jellyfin library", self, total)
    
    @acached_property
    async def __continue_watching_on_source(self) -> Set[str]:
        return {
            self.rewriter.on_source(path)
            for _, bucket in await self.__continue_watching
            for media in bucket
            for path in media
            if os.path.exists(self.rewriter.on_source(path))
        }

    @acached_property
    async def __continue_watching(self) -> List[Tuple[float, List[Set[str]]]]:
        cutoff = self.now - timedelta(weeks=1)
        pq = asyncio.PriorityQueue()

        async def get_for_user_id(user_id: str, allowed_ids: Set[str]):
            async def get_for_library(library_id: str):
                def parse_played_at(item) -> datetime:
                    raw_date = item.get("UserData", {}).get("LastPlayedDate", "")
                    try:
                        return datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
                    except ValueError:
                        return datetime(1970, 1, 1, tzinfo=timezone.utc)

                nextup_items = (await self._get("/Shows/NextUp", {
                    "userId": user_id,
                    "parentId": library_id,
                    "enableUserData": "true",
                    "enableResumable": "true",
                    "nextUpDateCutoff": cutoff.isoformat(),
                    "disableFirstEpisode": "true",
                    "fields": ["MediaSources"],
                })).get("Items", [])

                for item in nextup_items:
                    temp = []
                    if (series_id := item.get("SeriesId")) is None:
                        continue

                    lastPlayedAt = parse_played_at(item)
                    season = item.get("SeasonNumber", 1)
                    index = item.get("IndexNumber", 1) - 1

                    episodes = (await self._get(f"/Shows/{series_id}/Episodes", {
                        "userId": user_id,
                        "enableUserData": "true",
                        "season": season,
                        "startIndex": index,
                        "fields": "MediaSources",
                        "Recursive": True,
                        "sortBy": "SeasonNumber,IndexNumber",
                        "sortOrder": "Ascending",
                    })).get("Items", [])

                    while episodes:
                        for ep in episodes:
                            if ep.get("UserData", {}).get("Played"):
                                lastPlayedAt = max(parse_played_at(ep), lastPlayedAt)
                                continue

                            temp.append({media.get("Path") for media in ep.get("MediaSources", []) if "Path" in media})

                        season += 1
                        index = 0

                        episodes = (await self._get(f"/Shows/{series_id}/Episodes", {
                            "userId": user_id,
                            "enableUserData": "true",
                            "season": season,
                            "startIndex": index,
                            "fields": "MediaSources",
                            "Recursive": True,
                            "sortBy": "SeasonNumber,IndexNumber",
                            "sortOrder": "Ascending",
                        })).get("Items", [])

                    if lastPlayedAt < cutoff:
                        continue

                    if temp:
                        await pq.put((-lastPlayedAt.timestamp(), temp))

            asyncio.gather(*(get_for_library(library_id) for library_id in allowed_ids))

        await asyncio.gather(*(get_for_user_id(user, lib_ids) for user, lib_ids in (await self._get_library_ids).items()))
        
        result: List[List[Set[str]]] = []
        processed: Set[str] = set()
        while not pq.empty():
            key, media_list = await pq.get()
            temp: List[Set[str]] = []
            for media in media_list:
                m: Set[str] = set()
                for path in media:
                    if path in processed:
                        continue
                    processed.add(path)
                    m.add(path)
                temp.append(m)
            
            result.append((key, temp))
        
        self.logger.info("[%s] Detected %d watching files in Jellyfin library", self, len(result))
        return result

    @property
    def type(self):
        return MediaPlayerType.JELLYFIN

    def __str__(self):
        return f"{self.type.name}@{self.url}".lower()

    def __repr__(self):
        return str(self)

    async def aclose(self):
        await self._client.aclose()