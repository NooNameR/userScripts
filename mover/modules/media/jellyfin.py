import os
import httpx
import asyncio
import logging
import threading
from typing import Set, List, Tuple, Dict
from collections import defaultdict
from datetime import timedelta, datetime, timezone
from .media_player import MediaPlayer, MediaPlayerType
from ..rewriter import Rewriter
from ..helpers import get_stat
from functools import cached_property

class Jellyfin(MediaPlayer):
    def __init__(self, now: datetime, rewriter: Rewriter, url: str, api_key: str, libraries: List[str] = [], users: List[str] = []):
        self.now = now.astimezone(timezone.utc)
        self.rewriter = rewriter
        self.url = url.rstrip('/')
        self.api_key = api_key
        self.libraries = set(libraries)
        self.users = set(users)
        self._lock = threading.Lock()
        self._initialized: bool = False
        self.logger = logging.getLogger(__name__)

    @cached_property 
    def _client(self) -> httpx.AsyncClient:
        with self._lock:
            logging.getLogger("httpx").setLevel(logging.WARNING)
            
            self._initialized = True
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

    @cached_property
    def _get_users(self):
        async def process():
            users = await self._get("/Users")
            return {u["Id"] for u in users if not self.users or u["Name"] in self.users}
        
        return asyncio.create_task(process())

    @cached_property
    def _get_library_ids(self):
        async def process():
            user_ids = await self._get_users
            
            async def get_views(user_id):
                views = await self._get(f"/Users/{user_id}/Views")
                return user_id, {
                    v["Id"]
                    for v in views.get("Items", [])
                    if not self.libraries or v["Name"] in self.libraries
                }

            # Run all in parallel, each returning (user_id, set_of_ids)
            return dict(await asyncio.gather(*(get_views(uid) for uid in user_ids)))
    
        return asyncio.create_task(process())
        
    @cached_property
    def media(self) -> asyncio.Task[Dict[int, int]]:
        async def get_for_user_id(user_id: str, allowed_ids: Set[str]) -> Set[int]:
            async def get_for_library(library_id: str) -> None:
                local_state: Set[int] = set()
                params = {
                    "IncludeItemTypes": ["Episode", "Movie", "Video"],
                    "ParentId": library_id,
                    "UserId": user_id,
                    "Filters": "IsUnplayed",
                    "IsMissing": False,
                    "Fields": "MediaSources,MediaStreams",
                    "Recursive": True,
                    "SortBy": "IndexNumber",
                    "SortOrder": "Ascending",
                    "EnableUserData": True,
                    "Limit": 500
                }
                start_index = 0
                
                while True:
                    params["StartIndex"] = start_index
                    
                    result = await self._get("/Items", params)
                    items = result.get("Items", [])
                    
                    if not items:
                        break
                    
                    for item in items:
                        for media in item.get("MediaSources", []) + item.get("MediaStreams", []):
                            path = media.get("Path")
                            if not path:
                                continue
                                
                            local_path = self.rewriter.on_source(path)
                            if os.path.exists(local_path):
                                self.logger.debug("Processing %s: %s (%s)", item.get("Type"), item.get("Name"), local_path)
                                local_state.add(get_stat(local_path).st_ino)
                    
                    start_index += len(items)
                return local_state
            
            local_states = await asyncio.gather(*(get_for_library(lib_id) for lib_id in allowed_ids))
            return {p for lib in local_states for p in lib}

        async def process():
            lib_ids = await self._get_library_ids
            user_results = [get_for_user_id(user, lib_ids) for user, lib_ids in lib_ids.items()]
            
            un_watched_counts: Dict[int, int] = defaultdict(int)
            for user_result in asyncio.as_completed(user_results):
                for path in await user_result:
                    un_watched_counts[path] += 1
            
            self.logger.info("[%s] Found %d not-watched files in the Jellyfin library", self, len(un_watched_counts))
            return un_watched_counts

        return asyncio.create_task(process())

    async def is_active(self, file: str) -> bool:
        sessions = await self._get("/Sessions")
        for session in sessions:
            for item in filter(None, [session.get("NowPlayingItem")]):
                for media in item.get("MediaSources", []) + item.get("MediaStreams", []):
                    path = media.get("Path")
                    if path:
                        resolved = self.rewriter.on_source(path)
                        if not os.path.exists(resolved):
                            resolved = self.rewriter.on_destination(path)
                        if os.path.exists(resolved) and os.path.samefile(resolved, file):
                            return True
        return False
    
    async def get_sort_key(self, path: str) -> Tuple[bool, int]:
        inode = get_stat(path).st_ino
        un_watched, continue_watching = await asyncio.gather(
            self.media,
            self.__continue_watching_on_source
        )
        
        return (inode in continue_watching, un_watched.get(inode, 0))
    
    async def continue_watching(self, pq: asyncio.Queue[Tuple[float, int, str]]) -> None:
        total: int = 0
        max_count: int = 25
        
        for key, bucket in await self.__continue_watching:
            remaining = max_count
            for item in bucket:
                if not remaining:
                    break
                
                remaining -= 1
                
                for index, path in enumerate(item):
                    source_path = self.rewriter.on_source(path)
                    if os.path.exists(source_path):
                        continue
                    
                    detination_path = self.rewriter.on_destination(path)
                    if not os.path.exists(detination_path):
                        continue
                    await pq.put((key, index, detination_path))
                    total += 1
                
        self.logger.info("[%s] Detected %d watching files not currently available on source drives in Jellyfin library", self, total)
    
    @cached_property
    def __continue_watching_on_source(self) -> asyncio.Task[Set[int]]:
        async def process():
            return {
                get_stat(self.rewriter.on_source(path)).st_ino
                for _, bucket in await self.__continue_watching
                for media in bucket
                for path in media
                if os.path.exists(self.rewriter.on_source(path))
            }
        
        return asyncio.create_task(process())

    @cached_property
    def __continue_watching(self) -> asyncio.Task[List[Tuple[float, List[Set[str]]]]]:
        cutoff = self.now - timedelta(weeks=1)
        pq: asyncio.Queue[Tuple[float, List[str]]] = asyncio.PriorityQueue()
        
        def get_for_user_id(user_id: str, allowed_ids: Set[str]):
            async def get_for_library(library_id):
                def parse_played_at(item) -> datetime:
                    raw_date = item.get("UserData", {}).get("LastPlayedDate", "")
                    
                    try:
                        return datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
                    except ValueError:
                         return datetime(1970, 1, 1, tzinfo=timezone.utc)
                     
                tasks = [
                    self._get("/Shows/NextUp", {
                        "userId": user_id,
                        "parentId": library_id,
                        "enableUserData": True,
                        "enableResumable": True,
                        "nextUpDateCutoff": cutoff.isoformat(),
                        "disableFirstEpisode": True,
                        "fields": "MediaSources,MediaStreams",
                    }),
                    self._get(f"/Users/{user_id}/Items/Resume", {
                        "excludeActiveSessions": True,
                        "parentId": library_id,
                        "enableUserData": True,
                        "fields": "MediaSources,MediaStreams",
                    })
                ]
                
                processed_series: Set[str] = set()
                for task in asyncio.as_completed(tasks):
                    for item in (await task).get("Items", []):
                        temp: List[str] = []
                        if (series_id := item.get("SeriesId")) is None or series_id in processed_series:
                            continue
                        
                        processed_series.add(series_id)
                        
                        lastPlayedAt = parse_played_at(item)
                        season = item.get("SeasonNumber", 1)
                        index = item.get("IndexNumber", 1) - 1

                        while True:
                            episodes = (await self._get(f"/Shows/{series_id}/Episodes", {
                                "userId": user_id,
                                "enableUserData": True,
                                "season": season,
                                "startIndex": index,
                                "fields": "MediaSources,MediaStreams",
                                "sortBy": "SeasonNumber,IndexNumber",
                                "sortOrder": "Ascending",
                            })).get("Items", [])
                            
                            if not episodes:
                                break
                            
                            for ep in episodes:
                                if ep.get("UserData", {}).get("Played") or ep.get("UserData", {}).get("PlayedPercentage", 0.0) > 40.0:
                                    temp = []
                                    lastPlayedAt = max(parse_played_at(ep), lastPlayedAt)
                                    continue
                                
                                temp.append({media.get("Path") for media in ep.get("MediaSources", []) + ep.get("MediaStreams", []) if "Path" in media})
                            
                            season += 1
                            index = 0
                        
                        if lastPlayedAt < cutoff:
                            continue
                        
                        if temp:
                            await pq.put((-lastPlayedAt.timestamp(), temp))
                
            return asyncio.gather(*(get_for_library(library_id) for library_id in allowed_ids))
        
        async def process():
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
        
        return asyncio.create_task(process())

    @property
    def type(self):
        return MediaPlayerType.JELLYFIN

    def __str__(self):
        return f"{self.type.name}@{self.url}".lower()

    def __repr__(self):
        return str(self)

    async def aclose(self):
        with self._lock:
            if not self._initialized:
                return
        
        await self._client.aclose()