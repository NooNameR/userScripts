#!/usr/bin/env python3

import os
import sys
import json
import urllib.request
from urllib.error import URLError, HTTPError
import urllib.parse
import logging
import time
import base64
from typing import List, Callable, Tuple
from logging.handlers import RotatingFileHandler

script_name = "Radarr-Trailer"
log_file = "/config/logs/trailer_downloader.log"
id_file = "/config/logs/trailer_id.log"
encoding = "utf-8"
youtube_api = "http://ytptube:8081/api"

open(log_file, 'a').close()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler(
            log_file,
            maxBytes=5 * 1024 * 1024,  # 5 MB
            backupCount=3              # keep 3 old log files
        ),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(script_name)

# Env vars
event_type = os.getenv("radarr_eventtype")
movie_title = os.getenv("radarr_movie_title") 
movie_id = os.getenv("radarr_movie_id")
tmdb_id = os.getenv("radarr_movie_tmdbid")
movie_path = os.getenv("radarr_movie_path")
tmdb_api_key = os.getenv("TMDB_API_KEY")
language = os.getenv("EXTRA_LANGUAGE") or "en-US"
proxy = os.getenv("PROXY")
autopulse_instance_name = os.getenv("AUTOPULSE_INSTANCE_NAME") or "manual"
autopulse_username = os.getenv("AUTOPULSE_USERNAME")
autopulse_password = os.getenv("AUTOPULSE_PASSWORD")

def http_get(url, headers={}):
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req) as response:
            return response.getcode(), response.read().decode(encoding)
    except HTTPError as e:
        logger.error("HTTP error fetching %s - %d %s", url, e.code, e.reason)
    except URLError as e:
        logger.error("URL error fetching %s - %s", url, e.reason)
    except Exception as e:
        logger.error("Unexpected error fetching %s - %s", url, e)
    return None, None

def check_download_status(youtube_id, retries, delay) -> str | None:
    for attempt in range(1, retries + 1):
        time.sleep(delay * attempt)
        
        logger.info("Checking download status for %s (Attempt %d/%d)...", youtube_id, attempt, retries)
        status_code, history_body = http_get(f"{youtube_api}/history")
        
        if status_code != 200 or history_body is None:
            logger.warning("Failed to fetch download history on attempt %d", attempt)
        else:
            try:
                result = json.loads(history_body)
            except Exception as e:
                logger.error("Failed to parse download history JSON: %s", e)
                return None
            
            for item in result.get('history', []):
                if item.get("id") == youtube_id:
                    status = item.get("status")
                    if status == "error":
                        error_msg = item.get("error") or item.get("msg") or "Unknown error"
                        logger.error("Download failed for %s: %s", youtube_id, error_msg)
                        return None
                    elif status == "finished":
                        logger.info("Download succeeded for %s", youtube_id)
                        return os.path.join(item["download_dir"], item["filename"])
                    elif status == "downloading":
                        logger.info("Download is in progress for %s, waiting...", youtube_id)
                        break  # exit for-loop but stay in retry loop
                    else:
                        logger.info("Download status for %s is '%s', waiting...", youtube_id, status)
                        break

    logger.error("Download status check timed out for %s", youtube_id)
    return None

def http_post(url, data, headers=None):
    headers = headers or {}
    data_bytes = json.dumps(data).encode(encoding)
    req = urllib.request.Request(url, data=data_bytes, headers=headers, method='POST')

    try:
        with urllib.request.urlopen(req) as response:
            status_code = response.getcode()
            body = response.read().decode(encoding)

            if status_code == 200:
                return status_code, body
            else:
                logger.error("HTTP error %d from %s - %s", status_code, url, body)
                return status_code, body

    except Exception as e:
        logger.error("Request failed for %s - %s", url, e)
        return None, None
    
def try_link(dirs: List[Tuple[str, Callable[[str], str]]], youtube_id: str, retries: int=1, delay: int=0):
    file = check_download_status(youtube_id, retries, delay)
    if not file or not os.path.exists(file):
        return False
    
    for (dir, suplier) in dirs:
        dst = os.path.join(dir, suplier(os.path.basename(file)))
        if os.path.exists(dst):
            os.remove(dst)
            
        logger.info("Linking: %s to %s", file, dst)
        os.link(file, dst)
        
        logger.info("Linking: %s to %s", file, dst)
        params = urllib.parse.urlencode({"path": dst})
        status_code, body = http_get(f"http://autopulse:2875/triggers/{autopulse_instance_name}?{params}", autopulse_credentials())
        logger.info("%s - Autopulse - response: %s", status_code, body)
        
    return True

def autopulse_credentials():
    credentials = f"{autopulse_username}:{autopulse_password}"
    encoded_credentials = base64.b64encode(credentials.encode(encoding)).decode(encoding)
    return {"Authorization": f"Basic {encoded_credentials}"}

def main():
    if event_type == "Test":
        logger.info("Test event received - successful")
        sys.exit(0)

    if not tmdb_api_key:
        logger.error("Missing TMDB_API_KEY environment variable")
        sys.exit(1)

    if not tmdb_id or not movie_path:
        logger.error("Missing TMDb ID or movie path")
        sys.exit(1)

    logger.info("%s - Fetching trailers from TMDb", movie_title)
    tmdb_url = "https://api.themoviedb.org/3/movie/%s/videos?api_key=%s&language=%s" % (tmdb_id, tmdb_api_key, language)
    code, response_text = http_get(tmdb_url)
    if code != 200 or response_text is None:
        logger.error("%s - Failed to fetch trailers from TMDb", movie_title)
        sys.exit(1)

    try:
        data = json.loads(response_text)
    except json.JSONDecodeError:
        logger.error("%s - Failed to parse TMDb response", movie_title)
        sys.exit(1)

    trailers = [v for v in data.get("results", []) if v.get("site") == "YouTube" and v.get("type") == "Trailer"]

    if not trailers:
        logger.info("%s - No trailers found on TMDb", movie_title)
        sys.exit(0)
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    cookies_path = os.path.join(script_dir, "cookies.txt")
    cookies_str = None
    if os.path.exists(cookies_path):
        with open(cookies_path, "r", encoding="utf-8") as file:
            cookies_str = file.read()
    
    # to be able to match for Jellyfin and Plex
    trailer_dirs = [
        (os.path.join(movie_path, "Trailers"), lambda file: file),
        # Jellyfin?
        (os.path.join(movie_path, "extras"), lambda file: "trailer" + os.path.splitext(file)[1])
    ]
    
    for trailer in trailers:
        trailer_key = trailer.get("key")
        trailer_title = trailer.get("name")
        
        for dir_, _ in trailer_dirs:
            os.makedirs(dir_, exist_ok=True)
            
        if try_link(trailer_dirs, trailer_key):
            return

        logger.info("%s - Found trailer '%s' (Key: %s)", movie_title, trailer_title, trailer_key)
        
        youtube_url = "https://www.youtube.com/watch?v=%s" % trailer_key
        lang, country = language.split("-")
        
        payload = {
            "url": youtube_url,
            "preset": "default",
            "folder": f"/trailers/{language}",
            "cli": f"--geo-bypass --geo-bypass-country {country} --proxy {proxy} --write-subs --sub-lang {lang} --embed-subs"
        }
        
        if cookies_str:
            payload["cookies"]=cookies_str

        logger.info("%s - Sending download request to ytptube /history", movie_title)
        status_code, body = http_post(
            f"{youtube_api}/history",
            data=payload,
            headers={"Content-Type": "application/json"}
        )

        if status_code == 200:
            logger.info("%s - Download request accepted by ytptube - response: %s", movie_title, body)
            
            if try_link(trailer_dirs, trailer_key, retries=5, delay=30):
                return
        else:
            logger.error("%s - ytptube error response - %s (HTTP %d)", movie_title, body, status_code)

if __name__ == "__main__":
    main()
