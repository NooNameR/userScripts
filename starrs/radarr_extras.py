#!/usr/bin/env python3

import os
import sys
import json
import urllib.request
from urllib.error import URLError, HTTPError
import logging
import time
from logging.handlers import RotatingFileHandler

script_name = "Radarr-Trailer"
log_file = "/config/logs/trailer_downloader.log"
id_file = "/config/logs/trailer_id.log"
media_location = "/data/media"
encoding = "utf-8"

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
language = os.getenv("LANGUAGE") or "en-US"
proxy = os.getenv("PROXY")

def has_processed(trailer_key: str) -> bool:
    if not os.path.exists(id_file):
        return False
    
    with open(id_file, "r", encoding=encoding) as f:
        processed = set(line.strip() for line in f if line.strip())
        return trailer_key in processed

def mark_processed(trailer_key: str):
    with open(id_file, "a", encoding=encoding) as f:
        f.write(trailer_key)
        f.write(os.linesep)

def http_get(url):
    try:
        with urllib.request.urlopen(url) as response:
            return response.getcode(), response.read().decode(encoding)
    except HTTPError as e:
        logger.error("HTTP error fetching %s - %d %s", url, e.code, e.reason)
    except URLError as e:
        logger.error("URL error fetching %s - %s", url, e.reason)
    except Exception as e:
        logger.error("Unexpected error fetching %s - %s", url, e)
    return None, None

def check_download_status(youtube_id, retries=5, delay=15):
    """
    Poll ytptube /api/history endpoint to check if the download succeeded or failed.
    retries: number of polling attempts
    delay: seconds between attempts
    """
    for attempt in range(1, retries + 1):
        time.sleep(delay)
        
        logger.info("Checking download status for %s (Attempt %d/%d)...", youtube_id, attempt, retries)
        status_code, history_body = http_get("http://ytptube:8081/api/history")
        if status_code != 200 or history_body is None:
            logger.warning("Failed to fetch download history on attempt %d", attempt)
        else:
            try:
                result = json.loads(history_body)
            except Exception as e:
                logger.error("Failed to parse download history JSON: %s", e)
                return False
            
            for item in result.get('history', []):
                if item.get("id") == youtube_id:
                    status = item.get("status")
                    if status == "error":
                        error_msg = item.get("error") or item.get("msg") or "Unknown error"
                        logger.error("Download failed for %s: %s", youtube_id, error_msg)
                        return False
                    elif status == "finished":
                        logger.info("Download succeeded for %s", youtube_id)
                        return True
                    else:
                        logger.info("Download status for %s is '%s', waiting...", youtube_id, status)
                    break
            else:
                logger.warning("No download entry found for %s", youtube_id)

    logger.error("Download status check timed out for %s", youtube_id)
    return False

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
    
    for trailer in trailers:
        trailer_key = trailer.get("key")
        trailer_title = trailer.get("name")

        if has_processed(trailer_key):
            logger.info("Movie already processed - '%s' (TrailerKey: %s)", movie_title, trailer_key)
            sys.exit(0)

        logger.info("%s - Found trailer '%s' (Key: %s)", movie_title, trailer_title, trailer_key)
        
        youtube_url = "https://www.youtube.com/watch?v=%s" % trailer_key
        relative_path = os.path.relpath(movie_path, media_location)
        trailer_dir = os.path.join(relative_path, "Trailers")

        payload = {
            "url": youtube_url,
            "preset": "default",
            "folder": trailer_dir,
            "cli": f"--geo-bypass --geo-bypass-country {language.split("-")[1]} --proxy {proxy}"
        }

        logger.info("%s - Sending download request to ytptube /history", movie_title)
        status_code, body = http_post(
            "http://ytptube:8081/api/history",
            data=payload,
            headers={"Content-Type": "application/json"}
        )

        if status_code == 200:
            logger.info("%s - Download request accepted by MeTube - response: %s", movie_title, body)
            
            if check_download_status(trailer_key):
                mark_processed(trailer_key)
                return
        else:
            logger.error("%s - MeTube error response - %s (HTTP %d)", movie_title, body, status_code)

if __name__ == "__main__":
    main()
