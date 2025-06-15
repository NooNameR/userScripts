from pathlib import Path
import os
import shutil
import logging
import subprocess
from typing import Dict, Callable
from datetime import datetime
from functools import cache

_dry_run: bool = False
_now: datetime = datetime.now()

def init(now: datetime, dry_run: bool):
    global _dry_run, _now
    _dry_run = dry_run
    _now = now

def maybe_create_dir(src_file: str, dest_file: str) -> None:
    dest_dir = Path(dest_file).parent
    
    if dest_dir.exists():
        return
    
    dirs = []
    src_dir, dir = Path(src_file).parent, dest_dir
    
    while not dir.exists():
        dirs.append((src_dir, dir))
        dir = dir.parent
        src_dir = src_dir.parent
        
    while dirs:
        src_dir, dir = dirs.pop()
        
        def create_dir():
            dir.mkdir(parents=False)
            # Set permissions for new directory
            logging.debug("Getting permissions from source directory: %s", src_dir)
            src_stat = src_dir.stat()
            logging.debug("Setting permissions: [%s:%s] to %s", src_stat.st_uid, src_stat.st_gid, dir)
            os.chown(dir, src_stat.st_uid, src_stat.st_gid)
            logging.info("Set permissions [%s:%s] for destination directory: %s", src_stat.st_uid, src_stat.st_gid, dir)
        
        # Set permissions for new directory
        try:
           logging.info("Creating directory: %s", dir)
           execute(create_dir)
           logging.info("Created directory: %s", dir)
        except PermissionError as e:
            logging.error("Unable to set ownership for %s. %s", dir, e)
                
def is_same_file(src_file: str, dest_file: str) -> bool:
    if not os.path.exists(dest_file):
        return False
    
    src_stat = get_stat(src_file)
    dest_stat = get_stat(dest_file)
    return src_stat.st_size == dest_stat.st_size

def copy_file_with_metadata(src_file: str, dest_file: str, metadata: Dict[str, str] = {}) -> None:
    maybe_create_dir(src_file, dest_file)
    
    def copy():
        shutil.copy2(src_file, dest_file)
        src_stat = get_stat(src_file)
        os.chown(dest_file, src_stat.st_uid, src_stat.st_gid)
    
    try:
        logging.info("[%s] Copying: %s -> %s | Metadata: %s", get_age_str(src_file), src_file, dest_file, metadata)
        execute(copy)
        logging.info("Copied: %s -> %s", src_file, dest_file)
    except PermissionError as e:
        logging.error("Unable to preserve ownership for %s. Requires elevated privileges. %s", dest_file, e)

def link_file(link_file: str, src_file: str, dest_file: str):
    maybe_create_dir(src_file, dest_file)
    
     # If inode is already processed, create a hard link
    logging.info("Hardlinking: %s -> %s", link_file, dest_file)
    execute(lambda: os.link(link_file, dest_file))
    logging.info("Hardlinked: %s -> %s", link_file, dest_file)
    
def delete_file(path: str) -> None:   
    try:
        logging.debug("[%s] Deleting file: %s", get_age_str(path), path)
        
        execute(lambda: os.remove(path))
        logging.info("Deleted file: %s", path)
    except Exception as e:
        logging.error("Failed to delete %s: %s", path, e)
        
def delete_empty_dirs(root: str, is_ignored: Callable[[str], bool]) -> None:
    # Remove empty directories
    for root, dirs, _ in os.walk(root, topdown=False):
        for dir_ in dirs:
            dir_path = os.path.join(root, dir_)
            
            if is_ignored(dir_path):
                continue
            
            if not os.listdir(dir_path):  # Directory is empty
                logging.debug("Removing empty directory: %s", dir_path)
                execute(lambda: os.rmdir(dir_path))
                logging.info("Removed empty directory: %s", dir_path)

def format_bytes_to_gib(size_bytes: int) -> str:
    gib = size_bytes / (1024 ** 3)
    return f"{gib:.2f} GiB"

@cache
def get_ctime(file: str) -> float:
    stat = get_stat(file)
    return (
        getattr(stat, 'st_birthtime', None)
        or __get_birthtime(file)
        or stat.st_ctime
    )

def get_age_str(file: str) -> str:
    created_dt = datetime.fromtimestamp(get_ctime(file))
    return f"{(_now - created_dt).days}d"
    
def __get_birthtime(filepath) -> float:
    """
    Get the creation (birth) time of a file from ZFS using GNU stat.
    Returns epoch timestamp or None if unavailable.
    """
    try:
        result = subprocess.run(["stat", "--format=%W", filepath], capture_output=True, text=True)
        timestamp = float(result.stdout.strip())
        if timestamp <= 0:
            return None
        return timestamp
    except Exception as e:
        logging.error(f"Error getting birthtime for %s: %s", filepath, e)
        return None


@cache
def get_stat(file: str) -> os.stat_result:
    return os.stat(file)

def execute(callable: Callable[[], None]) -> None:
    if not _dry_run:
        callable()