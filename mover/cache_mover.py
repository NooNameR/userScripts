import os
import fcntl
import shutil
import sys
import logging
from logging.handlers import RotatingFileHandler
import modules.helpers as helpers
from datetime import datetime
from typing import Callable, Dict, Set, Iterable
from collections import defaultdict
from modules.config import Config, MovingMapping

def move_files(mapping: MovingMapping, files: Iterable[str], inodes: Dict[int, Set[str]], dest_func: Callable[[str], str], remaining: int) -> int:
    total: int = 0
    processed: Set[str] = set()
    
    for src_file in files:
        if src_file in processed:
            logging.debug("File was already processed: %s", src_file)
            continue
        
        if mapping.is_ignored(src_file):
            logging.debug("File is ignored: %s", src_file)
            continue
        
        # Check if the file is within the age range
        if remaining <= 0:
            logging.debug("Already reached required amount to move. Stopping mover...")
            break
        
        if mapping.is_active(src_file):
            logging.info("Skipping file, currently is being actively used: %s", src_file)
            continue
        
        logging.debug("Processing file: %s | Remaining bytes to move: %s", src_file, helpers.format_bytes_to_gib(remaining))
        
        stat = helpers.get_stat(src_file)
    
        mapping.pause(src_file)
        
        dest_file = dest_func(src_file)
        # Skip if the file already exists in the destination with the same size
        if helpers.is_same_file(src_file, dest_file):
            logging.info("Skipping existing file: %s", dest_file)
        else:
            helpers.copy_file_with_metadata(src_file, dest_file)
        
        processed.add(src_file)
        
        for link_src_file in inodes.get(stat.st_ino, set()):
            if link_src_file in processed:
                continue
            
            link_dest_file = dest_func(link_src_file)
            mapping.pause(link_src_file)
            if helpers.is_same_file(link_src_file, link_dest_file):
                logging.info("Skipping existing file: %s", link_dest_file)
            else:
                if os.path.exists(link_dest_file):
                    link_dest_stat = helpers.get_stat(link_dest_file)
                    logging.warning("Destination file: %s is not the same as: %s. Deleting before re-linking", link_dest_file, link_src_file)
                    helpers.delete_file(link_dest_file)
                    total += link_dest_stat.st_size
                
                helpers.link_file(dest_file, link_src_file, link_dest_file)
            
            processed.add(link_src_file)
            helpers.delete_file(link_src_file)
                
        helpers.delete_file(src_file)
        total += stat.st_size
        remaining -= stat.st_size
    
    return total

def move_to_destination(mapping: MovingMapping) -> int:
    needs_moving = mapping.needs_moving()
    if not needs_moving:
        logging.debug("Stopping mover, source: %s is below the threshold", mapping.source)
        return 0
    
    inodes_map: Dict[int, Set[str]] = defaultdict(set)
    files_to_move: Set[str] = set()
    logging.info("Scanning %s...", mapping.source)
    
    for root, dirs, files in os.walk(mapping.source):
        dirs.sort()

        for file in files:
            src_file = os.path.join(root, file)
        
            # Get the inode of the source file
            inode = helpers.get_stat(src_file).st_ino

            if inode not in inodes_map:
                files_to_move.add(src_file)
                
            inodes_map[inode].add(src_file)
    
    logging.info(
        "Starting mover (%s -> %s) for %d potential files with %d hardlinks to move. Moving approximately %s...",
        mapping.source, 
        mapping.destination, 
        len(files_to_move), 
        sum(len(v) for v in inodes_map.values()), 
        helpers.format_bytes_to_gib(needs_moving)
    )
    
    total = move_files(mapping, sorted(files_to_move, key=mapping.get_sort_key), inodes_map, mapping.get_dest_file, needs_moving)
    
    helpers.delete_empty_dirs(mapping.source, mapping.is_ignored)
    
    return total

def move_to_source(mapping: MovingMapping) -> int:
    can_move = mapping.can_move_to_source()
    if not can_move:
        return 0
    
    files_to_move: Set[str] = mapping.eligible_for_source()
    if not files_to_move:
        return 0
    
    logging.info("Scanning %s...", mapping.destination)
    
    inodes_map: Dict[int, Set[str]] = {helpers.get_stat(f).st_ino: set() for f in files_to_move}
    for root, dirs, files in os.walk(mapping.destination):
        dirs.sort()

        for file in files:
            src_file = os.path.join(root, file)

            # Get the inode of the source file
            inode = helpers.get_stat(src_file).st_ino
            if inode in inodes_map:
                inodes_map[inode].add(src_file)
                
    logging.info(
        "Starting mover (%s -> %s) for %d potential files with %d hardlinks to move. Moving max up to %s...",
        mapping.destination, 
        mapping.source, 
        len(files_to_move), 
        sum(len(v) for v in inodes_map.values()),
        helpers.format_bytes_to_gib(can_move)
    )
    total = move_files(mapping, files_to_move, inodes_map, mapping.get_src_file, can_move)
    
    helpers.delete_empty_dirs(mapping.destination, mapping.is_ignored)
    
    return total
                
if __name__ == "__main__":
    import argparse
    # Argument parsing
    parser = argparse.ArgumentParser(description="Migrate files and preserve hardlinks, only moving files within a specific age range. Deletes source files after successful migration.")
    parser.add_argument("--config", type=str, help="Path to config yaml", required=True)
    parser.add_argument("--dry-run", help="Dry-run mode", action="store_true", default=False)
    parser.add_argument("--log-level", type=str, help="Default logger level", choices=list(logging._nameToLevel.keys()), default="INFO")
    parser.add_argument("--log-file", type=str, help="Log filepath", required=False)
    parser.add_argument("--lock-file", type=str, help="Lock filepath. For UNRAID use: '/var/run/mover.pid'", default="/tmp/cache_mover.lock")
    args = parser.parse_args()
    
    handlers = [logging.StreamHandler(sys.stdout)]
    
    if args.log_file:
        open(args.log_file, 'a').close()
        handlers.append(RotatingFileHandler(
            args.log_file,
            maxBytes=5 * 1024 * 1024,  # 5 MB
            backupCount=3              # keep 3 old log files
        ))
    
    logging.basicConfig(
        level=args.log_level,
        format=f"{("[DRY-RUN]: " if args.dry_run else "")}%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
    )
    
    now = datetime.now()
    helpers.init(now, args.dry_run)
    
    config = Config(now, args.config)
    logging.info(config)

    lock_file = open(args.lock_file, 'w')
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        logging.error("Another instance is already running.")
        sys.exit()
    
    try:
        for mapping in config.mappings:
            try:            
                _, _, startingfree = shutil.disk_usage(mapping.source)
                emptiedspace = move_to_destination(mapping)
                moved_to_source = move_to_source(mapping)
                _, _, ending_free = shutil.disk_usage(mapping.source)
                logging.info("Migration and hardlink recreation completed successfully from '%s' to '%s'", mapping.source, mapping.destination)
                logging.info("Starting free space: %s -- Ending free space: %s", helpers.format_bytes_to_gib(startingfree), helpers.format_bytes_to_gib(ending_free))
                logging.info("FREED UP %s TOTAL SPACE", helpers.format_bytes_to_gib(emptiedspace))
                logging.info("MOVED BACK TO SOURCE %s", helpers.format_bytes_to_gib(moved_to_source))
            except IndexError as e:
                logging.error("Error: %s", e, exc_info=True)
            except Exception as e:
                logging.error("Error: %s", e, exc_info=True)
            finally:
                mapping.resume()
    finally:
        lock_file.close()
        os.remove(lock_file.name)