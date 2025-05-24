import os
import fcntl
import shutil
import sys
import logging
import modules.helpers as helpers
from typing import Dict, Tuple
from collections import defaultdict
from modules.config import Config, MovingMapping

lock_file_path = '/tmp/cache_mover.lock'
        
def migrate_files(mapping: MovingMapping, is_dry_run: bool) -> int:
    total = 0
    inodes_map = defaultdict(set)
    files_to_move = set()
    logging.info("Scanning %s...", mapping.source)
    
    for root, dirs, files in os.walk(mapping.source):
        dirs.sort()
                
        for file in files:
            src_file = os.path.join(root, file)
            # Get the inode of the source file
            inode = helpers.get_stat(src_file).st_ino

            files_to_move.add(src_file)
            inodes_map[inode].add(src_file)
    
    total += move_files(mapping, files_to_move, inodes_map, is_dry_run)
    if is_dry_run:
        delete_empty_dirs(mapping)
    
    return total

def sort_func(mapping, key: str, inode_map: Dict[int, set[str]]) -> Tuple[int, int, int, float]:
    stat = helpers.get_stat(key)
    age_priority = 0 if mapping.is_file_within_age_range(key) else 1
    hardlinks = len(inode_map.get(stat.st_ino, []))
    is_watched = 0 if mapping.is_watched(key) else 1
    return (age_priority, hardlinks, is_watched, helpers.get_ctime(key))
    

def move_files(mapping, files: set[str], inode_map: Dict[int, set[str]], dry_run: bool) -> int:
    total = 0
    processed = set()

    for src_file in sorted(files, key = lambda item: sort_func(mapping, item, inode_map)):
        if src_file in processed:
            logging.debug("File was already processed: %s", src_file)
            continue
        
        # Skip checking orphaned and recycled directories
        if mapping.is_ignored(src_file):
            logging.debug("Skipping file: %s, matched ignored", src_file)
            continue
        
        # Check if the file is within the age range
        if not mapping.is_file_within_age_range(src_file):
            logging.debug("Skipping file (out of age range): %s", src_file)
            continue
        
        # Check if the file is within the age range
        if not mapping.needs_moving():
            logging.debug("Stopping mover, source: %s is below the threshold", mapping.source)
            return total
        
        if mapping.is_active(src_file):
            logging.info("Skipping file, currently is being played on Plex: %s", src_file)
            continue
        
        stat = helpers.get_stat(src_file)
        
        if dry_run:
            logging.info("Skipping file %s, with %d hardlinks", src_file, len(inode_map.get(stat.st_ino, set())))
            total += stat.st_size
            continue
        
        mapping.pause(src_file)
        
        dest_file = mapping.get_dest_file(src_file)
        # Skip if the file already exists in the destination with the same size
        if helpers.is_same_file(src_file, dest_file):
            logging.info("Skipping existing file: %s", dest_file)
        else:
            helpers.copy_file_with_metadata(src_file, dest_file)
        
        processed.add(src_file)
        
        for link_src_file in inode_map.get(stat.st_ino, set()):
            if link_src_file in processed:
                continue
            
            link_dest_file = mapping.get_dest_file(link_src_file)
            mapping.pause(link_src_file)
            if helpers.is_same_file(link_src_file, link_dest_file):
                logging.info("Skipping existing file: %s", link_dest_file)
            else:
                if os.path.exists(link_dest_file):
                    logging.warning("Destination file: %s is not the same as: %s. Deleting before re-linking", link_dest_file, link_src_file)
                    helpers.delete_file(link_dest_file)
                    total += helpers.get_stat(link_dest_file).st_size
                
                helpers.link_file(dest_file, link_src_file, link_dest_file)
            
            processed.add(link_src_file)
            helpers.delete_file(link_src_file)
                
        helpers.delete_file(src_file)
        total += stat.st_size
        
    return total

def delete_empty_dirs(mapping: MovingMapping) -> None:
    # Remove empty directories
    for root, dirs, _ in os.walk(mapping.source, topdown=False):
        for dir_ in dirs:
            dir_path = os.path.join(root, dir_)
            
            if mapping.is_ignored(dir_path):
                continue
            
            if not os.listdir(dir_path):  # Directory is empty
                logging.debug("Removing empty directory: %s", dir_path)
                os.rmdir(dir_path)
                logging.info("Removed empty directory: %s", dir_path)
                
if __name__ == "__main__":
    import argparse
    # Argument parsing
    parser = argparse.ArgumentParser(description="Migrate files and preserve hardlinks, only moving files within a specific age range. Deletes source files after successful migration.")
    parser.add_argument("--config", type=str, help="Path to config yaml", required=True)
    parser.add_argument("--dry-run", help="Dry-run mode", action="store_true", default=False)
    parser.add_argument("--log-level", type=str, help="Default logger level", choices=list(logging._nameToLevel.keys()), default="INFO")
    args = parser.parse_args()
    
    logging.basicConfig(
        level=args.log_level,
        format=f"{("DRY-RUN: " if args.dry_run else "")}%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    
    config = Config(args.config)
    logging.info(config)

    lock_file = open(lock_file_path, 'w')
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        logging.error("Another instance is already running.")
        sys.exit()
    
    try:
        for mapping in config.mappings:
            if not mapping.needs_moving():
                continue
            
            try:            
                startingtotal, startingused, startingfree = shutil.disk_usage(mapping.source)
                emptiedspace = migrate_files(mapping, args.dry_run)    
                _, _, ending_free = shutil.disk_usage(mapping.source)
                logging.info("Migration and hardlink recreation completed successfully from '%s' to '%s'", mapping.source, mapping.destination)
                logging.info("Starting free space: %s -- Ending free space: %s", helpers.format_bytes_to_gib(startingfree), helpers.format_bytes_to_gib(ending_free))
                logging.info("FREED UP %s TOTAL SPACE", helpers.format_bytes_to_gib(emptiedspace))
            except IndexError as e:
                logging.error("Error: %s", e, exc_info=True)
            except Exception as e:
                logging.error("Error: %s", e, exc_info=True)
            finally:
                mapping.resume()
    finally:
        lock_file.close()