import os
import shutil
import sys
import logging
import modules.helpers as helpers
from collections import defaultdict
from modules.config import Config, MovingMapping

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
        
def migrate_files(mapping: MovingMapping, is_dry_run: bool) -> int:
    total = 0
    for path in mapping.includes:
        inodes_map = defaultdict(set)
        files_to_move = {}
        logging.info("Scanning %s...", path)
        
        for root, dirs, files in os.walk(path):
            dirs.sort()
            # Determine relative path from source to current directory
            rel_path = os.path.relpath(root, mapping.source)
            dest_dir = os.path.join(mapping.destination, rel_path)
            
            for file in files:
                src_file = os.path.join(root, file)
                dest_file = os.path.join(dest_dir, file)
                
                # Get the inode of the source file
                inode = helpers.get_stat(src_file).st_ino

                files_to_move[src_file] = dest_file    
                inodes_map[inode].add(src_file)
        
        if is_dry_run:
            continue
        
        total += move_files(mapping, files_to_move, inodes_map)
        delete_empty_dirs(mapping, path)
    
    return total

def sort_func(key: str, inode_map: dict[int, set[str]]) -> int:
    stat = helpers.get_stat(key)
    return (stat.st_mtime, len(inode_map.get(stat.st_ino, [])))
    

def move_files(mapping, files: dict[str, str], inode_map: dict[int, set[str]]) -> int:
    total = 0
    processed = set()

    for src_file, dest_file in sorted(files.items(), key = lambda item: sort_func(item[0], inode_map)):
        # Check if the file is within the age range
        if not mapping.needs_moving():
            logging.debug("Stopping mover, source: %s is below the threshold", mapping.source)
            return total
        
        # Check if the file is within the age range
        if not mapping.is_file_within_age_range(src_file):
            logging.debug("Skipping file (out of age range): %s", src_file)
            continue
        
        # Skip checking orphaned and recycled directories
        if mapping.is_ignored(src_file):
            logging.debug("Skipping file: %s, matched ignored", src_file)
            continue
        
        if src_file in processed:
            logging.debug("File was already processed: %s", src_file)
            continue
        
        mapping.pause(src_file)
        inode = helpers.get_stat(src_file).st_ino
        # Skip if the file already exists in the destination with the same size
        if helpers.is_same_file(src_file, dest_file):
            logging.info("Skipping existing file: %s", dest_file)
        else:
            helpers.maybe_create_dir(src_file, dest_file)
            helpers.copy_file_with_metadata(src_file, dest_file)
        
        processed.add(src_file)
        
        for link_src_file in inode_map.get(inode, set()):
            if link_src_file in processed:
                continue
            
            link_dest_file = files[link_src_file]
            mapping.pause(link_src_file)
            if helpers.is_same_file(link_src_file, link_dest_file):
                logging.info("Skipping existing file: %s", link_dest_file)
            else:
                if os.path.exists(link_dest_file):
                    logging.warning("Destination file: %s is not the same as: %s. Deleting before re-linking", link_dest_file, link_src_file)
                    total += helpers.delete_file(link_dest_file)
                
                helpers.maybe_create_dir(link_src_file, link_dest_file)
                helpers.link_file(dest_file, link_dest_file)
            
            processed.add(link_src_file)
            helpers.delete_file(link_src_file)
                
        total += helpers.delete_file(src_file)
        
    return total

def delete_empty_dirs(mapping: MovingMapping, dir: str):
    # Remove empty directories
    for root, dirs, _ in os.walk(dir, topdown=False):
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
    parser.add_argument("--config", help="Path to config yaml")

    args = parser.parse_args()
    config = Config(args.config)
    
    logging.info(config)
    
    for mapping in config.mappings:
        try:            
            startingtotal, startingused, startingfree = shutil.disk_usage(mapping.source)
            emptiedspace = migrate_files(mapping, config.dry_run)    
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