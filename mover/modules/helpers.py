import os
import shutil
import logging

def maybe_create_dir(src_file, dest_file):
    dest_dir = os.path.dirname(dest_file)
    
    if not os.path.exists(dest_dir):
        # Create directories in the destination
        logging.info("Creating directory: %s", dest_dir)
        os.makedirs(dest_dir, exist_ok=True)
        logging.info("Created directory: %s", dest_dir)
        # Set permissions for new directory
        try:
            src_dir = os.path.dirname(src_file)
            
            logging.debug("Getting permissions from source directory: %s", src_dir)
            src_stat = os.stat(src_dir)
            logging.debug("Setting permissions: [%s:%s] to %s", src_stat.st_uid, src_stat.st_gid, dest_dir)
            os.chown(dest_dir, src_stat.st_uid, src_stat.st_gid)
            logging.info("Set permissions [%s:%s] for destination directory: %s", src_stat.st_uid, src_stat.st_gid, dest_dir)
        except PermissionError as e:
            logging.error("Unable to set ownership for %s. %s", dest_dir, e)
                
def is_same_file(src_file: str, dest_file: str) -> bool:
    if not os.path.exists(dest_file):
        return False
    
    src_stat = os.stat(src_file)
    dest_stat = os.stat(dest_file)
    return src_stat.st_size == dest_stat.st_size

def copy_file_with_metadata(src_file: str, dest_file: str):
    try:
        logging.info("Copying: %s -> %s", src_file, dest_file)
        shutil.copy2(src_file, dest_file)
        src_stat = os.stat(src_file)
        os.chown(dest_file, src_stat.st_uid, src_stat.st_gid)
        logging.info("Copied: %s -> %s", src_file, dest_file)
    except PermissionError as e:
        logging.error("Unable to preserve ownership for %s. Requires elevated privileges. %s", dest_file, e)

def link_file(src_file: str, dest_file: str):
     # If inode is already processed, create a hard link
    logging.info("Hardlinking: %s -> %s", src_file, dest_file)
    os.link(src_file, dest_file)                
    logging.info("Hardlinked: %s -> %s", src_file, dest_file)
    
def delete_file(path: str) -> int:   
    try:
        size = os.path.getsize(path)
        logging.debug("Deleting file: %s", path)
        os.remove(path)
        logging.info("Deleted file: %s", path)
        return size
    except Exception as e:
        logging.error("Failed to delete %s: %s", path, e)
    return 0

def format_bytes_to_gib(size_bytes: int) -> str:
    gib = size_bytes / (1024 ** 3)
    return f"{gib:.2f} GiB"