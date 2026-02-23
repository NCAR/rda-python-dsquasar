#!/usr/bin/env python3
import os
import tarfile
import logging
from pathlib import Path

# Constants for size limits
ONE_TB = 1_099_511_627_776  # 1TB in bytes
THREE_TB = 3_298_534_883_328  # 3TB in bytes

def setup_logging(output_dir):
    import datetime
    log_filename = f"tarlog_{datetime.datetime.now().strftime('%Y%m%d')}.log"
    log_path = os.path.join(output_dir, log_filename)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    # Remove any existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    # File handler
    fh = logging.FileHandler(log_path)
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
    logger.addHandler(fh)
    logger.addHandler(ch)
    logging.info(f"Logging to file: {log_path}")

def get_file_size(path):
    try:
        return os.path.getsize(path)
    except Exception as e:
        logging.warning(f"Could not get size for {path}: {e}")
        return 0

def group_files_by_size(files, min_size, max_size):
    """
    Group files into batches where each batch's total size is between min_size and max_size.
    Returns a list of lists of file paths.
    """
    batches = []
    current_batch = []
    current_size = 0
    for f in files:
        fsize = get_file_size(f)
        if fsize > max_size:
            logging.warning(f"File {f} is larger than max tar size ({max_size} bytes), skipping.")
            continue
        if current_size + fsize > max_size:
            if current_size >= min_size:
                batches.append(current_batch)
                current_batch = [f]
                current_size = fsize
            else:
                # If adding this file exceeds max_size but current batch is too small, force add
                current_batch.append(f)
                current_size += fsize
                batches.append(current_batch)
                current_batch = []
                current_size = 0
        else:
            current_batch.append(f)
            current_size += fsize
    if current_batch:
        batches.append(current_batch)
    return batches

def tar_batches(dirpath, batches, output_dir):
    src_dir_name = Path(dirpath).name
    for idx, batch in enumerate(batches, 1):
        tar_name = os.path.join(output_dir, f"{src_dir_name}_part{idx}.tar")
        logging.info(f"Creating tar: {tar_name} with {len(batch)} files.")
        with tarfile.open(tar_name, "w") as tar:
            for f in batch:
                try:
                    tar.add(f, arcname=os.path.relpath(f, dirpath))
                except Exception as e:
                    logging.warning(f"Failed to add {f} to tar: {e}")

def process_directory_tree(root_dir, output_dir):
    for dirpath, dirnames, filenames in os.walk(root_dir):
        abs_files = [os.path.join(dirpath, f) for f in filenames]
        if not abs_files:
            continue
        batches = group_files_by_size(abs_files, ONE_TB, THREE_TB)
        tar_batches(dirpath, batches, output_dir)

def read_directories_from_file(input_file):
    dirs = []
    with open(input_file, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if os.path.isdir(line):
                dirs.append(os.path.abspath(line))
            else:
                logging.warning(f"Directory does not exist: {line}")
    return dirs

def collect_all_files(directories):
    files = []
    for d in directories:
        for dirpath, dirnames, filenames in os.walk(d):
            for fname in filenames:
                fpath = os.path.join(dirpath, fname)
                files.append(fpath)
    return files

def find_common_root(paths):
    return os.path.commonpath(paths) if paths else ''

def tar_batches_across_dirs(files, batches, output_dir, common_root, root_dirs):
    root_dir_paths = [Path(d).resolve() for d in root_dirs]
    for idx, batch in enumerate(batches, 1):
        # Find which root directories are represented in this batch
        batch_root_names = set()
        for f in batch:
            f_path = Path(f).resolve()
            for root_path in root_dir_paths:
                try:
                    if str(f_path).startswith(str(root_path)):
                        batch_root_names.add(root_path.name)
                        break
                except Exception:
                    continue
        prefix = "_".join(sorted(batch_root_names)) if batch_root_names else "batch"
        num_files = len(batch)
        tar_name = os.path.join(output_dir, f"{prefix}_batch_part{idx}_{num_files}files.tar")
        logging.info(f"Creating tar: {tar_name} with {num_files} files.")
        with tarfile.open(tar_name, "w") as tar:
            for f in batch:
                try:
                    # Preserve relative path from common root in tar
                    arcname = os.path.relpath(f, common_root)
                    tar.add(f, arcname=arcname)
                except Exception as e:
                    logging.warning(f"Failed to add {f} to tar: {e}")

def get_batch_size(batch):
    return sum(get_file_size(f) for f in batch)

def process_from_file(input_file, output_dir):
    directories = read_directories_from_file(input_file)
    files = collect_all_files(directories)
    if not files:
        logging.info("No files found in provided directories.")
        return
    batches = group_files_by_size(files, ONE_TB, THREE_TB)
    # If last batch is less than 1TB, append it to previous batch
    if len(batches) > 1 and get_batch_size(batches[-1]) < ONE_TB:
        logging.info(f"Last batch size ({get_batch_size(batches[-1])} bytes) < 1TB, appending to previous batch.")
        batches[-2].extend(batches[-1])
        batches.pop()
    common_root = find_common_root(directories)
    tar_batches_across_dirs(files, batches, output_dir, common_root, directories)

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Tar files from a list of directories into 1-3TB tar files.")
    parser.add_argument("--input-file", help="File containing list of directories to process (one per line)")
    parser.add_argument("--output-dir", help="Directory to store tar files (default: current directory)")
    parser.add_argument("root_dir", nargs='*', help="Root directory or directories to process")
    args = parser.parse_args()
    output_dir = args.output_dir if args.output_dir else os.getcwd()
    os.makedirs(output_dir, exist_ok=True)
    setup_logging(output_dir)
    if args.input_file:
        process_from_file(args.input_file, output_dir)
    elif args.root_dir:
        for root in args.root_dir:
            if not os.path.isdir(root):
                logging.warning(f"Root directory does not exist: {root}")
                continue
            process_directory_tree(root, output_dir)
    else:
        print("Error: Must provide either --input-file or root_dir.")

if __name__ == "__main__":
    main()
