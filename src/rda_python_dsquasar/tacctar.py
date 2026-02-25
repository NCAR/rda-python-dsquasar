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
        if fsize > 2*max_size:
            logging.warning(f"File {f} is larger than max tar size ({2*max_size} bytes), skipping.")
            continue
        if current_size + fsize > max_size:
            if current_size < min_size and batches:
                # If current batch is too small and there is a previous batch, append to previous
                batches[-1].extend(current_batch)
                current_batch = [f]
                current_size = fsize
            else:
                batches.append(current_batch)
                current_batch = [f]
                current_size = fsize
        else:
            current_batch.append(f)
            current_size += fsize
    if current_batch:
        if current_size < min_size and batches:
            batches[-1].extend(current_batch)
        else:
            batches.append(current_batch)
    return batches

def tar_batches(dirpath, batches, output_dir, dataset_path=None, dataset_name=None):
    # Use dataset_name for tar file naming and arcname
    for idx, batch in enumerate(batches, 1):
        num_files = len(batch)
        tar_name = os.path.join(output_dir, f"{dataset_name}_part{idx}_{num_files}files.tar")
        logging.info(f"Creating tar: {tar_name} with {num_files} files.")
        with tarfile.open(tar_name, "w") as tar:
            for f in batch:
                try:
                    # arcname should be relative to dataset_path, and always start with dataset_name
                    arcname = os.path.relpath(f, dataset_path)
                    arcname = os.path.join(dataset_name, arcname)
                    tar.add(f, arcname=arcname)
                except Exception as e:
                    logging.warning(f"Failed to add {f} to tar: {e}")

def process_directory_tree(dataset_path, output_dir, db_params=None):
    dataset_name = os.path.basename(os.path.abspath(dataset_path))
    wfile_table = f"dssdb.wfile_{dataset_name}"
    all_files = []
    conn = cur = None
    if db_params:
        import psycopg2
        try:
            conn = psycopg2.connect(**{k: v for k, v in db_params.items() if v is not None})
            cur = conn.cursor()
        except Exception as e:
            logging.error(f"Error connecting to database for tarred check: {e}")
            conn = cur = None
    for dirpath, dirnames, filenames in os.walk(dataset_path):
        for fname in filenames:
            fpath = os.path.join(dirpath, fname)
            relpath = os.path.relpath(fpath, dataset_path)
            # Skip file if record exists in wfile_table with tid > 0
            if cur:
                try:
                    cur.execute(f"SELECT 1 FROM {wfile_table} WHERE wfile=%s AND tid > 0 LIMIT 1", (relpath,))
                    exists = cur.fetchone()
                    if exists:
                        logging.info(f"Skipping tarred file: {fpath} (wfile={relpath}) in {wfile_table}")
                        continue
                except Exception as e:
                    logging.error(f"Error checking {wfile_table} for {relpath}: {e}")
            all_files.append(fpath)
    if cur:
        cur.close()
    if conn:
        conn.close()
    if not all_files:
        return
    batches = group_files_by_size(all_files, ONE_TB, THREE_TB)
    tar_batches(dataset_path, batches, output_dir, dataset_path=dataset_path, dataset_name=dataset_name)

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

def collect_all_files(dataset_dirs, db_params=None):
    files = []
    for d in dataset_dirs:
        dataset_name = os.path.basename(os.path.abspath(d))
        wfile_table = f"dssdb.wfile_{dataset_name}"
        conn = cur = None
        if db_params:
            import psycopg2
            try:
                conn = psycopg2.connect(**{k: v for k, v in db_params.items() if v is not None})
                cur = conn.cursor()
            except Exception as e:
                logging.error(f"Error connecting to database for tarred check: {e}")
                conn = cur = None
        for dirpath, dirnames, filenames in os.walk(d):
            for fname in filenames:
                fpath = os.path.join(dirpath, fname)
                relpath = os.path.relpath(fpath, d)
                # Skip file if record exists in wfile_table with tid > 0
                if cur:
                    try:
                        cur.execute(f"SELECT 1 FROM {wfile_table} WHERE wfile=%s AND tid > 0 LIMIT 1", (relpath,))
                        exists = cur.fetchone()
                        if exists:
                            logging.info(f"Skipping tarred file: {fpath} (wfile={relpath}) in {wfile_table}")
                            continue
                    except Exception as e:
                        logging.error(f"Error checking {wfile_table} for {relpath}: {e}")
                files.append(fpath)
        if cur:
            cur.close()
        if conn:
            conn.close()
    return files

def find_common_root(paths):
    return os.path.commonpath(paths) if paths else ''

def tar_batches_across_dirs(files, batches, output_dir, common_root, dataset_dirs):
    dataset_dir_paths = [Path(d).resolve() for d in dataset_dirs]
    for idx, batch in enumerate(batches, 1):
        batch_dataset_names = set()
        for f in batch:
            f_path = Path(f).resolve()
            for dataset_path in dataset_dir_paths:
                try:
                    if str(f_path).startswith(str(dataset_path)):
                        batch_dataset_names.add(dataset_path.name)
                        break
                except Exception:
                    continue
        num_files = len(batch)
        dataset_name_count = len(batch_dataset_names)
        if dataset_name_count > 3:
            prefix = f"{sorted(batch_dataset_names)[0]}_dn{dataset_name_count}"
        else:
            prefix = "_".join(sorted(batch_dataset_names)) if batch_dataset_names else "batch"
        tar_name = os.path.join(output_dir, f"{prefix}_part{idx}_{num_files}files.tar")
        logging.info(f"Creating tar: {tar_name} with {num_files} files.")
        with tarfile.open(tar_name, "w") as tar:
            for f in batch:
                try:
                    arcname = os.path.relpath(f, common_root)
                    tar.add(f, arcname=arcname)
                except Exception as e:
                    logging.warning(f"Failed to add {f} to tar: {e}")

def get_batch_size(batch):
    return sum(get_file_size(f) for f in batch)

def process_from_file(input_file, output_dir, db_params=None):
    directories = read_directories_from_file(input_file)
    files = collect_all_files(directories, db_params=db_params)
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
    parser.add_argument('--check-tarred', action='store_true', default=False, help='Skip files already tarred (tid > 0 in wfile_<dataset_name>)')
    parser.add_argument("dataset_paths", nargs='*', help="Dataset directory or directories to process")
    args = parser.parse_args()
    output_dir = args.output_dir if args.output_dir else os.getcwd()
    os.makedirs(output_dir, exist_ok=True)
    setup_logging(output_dir)
    db_params = None
    if args.check_tarred:
        db_params = {
            'host': args.db_host if hasattr(args, 'db_host') else 'rda-db.ucar.edu',
            'port': args.db_port if hasattr(args, 'db_port') else 5432,
            'dbname': args.db_name if hasattr(args, 'db_name') else 'rdadb',
            'user': args.db_user if hasattr(args, 'db_user') else 'dssdb',
            'password': args.db_password if hasattr(args, 'db_password') else None
        }
    if args.input_file:
        process_from_file(args.input_file, output_dir, db_params=db_params)
    elif args.dataset_paths:
        for dataset_path in args.dataset_paths:
            if not os.path.isdir(dataset_path):
                logging.warning(f"Dataset directory does not exist: {dataset_path}")
                continue
            process_directory_tree(dataset_path, output_dir, db_params=db_params)
    else:
        print("Error: Must provide either --input-file or dataset_paths.")

if __name__ == "__main__":
    main()
