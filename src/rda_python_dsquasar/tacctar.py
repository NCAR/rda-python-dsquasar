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

def tar_batches(dirpath, batches, output_dir, dataset_path=None, dataset_name=None, tar_batch=True):
    """
    If tar_batch is True, create tar files for each batch. Otherwise, dump file list to .batch files.
    """
    for idx, batch in enumerate(batches, 1):
        num_files = len(batch)
        tar_name = os.path.join(output_dir, f"{dataset_name}_part{idx}_{num_files}files.tar")
        batch_name = tar_name.replace(".tar", ".batch")
        if tar_batch:
            logging.info(f"Creating tar: {tar_name} with {num_files} files.")
            with tarfile.open(tar_name, "w") as tar:
                for f in batch:
                    try:
                        arcname = os.path.relpath(f, dataset_path)
                        arcname = os.path.join(dataset_name, arcname)
                        tar.add(f, arcname=arcname)
                    except Exception as e:
                        logging.warning(f"Failed to add {f} to tar: {e}")
        else:
            logging.info(f"Writing batch file list: {batch_name} with {num_files} files.")
            with open(batch_name, "w") as bf:
                for f in batch:
                    arcname = os.path.relpath(f, dataset_path)
                    arcname = os.path.join(dataset_name, arcname)
                    bf.write(arcname + "\n")

def process_directory_tree(dataset_path, output_dir, db_params=None, tar_batch=True, dsid=None):
    dataset_name = dsid if dsid else os.path.basename(os.path.abspath(dataset_path))
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
    tar_batches(dataset_path, batches, output_dir, dataset_path=dataset_path, dataset_name=dataset_name, tar_batch=tar_batch)

def read_directories_from_file(input_file, tar_root=None):
    dataset_ids = []
    dataset_paths = []
    with open(input_file, 'r') as f:
        for line in f:
            dsid = line.strip()
            if not dsid or dsid.startswith('#'):
                continue
            joined_path = os.path.join(tar_root, dsid) if tar_root else dsid
            if os.path.isdir(joined_path):
                dataset_ids.append(dsid)
                dataset_paths.append(os.path.abspath(joined_path))
            else:
                logging.warning(f"Directory does not exist: {joined_path}")
    return dataset_ids, dataset_paths

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

def tar_batches_across_dirs(files, batches, output_dir, tar_root, dataset_paths, tar_batch=True, dataset_ids=None):
    dataset_dir_paths = [Path(d).resolve() for d in dataset_paths]
    dsid_map = {str(Path(d).resolve()): dsid for dsid, d in zip(dataset_ids, dataset_paths)} if dataset_ids else {}
    for idx, batch in enumerate(batches, 1):
        batch_dataset_names = set()
        for f in batch:
            f_path = Path(f).resolve()
            for dataset_path in dataset_dir_paths:
                try:
                    if str(f_path).startswith(str(dataset_path)):
                        batch_dataset_names.add(dsid_map.get(str(dataset_path), dataset_path.name))
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
        batch_name = tar_name.replace(".tar", ".batch")
        if tar_batch:
            logging.info(f"Creating tar: {tar_name} with {num_files} files.")
            with tarfile.open(tar_name, "w") as tar:
                for f in batch:
                    try:
                        arcname = os.path.relpath(f, tar_root)
                        tar.add(f, arcname=arcname)
                    except Exception as e:
                        logging.warning(f"Failed to add {f} to tar: {e}")
        else:
            logging.info(f"Writing batch file list: {batch_name} with {num_files} files.")
            with open(batch_name, "w") as bf:
                for f in batch:
                    arcname = os.path.relpath(f, tar_root)
                    bf.write(arcname + "\n")

def get_batch_size(batch):
    return sum(get_file_size(f) for f in batch)

def tar_batch_file(batch_file, tar_root=None):
    """
    Read a filelist in batch_file and tar the filelist into a tar file named like batch file name by replacing .batch with .tar.
    Prepend path tar_root to the file names in each batch file for the full path.
    After tarring, dump a member file detail list (like 'tar -tvf') into a .mbr file named as tarfilename+'.mbr'.
    If the .mbr file already exists, skip the tar action.
    """
    import pwd, grp, time
    tar_file = batch_file.replace('.batch', '.tar')
    mbr_file = tar_file + '.mbr'
    if os.path.exists(mbr_file):
        logging.info(f"Member list file {mbr_file} exists, skipping tar for {tar_file}.")
        return
    with open(batch_file, 'r') as bf:
        file_list = [line.strip() for line in bf if line.strip()]
    logging.info(f"Creating tar: {tar_file} from batch file: {batch_file} with {len(file_list)} files.")
    with tarfile.open(tar_file, "w") as tar:
        for rel_path in file_list:
            abs_path = os.path.join(tar_root, rel_path) if tar_root else rel_path
            try:
                tar.add(abs_path, arcname=rel_path)
            except Exception as e:
                logging.warning(f"Failed to add {abs_path} as {rel_path} to tar: {e}")
    # Write member list file in tar -tvf format
    with tarfile.open(tar_file, "r") as tar:
        with open(mbr_file, "w") as mf:
            for member in tar.getmembers():
                mode = oct(member.mode)[-4:]
                typechar = '-' if member.isfile() else 'd' if member.isdir() else 'l' if member.issym() else '?'
                uname = member.uname or (pwd.getpwuid(member.uid).pw_name if hasattr(member, 'uid') else '')
                gname = member.gname or (grp.getgrgid(member.gid).gr_name if hasattr(member, 'gid') else '')
                size = member.size
                mtime = time.strftime("%Y-%m-%d %H:%M", time.localtime(member.mtime))
                # Format: -rw-r--r-- user/group size date name
                mf.write(f"{typechar}{mode} {uname}/{gname} {size:9d} {mtime} {member.name}\n")

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Tar files from a list of dataset IDs into 1-3TB tar files.")
    parser.add_argument('-bi', '--batch-input-file', type=str, default=None, help='A file containing a list of batch file names, one per line. Each batch file should contain relative file names to be tarred.')
    parser.add_argument('-bf', '--batch-files', nargs='*', default=None, help='List of batch files to tar. Each batch file should contain relative file names to be tarred.')
    parser.add_argument('-tr', '--tar-root', type=str, required=True, help='Root directory for relative tar member file names (arcname). REQUIRED for all modes.')
    parser.add_argument('-if', '--input-file', help='File containing list of dataset IDs to process (one per line)')
    parser.add_argument('-od', '--output-dir', help='Directory to store tar files (default: current directory)')
    parser.add_argument('-ct', '--check-tarred', action='store_true', default=False, help='Skip files already tarred (tid > 0 in wfile_<dataset_name>)')
    parser.add_argument('-tb', '--tar-batch', action='store_true', default=False, help='Tar files for each batch. If not set, dump file list to .batch files instead.')
    parser.add_argument('-ds', '--dataset-ids', nargs='*', help='Dataset IDs to process')
    parser.add_argument('-ht', '--db-host', type=str, default='rda-db.ucar.edu', help='Database host for tarred check')
    parser.add_argument('-pt', '--db-port', type=int, default=5432, help='Database port for tarred check')
    parser.add_argument('-db', '--db-name', type=str, default='rdadb', help='Database name for tarred check')
    parser.add_argument('-us', '--db-user', type=str, default='dssdb', help='Database user for tarred check')
    parser.add_argument('-pw', '--db-password', type=str, default=None, help='Database password for tarred check')
    args = parser.parse_args()
    output_dir = args.output_dir if args.output_dir else os.getcwd()
    os.makedirs(output_dir, exist_ok=True)
    setup_logging(output_dir)
    db_params = None
    if args.check_tarred:
        db_params = {
            'host': args.db_host,
            'port': args.db_port,
            'dbname': args.db_name,
            'user': args.db_user,
            'password': args.db_password
        }
    # Batch tar mode
    if args.batch_input_file or args.batch_files:
        batch_files = []
        if args.batch_input_file:
            with open(args.batch_input_file, 'r') as blf:
                batch_files.extend([line.strip() for line in blf if line.strip()])
        if args.batch_files:
            batch_files.extend(args.batch_files)
        for batch_file in batch_files:
            tar_batch_file(batch_file, tar_root=args.tar_root)
        return
    # Directory tree processing mode
    if args.input_file:
        dataset_ids, dataset_paths = read_directories_from_file(args.input_file, tar_root=args.tar_root)
        files = collect_all_files(dataset_paths, db_params=db_params)
        if not files:
            logging.info("No files found in provided dataset IDs.")
            return
        batches = group_files_by_size(files, ONE_TB, THREE_TB)
        if len(batches) > 1 and get_batch_size(batches[-1]) < ONE_TB:
            logging.info(f"Last batch size ({get_batch_size(batches[-1])} bytes) < 1TB, appending to previous batch.")
            batches[-2].extend(batches[-1])
            batches.pop()
        tar_batches_across_dirs(files, batches, output_dir, args.tar_root, dataset_paths, tar_batch=args.tar_batch, dataset_ids=None)
        return
    elif args.dataset_ids:
        for dsid in args.dataset_ids:
            dataset_path = os.path.join(args.tar_root, dsid)
            if not os.path.isdir(dataset_path):
                logging.warning(f"Dataset directory does not exist: {dataset_path}")
                continue
            process_directory_tree(dataset_path, output_dir, db_params=db_params, tar_batch=args.tar_batch, dsid=dsid)
        return
    else:
        print("Error: Must provide either --input-file or dataset_ids or --batch-files or --batch-input-file.")

if __name__ == "__main__":
    main()
