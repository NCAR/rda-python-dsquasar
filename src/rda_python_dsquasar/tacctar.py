import os
import tarfile
import logging
from pathlib import Path
import psycopg2
import hashlib
from datetime import datetime
import getpass

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

def process_directory_tree(root_dir, output_dir, db_params=None, update_on_conflict=False):
    for dirpath, dirnames, filenames in os.walk(root_dir):
        abs_files = [os.path.join(dirpath, f) for f in filenames]
        if not abs_files:
            continue
        batches = group_files_by_size(abs_files, ONE_TB, THREE_TB)
        # Use tar_batches_across_dirs for consistency and DB recording
        common_root = root_dir
        tar_batches_across_dirs(abs_files, batches, output_dir, common_root, [root_dir], db_params=db_params, update_on_conflict=update_on_conflict)

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

def tar_batches_across_dirs(files, batches, output_dir, common_root, root_dirs, db_params=None, update_on_conflict=False):
    root_dir_paths = [Path(d).resolve() for d in root_dirs]
    uid = None
    if db_params:
        try:
            uid = get_uid_from_logname(db_params)
        except Exception as e:
            logging.error(f"Could not get uid from dssgrp: {e}")
            uid = 9
    for idx, batch in enumerate(batches, 1):
        batch_root_names = set()
        member_infos = []
        dsid = None
        for file_idx, f in enumerate(batch):
            f_path = Path(f).resolve()
            for root_path in root_dir_paths:
                try:
                    if str(f_path).startswith(str(root_path)):
                        batch_root_names.add(root_path.name)
                        break
                except Exception:
                    continue
            # Gather member file info for note/dsid/dsids
            try:
                stat = os.stat(f)
                arcname = os.path.relpath(f, common_root)
                parts = arcname.split('/')
                root_dir = parts[0] if parts else ''
                if file_idx == 0:
                    dsid = root_dir
                member_infos.append({
                    'name': arcname,
                    'size': stat.st_size,
                    'mtime': int(stat.st_mtime),
                    'type': '0',  # regular file
                    'mode': stat.st_mode,
                    'uid': stat.st_uid,
                    'gid': stat.st_gid,
                    'uname': '',
                    'gname': ''
                })
            except Exception as e:
                logging.warning(f"Failed to stat {f} for tar member info: {e}")
        # Determine tar file name: first leading root dir and number of distinct root dirs
        batch_root_names_sorted = sorted(batch_root_names)
        first_root = batch_root_names_sorted[0] if batch_root_names_sorted else 'batch'
        num_roots = len(batch_root_names_sorted)
        num_files = len(batch)
        tar_name = os.path.join(output_dir, f"{first_root}_dn{num_roots}_gn{idx}_fn{num_files}.tar")
        logging.info(f"Creating tar: {tar_name} with {num_files} files.")
        with tarfile.open(tar_name, "w") as tar:
            for f in batch:
                try:
                    arcname = os.path.relpath(f, common_root)
                    tar.add(f, arcname=arcname)
                except Exception as e:
                    logging.warning(f"Failed to add {f} to tar: {e}")
        # After tar is created, insert info into tfile if db_params is provided
        if db_params:
            try:
                # Build note and dsids from cached member_infos
                note = '\n'.join([
                    f"name={m['name']};size={m['size']};mtime={m['mtime']};type={m['type']};mode={m['mode']};uid={m['uid']};gid={m['gid']};uname={m['uname']};gname={m['gname']}"
                    for m in member_infos
                ])
                dsids = ','.join(batch_root_names_sorted)
                tar_stat = os.stat(tar_name)
                ctime = datetime.fromtimestamp(tar_stat.st_ctime)
                mtime = datetime.fromtimestamp(tar_stat.st_mtime)
                summary = {
                    'tfile': os.path.basename(tar_name),
                    'data_size': tar_stat.st_size,
                    'wcount': len(member_infos),
                    'date_created': ctime.date(),
                    'time_created': ctime.time(),
                    'date_modified': mtime.date(),
                    'time_modified': mtime.time(),
                    'note': note,
                    'dsids': dsids,
                    'dsid': dsid
                }
                checksum = compute_md5(tar_name)
                extra = {
                    'uid': uid if uid is not None else 9,
                    'dsid': summary['dsid'],
                    'data_format': '',
                    'disp_order': 0,
                    'dsids': summary['dsids'],
                    'note': summary['note']
                }
                insert_tfile_row(summary, checksum, db_params, extra, update_on_conflict=update_on_conflict)
                logging.info(f"Inserted tar summary for {tar_name} into tfile.")
            except Exception as e:
                logging.error(f"Failed to insert tar info for {tar_name}: {e}")

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

def compute_md5(file_path, chunk_size=8192):
    md5 = hashlib.md5()
    with open(file_path, 'rb') as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            md5.update(chunk)
    return md5.hexdigest()

def get_tar_summary_and_details(tar_path):
    with tarfile.open(tar_path, 'r') as tar:
        member_files = [m for m in tar.getmembers() if m.isfile()]
        file_count = len(member_files)
        details = []
        root_dirs = set()
        dsid = None
        for idx, m in enumerate(member_files):
            parts = m.name.split('/')
            if len(parts) > 1:
                root_dirs.add(parts[0])
                if idx == 0:
                    dsid = parts[0]
            elif len(parts) == 1:
                root_dirs.add(parts[0])
                if idx == 0:
                    dsid = parts[0]
            details.append(f"name={m.name};size={m.size};mtime={m.mtime};type={m.type};mode={m.mode};uid={m.uid};gid={m.gid};uname={m.uname};gname={m.gname}")
        note = '\n'.join(details)
        dsids = ','.join(sorted(root_dirs))
    tar_stat = os.stat(tar_path)
    tar_size = tar_stat.st_size
    ctime = datetime.fromtimestamp(tar_stat.st_ctime)
    mtime = datetime.fromtimestamp(tar_stat.st_mtime)
    return {
        'tfile': os.path.basename(tar_path),
        'data_size': tar_size,
        'wcount': file_count,
        'date_created': ctime.date(),
        'time_created': ctime.time(),
        'date_modified': mtime.date(),
        'time_modified': mtime.time(),
        'note': note,
        'dsids': dsids,
        'dsid': dsid
    }

def get_uid_from_logname(db_params):
    logname = None
    try:
        logname = os.getlogin()
    except Exception:
        logname = os.environ.get('USER') or getpass.getuser()
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    cur.execute("SELECT userno FROM dssdb.dssgrp WHERE logname=%s LIMIT 1", (str(logname),))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if row:
        return row[0]
    else:
        raise ValueError(f"User logname '{logname}' not found in dssdb.dssgrp table.")

def insert_tfile_row(summary, checksum, db_params, extra, update_on_conflict=False):
    table_name = 'dssdb.tfile'
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    columns = [
        'tfile', 'data_size', 'wcount', 'date_created', 'time_created',
        'date_modified', 'time_modified', 'file_format', 'checksum', 'status',
        'uid', 'dsid', 'data_format', 'disp_order', 'dsids', 'note'
    ]
    values = [
        summary['tfile'], summary['data_size'], summary['wcount'],
        summary['date_created'], summary['time_created'],
        summary['date_modified'], summary['time_modified'],
        'tar', checksum, 'T',
        extra.get('uid'), extra.get('dsid'), extra.get('data_format'),
        extra.get('disp_order'), extra.get('dsids'), extra.get('note')
    ]
    placeholders = ','.join(['%s'] * len(columns))
    sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    if update_on_conflict:
        update_cols = [col for col in columns if col != 'tfile']
        set_clause = ', '.join([f"{col}=EXCLUDED.{col}" for col in update_cols])
        sql += f" ON CONFLICT (tfile) DO UPDATE SET {set_clause}"
    try:
        cur.execute(sql, values)
        conn.commit()
    except Exception as e:
        logging.error(f"Database error: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Tar files from a list of directories into 1-3TB tar files and record tar info in the database.")
    parser.add_argument("--input-file", help="File containing list of directories to process (one per line)")
    parser.add_argument("--output-dir", help="Directory to store tar files (default: current directory)")
    parser.add_argument("root_dir", nargs='?', help="(Deprecated) Root directory to process")
    parser.add_argument('--db-host', default='rda-db.ucar.edu', help='Database host (default: rda-db.ucar.edu)')
    parser.add_argument('--db-port', default=5432, type=int, help='Database port (default: 5432)')
    parser.add_argument('--db-name', default='rdadb', help='Database name (default: rdadb)')
    parser.add_argument('--db-user', default='dssdb', help='Database user (default: dssdb)')
    parser.add_argument('--db-password', help='Database password (optional, use .pgpass if omitted)')
    parser.add_argument('--update', action='store_true', help='Update row if tfile already exists')
    args = parser.parse_args()
    output_dir = args.output_dir if args.output_dir else os.getcwd()
    os.makedirs(output_dir, exist_ok=True)
    setup_logging(output_dir)
    # Prepare db_params for tar_batches_across_dirs
    db_params = None
    if args.db_host and args.db_name and args.db_user:
        db_params = {
            'host': args.db_host,
            'port': args.db_port,
            'dbname': args.db_name,
            'user': args.db_user
        }
        if args.db_password:
            db_params['password'] = args.db_password
    if args.input_file:
        # Pass db_params and update flag to tar_batches_across_dirs
        directories = read_directories_from_file(args.input_file)
        files = collect_all_files(directories)
        if not files:
            logging.info("No files found in provided directories.")
            return
        batches = group_files_by_size(files, ONE_TB, THREE_TB)
        if len(batches) > 1 and get_batch_size(batches[-1]) < ONE_TB:
            logging.info(f"Last batch size ({get_batch_size(batches[-1])} bytes) < 1TB, appending to previous batch.")
            batches[-2].extend(batches[-1])
            batches.pop()
        common_root = find_common_root(directories)
        tar_batches_across_dirs(files, batches, output_dir, common_root, directories, db_params=db_params, update_on_conflict=args.update)
    elif args.root_dir:
        process_directory_tree(args.root_dir, output_dir, db_params=db_params, update_on_conflict=args.update)
    else:
        print("Error: Must provide either --input-file or root_dir.")

if __name__ == "__main__":
    main()