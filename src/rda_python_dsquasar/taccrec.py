import os
import tarfile
import argparse
import psycopg2
import hashlib
from datetime import datetime

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
        # Collect details for note
        details = []
        root_dirs = set()
        dsid = None
        for idx, m in enumerate(member_files):
            # Extract root directory (first part of the path)
            parts = m.name.split('/')
            if len(parts) > 1:
                root_dirs.add(parts[0])
                if idx == 0:
                    dsid = parts[0]
            elif len(parts) == 1:
                root_dirs.add(parts[0])
                if idx == 0:
                    dsid = parts[0]
            # Collect file details
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
        print(f"Database error: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def get_uid_from_logname(db_params):
    import getpass
    logname = None
    try:
        logname = os.getlogin()
    except Exception:
        logname = os.environ.get('USER') or getpass.getuser()
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    # Ensure logname is quoted as a string in the query
    cur.execute("SELECT userno FROM dssdb.dssgrp WHERE logname=%s LIMIT 1", (str(logname),))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if row:
        return row[0]
    else:
        raise ValueError(f"User logname '{logname}' not found in dssdb.dssgrp table.")

def main():
    parser = argparse.ArgumentParser(description='Insert tar file summary into tfile table.')
    parser.add_argument('tarfile', help='Path to the tar file')
    parser.add_argument('--db-host', default='rda-db.ucar.edu', help='Database host (default: rda-db.ucar.edu)')
    parser.add_argument('--db-port', default=5432, type=int, help='Database port (default: 5432)')
    parser.add_argument('--db-name', default='rdadb', help='Database name (default: rdadb)')
    parser.add_argument('--db-user', default='dssdb', help='Database user (default: dssdb)')
    parser.add_argument('--db-password', help='Database password (optional, use .pgpass if omitted)')
    parser.add_argument('--update', action='store_true', help='Update row if tfile already exists')
    args = parser.parse_args()
    tar_path = args.tarfile
    if not os.path.isfile(tar_path):
        print(f"Tar file not found: {tar_path}")
        return
    summary = get_tar_summary_and_details(tar_path)
    checksum = compute_md5(tar_path)
    db_params = {
        'host': args.db_host,
        'port': args.db_port,
        'dbname': args.db_name,
        'user': args.db_user
    }
    if args.db_password:
        db_params['password'] = args.db_password
    try:
        uid = get_uid_from_logname(db_params)
    except Exception as e:
        print(f"Error getting uid from dssgrp: {e}")
        return
    extra = {
        'uid': uid,
        'dsid': summary['dsid'],
        'data_format': '',
        'disp_order': 0,
        'dsids': summary['dsids'],
        'note': summary['note']
    }
    insert_tfile_row(summary, checksum, db_params, extra, update_on_conflict=args.update)
    print(f"Inserted tar summary for {tar_path} into tfile.")

if __name__ == '__main__':
    main()