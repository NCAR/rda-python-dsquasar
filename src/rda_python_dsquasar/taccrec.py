#!/usr/bin/env python3
import os
import tarfile
import argparse
import psycopg2
import hashlib
from datetime import datetime

def get_tar_summary_and_details(member_list_path):
    details = []
    member_details = []
    root_dirs = set()
    dsid = None
    file_count = 0
    tar_size = 0
    if member_list_path and os.path.isfile(member_list_path):
        with open(member_list_path, 'r') as f:
            for idx, line in enumerate(f):
                line = line.strip()
                if not line or line.startswith('-rw') is False:
                    continue
                parts = line.split()
                if len(parts) < 6:
                    continue
                perms = parts[0]
                owner = parts[1]
                size = int(parts[2])
                date = parts[3]
                time = parts[4]
                name = ' '.join(parts[5:])
                # Ignore member directory names ending with '/'
                if name.endswith('/'):
                    continue
                tar_size += size
                mtime_str = f"{date} {time}"
                try:
                    mtime = int(datetime.strptime(mtime_str, "%Y-%m-%d %H:%M").timestamp())
                except Exception:
                    mtime = 0
                file_count += 1
                root = name.split('/')[0] if '/' in name else name
                root_dirs.add(root)
                if idx == 0:
                    dsid = root
                details.append(f"name={name};size={size};mtime={mtime};type=0;mode=0;uid=0;gid=0;uname=;gname=")
                member_details.append({'name': name})
    else:
        print('Error: member_list_path is required and must point to a valid file.')
        return None, None
    from datetime import datetime
    now = datetime.now()
    note = '\n'.join([m['name'] for m in member_details])
    dsids = ','.join(sorted(root_dirs))
    # Use member_list_path to infer tar file name
    tar_file_name = os.path.basename(member_list_path).replace('.mbr', '')
    ctime = now
    mtime = now
    return {
        'tfile': tar_file_name,
        'data_size': tar_size,
        'wcount': file_count,
        'date_created': ctime.date(),
        'time_created': ctime.time(),
        'date_modified': mtime.date(),
        'time_modified': mtime.time(),
        'note': note,
        'dsids': dsids,
        'dsid': dsid
    }, member_details

def insert_tfile_row(summary, db_params, extra, update_on_conflict=False, member_details=None):
    table_name = 'dssdb.tfile'
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    columns = [
        'tfile', 'data_size', 'wcount', 'date_created', 'time_created',
        'date_modified', 'time_modified', 'file_format', 'status',
        'uid', 'dsid', 'data_format', 'disp_order', 'dsids', 'note'
    ]
    values = [
        summary['tfile'], summary['data_size'], summary['wcount'],
        summary['date_created'], summary['time_created'],
        summary['date_modified'], summary['time_modified'],
        'tar', 'T',
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
        # Retrieve tid for the just-inserted tfile row
        cur.execute(f"SELECT tid FROM {table_name} WHERE tfile=%s", (summary['tfile'],))
        row = cur.fetchone()
        tid = row[0] if row else None
        # Update wfile tables if member_details provided
        if member_details and tid is not None:
            for m in member_details:
                name = m['name']
                if '/' in name:
                    cdsid, wfile = name.split('/', 1)
                else:
                    cdsid, wfile = name, ''
                wfile_table = f"dssdb.wfile_{cdsid}"
                # Check if table exists
                cur.execute("SELECT to_regclass(%s)", (wfile_table,))
                exists = cur.fetchone()[0]
                if not exists:
                    continue
                # Update tid if record exists
                cur.execute(f"UPDATE {wfile_table} SET tid=%s WHERE wfile=%s", (tid, wfile))
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
    parser.add_argument('--member-list', help='Path to tar member list file (from tar -tvf)')
    parser.add_argument('--db-host', default='rda-db.ucar.edu', help='Database host (default: rda-db.ucar.edu)')
    parser.add_argument('--db-port', default=5432, type=int, help='Database port (default: 5432)')
    parser.add_argument('--db-name', default='rdadb', help='Database name (default: rdadb)')
    parser.add_argument('--db-user', default='dssdb', help='Database user (default: dssdb)')
    parser.add_argument('--db-password', help='Database password (optional, use .pgpass if omitted)')
    parser.add_argument('--no-update', action='store_true', help='If tfile exists, skip all updates including wfile tables (default: False)')
    args = parser.parse_args()
    if not args.member_list or not os.path.isfile(args.member_list):
        print('Error: --member-list argument is required and must point to a valid file.')
        return
    summary, member_details = get_tar_summary_and_details(args.member_list)
    if summary is None:
        return
    db_params = {
        'host': args.db_host,
        'port': args.db_port,
        'dbname': args.db_name,
        'user': args.db_user
    }
    if args.db_password:
        db_params['password'] = args.db_password
    # Check if tfile exists if --no-update is set
    if args.no_update:
        try:
            conn = psycopg2.connect(**db_params)
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM dssdb.tfile WHERE tfile=%s LIMIT 1", (summary['tfile'],))
            exists = cur.fetchone()
            cur.close()
            conn.close()
            if exists:
                print(f"tfile '{summary['tfile']}' already exists in dssdb.tfile. Skipping all updates.")
                return
        except Exception as e:
            print(f"Database error during tfile existence check: {e}")
            return
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
    insert_tfile_row(summary, db_params, extra, update_on_conflict=True, member_details=member_details if not args.no_update else None)
    print(f"Inserted tar summary for {summary['tfile']} into tfile.")

if __name__ == '__main__':
    main()