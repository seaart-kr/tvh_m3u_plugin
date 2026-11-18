# -*- coding: utf-8 -*-
import os
import re
import sqlite3
import hashlib
import time
from datetime import datetime

import requests

from .custom_logo_image import LogoImageError, MAX_INPUT_BYTES, atomic_write, normalize_logo_image
from .public_access import check_upload_request
from .setup import P, logger
from .task_m3u import TaskM3U

WRITE_DB_PATH = '/data/db/ff_tvh_sheet_write.db'
DEFAULT_MIRROR_URL = 'https://ff.aha3011.mywire.org/tvh_m3u_plugin/normal/custom_logo_mirror'
DEFAULT_MIRROR_QUEUE_DIR = '/data/ff_tvh_m3u_logo_queue'
DEFAULT_MIRROR_QUEUE_MAX_BYTES = 512 * 1024 * 1024
DEFAULT_MIRROR_QUEUE_MAX_FILES = 5000
DEFAULT_MIRROR_DAILY_LIMIT = 500


def _now():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def _connect_write_db():
    con = sqlite3.connect(WRITE_DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def _normalize_name(value):
    text = str(value or '').strip().lower()
    if not text:
        return ''
    text = text.replace('&', ' and ')
    text = text.replace('+', ' plus ')
    return re.sub(r'[\s\-_./()\[\]{}]+', '', text)


def _first_present(columns, candidates):
    lowered = {str(col).lower(): col for col in columns}
    for candidate in candidates:
        actual = lowered.get(str(candidate).lower())
        if actual:
            return actual
    return None


def _table_columns(con, table_name):
    try:
        return [str(row['name']) for row in con.execute(f'PRAGMA table_info([{table_name}])').fetchall()]
    except Exception:
        return []


def _resolve_matched_channel_id(channel_name=''):
    channel_name = str(channel_name or '').strip()
    if not channel_name or not os.path.exists(WRITE_DB_PATH):
        return ''
    target_lower = channel_name.lower()
    target_norm = _normalize_name(channel_name)
    con = None
    try:
        con = _connect_write_db()
        targets = [
            ('channel_master', ['standard_name', 'channel_name', 'name'], ['id', 'channel_id', 'master_id']),
            ('channel_alias', ['alias_name', 'aka_name', 'aka', 'alias', 'name'], ['channel_id', 'matched_channel_id', 'master_id']),
        ]
        for table_name, name_candidates, id_candidates in targets:
            cols = _table_columns(con, table_name)
            if not cols:
                continue
            name_col = _first_present(cols, name_candidates)
            id_col = _first_present(cols, id_candidates)
            norm_col = _first_present(cols, ['alias_norm', 'standard_name_norm', 'channel_name_norm', 'name_norm', 'aka_norm'])
            if not name_col or not id_col:
                continue
            select_cols = [
                f'CAST({id_col} AS TEXT) AS matched_channel_id',
                f"COALESCE({name_col}, '') AS channel_name",
                f'{norm_col} AS channel_norm' if norm_col else "'' AS channel_norm",
            ]
            for row in con.execute(f"SELECT {', '.join(select_cols)} FROM {table_name}").fetchall():
                name = str(row['channel_name'] or '').strip()
                norm = str(row['channel_norm'] or '').strip()
                if name.lower() == target_lower or (target_norm and norm == target_norm):
                    return str(row['matched_channel_id'] or '').strip()
    except Exception as e:
        logger.warning(f'[ff_tvh_m3u] custom logo match lookup failed: {str(e)}')
    finally:
        try:
            if con is not None:
                con.close()
        except Exception:
            pass
    return ''


def _mirror_url():
    return (
        str(P.ModelSetting.get('basic_custom_logo_mirror_url') or '').strip()
        or DEFAULT_MIRROR_URL
    )


def _asset_dir():
    path = TaskM3U.get_custom_logo_asset_dir()
    os.makedirs(path, exist_ok=True)
    return path


def _mirror_queue_dir():
    path = str(os.environ.get('TVH_M3U_LOGO_QUEUE_DIR') or DEFAULT_MIRROR_QUEUE_DIR).strip()
    os.makedirs(path, exist_ok=True)
    return path


def _env_positive_int(name, default):
    try:
        value = int(str(os.environ.get(name) or default).strip())
        return value if value > 0 else default
    except Exception:
        return default


def _ensure_queue_capacity(output_dir, output_path, incoming_size):
    if os.path.exists(output_path):
        return
    max_bytes = _env_positive_int('TVH_M3U_LOGO_QUEUE_MAX_BYTES', DEFAULT_MIRROR_QUEUE_MAX_BYTES)
    max_files = _env_positive_int('TVH_M3U_LOGO_QUEUE_MAX_FILES', DEFAULT_MIRROR_QUEUE_MAX_FILES)
    file_count = 0
    total_bytes = 0
    for entry in os.scandir(output_dir):
        try:
            if entry.is_file(follow_symlinks=False) and entry.name.endswith('.png'):
                file_count += 1
                total_bytes += entry.stat(follow_symlinks=False).st_size
        except OSError:
            continue
    if file_count >= max_files or total_bytes + incoming_size > max_bytes:
        raise Exception('원격 로고 대기열이 가득 찼습니다. 다음 자동 게시 후 다시 시도하세요.')


def _check_mirror_rate_limit():
    daily_limit = _env_positive_int('TVH_M3U_LOGO_MIRROR_DAILY_LIMIT', DEFAULT_MIRROR_DAILY_LIMIT)
    day = datetime.now().strftime('%Y-%m-%d')
    bucket = 'public-upload'
    con = _connect_write_db()
    try:
        con.execute("BEGIN IMMEDIATE")
        con.execute("""
        CREATE TABLE IF NOT EXISTS custom_logo_upload_rate (
            upload_day TEXT NOT NULL,
            token_hash TEXT NOT NULL,
            upload_count INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (upload_day, token_hash)
        )
        """)
        con.execute('DELETE FROM custom_logo_upload_rate WHERE upload_day <> ?', (day,))
        global_count = int(con.execute(
            'SELECT COALESCE(SUM(upload_count), 0) FROM custom_logo_upload_rate WHERE upload_day = ?',
            (day,),
        ).fetchone()[0])
        row = con.execute(
            'SELECT upload_count FROM custom_logo_upload_rate WHERE upload_day = ? AND token_hash = ?',
            (day, bucket),
        ).fetchone()
        if global_count >= daily_limit:
            raise Exception('원격 로고의 일일 업로드 한도를 초과했습니다.')
        con.execute("""
        INSERT INTO custom_logo_upload_rate (upload_day, token_hash, upload_count)
        VALUES (?, ?, 1)
        ON CONFLICT(upload_day, token_hash)
        DO UPDATE SET upload_count = upload_count + 1
        """, (day, bucket))
        con.commit()
    finally:
        con.close()


def _check_mirror_attempt_limit(remote_addr):
    identity = hashlib.sha256(str(remote_addr or 'unknown').encode()).hexdigest()
    bucket = int(time.time() // 600)
    con = _connect_write_db()
    try:
        con.execute('BEGIN IMMEDIATE')
        con.execute("""CREATE TABLE IF NOT EXISTS custom_logo_attempt_rate (
            bucket INTEGER NOT NULL, client_hash TEXT NOT NULL, attempts INTEGER NOT NULL,
            PRIMARY KEY(bucket, client_hash))""")
        con.execute('DELETE FROM custom_logo_attempt_rate WHERE bucket < ?', (bucket - 1,))
        row = con.execute('SELECT attempts FROM custom_logo_attempt_rate WHERE bucket=? AND client_hash=?',
                          (bucket, identity)).fetchone()
        if row and row[0] >= _env_positive_int('TVH_M3U_LOGO_ATTEMPTS_PER_10_MIN', 30):
            raise ValueError('로고 업로드 요청이 너무 많습니다. 잠시 후 다시 시도하세요.')
        con.execute("""INSERT INTO custom_logo_attempt_rate VALUES (?, ?, 1)
            ON CONFLICT(bucket, client_hash) DO UPDATE SET attempts=attempts+1""", (bucket, identity))
        con.commit()
    finally:
        con.close()


def _save_file(file_storage, output_dir=None, enforce_queue_limits=False, reserve_quota=False):
    if file_storage is None:
        raise Exception('업로드할 로고 파일이 없습니다.')
    try:
        normalized = normalize_logo_image(file_storage.read(MAX_INPUT_BYTES + 1))
    except LogoImageError as e:
        raise Exception(str(e))

    stored_filename = normalized['filename']
    target_dir = output_dir or _asset_dir()
    output_path = os.path.join(target_dir, stored_filename)
    if enforce_queue_limits:
        _ensure_queue_capacity(target_dir, output_path, normalized['file_size'])
    if reserve_quota:
        _check_mirror_rate_limit()
    atomic_write(output_path, normalized['data'])
    return {
        'stored_filename': stored_filename,
        'output_path': output_path,
        'sha1': normalized['sha1'],
        'sha256': normalized['sha256'],
        'file_size': normalized['file_size'],
    }


def _ensure_custom_logo_table(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS custom_logo (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_channel_name TEXT DEFAULT '',
        standard_name TEXT DEFAULT '',
        aka_name TEXT DEFAULT '',
        matched_channel_id TEXT DEFAULT '',
        stored_filename TEXT DEFAULT '',
        logo_url_template TEXT DEFAULT '',
        final_url TEXT DEFAULT '',
        sha1 TEXT DEFAULT '',
        file_size INTEGER DEFAULT 0,
        created_at TEXT DEFAULT '',
        updated_at TEXT DEFAULT ''
    )
    """)


def _save_db(source_channel_name, standard_name='', aka_name='', stored_filename='', sha1='', file_size=0, replace_existing=True):
    source_channel_name = str(source_channel_name or '').strip()
    standard_name = str(standard_name or '').strip() or source_channel_name
    aka_name = str(aka_name or '').strip()
    stored_filename = os.path.basename(str(stored_filename or '').strip())
    matched_channel_id = _resolve_matched_channel_id(standard_name) or _resolve_matched_channel_id(source_channel_name)
    logo_url_template = TaskM3U._build_logo_public_template(stored_filename)
    con = _connect_write_db()
    con.execute('BEGIN IMMEDIATE')
    cur = con.cursor()
    _ensure_custom_logo_table(cur)
    cols = [row['name'] for row in cur.execute('PRAGMA table_info(custom_logo)').fetchall()]
    now = _now()
    if not replace_existing:
        clauses, arguments = [], []
        if matched_channel_id:
            for column in ['matched_channel_id', 'channel_master_id', 'channel_id', 'master_id', 'cm_id']:
                if column in cols:
                    clauses.append('CAST(' + column + ' AS TEXT) = ?')
                    arguments.append(matched_channel_id)
        for column in ['source_channel_name', 'standard_name', 'provider_channel_name', 'channel_name']:
            if column in cols:
                clauses.append('(' + column + ' = ? COLLATE NOCASE OR ' + column + ' = ? COLLATE NOCASE)')
                arguments.extend([source_channel_name, standard_name])
        if clauses and cur.execute('SELECT 1 FROM custom_logo WHERE ' + ' OR '.join(clauses) + ' LIMIT 1', arguments).fetchone():
            con.close()
            return {'matched_channel_id': matched_channel_id, 'standard_name': standard_name,
                    'logo_url_template': '', 'existing_preserved': True}
    if 'source_channel_name' in cols and 'matched_channel_id' in cols:
        cur.execute("DELETE FROM custom_logo WHERE source_channel_name = ? AND COALESCE(matched_channel_id, '') = ?", (source_channel_name, matched_channel_id))
    values = {
        'provider': 'custom',
        'source_channel_name': source_channel_name,
        'provider_channel_name': source_channel_name,
        'standard_name': standard_name,
        'aka_name': aka_name,
        'matched_channel_id': matched_channel_id,
        'stored_filename': stored_filename,
        'random_filename': stored_filename,
        'logo_url_template': logo_url_template,
        'final_url': logo_url_template,
        'custom_logo_url': logo_url_template,
        'sha1': sha1,
        'file_size': int(file_size or 0),
        'created_at': now,
        'updated_at': now,
        'created_time': now,
        'updated_time': now,
    }
    insert_cols = [col for col in cols if col in values]
    if insert_cols:
        placeholders = ','.join(['?'] * len(insert_cols))
        cur.execute(f"INSERT INTO custom_logo ({','.join(insert_cols)}) VALUES ({placeholders})", [values[col] for col in insert_cols])
    con.commit()
    con.close()
    try:
        TaskM3U._load_logo_cache(force=True)
    except Exception:
        pass
    return {'matched_channel_id': matched_channel_id, 'standard_name': standard_name, 'logo_url_template': logo_url_template}


def _mirror_to_owner(source_channel_name, standard_name, aka_name, file_path, stored_filename, sha1, file_size):
    token = str(os.environ.get('TVH_M3U_LOGO_SUBMIT_KEY') or P.ModelSetting.get('basic_logo_submit_key') or '').strip()
    if not token:
        return {'ret': 'warning', 'msg': '로컬 저장 완료 · 공용 로고 전송 키가 없어 원격 전송은 생략했습니다.'}
    try:
        with open(file_path, 'rb') as f:
            files = {'logo_file': (stored_filename, f)}
            data = {
                'source_channel_name': source_channel_name,
                'standard_name': standard_name,
                'aka_name': aka_name,
                'stored_filename': stored_filename,
                'sha1': sha1,
                'file_size': str(file_size),
            }
            resp = requests.post(_mirror_url(), data=data, files=files, timeout=30,
                headers={'X-Logo-Submit-Key': token},
                allow_redirects=False)
            resp.raise_for_status()
            if resp.status_code != 200:
                return {'ret': 'warning', 'msg': '원격 로고 수신 주소를 확인하세요.'}
            try:
                return resp.json()
            except Exception:
                return {'ret': 'success', 'msg': '원격 백업 완료'}
    except Exception as e:
        logger.warning(f'[ff_tvh_m3u] custom logo mirror failed: {str(e)}')
        return {'ret': 'warning', 'msg': f'원격 백업 실패: {str(e)}'}


def handle_custom_logo_upload(req):
    check_upload_request(req)
    source_channel_name = str(req.form.get('source_channel_name') or '').strip()
    standard_name = str(req.form.get('standard_name') or '').strip()
    aka_name = str(req.form.get('aka_name') or '').strip()
    logo_file = req.files.get('logo_file')
    if not source_channel_name:
        return {'ret': 'warning', 'msg': '원본 채널명을 입력하세요.'}

    saved = _save_file(logo_file)
    db_info = _save_db(source_channel_name, standard_name, aka_name, saved['stored_filename'], saved['sha1'], saved['file_size'])
    mirror = _mirror_to_owner(
        source_channel_name,
        db_info.get('standard_name') or standard_name or source_channel_name,
        aka_name,
        saved['output_path'],
        saved['stored_filename'],
        saved['sha1'],
        saved['file_size'],
    )
    msg = '커스텀 로고를 업로드했습니다.'
    if mirror.get('ret') in ['warning', 'danger']:
        msg += ' 단, 원격 백업은 실패했습니다.'
    elif mirror.get('ret') == 'success':
        msg += ' 원격 백업도 완료했습니다.'
    return {
        'ret': 'success',
        'msg': msg,
        'stored_filename': saved['stored_filename'],
        'standard_name': db_info.get('standard_name') or '',
        'matched_channel_id': db_info.get('matched_channel_id') or '',
        'logo_url_template': db_info.get('logo_url_template') or '',
        'mirror': mirror,
    }


def handle_custom_logo_mirror(req):
    check_upload_request(req)
    _check_mirror_attempt_limit(req.remote_addr)
    source_channel_name = str(req.form.get('source_channel_name') or '').strip()
    standard_name = str(req.form.get('standard_name') or '').strip()
    aka_name = str(req.form.get('aka_name') or '').strip()
    logo_file = req.files.get('logo_file')
    if not source_channel_name:
        return {'ret': 'warning', 'msg': '원본 채널명이 비어 있습니다.'}
    if logo_file is None:
        return {'ret': 'warning', 'msg': '업로드할 로고 파일이 없습니다.'}
    saved = _save_file(
        logo_file,
        output_dir=_mirror_queue_dir(),
        enforce_queue_limits=True,
        reserve_quota=True,
    )
    db_info = _save_db(source_channel_name, standard_name, aka_name, saved['stored_filename'], saved['sha1'], saved['file_size'], replace_existing=False)
    return {
        'ret': 'success',
        'msg': '원격 백업 저장 완료',
        'stored_filename': saved['stored_filename'],
        'standard_name': db_info.get('standard_name') or '',
        'matched_channel_id': db_info.get('matched_channel_id') or '',
    }
