# -*- coding: utf-8 -*-

import os
import re
import shutil
import sqlite3
import hashlib
import urllib.parse
import urllib.request
from datetime import datetime

from .setup import logger

try:
    from .model import DB_PATH, ModelSetting
except Exception:
    from .model import DB_PATH
    ModelSetting = None

from .task_connection import TaskConnection
from .task_sync import TaskSync
from .task_group import TaskGroup
from .task_sheet import TaskSheet
from .task_m3u import TaskM3U

try:
    from .task_profile import TaskProfile
except Exception:
    class TaskProfile(object):
        pass


LOCAL_WRITE_DB_PATH = '/data/db/ff_tvh_sheet_write.db'
REMOTE_CACHE_DIR = '/data/tmp/ff_tvh_m3u_remote_cache'
WRITE_DB_PATH = LOCAL_WRITE_DB_PATH


def _safe_setting_get(key, default=''):
    try:
        if ModelSetting is None:
            return default
        value = ModelSetting.get(key)
        if value is None or value == '':
            return default
        return value
    except Exception:
        return default


class Task(TaskConnection, TaskSync, TaskGroup, TaskSheet, TaskProfile, TaskM3U):

    @staticmethod
    def _normalize_alias(text):
        value = str(text or '').strip().lower()
        if not value:
            return ''
        value = re.sub(r'[\s\-_./()\[\]{}]+', '', value)
        return value

    @staticmethod
    def _now():
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def _connect_write_db():
        con = sqlite3.connect(LOCAL_WRITE_DB_PATH)
        con.row_factory = sqlite3.Row
        return con

    @staticmethod
    def _connect_plugin_db():
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        return con

    @staticmethod
    def _connect_source_db(source_info=None):
        if source_info is None:
            source_info = Task.get_match_source_info()
        con = sqlite3.connect(source_info['db_path'])
        con.row_factory = sqlite3.Row
        return con

    @staticmethod
    def _ensure_remote_cache_dir():
        os.makedirs(REMOTE_CACHE_DIR, exist_ok=True)
        return REMOTE_CACHE_DIR

    @staticmethod
    def _build_remote_cache_path(remote_uri):
        Task._ensure_remote_cache_dir()
        key = hashlib.sha1(str(remote_uri or '').encode('utf-8')).hexdigest()
        return os.path.join(REMOTE_CACHE_DIR, f'{key}.db')

    @staticmethod
    def _validate_sqlite_file(path):
        path = str(path or '').strip()
        if path == '' or (not os.path.exists(path)):
            return False, 'not_found'
        con = None
        try:
            con = sqlite3.connect(path)
            cur = con.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 1")
            cur.fetchone()
            con.close()
            return True, 'ok'
        except Exception:
            try:
                if con is not None:
                    con.close()
            except Exception:
                pass
            return False, 'invalid_sqlite'

    @staticmethod
    def _copy_local_db_to_cache(source_path, remote_uri):
        source_path = str(source_path or '').strip()
        if source_path == '':
            return False, None, 'empty_remote_path'
        if not os.path.exists(source_path):
            return False, None, 'remote_path_not_found'

        ok, reason = Task._validate_sqlite_file(source_path)
        if not ok:
            return False, None, reason

        cache_path = Task._build_remote_cache_path(remote_uri)
        shutil.copy2(source_path, cache_path)

        ok2, reason2 = Task._validate_sqlite_file(cache_path)
        if not ok2:
            return False, None, reason2
        return True, cache_path, 'remote_copy_ok'

    @staticmethod
    def _download_http_db_to_cache(remote_uri):
        remote_uri = str(remote_uri or '').strip()
        if remote_uri == '':
            return False, None, 'empty_http_remote'

        cache_path = Task._build_remote_cache_path(remote_uri)
        tmp_path = cache_path + '.download'

        try:
            req = urllib.request.Request(remote_uri, headers={'User-Agent': 'ff_tvh_m3u/phase3'})
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = resp.read()
            with open(tmp_path, 'wb') as f:
                f.write(data)
            os.replace(tmp_path, cache_path)
        except Exception:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass
            return False, None, 'http_download_failed'

        ok, reason = Task._validate_sqlite_file(cache_path)
        if not ok:
            return False, None, reason
        return True, cache_path, 'http_remote_ok'

    @staticmethod
    def resolve_remote_db(remote_uri):
        remote_uri = str(remote_uri or '').strip()
        if remote_uri == '':
            return False, None, 'empty_remote'

        if remote_uri.startswith('file://'):
            parsed = urllib.parse.urlparse(remote_uri)
            source_path = urllib.request.url2pathname(parsed.path or '')
            return Task._copy_local_db_to_cache(source_path, remote_uri)

        if remote_uri.startswith('remote:'):
            source_path = remote_uri[len('remote:'):].strip()
            return Task._copy_local_db_to_cache(source_path, remote_uri)

        if remote_uri.startswith('/'):
            return Task._copy_local_db_to_cache(remote_uri, remote_uri)

        if remote_uri.startswith('http://') or remote_uri.startswith('https://'):
            return Task._download_http_db_to_cache(remote_uri)

        return False, None, 'unsupported_remote'

    @staticmethod
    def get_match_source_info():
        requested_mode = str(_safe_setting_get('basic_match_source_mode', 'auto')).strip().lower()
        remote_uri = str(_safe_setting_get('basic_match_source_remote', '')).strip()

        if requested_mode not in ['auto', 'local', 'remote']:
            requested_mode = 'auto'

        info = {
            'requested_mode': requested_mode,
            'effective_mode': 'local',
            'db_path': LOCAL_WRITE_DB_PATH,
            'label': '내부 기준 DB',
            'fallback_used': False,
            'reason': 'default_local',
            'remote_uri': remote_uri,
        }

        if requested_mode == 'local':
            info['reason'] = 'forced_local'
            return info

        ok, resolved_path, reason = Task.resolve_remote_db(remote_uri)

        if ok and resolved_path:
            info.update({
                'effective_mode': 'remote',
                'db_path': resolved_path,
                'label': '원격 기준 DB',
                'fallback_used': False,
                'reason': reason,
            })
            return info

        info.update({
            'effective_mode': 'local',
            'db_path': LOCAL_WRITE_DB_PATH,
            'fallback_used': True if requested_mode in ['auto', 'remote'] else False,
            'reason': reason,
        })

        if requested_mode == 'remote':
            info['label'] = '원격 기준 DB 실패 → 내부 기준 DB'
        else:
            info['label'] = '내부 기준 DB'

        return info

    @staticmethod
    def log_match_source(prefix='match_source'):
        info = Task.get_match_source_info()
        logger.info(
            '[ff_tvh_m3u] %s resolved requested=%s effective=%s fallback=%s reason=%s label=%s'
            % (
                prefix,
                info.get('requested_mode'),
                info.get('effective_mode'),
                info.get('fallback_used'),
                info.get('reason'),
                info.get('label'),
            )
        )
        return info

    @staticmethod
    def _find_channel_table_and_name_col():
        con = Task._connect_plugin_db()
        cur = con.cursor()
        tables = cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%channel%' ORDER BY name"
        ).fetchall()
        selected = (None, None)
        for row in tables:
            table_name = str(row['name'])
            cols = [r['name'] for r in cur.execute(f"PRAGMA table_info([{table_name}])").fetchall()]
            if 'channel_uuid' in cols:
                if 'name' in cols:
                    selected = (table_name, 'name')
                    break
                if 'channel_name' in cols:
                    selected = (table_name, 'channel_name')
                    break
        con.close()
        return selected

    @staticmethod
    def _get_channel_row(channel_uuid):
        channel_uuid = str(channel_uuid or '').strip()
        if channel_uuid == '':
            return None

        table_name, name_col = Task._find_channel_table_and_name_col()
        if not table_name or not name_col:
            return None

        con = Task._connect_plugin_db()
        cur = con.cursor()
        row = cur.execute(
            f"SELECT channel_uuid, [{name_col}] AS name FROM [{table_name}] WHERE channel_uuid = ? LIMIT 1",
            (channel_uuid,)
        ).fetchone()
        con.close()
        return row

    @staticmethod
    def search_master_channels(keyword, limit=30, source_info=None):
        keyword = str(keyword or '').strip()
        if keyword == '':
            return {
                'ret': 'warning',
                'msg': '검색어를 입력하세요.',
                'list': []
            }

        try:
            if source_info is None:
                source_info = Task.get_match_source_info()

            try:
                limit = int(limit)
            except Exception:
                limit = 30
            limit = max(1, min(limit, 100))

            con = Task._connect_source_db(source_info)
            cur = con.cursor()

            sql = """
            SELECT DISTINCT
                CAST(m.id AS TEXT) AS channel_id,
                m.standard_name,
                m.group_category AS group_name,
                m.receive_category
            FROM channel_master m
            LEFT JOIN channel_alias a
              ON CAST(a.channel_id AS TEXT) = CAST(m.id AS TEXT)
            WHERE m.standard_name LIKE ?
               OR a.alias_name LIKE ?
            ORDER BY
              CASE WHEN m.standard_name = ? THEN 0 ELSE 1 END,
              m.standard_name
            LIMIT ?
            """

            rows = cur.execute(sql, (f'%{keyword}%', f'%{keyword}%', keyword, limit)).fetchall()
            con.close()

            items = []
            for row in rows:
                items.append({
                    'channel_id': str(row['channel_id'] or ''),
                    'standard_name': str(row['standard_name'] or ''),
                    'group_name': str(row['group_name'] or ''),
                    'receive_category': str(row['receive_category'] or ''),
                })

            return {
                'ret': 'success',
                'msg': f'{len(items)}건 검색',
                'list': items,
                'source_label': source_info.get('label', ''),
                'effective_mode': source_info.get('effective_mode', 'local'),
            }

        except Exception as e:
            logger.exception(f'[tvh_m3u_plugin] search_master_channels exception: {str(e)}')
            return {
                'ret': 'danger',
                'msg': f'표준 채널 검색 실패: {str(e)}',
                'list': []
            }

    @staticmethod
    def add_db_match_channel(channel_uuid, channel_id, source_info=None):
        channel_uuid = str(channel_uuid or '').strip()
        channel_id = str(channel_id or '').strip()

        if channel_uuid == '':
            return {'ret': 'warning', 'msg': '채널 UUID가 비어 있습니다.'}
        if channel_id == '':
            return {'ret': 'warning', 'msg': '표준 채널 ID가 비어 있습니다.'}

        try:
            channel_row = Task._get_channel_row(channel_uuid)
            if channel_row is None:
                return {
                    'ret': 'warning',
                    'msg': f'플러그인 DB에서 채널을 찾지 못했습니다: {channel_uuid}'
                }

            alias_name = str(channel_row['name'] or '').strip()
            alias_norm = Task._normalize_alias(alias_name)

            if alias_name == '':
                return {'ret': 'warning', 'msg': '현재 채널명이 비어 있습니다.'}
            if alias_norm == '':
                return {'ret': 'warning', 'msg': '정규화된 채널명이 비어 있습니다.'}

            write_source_info = {
                'requested_mode': 'local',
                'effective_mode': 'local',
                'db_path': LOCAL_WRITE_DB_PATH,
                'label': '내부 기준 DB',
                'fallback_used': False,
                'reason': 'forced_local_write',
            }

            con = Task._connect_source_db(write_source_info)
            cur = con.cursor()

            master = cur.execute(
                "SELECT CAST(id AS TEXT) AS id, standard_name, group_category FROM channel_master WHERE CAST(id AS TEXT) = ? LIMIT 1",
                (channel_id,)
            ).fetchone()
            if master is None:
                con.close()
                return {
                    'ret': 'warning',
                    'msg': f'기준 DB에서 표준 채널을 찾지 못했습니다: {channel_id}'
                }

            exists = cur.execute(
                """
                SELECT 1
                FROM channel_alias
                WHERE CAST(channel_id AS TEXT) = ?
                  AND alias_norm = ?
                LIMIT 1
                """,
                (channel_id, alias_norm)
            ).fetchone()

            if exists is not None:
                con.close()
                return {
                    'ret': 'warning',
                    'msg': f'이미 등록된 매칭채널입니다: {alias_name}',
                    'channel_uuid': channel_uuid,
                    'channel_id': channel_id,
                    'alias_name': alias_name,
                    'standard_name': str(master['standard_name'] or ''),
                    'group_name': str(master['group_category'] or ''),
                    'source_label': write_source_info.get('label', ''),
                }

            cur.execute(
                """
                INSERT INTO channel_alias
                  (channel_id, alias_name, alias_norm, source, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (channel_id, alias_name, alias_norm, 'manual_add_from_m3u', Task._now())
            )
            con.commit()
            con.close()

            return {
                'ret': 'success',
                'msg': f'DB에 매칭채널 추가 완료: {alias_name}',
                'channel_uuid': channel_uuid,
                'channel_id': channel_id,
                'alias_name': alias_name,
                'standard_name': str(master['standard_name'] or ''),
                'group_name': str(master['group_category'] or ''),
                'source_label': write_source_info.get('label', ''),
            }

        except Exception as e:
            logger.exception(f'[tvh_m3u_plugin] add_db_match_channel exception: {str(e)}')
            return {
                'ret': 'danger',
                'msg': f'DB에 매칭채널 추가 실패: {str(e)}'
            }

    @staticmethod
    def reset_plugin_db():
        try:
            con = Task._connect_plugin_db()
            cur = con.cursor()
            cur.execute('PRAGMA foreign_keys=OFF')

            rows = cur.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type='table'
                  AND name LIKE 'ff_tvh_m3u_%'
                ORDER BY name
                """
            ).fetchall()

            table_names = [str(x['name']) for x in rows]

            for table in table_names:
                cur.execute(f'DELETE FROM [{table}]')

            try:
                cur.execute("DELETE FROM sqlite_sequence WHERE name LIKE 'ff_tvh_m3u_%'")
            except Exception:
                pass

            con.commit()
            con.close()

            logger.info(f"[ff_tvh_m3u] reset_plugin_db success tables={table_names}")
            return {
                'ret': 'success',
                'msg': '플러그인 디비를 초기화 했습니다.',
                'tables': table_names,
            }
        except Exception as e:
            logger.exception(f"[ff_tvh_m3u] reset_plugin_db exception: {str(e)}")
            return {
                'ret': 'danger',
                'msg': f'플러그인 디비 초기화 실패: {str(e)}'
            }

    @staticmethod
    def apply_db_rules(*args, **kwargs):
        source_info = kwargs.pop('source_info', None)
        if source_info is None:
            source_info = Task.log_match_source(prefix='apply_db_rules_source')

        prev = getattr(TaskSheet, '_ff_current_source_info', None)
        TaskSheet._ff_current_source_info = source_info

        try:
            result = TaskSheet.apply_db_rules(*args, **kwargs)
        finally:
            if prev is None:
                try:
                    delattr(TaskSheet, '_ff_current_source_info')
                except Exception:
                    pass
            else:
                TaskSheet._ff_current_source_info = prev

        try:
            if isinstance(result, dict):
                result['source_label'] = source_info.get('label', '')
                result['effective_mode'] = source_info.get('effective_mode', 'local')
                result['fallback_used'] = source_info.get('fallback_used', False)
        except Exception:
            pass

        return result


# === REMOTE MODE PATCH START ===
try:
    import sqlite3
    from .task_remote_backend import TaskRemoteBackend

    _orig_search_master_channels = getattr(Task, 'search_master_channels', None)
    _orig_add_db_match_channel = getattr(Task, 'add_db_match_channel', None)

    def _remote_get_channel_name(channel_uuid):
        try:
            con = sqlite3.connect(DB_PATH)
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            tables = cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%channel%' ORDER BY name").fetchall()
            selected = (None, None)
            for row in tables:
                table_name = str(row['name'])
                cols = [r['name'] for r in cur.execute(f"PRAGMA table_info([{table_name}])").fetchall()]
                if 'channel_uuid' in cols:
                    if 'name' in cols:
                        selected = (table_name, 'name')
                        break
                    if 'channel_name' in cols:
                        selected = (table_name, 'channel_name')
                        break
            if not selected[0]:
                con.close()
                return ''
            row = cur.execute(
                f"SELECT [{selected[1]}] AS name FROM [{selected[0]}] WHERE channel_uuid = ? LIMIT 1",
                (str(channel_uuid or '').strip(),)
            ).fetchone()
            con.close()
            return str((row['name'] if row else '') or '').strip()
        except Exception:
            return ''

    def _remote_search_master_channels(keyword, limit=30):
        if TaskRemoteBackend.is_remote_enabled():
            ret = TaskRemoteBackend.search_master_channels(keyword, limit=limit)
            if isinstance(ret, dict) and ret.get('ret') != 'danger':
                return ret
        if _orig_search_master_channels:
            return _orig_search_master_channels(keyword, limit=limit)
        return {'ret': 'warning', 'msg': 'search_master_channels unavailable', 'list': []}

    def _remote_add_db_match_channel(channel_uuid, channel_id):
        if TaskRemoteBackend.is_remote_enabled():
            alias_name = _remote_get_channel_name(channel_uuid)
            if alias_name:
                ret = TaskRemoteBackend.add_alias(channel_id, alias_name)
                if isinstance(ret, dict) and ret.get('ret') != 'danger':
                    return ret
        if _orig_add_db_match_channel:
            return _orig_add_db_match_channel(channel_uuid, channel_id)
        return {'ret': 'warning', 'msg': 'add_db_match_channel unavailable'}

    Task.search_master_channels = staticmethod(_remote_search_master_channels)
    Task.add_db_match_channel = staticmethod(_remote_add_db_match_channel)
except Exception:
    pass
# === REMOTE MODE PATCH END ===


# === REMOTE SOURCE INFO PATCH V2 START ===
try:
    from .task_remote_backend import TaskRemoteBackend

    def _remote_get_match_source_info_v2():
        info = {
            'ret': 'success',
            'requested_mode': 'auto',
            'effective_mode': 'local',
            'db_path': LOCAL_WRITE_DB_PATH if 'LOCAL_WRITE_DB_PATH' in globals() else '/data/db/ff_tvh_sheet_write.db',
            'label': '내부 기준 DB',
            'fallback_used': False,
            'reason': 'default_local',
            'remote_uri': TaskRemoteBackend.describe_remote() if hasattr(TaskRemoteBackend, 'describe_remote') else '',
        }

        if TaskRemoteBackend.is_remote_enabled():
            try:
                _rules = TaskRemoteBackend.fetch_match_rules()
                if isinstance(_rules, dict) and any(k in _rules for k in ['master_exact', 'alias_exact', 'master_norm', 'alias_norm']):
                    info.update({
                        'requested_mode': 'auto',
                        'effective_mode': 'remote',
                        'db_path': TaskRemoteBackend.describe_remote(),
                        'label': '원격 기준 규칙',
                        'fallback_used': False,
                        'reason': 'remote_rules',
                        'remote_uri': TaskRemoteBackend.describe_remote(),
                    })
                    return info
                info.update({
                    'requested_mode': 'auto',
                    'effective_mode': 'local',
                    'db_path': LOCAL_WRITE_DB_PATH if 'LOCAL_WRITE_DB_PATH' in globals() else '/data/db/ff_tvh_sheet_write.db',
                    'label': '원격 기준 규칙 실패 → 내부 기준 DB',
                    'fallback_used': True,
                    'reason': 'empty_remote',
                    'remote_uri': TaskRemoteBackend.describe_remote(),
                })
                return info
            except Exception:
                info.update({
                    'requested_mode': 'auto',
                    'effective_mode': 'local',
                    'db_path': LOCAL_WRITE_DB_PATH if 'LOCAL_WRITE_DB_PATH' in globals() else '/data/db/ff_tvh_sheet_write.db',
                    'label': '원격 기준 규칙 실패 → 내부 기준 DB',
                    'fallback_used': True,
                    'reason': 'remote_exception',
                    'remote_uri': TaskRemoteBackend.describe_remote(),
                })
                return info

        info.update({
            'requested_mode': 'auto',
            'effective_mode': 'local',
            'db_path': LOCAL_WRITE_DB_PATH if 'LOCAL_WRITE_DB_PATH' in globals() else '/data/db/ff_tvh_sheet_write.db',
            'label': '내부 기준 DB',
            'fallback_used': False,
            'reason': 'disabled_remote',
            'remote_uri': TaskRemoteBackend.describe_remote() if hasattr(TaskRemoteBackend, 'describe_remote') else '',
        })
        return info

    Task.get_match_source_info = staticmethod(_remote_get_match_source_info_v2)
except Exception:
    pass
# === REMOTE SOURCE INFO PATCH V2 END ===
