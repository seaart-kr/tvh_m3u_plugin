# -*- coding: utf-8 -*-
import os
import re
import sqlite3
import xml.etree.ElementTree as ET
from datetime import datetime
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

from .setup import P, logger
from .model import ModelChannel, ModelGroupProfile, ModelChannelProfile, ModelLogoOverride
from .task_base import TaskBase


class TaskM3U(TaskBase):
    WRITE_DB_PATH = '/data/db/ff_tvh_sheet_write.db'
    EPG_CACHE_XML_PATH = '/data/tmp/ff_tvh_m3u_epg_cache/myepg_raw.xml'
    FF_URL_PLACEHOLDER = '__FF_BASE_URL__'
    STATIC_HOST_PATH = '/customlogo'
    ASSETS_PATH_PREFIX = '/tvh_m3u_plugin/docs/assets'
    DEFAULT_LOGO_PRIORITY = ['custom', 'kt', 'wavve', 'tving', 'sk']
    UPLOAD_ASSET_DIR = os.path.join(os.path.dirname(__file__), 'docs', 'assets')
    _logo_cache = {
        'db_mtime': None,
        'custom_name_map': {},
        'custom_id_map': {},
        'provider_name_map': {},
        'provider_id_map': {},
    }
    _write_match_cache = {
        'db_mtime': None,
        'exact': {},
        'norm': {},
    }
    _sheet_rule_match_cache = {
        'db_mtime': None,
        'rules': None,
    }
    _override_cache = {
        'uuid_map': {},
        'name_map': {},
    }

    @staticmethod
    def fetch_playlist_map():
        """
        TVH 원본 /playlist/channels 에서
        tvg-id(UUID) -> 실제 재생 URL 맵을 만든다.
        """
        try:
            api_base = TaskM3U.get_api_base()
            if not api_base:
                return {}

            session = TaskM3U.get_session()
            playlist_url = f'{api_base}/playlist/channels'
            logger.debug(f'[ff_tvh_m3u] playlist_url={playlist_url}')

            resp = session.get(playlist_url, timeout=30)
            resp.raise_for_status()
            text = resp.text or ''

            mapping = {}
            current_uuid = None

            for raw_line in text.splitlines():
                line = raw_line.strip()
                if not line:
                    continue

                if line.startswith('#EXTINF'):
                    match = re.search(r'tvg-id="([^"]+)"', line)
                    current_uuid = match.group(1).strip() if match else None
                    continue

                if line.startswith('http://') or line.startswith('https://'):
                    if current_uuid:
                        mapping[current_uuid] = line
                        current_uuid = None

            logger.debug(f'[ff_tvh_m3u] playlist map count={len(mapping)}')
            return mapping

        except Exception as e:
            logger.exception(f'[ff_tvh_m3u] fetch_playlist_map exception: {str(e)}')
            return {}

    @staticmethod
    def normalize_stream_url(source_url, profile=''):
        """
        TVH 원본 playlist URL의 path/query는 유지하고,
        호스트/포트/스킴은 플러그인 설정의 TVH 스트림 주소를 기준으로 강제한다.
        이후 재생 계정/프로필 옵션을 반영한다.
        """
        if not source_url:
            return ''

        stream_base = TaskM3U.get_stream_base()
        if not stream_base:
            return ''

        source_parts = urlsplit(source_url)
        base_parts = urlsplit(stream_base)

        url = urlunsplit((
            base_parts.scheme,
            base_parts.netloc,
            source_parts.path,
            source_parts.query,
            source_parts.fragment
        ))

        if TaskM3U.get_include_auth_in_url():
            username = TaskM3U.get_play_username()
            password = TaskM3U.get_play_password()
            url = TaskM3U.remove_auth_from_url(url)
            url = TaskM3U.inject_auth_to_url(url, username, password)
        else:
            url = TaskM3U.remove_auth_from_url(url)

        if profile:
            url = TaskM3U.set_query_param(url, 'profile', profile)
        else:
            url = TaskM3U.set_query_param(url, 'profile', '')

        return url

    @staticmethod
    def get_effective_profile(channel_uuid='', group_name=''):
        channel_uuid = str(channel_uuid or '').strip()
        group_name = str(group_name or '').strip()

        channel_profile = ModelChannelProfile.get_profile(channel_uuid)
        if channel_profile:
            return channel_profile

        group_profile = ModelGroupProfile.get_profile(group_name)
        if group_profile:
            return group_profile

        return TaskM3U.get_stream_profile()

    @staticmethod
    def _sanitize_attr(value):
        value = '' if value is None else str(value)
        return value.replace('&', '&amp;').replace('"', '&quot;').replace('\n', ' ').replace('\r', ' ').strip()

    @staticmethod
    def _sanitize_name(value):
        value = '' if value is None else str(value)
        return value.replace('\n', ' ').replace('\r', ' ').strip()

    @staticmethod
    def _normalize_logo_name(value):
        text = str(value or '').strip().lower()
        if not text:
            return ''
        return re.sub(r'[^a-z0-9가-힣]+', '', text)

    @staticmethod
    def _normalize_xml_tag(tag):
        return str(tag or '').split('}', 1)[-1]

    @staticmethod
    def _safe_model_setting_get(key, default=''):
        try:
            value = P.ModelSetting.get(key)
            if value is None or value == '':
                return default
            return value
        except Exception:
            return default

    @staticmethod
    def _normalize_static_host_logo_url(url_value):
        text = str(url_value or '').strip()
        if not text:
            return ''
        text = TaskM3U._normalize_asset_layout_logo_url(text)
        assets_prefix = TaskM3U.ASSETS_PATH_PREFIX.rstrip('/')
        static_prefix = TaskM3U.STATIC_HOST_PATH.rstrip('/')
        text = text.replace(assets_prefix + '/', static_prefix + '/')
        if text.endswith(assets_prefix):
            text = text[:-len(assets_prefix)] + static_prefix
        return text

    @staticmethod
    def _normalize_asset_layout_logo_url(url_value):
        text = str(url_value or '').strip()
        if not text:
            return ''

        assets_prefix = TaskM3U.ASSETS_PATH_PREFIX.rstrip('/') + '/'
        if assets_prefix not in text:
            return text

        rel = text.split(assets_prefix, 1)[-1].lstrip('/')
        if not rel or '/' not in rel:
            return text

        nested_path = os.path.join(TaskM3U.UPLOAD_ASSET_DIR, *rel.split('/'))
        flat_name = os.path.basename(rel)
        flat_path = os.path.join(TaskM3U.UPLOAD_ASSET_DIR, flat_name)

        if os.path.exists(nested_path):
            return text
        if not os.path.exists(flat_path):
            return text

        return text.replace(assets_prefix + rel, assets_prefix + flat_name)

    @staticmethod
    def _replace_placeholder_url(url_value, base_url=''):
        text = TaskM3U._normalize_static_host_logo_url(url_value)
        if not text:
            return ''
        if TaskM3U.FF_URL_PLACEHOLDER in text:
            base = str(base_url or '').rstrip('/')
            if base:
                return text.replace(TaskM3U.FF_URL_PLACEHOLDER, base)
            return text.replace(TaskM3U.FF_URL_PLACEHOLDER, '')
        return text

    @staticmethod
    def _get_request_base_url():
        try:
            from flask import request
            return request.host_url.rstrip('/')
        except Exception:
            return ''

    @staticmethod
    def _get_logo_priority():
        raw = str(TaskM3U._safe_model_setting_get('basic_logo_priority', '') or '').strip().lower()
        items = []
        seen = set()
        for piece in raw.split(','):
            key = re.sub(r'[^a-z0-9]+', '', piece.strip().lower())
            if not key or key in seen:
                continue
            if key not in TaskM3U.DEFAULT_LOGO_PRIORITY:
                continue
            seen.add(key)
            items.append(key)
        if not items:
            items = list(TaskM3U.DEFAULT_LOGO_PRIORITY)
        for key in TaskM3U.DEFAULT_LOGO_PRIORITY:
            if key not in items:
                items.append(key)
        return items

    @staticmethod
    def get_logo_priority_text():
        return ','.join(TaskM3U._get_logo_priority())

    @staticmethod
    def _get_write_db_mtime():
        try:
            return os.path.getmtime(TaskM3U.WRITE_DB_PATH)
        except Exception:
            return None

    @staticmethod
    def _table_exists(con, table_name):
        try:
            row = con.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (str(table_name or '').strip(),)
            ).fetchone()
            return row is not None
        except Exception:
            return False

    @staticmethod
    def _get_table_columns(con, table_name):
        try:
            return [str(row['name']) for row in con.execute(f"PRAGMA table_info([{table_name}])").fetchall()]
        except Exception:
            return []

    @staticmethod
    def _first_present(columns, candidates):
        lowered = {str(col).lower(): col for col in columns}
        for candidate in candidates:
            actual = lowered.get(str(candidate).lower())
            if actual:
                return actual
        return None

    @staticmethod
    def _canonical_logo_provider(provider):
        value = str(provider or '').strip().lower()
        if value == '':
            return ''
        if value in ['custom', 'manual', 'user']:
            return 'custom'
        if 'custom' in value or 'manual' in value or 'user' in value:
            return 'custom'
        return value

    @staticmethod
    def _normalize_match_name(value):
        text = str(value or '').strip().lower()
        if not text:
            return ''
        text = text.replace('&', ' and ')
        text = text.replace('+', ' plus ')
        return re.sub(r'[\s\-_./()\[\]{}]+', '', text)

    @staticmethod
    def _connect_write_db():
        con = sqlite3.connect(TaskM3U.WRITE_DB_PATH)
        con.row_factory = sqlite3.Row
        return con

    @staticmethod
    def _load_write_match_cache(force=False):
        db_mtime = TaskM3U._get_write_db_mtime()
        cache = TaskM3U._write_match_cache
        if (not force) and cache.get('db_mtime') == db_mtime and (cache.get('exact') or cache.get('norm')):
            return cache

        new_cache = {
            'db_mtime': db_mtime,
            'exact': {},
            'norm': {},
        }

        if not os.path.exists(TaskM3U.WRITE_DB_PATH):
            TaskM3U._write_match_cache = new_cache
            return new_cache

        con = None
        try:
            con = TaskM3U._connect_write_db()
            master_cols = TaskM3U._get_table_columns(con, 'channel_master')
            alias_cols = TaskM3U._get_table_columns(con, 'channel_alias')

            master_id_col = TaskM3U._first_present(master_cols, ['id', 'channel_id', 'master_id'])
            master_name_col = TaskM3U._first_present(master_cols, ['standard_name', 'channel_name', 'name'])
            master_norm_col = TaskM3U._first_present(master_cols, ['standard_name_norm', 'channel_name_norm', 'name_norm'])
            if master_id_col and master_name_col:
                select_cols = [
                    f"CAST({master_id_col} AS TEXT) AS matched_channel_id",
                    f"COALESCE({master_name_col}, '') AS channel_name",
                    f"{master_norm_col} AS channel_norm" if master_norm_col else "'' AS channel_norm",
                ]
                rows = con.execute(f"SELECT {', '.join(select_cols)} FROM channel_master").fetchall()
                for row in rows:
                    matched_channel_id = str(row['matched_channel_id'] or '').strip()
                    channel_name = str(row['channel_name'] or '').strip()
                    channel_norm = str(row['channel_norm'] or '').strip()
                    if not matched_channel_id:
                        continue
                    if channel_name:
                        new_cache['exact'].setdefault(channel_name.lower(), matched_channel_id)
                    if channel_norm:
                        new_cache['norm'].setdefault(channel_norm, matched_channel_id)

            alias_channel_col = TaskM3U._first_present(alias_cols, ['channel_id', 'matched_channel_id', 'channel_master_id', 'master_id'])
            alias_name_col = TaskM3U._first_present(alias_cols, ['alias_name', 'aka_name', 'aka', 'alias', 'name'])
            alias_norm_col = TaskM3U._first_present(alias_cols, ['alias_norm', 'aka_norm', 'name_norm'])
            if alias_channel_col and alias_name_col:
                select_cols = [
                    f"CAST({alias_channel_col} AS TEXT) AS matched_channel_id",
                    f"COALESCE({alias_name_col}, '') AS channel_name",
                    f"{alias_norm_col} AS channel_norm" if alias_norm_col else "'' AS channel_norm",
                ]
                rows = con.execute(f"SELECT {', '.join(select_cols)} FROM channel_alias").fetchall()
                for row in rows:
                    matched_channel_id = str(row['matched_channel_id'] or '').strip()
                    channel_name = str(row['channel_name'] or '').strip()
                    channel_norm = str(row['channel_norm'] or '').strip()
                    if not matched_channel_id:
                        continue
                    if channel_name:
                        new_cache['exact'].setdefault(channel_name.lower(), matched_channel_id)
                    if channel_norm:
                        new_cache['norm'].setdefault(channel_norm, matched_channel_id)

            for table_name in ['provider_logo_local', 'custom_logo']:
                if not TaskM3U._table_exists(con, table_name):
                    continue
                cols = TaskM3U._get_table_columns(con, table_name)
                match_col = TaskM3U._first_present(cols, ['matched_channel_id', 'channel_id', 'master_id'])
                src_col = TaskM3U._first_present(cols, ['source_channel_name', 'provider_channel_name', 'original_name', 'source_name'])
                norm_col = TaskM3U._first_present(cols, ['source_channel_name_norm', 'channel_name_norm', 'name_norm'])
                if not match_col or not src_col:
                    continue
                select_cols = [
                    f"CAST({match_col} AS TEXT) AS matched_channel_id",
                    f"COALESCE({src_col}, '') AS channel_name",
                    f"{norm_col} AS channel_norm" if norm_col else "'' AS channel_norm",
                ]
                rows = con.execute(f"SELECT {', '.join(select_cols)} FROM {table_name} WHERE COALESCE({match_col}, '') <> ''").fetchall()
                for row in rows:
                    matched_channel_id = str(row['matched_channel_id'] or '').strip()
                    channel_name = str(row['channel_name'] or '').strip()
                    channel_norm = str(row['channel_norm'] or '').strip()
                    if not matched_channel_id:
                        continue
                    if channel_name:
                        new_cache['exact'].setdefault(channel_name.lower(), matched_channel_id)
                    if channel_norm:
                        new_cache['norm'].setdefault(channel_norm, matched_channel_id)

        except Exception as e:
            logger.warning(f'[ff_tvh_m3u] load write match cache failed: {str(e)}')
        finally:
            try:
                if con is not None:
                    con.close()
            except Exception:
                pass

        TaskM3U._write_match_cache = new_cache
        return new_cache

    @staticmethod
    def _resolve_lookup_matched_channel_id(channel_name='', matched_channel_id=''):
        matched_channel_id = str(matched_channel_id or '').strip()
        if matched_channel_id:
            return matched_channel_id

        channel_name = str(channel_name or '').strip()
        if not channel_name:
            return ''

        cache = TaskM3U._load_write_match_cache()
        exact = cache.get('exact', {}) or {}
        norm_map = cache.get('norm', {}) or {}

        found = exact.get(channel_name.lower())
        if found:
            return str(found or '').strip()

        norm = TaskM3U._normalize_match_name(channel_name)
        if norm:
            found = norm_map.get(norm)
            if found:
                return str(found or '').strip()
        info = TaskM3U._match_channel_via_task_sheet(channel_name=channel_name)
        if info:
            return str(info.get('channel_master_id') or '').strip()
        return ''

    @staticmethod
    def _get_task_sheet_rules():
        db_mtime = TaskM3U._get_write_db_mtime()
        cache = TaskM3U._sheet_rule_match_cache
        if cache.get('db_mtime') == db_mtime and cache.get('rules') is not None:
            return cache.get('rules')
        try:
            from .task_sheet import TaskSheet
            rules = TaskSheet.load_db_rules()
            TaskM3U._sheet_rule_match_cache = {
                'db_mtime': db_mtime,
                'rules': rules,
            }
            return rules
        except Exception as e:
            logger.warning(f'[ff_tvh_m3u] load task sheet rules failed: {str(e)}')
            TaskM3U._sheet_rule_match_cache = {
                'db_mtime': db_mtime,
                'rules': None,
            }
            return None

    @staticmethod
    def _match_channel_via_task_sheet(channel_name=''):
        channel_name = str(channel_name or '').strip()
        if not channel_name:
            return None
        rules = TaskM3U._get_task_sheet_rules()
        if not rules:
            return None
        try:
            from .task_sheet import TaskSheet
            info, _match_type = TaskSheet.match_channel(channel_name, rules)
            return info
        except Exception as e:
            logger.warning(f'[ff_tvh_m3u] task sheet match failed: {str(e)}')
            return None

    @staticmethod
    def _build_logo_public_template(stored_filename):
        stored_filename = os.path.basename(str(stored_filename or '').strip())
        if not stored_filename:
            return ''
        return f'{TaskM3U.FF_URL_PLACEHOLDER}{TaskM3U.STATIC_HOST_PATH}/{stored_filename}'

    @staticmethod
    def _coalesce_logo_template(raw_url='', stored_filename=''):
        raw_url = str(raw_url or '').strip()
        if raw_url:
            return raw_url
        return TaskM3U._build_logo_public_template(stored_filename)

    @staticmethod
    def get_custom_logo_asset_dir():
        return TaskM3U.UPLOAD_ASSET_DIR

    @staticmethod
    def _ensure_custom_logo_asset_dir():
        os.makedirs(TaskM3U.UPLOAD_ASSET_DIR, exist_ok=True)
        return TaskM3U.UPLOAD_ASSET_DIR

    @staticmethod
    def _make_uploaded_logo_filename(source_channel_name, original_filename):
        source_channel_name = str(source_channel_name or '').strip()
        original_filename = os.path.basename(str(original_filename or '').strip())
        stem = re.sub(r'[^0-9A-Za-z가-힣]+', '_', source_channel_name).strip('_').lower()
        if not stem:
            stem = 'custom_logo'
        ext = os.path.splitext(original_filename)[1].lower()
        if ext not in ['.png', '.jpg', '.jpeg', '.gif', '.webp', '.svg']:
            ext = '.png'
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f'{stem}_{timestamp}_{uuid4().hex[:8]}{ext}'

    @staticmethod
    def _score_logo_entry(entry):
        if not entry:
            return -1
        url_template = str(entry.get('url_template') or '').strip()
        original_url = str(entry.get('original_url') or '').strip()
        score = 0
        if url_template:
            score += 10
            lowered = url_template.lower()
            if TaskM3U.FF_URL_PLACEHOLDER in url_template:
                score += 8
            if '/customlogo/' in lowered:
                score += 6
            if TaskM3U.ASSETS_PATH_PREFIX.lower() in lowered:
                score += 4
        if original_url:
            score += 2
        return score

    @staticmethod
    def _prefer_logo_entry(existing_entry, new_entry):
        if not existing_entry:
            return True
        return TaskM3U._score_logo_entry(new_entry) >= TaskM3U._score_logo_entry(existing_entry)

    @staticmethod

    def _append_logo_entry(name_map, id_map, provider, name_value='', matched_channel_id='', url_value='', original_url=''):
        url_text = TaskM3U._normalize_static_host_logo_url(url_value)
        original_text = str(original_url or '').strip()
        if not url_text and not original_text:
            return
        provider = TaskM3U._canonical_logo_provider(provider)
        if provider == '':
            return

        entry = {
            'provider': provider,
            'url_template': url_text,
            'original_url': original_text,
        }

        matched_channel_id = str(matched_channel_id or '').strip()
        if matched_channel_id:
            id_map.setdefault(provider, {})
            existing = id_map[provider].get(matched_channel_id)
            if TaskM3U._prefer_logo_entry(existing, entry):
                id_map[provider][matched_channel_id] = dict(entry)

        norm = TaskM3U._normalize_logo_name(name_value)
        if norm:
            name_map.setdefault(provider, {})
            existing = name_map[provider].get(norm)
            if TaskM3U._prefer_logo_entry(existing, entry):
                name_map[provider][norm] = dict(entry)

    @staticmethod
    def _register_logo_cache_entry(cache, provider, name_value='', matched_channel_id='', url_value='', original_url=''):
        provider = TaskM3U._canonical_logo_provider(provider)
        if provider == 'custom':
            TaskM3U._append_logo_entry(
                cache['custom_name_map'],
                cache['custom_id_map'],
                'custom',
                name_value=name_value,
                matched_channel_id=matched_channel_id,
                url_value=url_value,
                original_url=original_url,
            )
        else:
            TaskM3U._append_logo_entry(
                cache['provider_name_map'],
                cache['provider_id_map'],
                provider,
                name_value=name_value,
                matched_channel_id=matched_channel_id,
                url_value=url_value,
                original_url=original_url,
            )

    @staticmethod
    def _register_matched_channel_names(con, cache):
        matched_entries = {}

        for provider, id_map in (cache.get('custom_id_map') or {}).items():
            for matched_channel_id, entry in (id_map or {}).items():
                matched_channel_id = str(matched_channel_id or '').strip()
                if matched_channel_id:
                    matched_entries[(provider, matched_channel_id)] = dict(entry)

        for provider, id_map in (cache.get('provider_id_map') or {}).items():
            for matched_channel_id, entry in (id_map or {}).items():
                matched_channel_id = str(matched_channel_id or '').strip()
                if matched_channel_id:
                    matched_entries[(provider, matched_channel_id)] = dict(entry)

        channel_ids = sorted({matched_channel_id for _, matched_channel_id in matched_entries.keys()})
        if not channel_ids:
            return

        master_cols = TaskM3U._get_table_columns(con, 'channel_master')
        alias_cols = TaskM3U._get_table_columns(con, 'channel_alias')
        master_id_col = TaskM3U._first_present(master_cols, ['id', 'channel_id', 'master_id'])
        master_name_col = TaskM3U._first_present(master_cols, ['standard_name', 'channel_name', 'name'])
        if not master_id_col or not master_name_col:
            return

        name_map = {}
        placeholders = ','.join(['?'] * len(channel_ids))
        rows = con.execute(
            f"SELECT CAST({master_id_col} AS TEXT) AS channel_id, COALESCE({master_name_col}, '') AS standard_name "
            f"FROM channel_master WHERE CAST({master_id_col} AS TEXT) IN ({placeholders})",
            tuple(channel_ids),
        ).fetchall()
        for row in rows:
            channel_id = str(row['channel_id'] or '').strip()
            standard_name = str(row['standard_name'] or '').strip()
            if channel_id and standard_name:
                name_map.setdefault(channel_id, []).append(standard_name)

        alias_channel_col = TaskM3U._first_present(alias_cols, ['channel_id', 'matched_channel_id', 'channel_master_id', 'master_id'])
        alias_name_col = TaskM3U._first_present(alias_cols, ['alias_name', 'aka_name', 'aka', 'alias', 'name'])
        if alias_channel_col and alias_name_col:
            rows = con.execute(
                f"SELECT CAST({alias_channel_col} AS TEXT) AS channel_id, COALESCE({alias_name_col}, '') AS alias_name "
                f"FROM channel_alias WHERE CAST({alias_channel_col} AS TEXT) IN ({placeholders})",
                tuple(channel_ids),
            ).fetchall()
            for row in rows:
                channel_id = str(row['channel_id'] or '').strip()
                alias_name = str(row['alias_name'] or '').strip()
                if channel_id and alias_name:
                    name_map.setdefault(channel_id, []).append(alias_name)

        for (provider, matched_channel_id), entry in matched_entries.items():
            for name_value in name_map.get(matched_channel_id, []):
                TaskM3U._register_logo_cache_entry(
                    cache,
                    provider,
                    name_value=name_value,
                    matched_channel_id='',
                    url_value=entry.get('url_template', ''),
                    original_url=entry.get('original_url', ''),
                )

    @staticmethod
    def _resolve_write_custom_storage(con):
        candidates = [
            ('provider_logo_local', {
                'matched_channel_id': ['matched_channel_id', 'channel_id', 'master_id'],
                'standard_name': ['standard_name', 'channel_name', 'name'],
                'source_channel_name': ['source_channel_name', 'provider_channel_name', 'original_name', 'source_name'],
                'stored_filename': ['stored_filename', 'filename', 'logo_file', 'file_name'],
                'logo_url_template': ['logo_url_template', 'logo_url', 'preview_url', 'local_url', 'stored_path', 'path', 'url'],
                'updated_at': ['updated_at', 'modified_at', 'created_at', 'created_time'],
                'created_at': ['created_at', 'created_time'],
                'match_rule': ['match_rule'],
                'match_status': ['match_status'],
                'provider': ['provider', 'provider_name', 'provider_type', 'logo_type', 'kind', 'source_type', 'source'],
                'original_logo_url': ['original_logo_url', 'origin_logo_url'],
            }),
            ('custom_logo', {
                'matched_channel_id': ['matched_channel_id', 'channel_id', 'master_id'],
                'standard_name': ['standard_name', 'channel_name', 'name'],
                'source_channel_name': ['source_channel_name', 'provider_channel_name', 'original_name', 'source_name'],
                'stored_filename': ['stored_filename', 'filename', 'logo_file', 'file_name'],
                'logo_url_template': ['logo_url_template', 'logo_url', 'preview_url', 'local_url', 'stored_path', 'path', 'url'],
                'updated_at': ['updated_at', 'modified_at', 'created_at', 'created_time'],
                'created_at': ['created_at', 'created_time'],
                'original_filename': ['original_filename'],
                'source_type': ['source_type'],
                'is_active': ['is_active'],
                'raw_json': ['raw_json'],
                'source_channel_name_norm': ['source_channel_name_norm', 'channel_name_norm', 'name_norm'],
            }),
        ]

        for table_name, mapping_candidates in candidates:
            if not TaskM3U._table_exists(con, table_name):
                continue
            columns = TaskM3U._get_table_columns(con, table_name)
            resolved = {}
            for key, names in mapping_candidates.items():
                actual = TaskM3U._first_present(columns, names)
                if actual:
                    resolved[key] = actual
            if resolved.get('source_channel_name') or resolved.get('stored_filename') or resolved.get('logo_url_template'):
                return table_name, resolved
        return None, {}

    @staticmethod
    def _find_write_channel_match(con, names):
        candidates = [str(name or '').strip() for name in (names or []) if str(name or '').strip()]
        if not candidates:
            return {'matched_channel_id': '', 'standard_name': '', 'match_rule': 'manual_unconfirmed', 'match_status': 'unmatched'}

        master_cols = TaskM3U._get_table_columns(con, 'channel_master')
        alias_cols = TaskM3U._get_table_columns(con, 'channel_alias')
        master_id_col = TaskM3U._first_present(master_cols, ['id', 'channel_id'])
        master_name_col = TaskM3U._first_present(master_cols, ['standard_name', 'channel_name', 'name'])
        master_norm_col = TaskM3U._first_present(master_cols, ['standard_name_norm', 'channel_name_norm', 'name_norm'])
        alias_channel_col = TaskM3U._first_present(alias_cols, ['channel_id', 'matched_channel_id', 'channel_master_id', 'master_id'])
        alias_name_col = TaskM3U._first_present(alias_cols, ['alias_name', 'aka_name', 'aka', 'alias', 'name'])
        alias_norm_col = TaskM3U._first_present(alias_cols, ['alias_norm', 'aka_norm', 'name_norm'])

        for candidate in candidates:
            if master_id_col and master_name_col:
                row = con.execute(
                    f"SELECT CAST({master_id_col} AS TEXT) AS channel_id, {master_name_col} AS standard_name FROM channel_master WHERE TRIM(COALESCE({master_name_col}, '')) = ? LIMIT 1",
                    (candidate,),
                ).fetchone()
                if row:
                    return {
                        'matched_channel_id': str(row['channel_id'] or '').strip(),
                        'standard_name': str(row['standard_name'] or '').strip(),
                        'match_rule': 'standard_exact',
                        'match_status': 'matched',
                    }

            if alias_channel_col and alias_name_col and master_id_col and master_name_col:
                row = con.execute(
                    f"""
                    SELECT CAST(a.{alias_channel_col} AS TEXT) AS channel_id,
                           COALESCE(m.{master_name_col}, '') AS standard_name
                    FROM channel_alias a
                    LEFT JOIN channel_master m
                      ON CAST(m.{master_id_col} AS TEXT) = CAST(a.{alias_channel_col} AS TEXT)
                    WHERE TRIM(COALESCE(a.{alias_name_col}, '')) = ?
                    LIMIT 1
                    """,
                    (candidate,),
                ).fetchone()
                if row:
                    return {
                        'matched_channel_id': str(row['channel_id'] or '').strip(),
                        'standard_name': str(row['standard_name'] or '').strip(),
                        'match_rule': 'aka_exact',
                        'match_status': 'matched',
                    }

            norm = TaskM3U._normalize_match_name(candidate)
            if not norm:
                continue

            if master_id_col and master_name_col and master_norm_col:
                row = con.execute(
                    f"SELECT CAST({master_id_col} AS TEXT) AS channel_id, {master_name_col} AS standard_name FROM channel_master WHERE {master_norm_col} = ? LIMIT 1",
                    (norm,),
                ).fetchone()
                if row:
                    return {
                        'matched_channel_id': str(row['channel_id'] or '').strip(),
                        'standard_name': str(row['standard_name'] or '').strip(),
                        'match_rule': 'standard_norm',
                        'match_status': 'matched',
                    }

            if alias_channel_col and alias_norm_col and master_id_col and master_name_col:
                row = con.execute(
                    f"""
                    SELECT CAST(a.{alias_channel_col} AS TEXT) AS channel_id,
                           COALESCE(m.{master_name_col}, '') AS standard_name
                    FROM channel_alias a
                    LEFT JOIN channel_master m
                      ON CAST(m.{master_id_col} AS TEXT) = CAST(a.{alias_channel_col} AS TEXT)
                    WHERE a.{alias_norm_col} = ?
                    LIMIT 1
                    """,
                    (norm,),
                ).fetchone()
                if row:
                    return {
                        'matched_channel_id': str(row['channel_id'] or '').strip(),
                        'standard_name': str(row['standard_name'] or '').strip(),
                        'match_rule': 'aka_norm',
                        'match_status': 'matched',
                    }

        return {'matched_channel_id': '', 'standard_name': '', 'match_rule': 'manual_unconfirmed', 'match_status': 'unmatched'}

    @staticmethod
    def _insert_uploaded_custom_logo_record(con, source_channel_name, stored_filename, original_filename='', standard_name='', aka_name=''):
        table_name, mapping = TaskM3U._resolve_write_custom_storage(con)
        if not table_name:
            raise RuntimeError('쓰기 플러그인 커스텀 로고 저장 테이블을 찾지 못했습니다.')

        match = TaskM3U._find_write_channel_match(con, [standard_name, source_channel_name, aka_name])
        matched_channel_id = str(match.get('matched_channel_id') or '').strip()
        resolved_standard_name = str(match.get('standard_name') or standard_name or '').strip()
        logo_url_template = TaskM3U._build_logo_public_template(stored_filename)
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        record = {}
        if mapping.get('matched_channel_id'):
            record[mapping['matched_channel_id']] = matched_channel_id
        if mapping.get('standard_name'):
            record[mapping['standard_name']] = resolved_standard_name
        if mapping.get('source_channel_name'):
            record[mapping['source_channel_name']] = source_channel_name
        if mapping.get('stored_filename'):
            record[mapping['stored_filename']] = stored_filename
        if mapping.get('logo_url_template'):
            record[mapping['logo_url_template']] = logo_url_template
        if mapping.get('updated_at'):
            record[mapping['updated_at']] = now
        if mapping.get('created_at'):
            record[mapping['created_at']] = now
        if mapping.get('match_rule'):
            record[mapping['match_rule']] = str(match.get('match_rule') or '')
        if mapping.get('match_status'):
            record[mapping['match_status']] = str(match.get('match_status') or '')
        if mapping.get('provider'):
            record[mapping['provider']] = 'CUSTOM'
        if mapping.get('original_logo_url'):
            record[mapping['original_logo_url']] = ''
        if mapping.get('original_filename'):
            record[mapping['original_filename']] = os.path.basename(str(original_filename or '').strip())
        if mapping.get('source_type'):
            record[mapping['source_type']] = 'm3u_upload'
        if mapping.get('is_active'):
            record[mapping['is_active']] = 1
        if mapping.get('source_channel_name_norm'):
            record[mapping['source_channel_name_norm']] = TaskM3U._normalize_match_name(source_channel_name)
        if mapping.get('raw_json'):
            record[mapping['raw_json']] = json.dumps({
                'source_channel_name': source_channel_name,
                'standard_name': standard_name,
                'aka_name': aka_name,
                'stored_filename': stored_filename,
                'saved_from': 'tvh_m3u_plugin',
            }, ensure_ascii=False)

        columns = list(record.keys())
        values = [record[col] for col in columns]
        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})"
        cur = con.execute(sql, values)

        return {
            'storage_table': table_name,
            'rowid': cur.lastrowid,
            'matched_channel_id': matched_channel_id,
            'standard_name': resolved_standard_name,
            'match_rule': str(match.get('match_rule') or ''),
            'match_status': str(match.get('match_status') or ''),
            'logo_url_template': logo_url_template,
        }

    @staticmethod
    def save_uploaded_custom_logo(logo_file=None, source_channel_name='', standard_name='', aka_name=''):
        source_channel_name = str(source_channel_name or '').strip()
        standard_name = str(standard_name or '').strip()
        aka_name = str(aka_name or '').strip()

        if not source_channel_name:
            return {'ret': 'warning', 'msg': '원본 채널명을 입력하세요.'}
        if logo_file is None or not getattr(logo_file, 'filename', ''):
            return {'ret': 'warning', 'msg': '업로드할 로고 파일을 선택하세요.'}

        original_filename = os.path.basename(str(logo_file.filename or '').strip())
        stored_filename = TaskM3U._make_uploaded_logo_filename(source_channel_name, original_filename)
        asset_dir = TaskM3U._ensure_custom_logo_asset_dir()
        target_path = os.path.join(asset_dir, stored_filename)

        con = None
        saved_file = False
        try:
            logo_file.save(target_path)
            saved_file = True

            con = TaskM3U._connect_write_db()
            info = TaskM3U._insert_uploaded_custom_logo_record(
                con=con,
                source_channel_name=source_channel_name,
                stored_filename=stored_filename,
                original_filename=original_filename,
                standard_name=standard_name,
                aka_name=aka_name,
            )
            con.commit()
            TaskM3U._load_logo_cache(force=True)

            return {
                'ret': 'success',
                'msg': '커스텀 로고를 저장했습니다.',
                'stored_filename': stored_filename,
                'target_path': target_path,
                **info,
            }
        except Exception as e:
            try:
                if con is not None:
                    con.rollback()
            except Exception:
                pass
            if saved_file:
                try:
                    os.remove(target_path)
                except Exception:
                    pass
            logger.exception(f'[ff_tvh_m3u] save_uploaded_custom_logo failed: {str(e)}')
            return {'ret': 'danger', 'msg': f'커스텀 로고 저장 실패: {str(e)}'}
        finally:
            try:
                if con is not None:
                    con.close()
            except Exception:
                pass

    @staticmethod

    def _load_logo_cache(force=False):
        db_mtime = TaskM3U._get_write_db_mtime()
        cache = TaskM3U._logo_cache
        if (not force) and cache.get('db_mtime') == db_mtime and (cache.get('custom_name_map') or cache.get('provider_name_map')):
            return cache

        new_cache = {
            'db_mtime': db_mtime,
            'custom_name_map': {},
            'custom_id_map': {},
            'provider_name_map': {},
            'provider_id_map': {},
        }

        if not os.path.exists(TaskM3U.WRITE_DB_PATH):
            TaskM3U._logo_cache = new_cache
            return new_cache

        con = None
        try:
            con = sqlite3.connect(TaskM3U.WRITE_DB_PATH)
            con.row_factory = sqlite3.Row

            if TaskM3U._table_exists(con, 'custom_logo'):
                custom_cols = TaskM3U._get_table_columns(con, 'custom_logo')
                custom_match_col = TaskM3U._first_present(custom_cols, ['matched_channel_id', 'channel_id', 'master_id'])
                custom_source_col = TaskM3U._first_present(custom_cols, ['source_channel_name', 'provider_channel_name', 'original_name', 'source_name'])
                custom_std_col = TaskM3U._first_present(custom_cols, ['standard_name', 'channel_name', 'name'])
                custom_url_col = TaskM3U._first_present(custom_cols, ['logo_url_template', 'logo_url', 'preview_url', 'local_url', 'stored_path', 'path', 'url'])
                custom_file_col = TaskM3U._first_present(custom_cols, ['stored_filename', 'filename', 'logo_file', 'file_name'])
                custom_active_col = TaskM3U._first_present(custom_cols, ['is_active'])
                select_cols = [
                    f"{custom_match_col} AS matched_channel_id" if custom_match_col else "'' AS matched_channel_id",
                    f"{custom_source_col} AS source_channel_name" if custom_source_col else "'' AS source_channel_name",
                    f"{custom_std_col} AS standard_name" if custom_std_col else "'' AS standard_name",
                    f"{custom_url_col} AS logo_url_template" if custom_url_col else "'' AS logo_url_template",
                    f"{custom_file_col} AS stored_filename" if custom_file_col else "'' AS stored_filename",
                ]
                where_parts = []
                if custom_active_col:
                    where_parts.append(f"COALESCE({custom_active_col}, 1) = 1")
                if custom_url_col and custom_file_col:
                    where_parts.append(f"(COALESCE({custom_url_col}, '') <> '' OR COALESCE({custom_file_col}, '') <> '')")
                elif custom_url_col:
                    where_parts.append(f"COALESCE({custom_url_col}, '') <> ''")
                elif custom_file_col:
                    where_parts.append(f"COALESCE({custom_file_col}, '') <> ''")
                rows = con.execute(f"""
                    SELECT {', '.join(select_cols)}
                    FROM custom_logo
                    {'WHERE ' + ' AND '.join(where_parts) if where_parts else ''}
                """).fetchall()
                for row in rows:
                    logo_template = TaskM3U._coalesce_logo_template(row['logo_url_template'], row['stored_filename'])
                    TaskM3U._register_logo_cache_entry(
                        new_cache,
                        'custom',
                        name_value=row['source_channel_name'],
                        matched_channel_id=row['matched_channel_id'],
                        url_value=logo_template,
                        original_url='',
                    )
                    TaskM3U._register_logo_cache_entry(
                        new_cache,
                        'custom',
                        name_value=row['standard_name'],
                        matched_channel_id=row['matched_channel_id'],
                        url_value=logo_template,
                        original_url='',
                    )

            if TaskM3U._table_exists(con, 'provider_logo_local'):
                provider_cols = TaskM3U._get_table_columns(con, 'provider_logo_local')
                provider_provider_col = TaskM3U._first_present(provider_cols, ['provider', 'provider_name', 'provider_type', 'logo_type', 'kind', 'source'])
                provider_match_col = TaskM3U._first_present(provider_cols, ['matched_channel_id', 'channel_id', 'master_id'])
                provider_std_col = TaskM3U._first_present(provider_cols, ['standard_name', 'channel_name', 'name'])
                provider_src_col = TaskM3U._first_present(provider_cols, ['source_channel_name', 'provider_channel_name', 'original_name', 'source_name'])
                provider_url_col = TaskM3U._first_present(provider_cols, ['logo_url_template', 'logo_url', 'preview_url', 'local_url', 'stored_path', 'path', 'url'])
                provider_file_col = TaskM3U._first_present(provider_cols, ['stored_filename', 'filename', 'logo_file', 'file_name'])
                provider_orig_col = TaskM3U._first_present(provider_cols, ['original_logo_url', 'origin_logo_url'])
                select_cols = [
                    f"{provider_provider_col} AS provider" if provider_provider_col else "'' AS provider",
                    f"{provider_match_col} AS matched_channel_id" if provider_match_col else "'' AS matched_channel_id",
                    f"{provider_std_col} AS standard_name" if provider_std_col else "'' AS standard_name",
                    f"{provider_src_col} AS source_channel_name" if provider_src_col else "'' AS source_channel_name",
                    f"{provider_url_col} AS logo_url_template" if provider_url_col else "'' AS logo_url_template",
                    f"{provider_file_col} AS stored_filename" if provider_file_col else "'' AS stored_filename",
                    f"{provider_orig_col} AS original_logo_url" if provider_orig_col else "'' AS original_logo_url",
                ]
                where_parts = []
                if provider_url_col and provider_file_col:
                    where_parts.append(f"(COALESCE({provider_url_col}, '') <> '' OR COALESCE({provider_file_col}, '') <> '')")
                elif provider_url_col:
                    where_parts.append(f"COALESCE({provider_url_col}, '') <> ''")
                elif provider_file_col:
                    where_parts.append(f"COALESCE({provider_file_col}, '') <> ''")
                rows = con.execute(f"""
                    SELECT {', '.join(select_cols)}
                    FROM provider_logo_local
                    {'WHERE ' + ' AND '.join(where_parts) if where_parts else ''}
                """).fetchall()
                for row in rows:
                    provider = TaskM3U._canonical_logo_provider(row['provider'])
                    logo_template = TaskM3U._coalesce_logo_template(row['logo_url_template'], row['stored_filename'])
                    TaskM3U._register_logo_cache_entry(
                        new_cache,
                        provider,
                        name_value=row['standard_name'],
                        matched_channel_id=row['matched_channel_id'],
                        url_value=logo_template,
                        original_url=row['original_logo_url'],
                    )
                    TaskM3U._register_logo_cache_entry(
                        new_cache,
                        provider,
                        name_value=row['source_channel_name'],
                        matched_channel_id=row['matched_channel_id'],
                        url_value=logo_template,
                        original_url=row['original_logo_url'],
                    )

            TaskM3U._register_matched_channel_names(con, new_cache)

        except Exception as e:
            logger.warning(f'[ff_tvh_m3u] load logo cache failed: {str(e)}')
        finally:
            try:
                if con is not None:
                    con.close()
            except Exception:
                pass

        TaskM3U._logo_cache = new_cache
        return new_cache

    @staticmethod
    def _lookup_logo_entry(cache, provider, matched_channel_id='', norm=''):
        provider = TaskM3U._canonical_logo_provider(provider)
        matched_channel_id = str(matched_channel_id or '').strip()
        norm = str(norm or '').strip()

        if provider == 'custom':
            if matched_channel_id:
                entry = cache.get('custom_id_map', {}).get('custom', {}).get(matched_channel_id)
                if entry:
                    return entry
                entry = cache.get('provider_id_map', {}).get('custom', {}).get(matched_channel_id)
                if entry:
                    return entry
            if norm:
                entry = cache.get('custom_name_map', {}).get('custom', {}).get(norm)
                if entry:
                    return entry
                entry = cache.get('provider_name_map', {}).get('custom', {}).get(norm)
                if entry:
                    return entry
            return None

        if matched_channel_id:
            entry = cache.get('provider_id_map', {}).get(provider, {}).get(matched_channel_id)
            if entry:
                return entry
        if norm:
            entry = cache.get('provider_name_map', {}).get(provider, {}).get(norm)
            if entry:
                return entry
        return None

    @staticmethod
    def get_effective_logo_url(channel_name='', sheet_logo_url='', matched_channel_id='', base_url=''):
        cache = TaskM3U._load_logo_cache()
        base_url = str(base_url or '').rstrip('/')
        channel_name = str(channel_name or '').strip()
        matched_channel_id = TaskM3U._resolve_lookup_matched_channel_id(channel_name=channel_name, matched_channel_id=matched_channel_id)
        norm = TaskM3U._normalize_logo_name(channel_name)
        priority = TaskM3U._get_logo_priority()

        for provider in priority:
            url_value = ''
            if provider == 'custom':
                if matched_channel_id:
                    entry = TaskM3U._lookup_logo_entry(cache, 'custom', matched_channel_id=matched_channel_id)
                    url_value = entry or ''
                if not url_value and norm:
                    entry = TaskM3U._lookup_logo_entry(cache, 'custom', norm=norm)
                    url_value = entry or ''
            else:
                entry = TaskM3U._lookup_logo_entry(cache, provider, matched_channel_id=matched_channel_id, norm=norm)
                url_value = entry or ''

            if url_value:
                return TaskM3U._replace_placeholder_url(url_value, base_url=base_url)

        fallback = str(sheet_logo_url or '').strip()
        if fallback:
            return TaskM3U._replace_placeholder_url(fallback, base_url=base_url)
        return ''


    @staticmethod
    def _load_override_cache():
        try:
            TaskM3U._override_cache = ModelLogoOverride.get_maps()
        except Exception as e:
            logger.warning(f'[ff_tvh_m3u] load override cache failed: {str(e)}')
            TaskM3U._override_cache = {'uuid_map': {}, 'name_map': {}}
        return TaskM3U._override_cache

    @staticmethod
    def _get_override_entry(channel_uuid='', channel_name=''):
        cache = TaskM3U._override_cache or {}
        uuid_map = cache.get('uuid_map', {}) or {}
        name_map = cache.get('name_map', {}) or {}
        channel_uuid = str(channel_uuid or '').strip()
        if channel_uuid and channel_uuid in uuid_map:
            return uuid_map[channel_uuid]
        norm = TaskM3U._normalize_logo_name(channel_name)
        if norm and norm in name_map:
            return name_map[norm]
        return None

    @staticmethod

    def _asset_file_from_url_template(url_value):
        text = str(url_value or '').strip()
        if not text or TaskM3U.FF_URL_PLACEHOLDER not in text:
            return ''
        suffix = text.split(TaskM3U.FF_URL_PLACEHOLDER, 1)[-1].strip()
        if suffix.startswith('/customlogo/'):
            filename = os.path.basename(suffix)
            for base_dir in ['/data/custom/customlogo', '/data/data/customlogo', '/data/custom/logo', '/data/custom/tvhlogo']:
                candidate = os.path.join(base_dir, filename)
                if os.path.exists(candidate):
                    return candidate
            return ''
        if suffix.startswith('/tvh_m3u_plugin/docs/assets/'):
            rel = suffix[len('/tvh_m3u_plugin/docs/assets/'):].lstrip('/')
            candidate = os.path.join('/data/plugins/tvh_m3u_plugin/docs/assets', rel)
            if os.path.exists(candidate):
                return candidate
            flat_candidate = os.path.join('/data/plugins/tvh_m3u_plugin/docs/assets', os.path.basename(rel))
            return flat_candidate if os.path.exists(flat_candidate) else ''
        return ''

    @staticmethod
    def _entry_to_preview_url(entry, base_url=''):
        if not entry:
            return ''
        template = str(entry.get('url_template') or '').strip()
        original = str(entry.get('original_url') or '').strip()
        preview = TaskM3U._replace_placeholder_url(template, base_url=base_url) if template else ''
        if template and TaskM3U.FF_URL_PLACEHOLDER in template:
            asset_path = TaskM3U._asset_file_from_url_template(template)
            if asset_path:
                return preview
            if original:
                return original
        return preview or original

    @staticmethod
    def _entry_to_effective_url(entry, base_url=''):
        return TaskM3U._entry_to_preview_url(entry, base_url=base_url)

    @staticmethod
    def _ordered_logo_keys(candidates):
        ordered = []
        for provider in TaskM3U._get_logo_priority():
            if provider in candidates and provider not in ordered:
                ordered.append(provider)
        if 'sheet' in candidates and 'sheet' not in ordered:
            ordered.append('sheet')
        for key in ['custom', 'kt', 'wavve', 'tving', 'sk', 'sheet']:
            if key in candidates and key not in ordered:
                ordered.append(key)
        return ordered

    @staticmethod


    def _get_logo_candidates(channel_name='', sheet_logo_url='', matched_channel_id='', base_url=''):
        cache = TaskM3U._load_logo_cache()
        base_url = str(base_url or '').rstrip('/')
        channel_name = str(channel_name or '').strip()
        matched_channel_id = TaskM3U._resolve_lookup_matched_channel_id(channel_name=channel_name, matched_channel_id=matched_channel_id)
        norm = TaskM3U._normalize_logo_name(channel_name)
        candidates = {}

        for provider in TaskM3U.DEFAULT_LOGO_PRIORITY:
            entry = TaskM3U._lookup_logo_entry(
                cache,
                provider,
                matched_channel_id=matched_channel_id,
                norm=norm,
            )

            if entry:
                item = dict(entry)
                item['preview_url'] = TaskM3U._entry_to_preview_url(item, base_url=base_url)
                item['effective_url'] = TaskM3U._entry_to_effective_url(item, base_url=base_url)
                candidates[provider] = item

        fallback = str(sheet_logo_url or '').strip()
        if fallback:
            candidates['sheet'] = {
                'provider': 'sheet',
                'url_template': fallback,
                'original_url': fallback,
                'preview_url': TaskM3U._replace_placeholder_url(fallback, base_url=base_url),
                'effective_url': TaskM3U._replace_placeholder_url(fallback, base_url=base_url),
            }

        info = TaskM3U._match_channel_via_task_sheet(channel_name=channel_name)
        if info:
            custom_logo_url = str(info.get('custom_logo_url') or '').strip()
            provider_logo_url = str(info.get('provider_logo_url') or '').strip()
            final_logo_url = str(info.get('final_logo_url') or '').strip()

            if custom_logo_url and 'custom' not in candidates:
                candidates['custom'] = {
                    'provider': 'custom',
                    'url_template': custom_logo_url,
                    'original_url': '',
                    'preview_url': TaskM3U._replace_placeholder_url(custom_logo_url, base_url=base_url),
                    'effective_url': TaskM3U._replace_placeholder_url(custom_logo_url, base_url=base_url),
                }

            # Use the already matched final logo from TaskSheet as a reliable fallback
            # for the logo settings screen, even when provider/custom cache linkage lags.
            sheet_fallback = final_logo_url or provider_logo_url
            if sheet_fallback and 'sheet' not in candidates:
                candidates['sheet'] = {
                    'provider': 'sheet',
                    'url_template': sheet_fallback,
                    'original_url': sheet_fallback,
                    'preview_url': TaskM3U._replace_placeholder_url(sheet_fallback, base_url=base_url),
                    'effective_url': TaskM3U._replace_placeholder_url(sheet_fallback, base_url=base_url),
                }
        return candidates

    @staticmethod


    def get_effective_logo_choice(channel_uuid='', channel_name='', sheet_logo_url='', matched_channel_id='', base_url=''):
        TaskM3U._load_override_cache()
        candidates = TaskM3U._get_logo_candidates(
            channel_name=channel_name,
            sheet_logo_url=sheet_logo_url,
            matched_channel_id=matched_channel_id,
            base_url=base_url,
        )
        override = TaskM3U._get_override_entry(channel_uuid=channel_uuid, channel_name=channel_name)
        if override:
            provider = str(override.get('selected_provider') or '').strip().lower()
            url_template = str(override.get('logo_url_template') or '').strip()
            preview_url = TaskM3U._replace_placeholder_url(url_template, base_url=base_url)
            if url_template:
                return {
                    'provider': provider or 'manual',
                    'url_template': url_template,
                    'preview_url': preview_url,
                    'effective_url': preview_url,
                    'is_manual': True,
                    'candidates': candidates,
                }

        for provider in TaskM3U._ordered_logo_keys(candidates):
            item = dict(candidates.get(provider) or {})
            if not item:
                continue
            item['provider'] = provider
            item['preview_url'] = item.get('preview_url') or item.get('effective_url') or ''
            item['effective_url'] = item.get('effective_url') or item.get('preview_url') or ''
            item['is_manual'] = False
            item['candidates'] = candidates
            return item

        return {
            'provider': '',
            'url_template': '',
            'preview_url': '',
            'effective_url': '',
            'is_manual': False,
            'candidates': candidates,
        }

    @staticmethod

    def get_effective_logo_url(channel_uuid='', channel_name='', sheet_logo_url='', matched_channel_id='', base_url=''):
        choice = TaskM3U.get_effective_logo_choice(
            channel_uuid=channel_uuid,
            channel_name=channel_name,
            sheet_logo_url=sheet_logo_url,
            matched_channel_id=matched_channel_id,
            base_url=base_url,
        )
        return str(choice.get('effective_url') or choice.get('preview_url') or '').strip()

    @staticmethod
    def get_logo_preview_rows(base_url='', query='', filter_mode='all'):
        base_url = str(base_url or '').rstrip('/')
        query_norm = TaskM3U._normalize_logo_name(query)
        override_maps = TaskM3U._load_override_cache()
        rows = []
        grouped = ModelChannel.get_grouped()
        for group_name, channels in grouped.items():
            for ch in channels:
                channel_uuid = str(ch.get('channel_uuid') or '').strip()
                channel_name = str(ch.get('name') or '').strip()
                matched_channel_id = TaskM3U._resolve_lookup_matched_channel_id(
                    channel_name=channel_name,
                    matched_channel_id=(ch.get('sheet_id') or ch.get('sheet_channel_id') or ch.get('matched_channel_id') or ''),
                )
                sheet_logo_url = str(ch.get('sheet_logo_url') or '').strip()

                if query_norm:
                    hay = [
                        TaskM3U._normalize_logo_name(channel_name),
                        TaskM3U._normalize_logo_name(group_name),
                        TaskM3U._normalize_logo_name(str(ch.get('sheet_group_name') or '')),
                    ]
                    if not any(query_norm in part for part in hay if part):
                        continue

                choice = TaskM3U.get_effective_logo_choice(
                    channel_uuid=channel_uuid,
                    channel_name=channel_name,
                    sheet_logo_url=sheet_logo_url,
                    matched_channel_id=matched_channel_id,
                    base_url=base_url,
                )
                candidates = choice.get('candidates', {}) or {}
                provider_candidates = [key for key in ['custom', 'kt', 'wavve', 'tving', 'sk'] if key in candidates]
                has_custom = 'custom' in candidates
                has_provider = any(key in candidates for key in ['kt', 'wavve', 'tving', 'sk'])
                has_multi = len(provider_candidates) >= 2 or (has_custom and has_provider)
                has_logo = bool(choice.get('preview_url'))
                has_manual = bool(TaskM3U._get_override_entry(channel_uuid=channel_uuid, channel_name=channel_name))

                if filter_mode == 'missing' and has_logo:
                    continue
                if filter_mode == 'multi' and not has_multi:
                    continue
                if filter_mode == 'custom' and not has_custom:
                    continue
                if filter_mode == 'provider' and not has_provider:
                    continue
                if filter_mode == 'manual' and not has_manual:
                    continue

                row = {
                    'channel_uuid': channel_uuid,
                    'number': ch.get('number') or 0,
                    'name': channel_name,
                    'group_name': group_name,
                    'matched_channel_id': matched_channel_id,
                    'sheet_logo_url': sheet_logo_url,
                    'current_provider': choice.get('provider', ''),
                    'current_logo_url': choice.get('preview_url', ''),
                    'is_manual': bool(choice.get('is_manual')),
                    'candidate_count': len(candidates),
                    'candidates': [],
                }

                for key in ['custom', 'kt', 'wavve', 'tving', 'sk', 'sheet']:
                    cand = candidates.get(key)
                    if not cand:
                        continue
                    row['candidates'].append({
                        'provider': key,
                        'label': '기존' if key == 'sheet' else key.upper(),
                        'preview_url': cand.get('preview_url', ''),
                        'url_template': cand.get('url_template', ''),
                        'is_selected': key == row['current_provider'],
                    })
                rows.append(row)

        rows.sort(key=lambda item: (
            1 if not item.get('current_logo_url') else 0,
            1 if not item.get('is_manual') else 0,
            str(item.get('group_name') or ''),
            int(item.get('number') or 0),
            str(item.get('name') or '').lower(),
        ))
        return rows

    @staticmethod
    def save_logo_override(channel_uuid='', provider='', url_template=''):
        channel_uuid = str(channel_uuid or '').strip()
        provider = str(provider or '').strip().lower()
        url_template = str(url_template or '').strip()
        if not channel_uuid:
            return {'ret': 'warning', 'msg': '채널 UUID가 비어 있습니다.'}
        channel_row = ModelChannel.get_channel_map().get(channel_uuid)
        if channel_row is None:
            return {'ret': 'warning', 'msg': '채널 정보를 찾지 못했습니다.'}
        channel_name = str(channel_row.name or '').strip()
        if provider == '' or url_template == '':
            return {'ret': 'warning', 'msg': '로고 후보 정보가 비어 있습니다.'}
        ok = ModelLogoOverride.save(channel_uuid, channel_name, provider, url_template)
        TaskM3U._load_override_cache()
        return {'ret': 'success' if ok else 'danger', 'msg': '로고 선택을 저장했습니다.' if ok else '로고 선택 저장 실패'}

    @staticmethod
    def clear_logo_override(channel_uuid=''):
        changed = ModelLogoOverride.delete(channel_uuid)
        TaskM3U._load_override_cache()
        return {'ret': 'success', 'msg': '수동 선택을 해제했습니다.' if changed else '해제할 수동 선택이 없습니다.'}

    @staticmethod
    def build_extinf(target, channel_uuid, tvg_name, tvg_chno, group_name, logo_url=''):
        target = str(target or 'tivimate').strip().lower()
        channel_uuid = TaskM3U._sanitize_attr(channel_uuid)
        tvg_name_attr = TaskM3U._sanitize_attr(tvg_name)
        tvg_name_text = TaskM3U._sanitize_name(tvg_name)
        tvg_chno = TaskM3U._sanitize_attr(tvg_chno)
        group_name = TaskM3U._sanitize_attr(group_name)
        logo_url = TaskM3U._sanitize_attr(logo_url)

        attrs = [
            f'tvg-id="{channel_uuid}"',
            f'tvg-name="{tvg_name_attr}"',
            f'tvg-chno="{tvg_chno}"',
            f'group-title="{group_name}"',
        ]

        if target == 'tivimate' and logo_url:
            attrs.append(f'tvg-logo="{logo_url}"')

        return f'#EXTINF:-1 {" ".join(attrs)},{tvg_name_text}'

    @staticmethod
    def build_m3u(target='tivimate'):
        try:
            target = str(target or 'tivimate').strip().lower()
            if target not in ['tvh', 'tivimate']:
                target = 'tivimate'

            logger.info(f'[ff_tvh_m3u] build_m3u start target={target}')

            grouped_rows = ModelChannel.get_grouped()
            total_groups = len(grouped_rows)
            total_channels = sum(len(x) for x in grouped_rows.values())
            logger.info(f'[ff_tvh_m3u] build_m3u source groups={total_groups} channels={total_channels}')

            lines = ['#EXTM3U']
            playlist_map = TaskM3U.fetch_playlist_map()
            playlist_map_count = len(playlist_map)
            base_url = TaskM3U._get_request_base_url()
            logger.info(f'[ff_tvh_m3u] build_m3u playlist_map_count={playlist_map_count}')

            added_count = 0
            skipped_disabled = 0
            skipped_no_playlist = 0
            skipped_empty_url = 0

            for group_name, channels in grouped_rows.items():
                for ch in channels:
                    if not ch.get('enabled', True):
                        skipped_disabled += 1
                        continue

                    tvg_name = ch.get('name') or ''
                    tvg_chno = ch.get('number') or 0
                    channel_uuid = ch.get('channel_uuid')
                    matched_channel_id = ch.get('sheet_id') or ch.get('sheet_channel_id') or ch.get('matched_channel_id') or ''
                    sheet_logo_url = str(ch.get('sheet_logo_url') or '').strip()
                    final_logo_url = TaskM3U.get_effective_logo_url(
                        channel_uuid=channel_uuid,
                        channel_name=tvg_name,
                        sheet_logo_url=sheet_logo_url,
                        matched_channel_id=matched_channel_id,
                        base_url=base_url,
                    )

                    source_url = playlist_map.get(channel_uuid, '')
                    if not source_url:
                        skipped_no_playlist += 1
                        continue

                    effective_profile = TaskM3U.get_effective_profile(channel_uuid, group_name)
                    stream_url = TaskM3U.normalize_stream_url(source_url, effective_profile)
                    if not stream_url:
                        skipped_empty_url += 1
                        continue

                    extinf = TaskM3U.build_extinf(
                        target=target,
                        channel_uuid=channel_uuid,
                        tvg_name=tvg_name,
                        tvg_chno=tvg_chno,
                        group_name=group_name,
                        logo_url=final_logo_url,
                    )
                    lines.append(extinf)
                    lines.append(stream_url)
                    added_count += 1

            logger.info(
                f'[ff_tvh_m3u] build_m3u done target={target} added={added_count} '
                f'skipped_disabled={skipped_disabled} '
                f'skipped_no_playlist={skipped_no_playlist} '
                f'skipped_empty_url={skipped_empty_url}'
            )

            return '\n'.join(lines) + '\n'

        except Exception as e:
            logger.exception(f'[ff_tvh_m3u] build_m3u exception: {str(e)}')
            return '#EXTM3U\n'

    @staticmethod
    def build_epg_xml(target='tvh'):
        target = str(target or 'tvh').strip().lower()
        xml_path = TaskM3U.EPG_CACHE_XML_PATH
        if not os.path.exists(xml_path):
            return b''

        try:
            tree = ET.parse(xml_path)
            root = tree.getroot()
            base_url = TaskM3U._get_request_base_url()

            for channel_elem in root.iter():
                if TaskM3U._normalize_xml_tag(channel_elem.tag) != 'channel':
                    continue

                display_names = []
                icon_elem = None
                current_icon_url = ''
                for child in list(channel_elem):
                    tag = TaskM3U._normalize_xml_tag(child.tag)
                    if tag == 'display-name':
                        name_text = str(child.text or '').strip()
                        if name_text:
                            display_names.append(name_text)
                    elif tag == 'icon':
                        icon_elem = child
                        current_icon_url = str(child.attrib.get('src') or '').strip()

                channel_name = display_names[0] if display_names else str(channel_elem.attrib.get('id') or '').strip()
                final_logo_url = TaskM3U.get_effective_logo_url(
                    channel_name=channel_name,
                    sheet_logo_url=current_icon_url,
                    matched_channel_id='',
                    base_url=base_url,
                )
                if not final_logo_url:
                    continue

                if icon_elem is None:
                    icon_elem = ET.SubElement(channel_elem, 'icon')
                icon_elem.set('src', final_logo_url)

            return ET.tostring(root, encoding='utf-8', xml_declaration=True)
        except Exception as e:
            logger.exception(f'[ff_tvh_m3u] build_epg_xml exception: {str(e)}')
            return b''

    @staticmethod
    def get_m3u_url(target='tivimate'):
        try:
            from flask import request
            base_url = request.host_url.rstrip('/')
            target = str(target or 'tivimate').strip().lower()
            if target == 'tvh':
                return f'{base_url}/{P.package_name}/api/m3u_tvh'
            if target == 'tivimate':
                return f'{base_url}/{P.package_name}/api/m3u_tivimate'
            return f'{base_url}/{P.package_name}/api/m3u'
        except Exception as e:
            logger.exception(f'[ff_tvh_m3u] get_m3u_url exception: {str(e)}')
            if str(target or '').strip().lower() == 'tvh':
                return f'/{P.package_name}/api/m3u_tvh'
            if str(target or '').strip().lower() == 'tivimate':
                return f'/{P.package_name}/api/m3u_tivimate'
            return f'/{P.package_name}/api/m3u'

    @staticmethod
    def get_epg_url(target='tvh'):
        try:
            from flask import request
            base_url = request.host_url.rstrip('/')
            target = str(target or 'tvh').strip().lower()
            if target == 'tivimate':
                return f'{base_url}/{P.package_name}/api/epg_tivimate'
            if target == 'tvh':
                return f'{base_url}/{P.package_name}/api/epg_tvh'
            return f'{base_url}/{P.package_name}/api/epg_raw'
        except Exception as e:
            logger.exception(f'[ff_tvh_m3u] get_epg_url exception: {str(e)}')
            if str(target or '').strip().lower() == 'tivimate':
                return f'/{P.package_name}/api/epg_tivimate'
            if str(target or '').strip().lower() == 'tvh':
                return f'/{P.package_name}/api/epg_tvh'
            return f'/{P.package_name}/api/epg_raw'
