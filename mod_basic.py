# -*- coding: utf-8 -*-
import importlib
import sys
import os
import re
import hashlib
import threading
import time
import datetime as _epg_auto_dt
import json
import gzip
import shutil
from datetime import datetime
import xml.etree.ElementTree as ET
from xml.sax.saxutils import quoteattr

import requests
from flask import request, render_template, jsonify, redirect, Response, render_template_string

from .setup import *
from .model import ModelTag, ModelChannel, ModelGroupOrder, ModelGroupProfile, ModelChannelProfile, ModelLogoOverride, DB_PATH
from .task import Task
from .task_custom_logo import handle_custom_logo_upload, handle_custom_logo_mirror
from .task_epg_extra import build_dlive_epg_xml_bytes, merge_xmltv_files, DEFAULT_DLIVE_SCHEDULE_URL, DEFAULT_DLIVE_SOURCE_NAME


def _is_sync_form(req):
    try:
        if req is None or not getattr(req, 'form', None):
            return False
        keys = set(req.form.keys())
        sync_markers = {
            'basic_tvh_api_base',
            'basic_tvh_stream_base',
            'basic_tvh_admin_username',
            'basic_tvh_admin_password',
            'basic_tvh_play_username',
            'basic_tvh_play_password',
            'basic_tvh_use_verify_ssl',
            'basic_tvh_include_auth_in_url',
            'basic_tvh_stream_profile',
        }
        return len(keys.intersection(sync_markers)) > 0
    except Exception:
        return False


def _save_runtime_settings(req):
    result = {
        'saved': False,
        'is_sync_form': False,
    }
    try:
        if req is None or not getattr(req, 'form', None):
            return result
        result['is_sync_form'] = _is_sync_form(req)
        P.ModelSetting.setting_save(req)
        result['saved'] = True
    except Exception as e:
        logger.warning(f'[ff_tvh_m3u] runtime setting_save skipped: {str(e)}')
    return result


def _load_sjva_module():
    for mod_name in ['sjva.setup', 'sjva']:
        if mod_name in sys.modules:
            return sys.modules.get(mod_name)
    for mod_name in ['sjva.setup', 'sjva']:
        try:
            return importlib.import_module(mod_name)
        except Exception:
            continue
    return None


def _is_truthy(value):
    if isinstance(value, bool):
        return value
    text = str(value or '').strip().lower()
    return text in ['true', '1', 'yes', 'y', 'ok', 'success', 'authenticated', 'auth']


def _is_sjva_auth_ok(raw):
    if raw is True:
        return True
    if raw in [None, False, '', 0]:
        return False

    deny_words = ['미인증', '인증필요', 'need auth', 'not auth', 'unauth', 'expired', 'fail', 'error']
    allow_words = ['인증되었습니다', '인증완료', 'authenticated', 'auth ok', 'success']

    if isinstance(raw, dict):
        for key in ['is_auth', 'auth', 'authenticated', 'success', 'ok']:
            if key in raw:
                return _is_truthy(raw.get(key))

        if 'ret' in raw and isinstance(raw.get('ret'), bool):
            return raw.get('ret') is True

        if 'ret' in raw:
            ret_text = str(raw.get('ret') or '').strip().lower()
            if ret_text in ['true', 'success', 'ok', 'authenticated']:
                return True
            if ret_text in ['false', 'warning', 'danger', 'error', 'fail']:
                return False

        joined = ' '.join([
            str(raw.get('msg', '') or ''),
            str(raw.get('desc', '') or ''),
            str(raw.get('status', '') or ''),
            str(raw.get('result', '') or ''),
            str(raw.get('ret', '') or ''),
            str(raw.get('sjva_id', '') or ''),
            str(raw.get('level', '') or ''),
            str(raw.get('point', '') or ''),
        ]).strip().lower()

        if any(word in joined for word in [w.lower() for w in deny_words]):
            return False
        if any(word in joined for word in [w.lower() for w in allow_words]):
            return True
        return False

    text = str(raw).strip().lower()
    if any(word in text for word in [w.lower() for w in deny_words]):
        return False
    if any(word in text for word in [w.lower() for w in allow_words]):
        return True
    return False


def _get_sjva_auth_info():
    mod = _load_sjva_module()
    if mod is None:
        return {
            'ok': False,
            'reason': 'SJVA 플러그인이 설치되어 있지 않습니다.',
            'raw': None,
        }

    getter = None

    try:
        plugin_obj = getattr(mod, 'P', None)
        if plugin_obj is not None:
            getter = getattr(plugin_obj, 'get_auth_status', None)
    except Exception:
        getter = None

    if getter is None:
        getter = getattr(mod, 'get_auth_status', None)

    if getter is None:
        return {
            'ok': False,
            'reason': 'SJVA 인증 상태 조회 함수를 찾지 못했습니다.',
            'raw': None,
        }

    try:
        raw = getter()
        ok = _is_sjva_auth_ok(raw)
        return {
            'ok': ok,
            'reason': '' if ok else 'SJVA 설정에서 APIKEY 확인 후 [인증하기]를 먼저 실행하세요.',
            'raw': raw,
        }
    except Exception as e:
        logger.exception(f'[ff_tvh_m3u] sjva get_auth_status exception: {str(e)}')
        return {
            'ok': False,
            'reason': f'SJVA 인증 상태 조회 실패: {str(e)}',
            'raw': None,
        }


def _render_sjva_auth_required_page(message):
    return render_template_string(
        """
        <div style="padding:24px; max-width:780px; margin:0 auto; font-family:Arial, sans-serif;">
          <div style="border:1px solid #f0d98c; background:#fffaf0; padding:18px 20px; border-radius:8px;">
            <h3 style="margin:0 0 10px 0;">SJVA 인증 필요</h3>
            <div style="margin-bottom:10px;">{{ message }}</div>
            <div style="color:#666;">SJVA 설정 화면에서 APIKEY를 확인하고 <strong>인증하기</strong>를 1회 실행한 뒤 다시 접속하세요.</div>
          </div>
        </div>
        """,
        message=message
    )


EPG_PROVIDER_OPTIONS = [
    ('kt', 'KT'),
    ('lgu', 'LG'),
    ('sk', 'SK'),
    ('daum', 'DAUM'),
    ('naver', 'NAVER'),
    ('wavve', 'WAVVE'),
    ('tving', 'TVING'),
    ('spotv', 'SPOTV'),
]
EPG_PROVIDER_LABEL_MAP = {item[0]: item[1] for item in EPG_PROVIDER_OPTIONS}


LOGO_PRIORITY_OPTIONS = [
    ('custom', '커스텀'),
    ('kt', 'KT'),
    ('wavve', 'WAVVE'),
    ('tving', 'TVING'),
    ('sk', 'SK'),
]
LOGO_PRIORITY_LABEL_MAP = {item[0]: item[1] for item in LOGO_PRIORITY_OPTIONS}


def _parse_logo_priority_csv(text_value):
    items = []
    seen = set()
    for raw in str(text_value or '').split(','):
        key = ''.join(ch for ch in str(raw).strip().lower() if ch.isalnum())
        if not key or key in seen:
            continue
        if key not in LOGO_PRIORITY_LABEL_MAP:
                continue
        seen.add(key)
        items.append(key)
    for key, _label in LOGO_PRIORITY_OPTIONS:
        if key not in seen:
            items.append(key)
    return items


def _build_logo_priority_state(text_value):
    items = _parse_logo_priority_csv(text_value)
    return {
        'items': [{'key': key, 'label': LOGO_PRIORITY_LABEL_MAP.get(key, key.upper())} for key in items],
        'text': ','.join(items),
    }


def _detect_provider_from_channel_id(channel_id):
    cid = str(channel_id or '').strip().lower()
    if not cid:
        return ''

    for sep in ['.', '_', '-', ':', '/']:
        if sep in cid:
            parts = [x for x in cid.split(sep) if x]
            for token in reversed(parts):
                token = ''.join(ch for ch in token if ch.isalnum())
                if token in EPG_PROVIDER_LABEL_MAP:
                    return token

    compact = ''.join(ch for ch in cid if ch.isalnum())
    for key in EPG_PROVIDER_LABEL_MAP.keys():
        if compact.endswith(key) or compact.startswith(key):
            return key
    return ''


def _parse_provider_csv(text_value):
    items = []
    seen = set()
    for raw in str(text_value or '').split(','):
        key = ''.join(ch for ch in str(raw).strip().lower() if ch.isalnum())
        if not key or key in seen:
            continue
        seen.add(key)
        items.append(key)
    return items


def _build_epg_provider_rows(enabled_csv='', priority_csv=''):
    base_ids = [item[0] for item in EPG_PROVIDER_OPTIONS]
    base_map = {item[0]: item[1] for item in EPG_PROVIDER_OPTIONS}

    enabled_list = _parse_provider_csv(enabled_csv)
    if not enabled_list:
        enabled_list = list(base_ids)
    enabled_set = {x for x in enabled_list if x in base_map}

    priority_list = [x for x in _parse_provider_csv(priority_csv) if x in base_map]
    for key in base_ids:
        if key not in priority_list:
            priority_list.append(key)

    rows = []
    for key in priority_list:
        rows.append({
            'key': key,
            'label': base_map[key],
            'enabled': key in enabled_set,
        })

    return {
        'rows': rows,
        'priority_csv': ','.join(priority_list),
        'enabled_csv': ','.join([row['key'] for row in rows if row.get('enabled')]),
        'enabled_labels': [row['label'] for row in rows if row.get('enabled')],
        'disabled_labels': [row['label'] for row in rows if not row.get('enabled')],
    }


def _check_sjva_or_block(mode='html'):
    auth_info = _get_sjva_auth_info()
    if auth_info.get('ok'):
        return None

    logger.warning(f"[ff_tvh_m3u] sjva auth denied reason={auth_info.get('reason')} raw={auth_info.get('raw')}")
    message = auth_info.get('reason') or 'SJVA 인증이 필요합니다.'

    if mode == 'html':
        return _render_sjva_auth_required_page(message)

    if mode == 'json':
        return jsonify({
            'ret': 'warning',
            'msg': message,
            'sjva_auth_required': True,
        })

    return Response(message, status=403, mimetype='text/plain')


def _epg_cache_dir():
    path = '/data/ff_tvh_m3u_epg_cache'
    old_paths = [
        '/data/data/tvh_m3u_plugin/epg_cache',
        '/data/tmp/ff_tvh_m3u_epg_cache',
    ]
    os.makedirs(path, exist_ok=True)
    try:
        for old_path in old_paths:
            if not os.path.isdir(old_path):
                continue
            for name in ['myepg_raw.xml', 'myepg_raw.meta.json', 'myepg_tvh.xml', 'myepg_tvh.match.json', 'dlive_extra.xml']:
                src = os.path.join(old_path, name)
                dst = os.path.join(path, name)
                if os.path.exists(src) and not os.path.exists(dst):
                    shutil.copy2(src, dst)
    except Exception as e:
        logger.warning(f'[ff_tvh_m3u] migrate epg cache failed: {str(e)}')
    return path


def _epg_cache_xml_path():
    return os.path.join(_epg_cache_dir(), 'myepg_raw.xml')


def _epg_cache_tvh_xml_path():
    return os.path.join(_epg_cache_dir(), 'myepg_tvh.xml')


def _epg_cache_match_json_path():
    return os.path.join(_epg_cache_dir(), 'myepg_tvh.match.json')


def _epg_cache_dlive_xml_path():
    return os.path.join(_epg_cache_dir(), 'dlive_extra.xml')


def _epg_cache_meta_path():
    return os.path.join(_epg_cache_dir(), 'myepg_raw.meta.json')


def _epg_now():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def _safe_tag_name(tag):
    return str(tag).split('}', 1)[-1] if tag else ''


def _iter_file_chunks(path, chunk_size=1024 * 256):
    with open(path, 'rb') as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            yield chunk


def _normalize_epg_match_name(value):
    text = str(value or '').strip().lower()
    return ''.join(ch for ch in text if ch.isalnum())


def _strip_epg_channel_number_prefix(value):
    text = str(value or '').strip()
    return re.sub(r'^\s*\d+(?:\.\d+)?\s+', '', text).strip()


def _iter_epg_match_values(value):
    text = str(value or '').strip()
    if not text:
        return []
    values = []

    def append(value):
        item = str(value or '').strip()
        if item and item not in values:
            values.append(item)

    append(text)
    stripped = _strip_epg_channel_number_prefix(text)
    append(stripped)
    return values


def _get_epg_provider_state_from_settings():
    return _build_epg_provider_rows(
        P.ModelSetting.get('basic_epg_provider_enabled') or '',
        P.ModelSetting.get('basic_epg_provider_priority') or '',
    )


def _get_epg_enabled_provider_set():
    state = _get_epg_provider_state_from_settings()
    return {row.get('key') for row in state.get('rows', []) if row.get('enabled')}


def _get_epg_provider_rank_map():
    state = _get_epg_provider_state_from_settings()
    return {
        row.get('key'): index
        for index, row in enumerate(state.get('rows', []))
        if row.get('key')
    }


def _xml_start_tag(name, attrs):
    pieces = [f'<{name}']
    for key, value in (attrs or {}).items():
        pieces.append(f' {key}={quoteattr(str(value))}')
    pieces.append('>\n')
    return ''.join(pieces)


def _update_epg_channel_icon(channel_elem, base_url=''):
    display_names = []
    icon_elem = None
    current_icon_url = ''

    for child in list(channel_elem):
        tag = _safe_tag_name(child.tag)
        if tag == 'display-name':
            name_text = str(child.text or '').strip()
            if name_text:
                display_names.append(name_text)
        elif tag == 'icon':
            icon_elem = child
            current_icon_url = str(child.attrib.get('src') or '').strip()

    channel_name = display_names[0] if display_names else str(channel_elem.attrib.get('id') or '').strip()
    final_logo_url = Task.get_effective_logo_url(
        channel_name=channel_name,
        sheet_logo_url=current_icon_url,
        matched_channel_id='',
        base_url=base_url,
    )
    if not final_logo_url:
        return

    if icon_elem is None:
        icon_elem = ET.SubElement(channel_elem, 'icon')
    icon_elem.set('src', final_logo_url)


def _build_epg_tvh_cache(xml_path=None):
    raw_xml_path = xml_path or _epg_cache_xml_path()
    if not os.path.exists(raw_xml_path):
        return {
            'tvh_cache_exists': False,
            'tvh_file_size': 0,
            'tvh_channel_count': 0,
            'tvh_programme_count': 0,
        }

    cache_dir = _epg_cache_dir()
    tmp_path = os.path.join(cache_dir, 'myepg_tvh.tmp.xml')
    final_path = _epg_cache_tvh_xml_path()
    enabled_set = _get_epg_enabled_provider_set()
    rank_map = _get_epg_provider_rank_map()
    provider_state = _get_epg_provider_state_from_settings()
    provider_order = [
        row.get('key')
        for row in provider_state.get('rows', [])
        if row.get('enabled') and row.get('key')
    ]
    extra_provider_keys = ['']
    provider_match_order = list(provider_order)
    for key in extra_provider_keys:
        if key not in provider_match_order:
            provider_match_order.append(key)
    db_rules = {}
    try:
        db_rules = Task.load_db_rules()
    except Exception as e:
        logger.warning(f'[ff_tvh_m3u] load db rules for epg tvh cache failed: {str(e)}')

    def append_unique(values, value):
        text = str(value or '').strip()
        if text and text not in values:
            values.append(text)

    def get_db_rule_candidate_values(channel_name):
        values = []
        if not db_rules:
            return values
        try:
            info, _match_type = Task.match_channel(channel_name, db_rules)
            if not info:
                return values

            matched_id = str(info.get('channel_master_id') or '').strip()
            append_unique(values, info.get('standard_name'))
            if not matched_id:
                return values

            for rule_map in (db_rules or {}).values():
                if not isinstance(rule_map, dict):
                    continue
                for candidate_name, candidate_info in rule_map.items():
                    if str((candidate_info or {}).get('channel_master_id') or '').strip() == matched_id:
                        append_unique(values, candidate_name)
        except Exception as e:
            logger.warning(f'[ff_tvh_m3u] build epg db candidates failed channel={channel_name}: {str(e)}')
        return values

    base_url = ''
    try:
        base_url = Task._get_request_base_url()
    except Exception:
        base_url = ''

    root_tag = 'tv'
    root_attrs = {}
    root_seen = False
    epg_index = {}
    epg_entries = []

    def add_index(name_value, provider_key, entry):
        for candidate_value in _iter_epg_match_values(name_value):
            norm = _normalize_epg_match_name(candidate_value)
            if not norm:
                continue
            epg_index.setdefault(norm, {})
            previous = epg_index[norm].get(provider_key)
            if previous is None or entry.get('rank', 9999) < previous.get('rank', 9999):
                epg_index[norm][provider_key] = entry

    context = ET.iterparse(raw_xml_path, events=('start', 'end'))
    for event, elem in context:
        if event == 'start' and not root_seen:
            root_tag = _safe_tag_name(elem.tag) or 'tv'
            root_attrs = dict(elem.attrib or {})
            root_seen = True
            continue

        if event != 'end':
            continue

        tag = _safe_tag_name(elem.tag)
        if tag == 'channel':
            channel_id = str(elem.attrib.get('id') or '').strip()
            provider_key = _detect_provider_from_channel_id(channel_id)
            if provider_key and enabled_set and provider_key not in enabled_set:
                elem.clear()
                continue

            display_names = []
            for child in list(elem):
                if _safe_tag_name(child.tag) == 'display-name':
                    name_text = str(child.text or '').strip()
                    if name_text:
                        display_names.append(name_text)

            rank = rank_map.get(provider_key, 9999)
            entry = {
                'rank': rank,
                'provider': provider_key,
                'channel_id': channel_id,
                'display_names': display_names,
                'xml': ET.tostring(elem, encoding='utf-8'),
            }
            for name in display_names:
                add_index(name, provider_key, entry)
            add_index(channel_id, provider_key, entry)
            epg_entries.append(entry)
            elem.clear()
        elif tag == 'programme':
            elem.clear()

    selected_channels = []
    raw_id_to_uuids = {}
    matched_count = 0
    unmatched_count = 0
    match_rows = []

    def find_fallback_candidate(search_keys):
        for provider_key in provider_match_order:
            provider_entries = [
                item for item in epg_entries
                if item.get('provider') == provider_key
            ]
            for search_key in search_keys:
                if len(search_key) < 3:
                    continue
                for item in provider_entries:
                    for display_name in item.get('display_names') or []:
                        for candidate_value in _iter_epg_match_values(display_name):
                            epg_key = _normalize_epg_match_name(candidate_value)
                            if len(epg_key) < 3:
                                continue
                            if search_key in epg_key or epg_key in search_key:
                                return item, 'contains'
        return None, ''

    for row in ModelChannel.get_all():
        try:
            enabled = bool(getattr(row, 'enabled', True))
        except Exception:
            enabled = True
        if not enabled:
            continue

        channel_uuid = str(getattr(row, 'channel_uuid', '') or '').strip()
        channel_name = str(getattr(row, 'name', '') or '').strip()
        if not channel_uuid or not channel_name:
            continue

        search_keys = []
        candidate_values = [
            channel_name,
            getattr(row, 'sheet_channel_id', ''),
            getattr(row, 'sheet_group_name', ''),
        ] + get_db_rule_candidate_values(channel_name)
        for value in candidate_values:
            for candidate_value in _iter_epg_match_values(value):
                norm = _normalize_epg_match_name(candidate_value)
                if norm and norm not in search_keys:
                    search_keys.append(norm)

        selected = None
        match_rule = ''
        for provider_key in provider_match_order:
            for search_key in search_keys:
                candidate = epg_index.get(search_key, {}).get(provider_key)
                if candidate is not None:
                    selected = candidate
                    match_rule = 'exact'
                    break
            if selected is not None:
                break

        if selected is None:
            selected, match_rule = find_fallback_candidate(search_keys)

        if selected is None:
            unmatched_count += 1
            match_rows.append({
                'channel_uuid': channel_uuid,
                'channel_name': channel_name,
                'matched': False,
                'search_keys': search_keys,
            })
            continue

        try:
            channel_elem = ET.fromstring(selected.get('xml') or b'')
        except Exception:
            unmatched_count += 1
            match_rows.append({
                'channel_uuid': channel_uuid,
                'channel_name': channel_name,
                'matched': False,
                'reason': 'selected_xml_parse_failed',
                'search_keys': search_keys,
                'source_channel_id': selected.get('channel_id'),
                'source_display_names': selected.get('display_names') or [],
            })
            continue

        channel_elem.set('id', channel_uuid)
        first_display = None
        for child in list(channel_elem):
            if _safe_tag_name(child.tag) == 'display-name':
                first_display = child
                break
        if first_display is None:
            first_display = ET.SubElement(channel_elem, 'display-name')
        first_display.text = channel_name

        _update_epg_channel_icon(channel_elem, base_url=base_url)
        selected_channels.append({
            'channel_uuid': channel_uuid,
            'source_channel_id': selected.get('channel_id'),
            'xml': ET.tostring(channel_elem, encoding='utf-8'),
        })
        raw_id_to_uuids.setdefault(selected.get('channel_id'), []).append(channel_uuid)
        matched_count += 1
        match_rows.append({
            'channel_uuid': channel_uuid,
            'channel_name': channel_name,
            'matched': True,
            'match_rule': match_rule,
            'matched_provider': selected.get('provider'),
            'source_channel_id': selected.get('channel_id'),
            'source_display_names': selected.get('display_names') or [],
            'search_keys': search_keys,
        })

    channel_count = 0
    programme_count = 0
    with open(tmp_path, 'wb') as fw:
        fw.write(b'<?xml version="1.0" encoding="UTF-8"?>\n')
        fw.write(_xml_start_tag(root_tag, root_attrs).encode('utf-8'))

        for item in selected_channels:
            fw.write(item.get('xml') or b'')
            fw.write(b'\n')
            channel_count += 1

        context = ET.iterparse(raw_xml_path, events=('end',))
        for _event, elem in context:
            tag = _safe_tag_name(elem.tag)
            if tag == 'programme':
                channel_id = str(elem.attrib.get('channel') or '').strip()
                target_uuids = raw_id_to_uuids.get(channel_id) or []
                for target_uuid in target_uuids:
                    elem.set('channel', target_uuid)
                    fw.write(ET.tostring(elem, encoding='utf-8'))
                    fw.write(b'\n')
                    programme_count += 1
                elem.clear()
            elif tag == 'channel':
                elem.clear()

        fw.write(f'</{root_tag}>\n'.encode('utf-8'))

    os.replace(tmp_path, final_path)
    try:
        with open(_epg_cache_match_json_path(), 'w', encoding='utf-8') as f:
            json.dump({
                'created_at': _epg_now(),
                'matched_count': matched_count,
                'unmatched_count': unmatched_count,
                'provider_order': provider_order,
                'provider_match_order': provider_match_order,
                'rows': match_rows,
            }, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f'[ff_tvh_m3u] save epg tvh match json failed: {str(e)}')

    file_size = os.path.getsize(final_path) if os.path.exists(final_path) else 0
    return {
        'tvh_cache_exists': os.path.exists(final_path),
        'tvh_cache_path': final_path,
        'tvh_file_size': file_size,
        'tvh_channel_count': channel_count,
        'tvh_programme_count': programme_count,
        'tvh_matched_channel_count': matched_count,
        'tvh_unmatched_channel_count': unmatched_count,
        'tvh_provider_enabled': provider_state.get('enabled_csv', ''),
        'tvh_provider_priority': provider_state.get('priority_csv', ''),
    }


def _epg_tvh_cache_needs_rebuild():
    raw_path = _epg_cache_xml_path()
    tvh_path = _epg_cache_tvh_xml_path()
    if not os.path.exists(raw_path):
        return False
    if not os.path.exists(tvh_path):
        return True
    try:
        if os.path.getmtime(raw_path) > os.path.getmtime(tvh_path):
            return True
    except Exception:
        return True

    meta = _load_epg_meta()
    provider_state = _get_epg_provider_state_from_settings()
    if str(meta.get('tvh_provider_enabled') or '') != str(provider_state.get('enabled_csv') or ''):
        return True
    if str(meta.get('tvh_provider_priority') or '') != str(provider_state.get('priority_csv') or ''):
        return True
    return False


def _load_epg_meta():
    path = _epg_cache_meta_path()
    if not os.path.exists(path):
        return {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except Exception as e:
        logger.warning(f'[ff_tvh_m3u] load epg meta failed: {str(e)}')
    return {}


def _save_epg_meta(meta):
    try:
        with open(_epg_cache_meta_path(), 'w', encoding='utf-8') as f:
            json.dump(meta or {}, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f'[ff_tvh_m3u] save epg meta failed: {str(e)}')


def _summarize_epg_xml(xml_path, sample_limit=None):
    channel_count = 0
    programme_count = 0
    icon_count = 0
    sample_channels = []
    provider_channel_counts = {}

    context = ET.iterparse(xml_path, events=('end',))
    for _event, elem in context:
        tag = _safe_tag_name(elem.tag)
        if tag == 'channel':
            channel_count += 1
            channel_id = (elem.attrib.get('id') or '').strip()
            provider_key = _detect_provider_from_channel_id(channel_id)
            if provider_key:
                provider_channel_counts[provider_key] = provider_channel_counts.get(provider_key, 0) + 1
            display_names = []
            icon_url = ''
            for child in list(elem):
                child_tag = _safe_tag_name(child.tag)
                if child_tag == 'display-name':
                    name = (child.text or '').strip()
                    if name:
                        display_names.append(name)
                elif child_tag == 'icon':
                    src = (child.attrib.get('src') or '').strip()
                    if src:
                        icon_url = src
                        icon_count += 1
            if sample_limit is None or len(sample_channels) < sample_limit:
                sample_channels.append({
                    'id': channel_id,
                    'name': display_names[0] if display_names else '',
                    'icon_url': icon_url,
                    'display_names': display_names,
                    'provider': provider_key,
                    'provider_label': EPG_PROVIDER_LABEL_MAP.get(provider_key, ''),
                })
            elem.clear()
        elif tag == 'programme':
            programme_count += 1
            elem.clear()

    provider_rows = []
    for key, label in EPG_PROVIDER_OPTIONS:
        count = int(provider_channel_counts.get(key, 0) or 0)
        if count > 0:
            provider_rows.append({
                'key': key,
                'label': label,
                'count': count,
            })

    detected_provider_keys = [row['key'] for row in provider_rows]
    if len(detected_provider_keys) >= 2:
        provider_mode = 'integrated'
    elif len(detected_provider_keys) == 1:
        provider_mode = 'single'
    else:
        provider_mode = 'unknown'

    file_size = os.path.getsize(xml_path) if os.path.exists(xml_path) else 0
    return {
        'exists': os.path.exists(xml_path),
        'xml_path': xml_path,
        'file_size': file_size,
        'channel_count': channel_count,
        'programme_count': programme_count,
        'icon_count': icon_count,
        'sample_channels': sample_channels,
        'detected_provider_keys': detected_provider_keys,
        'provider_rows': provider_rows,
        'provider_mode': provider_mode,
    }


def _prepare_epg_xml_from_url(url, verify_ssl=True, timeout=60):
    cache_dir = _epg_cache_dir()
    tmp_download = os.path.join(cache_dir, 'myepg_download.tmp')
    tmp_xml = os.path.join(cache_dir, 'myepg_raw.tmp.xml')
    tmp_merged = os.path.join(cache_dir, 'myepg_merged.tmp.xml')
    final_xml = _epg_cache_xml_path()

    with requests.get(url, stream=True, timeout=(10, timeout), verify=verify_ssl, headers={'User-Agent': 'ff_tvh_m3u/epg'}) as resp:
        resp.raise_for_status()
        with open(tmp_download, 'wb') as fw:
            for chunk in resp.iter_content(chunk_size=1024 * 128):
                if chunk:
                    fw.write(chunk)

    is_gzip = False
    try:
        with open(tmp_download, 'rb') as fr:
            head = fr.read(2)
            is_gzip = head == b'\x1f\x8b'
    except Exception:
        is_gzip = False

    if is_gzip or str(url).lower().endswith('.gz'):
        with gzip.open(tmp_download, 'rb') as fr, open(tmp_xml, 'wb') as fw:
            shutil.copyfileobj(fr, fw)
    else:
        shutil.copyfile(tmp_download, tmp_xml)

    extra_sources = []
    if _is_epg_dlive_enabled():
        dlive_channel_name = str(P.ModelSetting.get('basic_epg_dlive_channel_name') or '').strip() or '지역채널'
        dlive_channel_id = str(P.ModelSetting.get('basic_epg_dlive_channel_id') or '').strip() or 'DLIVE_SONGPA'
        dlive_schedule_url = str(P.ModelSetting.get('basic_epg_dlive_schedule_url') or '').strip() or DEFAULT_DLIVE_SCHEDULE_URL
        dlive_xml = build_dlive_epg_xml_bytes(
            channel_name=dlive_channel_name,
            channel_id=dlive_channel_id,
            url_template=dlive_schedule_url,
            days=2,
        )
        with open(_epg_cache_dlive_xml_path(), 'wb') as fw:
            fw.write(dlive_xml)
        extra_sources.append({
            'key': 'dlive',
            'xml_bytes': dlive_xml,
        })

    if extra_sources:
        merge_xmltv_files(tmp_xml, extra_sources, tmp_merged)
        os.replace(tmp_merged, final_xml)
        try:
            os.remove(tmp_xml)
        except Exception:
            pass
    else:
        os.replace(tmp_xml, final_xml)

    try:
        os.remove(tmp_download)
    except Exception:
        pass
    return final_xml


def _fetch_epg_and_build_meta(url, verify_ssl=True):
    xml_path = _prepare_epg_xml_from_url(url, verify_ssl=verify_ssl)
    summary = _summarize_epg_xml(xml_path)
    tvh_summary = _build_epg_tvh_cache(xml_path)
    meta = {
        'ret': 'success',
        'fetched_at': _epg_now(),
        'source_url': url,
        'cache_exists': summary.get('exists', False),
        'file_size': summary.get('file_size', 0),
        'channel_count': summary.get('channel_count', 0),
        'programme_count': summary.get('programme_count', 0),
        'icon_count': summary.get('icon_count', 0),
        'sample_channels': summary.get('sample_channels', []),
        'detected_provider_keys': summary.get('detected_provider_keys', []),
        'provider_rows': summary.get('provider_rows', []),
        'provider_mode': summary.get('provider_mode', 'unknown'),
        'tvh_cache_exists': tvh_summary.get('tvh_cache_exists', False),
        'tvh_file_size': tvh_summary.get('tvh_file_size', 0),
        'tvh_channel_count': tvh_summary.get('tvh_channel_count', 0),
        'tvh_programme_count': tvh_summary.get('tvh_programme_count', 0),
        'tvh_provider_enabled': tvh_summary.get('tvh_provider_enabled', ''),
        'tvh_provider_priority': tvh_summary.get('tvh_provider_priority', ''),
        'dlive_enabled': _is_epg_dlive_enabled(),
    }
    _save_epg_meta(meta)
    return meta


def _get_epg_status_payload():
    meta = _load_epg_meta()
    xml_path = _epg_cache_xml_path()
    exists = os.path.exists(xml_path)
    tvh_xml_path = _epg_cache_tvh_xml_path()
    tvh_exists = os.path.exists(tvh_xml_path)
    payload = {
        'ret': 'success' if exists else 'warning',
        'cache_exists': exists,
        'tvh_cache_exists': tvh_exists,
        'fetched_at': meta.get('fetched_at', ''),
        'source_url': meta.get('source_url', ''),
        'file_size': meta.get('file_size', 0),
        'channel_count': meta.get('channel_count', 0),
        'programme_count': meta.get('programme_count', 0),
        'icon_count': meta.get('icon_count', 0),
        'tvh_file_size': meta.get('tvh_file_size', os.path.getsize(tvh_xml_path) if tvh_exists else 0),
        'tvh_channel_count': meta.get('tvh_channel_count', 0),
        'tvh_programme_count': meta.get('tvh_programme_count', 0),
        'sample_channels': meta.get('sample_channels', []),
        'detected_provider_keys': meta.get('detected_provider_keys', []),
        'provider_rows': meta.get('provider_rows', []),
        'provider_mode': meta.get('provider_mode', 'unknown'),
        'msg': '캐시된 EPG 상태를 불러왔습니다.' if exists else '저장된 EPG 캐시가 없습니다.',
    }
    return payload



_EPG_AUTO_THREAD_STARTED = False
_EPG_AUTO_LOCK = threading.Lock()


def _normalize_epg_auto_time(value):
    text = str(value or '').strip()
    try:
        hour, minute = text.split(':', 1)
        hour = int(hour)
        minute = int(minute)
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            return f'{hour:02d}:{minute:02d}'
    except Exception:
        pass
    return '03:30'


def _is_epg_dlive_enabled():
    value = str(P.ModelSetting.get('basic_epg_dlive_enabled') or 'False').strip().lower()
    return value in ['true', 'on', '1', 'yes', 'y']


def _is_epg_auto_enabled():
    value = str(P.ModelSetting.get('basic_epg_auto_enabled') or 'False').strip().lower()
    return value in ['true', 'on', '1', 'yes', 'y']


def _run_epg_fetch_from_settings(trigger='manual'):
    epg_url = (P.ModelSetting.get('basic_epg_xml_url') or '').strip()
    if not epg_url:
        raise Exception('myepg XML 주소가 비어 있습니다.')

    verify_ssl = str(P.ModelSetting.get('basic_tvh_use_verify_ssl') or 'False').strip().lower() in ['true', 'on', '1', 'yes', 'y']
    meta = _fetch_epg_and_build_meta(epg_url, verify_ssl=verify_ssl)

    P.ModelSetting.set('basic_epg_last_fetch_time', meta.get('fetched_at', ''))
    P.ModelSetting.set('basic_epg_channel_count', str(meta.get('channel_count', 0)))
    P.ModelSetting.set('basic_epg_programme_count', str(meta.get('programme_count', 0)))
    P.ModelSetting.set('basic_epg_icon_count', str(meta.get('icon_count', 0)))
    P.ModelSetting.set('basic_epg_file_size', str(meta.get('file_size', 0)))

    if trigger == 'auto':
        P.ModelSetting.set(
            'basic_epg_auto_last_result',
            f"{meta.get('fetched_at', '')} 성공 / 채널 {meta.get('channel_count', 0)} / 편성 {meta.get('programme_count', 0)}"
        )

    return meta


def _epg_auto_scheduler_loop():
    while True:
        try:
            if _is_epg_auto_enabled():
                target_time = _normalize_epg_auto_time(P.ModelSetting.get('basic_epg_auto_time') or '03:30')
                now = _epg_auto_dt.datetime.now()
                today = now.strftime('%Y-%m-%d')
                now_hm = now.strftime('%H:%M')
                last_run_date = str(P.ModelSetting.get('basic_epg_auto_last_run_date') or '').strip()

                if now_hm >= target_time and last_run_date != today:
                    try:
                        _run_epg_fetch_from_settings(trigger='auto')
                        P.ModelSetting.set('basic_epg_auto_last_run_date', today)
                    except Exception as e:
                        P.ModelSetting.set('basic_epg_auto_last_result', f"{now.strftime('%Y-%m-%d %H:%M:%S')} 실패 / {str(e)}")
                        logger.exception(f'[ff_tvh_m3u] epg auto refresh failed: {str(e)}')
        except Exception as e:
            logger.exception(f'[ff_tvh_m3u] epg auto scheduler loop failed: {str(e)}')

        time.sleep(30)


def _ensure_epg_auto_scheduler_started():
    global _EPG_AUTO_THREAD_STARTED
    if _EPG_AUTO_THREAD_STARTED:
        return

    with _EPG_AUTO_LOCK:
        if _EPG_AUTO_THREAD_STARTED:
            return

        thread = threading.Thread(target=_epg_auto_scheduler_loop, name='ff_tvh_m3u_epg_auto', daemon=True)
        thread.start()
        _EPG_AUTO_THREAD_STARTED = True
        logger.info('[ff_tvh_m3u] epg auto scheduler started')


CUSTOM_LOGO_MAX_BYTES = 5 * 1024 * 1024
CUSTOM_LOGO_ALLOWED_EXTS = ['.png', '.jpg', '.jpeg', '.gif', '.webp', '.svg']


def _custom_logo_mirror_token():
    return (
        str(P.ModelSetting.get('basic_custom_logo_mirror_token') or '').strip()
        or str(os.environ.get('TVH_M3U_CUSTOM_LOGO_MIRROR_TOKEN') or '').strip()
    )


def _custom_logo_mirror_url():
    return (
        str(P.ModelSetting.get('basic_custom_logo_mirror_url') or '').strip()
        or 'https://ff.aha3011.mywire.org/tvh_m3u_plugin/api/custom_logo_mirror'
    )


def _sha1_bytes(data):
    return hashlib.sha1(data or b'').hexdigest()


def _save_custom_logo_file(file_storage, source_channel_name):
    if file_storage is None:
        raise Exception('업로드할 로고 파일이 없습니다.')

    original_filename = os.path.basename(str(file_storage.filename or '').strip())
    stored_filename = Task._make_uploaded_logo_filename(source_channel_name, original_filename)
    ext = os.path.splitext(stored_filename)[1].lower()
    if ext not in CUSTOM_LOGO_ALLOWED_EXTS:
        raise Exception('지원하지 않는 이미지 확장자입니다.')

    data = file_storage.read()
    if not data:
        raise Exception('업로드한 파일이 비어 있습니다.')
    if len(data) > CUSTOM_LOGO_MAX_BYTES:
        raise Exception('로고 파일은 5MB 이하만 업로드할 수 있습니다.')

    asset_dir = Task._ensure_custom_logo_asset_dir()
    output_path = os.path.join(asset_dir, stored_filename)
    with open(output_path, 'wb') as f:
        f.write(data)

    return {
        'stored_filename': stored_filename,
        'output_path': output_path,
        'sha1': _sha1_bytes(data),
        'file_size': len(data),
    }


def _save_custom_logo_db(source_channel_name, standard_name='', aka_name='', stored_filename='', sha1='', file_size=0):
    source_channel_name = str(source_channel_name or '').strip()
    standard_name = str(standard_name or '').strip() or source_channel_name
    aka_name = str(aka_name or '').strip()
    stored_filename = os.path.basename(str(stored_filename or '').strip())
    matched_channel_id = Task._resolve_lookup_matched_channel_id(standard_name) or Task._resolve_lookup_matched_channel_id(source_channel_name)
    logo_url_template = Task._build_logo_public_template(stored_filename)

    con = Task._connect_write_db()
    cur = con.cursor()
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

    cols = [row['name'] for row in cur.execute("PRAGMA table_info(custom_logo)").fetchall()]
    now = _epg_now()

    if 'source_channel_name' in cols and 'matched_channel_id' in cols:
        cur.execute(
            "DELETE FROM custom_logo WHERE source_channel_name = ? AND COALESCE(matched_channel_id, '') = ?",
            (source_channel_name, matched_channel_id),
        )

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
        'file_size': file_size,
        'created_at': now,
        'updated_at': now,
        'created_time': now,
        'updated_time': now,
    }

    insert_cols = [col for col in cols if col in values]
    if insert_cols:
        placeholders = ','.join(['?'] * len(insert_cols))
        cur.execute(
            f"INSERT INTO custom_logo ({','.join(insert_cols)}) VALUES ({placeholders})",
            [values[col] for col in insert_cols],
        )

    con.commit()
    con.close()

    try:
        Task._load_logo_cache(force=True)
    except Exception:
        pass

    return {
        'matched_channel_id': matched_channel_id,
        'standard_name': standard_name,
        'logo_url_template': logo_url_template,
    }


def _mirror_custom_logo_to_owner(source_channel_name, standard_name, aka_name, file_path, stored_filename, sha1, file_size):
    token = _custom_logo_mirror_token()
    if not token:
        return {'ret': 'skipped', 'msg': '원격 백업 토큰이 설정되지 않았습니다.'}

    url = _custom_logo_mirror_url()
    if not url:
        return {'ret': 'skipped', 'msg': '원격 백업 주소가 설정되지 않았습니다.'}

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
            headers = {'X-Custom-Logo-Token': token}
            resp = requests.post(url, data=data, files=files, headers=headers, timeout=30)
            resp.raise_for_status()
            try:
                return resp.json()
            except Exception:
                return {'ret': 'success', 'msg': '원격 백업 완료'}
    except Exception as e:
        logger.warning(f'[ff_tvh_m3u] custom logo mirror failed: {str(e)}')
        return {'ret': 'warning', 'msg': f'원격 백업 실패: {str(e)}'}


def _handle_custom_logo_upload(req):
    source_channel_name = str(req.form.get('source_channel_name') or '').strip()
    standard_name = str(req.form.get('standard_name') or '').strip()
    aka_name = str(req.form.get('aka_name') or '').strip()
    logo_file = req.files.get('logo_file')

    if not source_channel_name:
        return {'ret': 'warning', 'msg': '원본 채널명을 입력하세요.'}

    saved = _save_custom_logo_file(logo_file, source_channel_name)
    db_info = _save_custom_logo_db(
        source_channel_name=source_channel_name,
        standard_name=standard_name,
        aka_name=aka_name,
        stored_filename=saved['stored_filename'],
        sha1=saved['sha1'],
        file_size=saved['file_size'],
    )

    mirror = _mirror_custom_logo_to_owner(
        source_channel_name=source_channel_name,
        standard_name=db_info.get('standard_name') or standard_name or source_channel_name,
        aka_name=aka_name,
        file_path=saved['output_path'],
        stored_filename=saved['stored_filename'],
        sha1=saved['sha1'],
        file_size=saved['file_size'],
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


def _handle_custom_logo_mirror(req):
    expected = _custom_logo_mirror_token()
    received = str(req.headers.get('X-Custom-Logo-Token') or '').strip()

    if not expected:
        return {'ret': 'danger', 'msg': '원격 백업 토큰이 설정되지 않았습니다.'}
    if received != expected:
        return {'ret': 'danger', 'msg': '원격 백업 토큰이 올바르지 않습니다.'}

    source_channel_name = str(req.form.get('source_channel_name') or '').strip()
    standard_name = str(req.form.get('standard_name') or '').strip()
    aka_name = str(req.form.get('aka_name') or '').strip()
    logo_file = req.files.get('logo_file')

    if not source_channel_name:
        return {'ret': 'warning', 'msg': '원본 채널명이 비어 있습니다.'}

    saved = _save_custom_logo_file(logo_file, source_channel_name)
    db_info = _save_custom_logo_db(
        source_channel_name=source_channel_name,
        standard_name=standard_name,
        aka_name=aka_name,
        stored_filename=saved['stored_filename'],
        sha1=saved['sha1'],
        file_size=saved['file_size'],
    )

    return {
        'ret': 'success',
        'msg': '원격 백업 저장 완료',
        'stored_filename': saved['stored_filename'],
        'standard_name': db_info.get('standard_name') or '',
        'matched_channel_id': db_info.get('matched_channel_id') or '',
    }

class ModuleBasic(PluginModuleBase):
    db_default = {
        'basic_tvh_api_base': '',
        'basic_tvh_stream_base': '',
        'basic_tvh_admin_username': '',
        'basic_tvh_admin_password': '',
        'basic_tvh_play_username': '',
        'basic_tvh_play_password': '',
        'basic_tvh_stream_profile': '',
        'basic_tvh_include_auth_in_url': 'False',
        'basic_tvh_use_verify_ssl': 'False',
        'basic_last_sync_time': '',
        'basic_last_sync_count': '0',
        'basic_match_last_run_time': '',
        'basic_match_last_count': '0',
        'basic_match_last_unmatched_count': '0',
        'basic_match_source': 'https://ff.aha3011.mywire.org/ff_tvh_sheet_write/api/basic',
        'basic_match_source_mode': 'remote',
        'basic_match_source_remote': 'https://ff.aha3011.mywire.org/ff_tvh_sheet_write/api/basic',
        'basic_epg_xml_url': '',
        'basic_epg_last_fetch_time': '',
        'basic_epg_channel_count': '0',
        'basic_epg_programme_count': '0',
        'basic_epg_icon_count': '0',
        'basic_epg_file_size': '0',
        'basic_epg_provider_priority': 'kt,lgu,sk,daum,naver,wavve,tving,spotv',
        'basic_epg_provider_enabled': 'kt,lgu,sk,daum,naver,wavve,tving,spotv',
        'basic_epg_dlive_enabled': 'False',
        'basic_epg_dlive_channel_name': '지역채널',
        'basic_epg_dlive_channel_id': 'DLIVE_SONGPA',
        'basic_epg_dlive_schedule_url': DEFAULT_DLIVE_SCHEDULE_URL,
        'basic_epg_auto_enabled': 'False',
        'basic_epg_auto_time': '03:30',
        'basic_epg_auto_last_run_date': '',
        'basic_epg_auto_last_result': '',
        'basic_custom_logo_mirror_url': 'https://ff.aha3011.mywire.org/tvh_m3u_plugin/normal/custom_logo_mirror',
        'basic_custom_logo_mirror_token': '',
        'basic_logo_priority': 'custom,kt,wavve,tving,sk',
    }

    def __init__(self, P):
        super(ModuleBasic, self).__init__(P, name='basic', first_menu='sync')
        try:
            _ensure_epg_auto_scheduler_started()
        except Exception as e:
            logger.exception(f'[ff_tvh_m3u] epg auto scheduler start failed: {str(e)}')

    def process_menu(self, sub, req):
        try:
            gate = _check_sjva_or_block('html')
            if gate is not None:
                return gate

            sub = sub or 'sync'
            if sub == 'logoadd':
                sub = 'addlogo'

            referrer = request.headers.get('Referer', '') or ''
            is_internal_referrer = f'/{P.package_name}/' in referrer

            if sub != 'sync' and not is_internal_referrer:
                return redirect(f'/{P.package_name}/basic/sync')

            logger.debug(f'[ff_tvh_m3u] process_menu sub={sub}')
            logger.debug(f'[ff_tvh_m3u] db engine url = {db.engine.url}')
            logger.debug(f'[ff_tvh_m3u] dedicated db path = {DB_PATH}')

            arg = P.ModelSetting.to_dict()
            arg['package_name'] = P.package_name
            arg['page_sub'] = sub
            arg['ajax_sub'] = self.name
            arg['m3u_url'] = ToolUtil.make_apikey_url(f"/{P.package_name}/api/m3u")
            arg['m3u_tvh_url'] = ToolUtil.make_apikey_url(f"/{P.package_name}/api/m3u_tvh")
            arg['m3u_tivimate_url'] = ToolUtil.make_apikey_url(f"/{P.package_name}/api/m3u_tivimate")
            arg['epg_raw_url'] = ToolUtil.make_apikey_url(f"/{P.package_name}/api/epg_raw")
            arg['epg_tvh_url'] = ToolUtil.make_apikey_url(f"/{P.package_name}/api/epg_tvh")
            arg['epg_tivimate_url'] = ToolUtil.make_apikey_url(f"/{P.package_name}/api/epg_tivimate")
            logo_priority_state = _build_logo_priority_state(P.ModelSetting.get('basic_logo_priority') or Task.get_logo_priority_text())
            arg['logo_priority_text'] = logo_priority_state.get('text', '')
            arg['logo_priority_items'] = logo_priority_state.get('items', [])
            arg['last_sync_time'] = P.ModelSetting.get('basic_last_sync_time')
            arg['last_sync_count'] = P.ModelSetting.get('basic_last_sync_count')
            arg['match_last_run_time'] = P.ModelSetting.get('basic_match_last_run_time')
            arg['match_last_count'] = P.ModelSetting.get('basic_match_last_count')
            arg['match_last_unmatched_count'] = P.ModelSetting.get('basic_match_last_unmatched_count')
            arg['match_source'] = P.ModelSetting.get('basic_match_source') or 'https://ff.aha3011.mywire.org/ff_tvh_sheet_write/api/basic'
            arg['basic_match_source_mode'] = 'remote'
            arg['basic_match_source_remote'] = 'https://ff.aha3011.mywire.org/ff_tvh_sheet_write/api/basic'
            try:
                arg['match_source_info'] = Task.get_match_source_info()
            except Exception:
                arg['match_source_info'] = {}
            arg['grouped_channels'] = ModelChannel.get_grouped()
            arg['tag_count'] = len(ModelTag.get_all())
            arg['channel_count'] = len(ModelChannel.get_all())
            arg['group_count'] = len(ModelGroupOrder.get_all())
            arg['ungrouped_channels'] = ModelChannel.get_ungrouped()
            arg['ungrouped_count'] = len(arg['ungrouped_channels'])
            arg['assignable_group_names'] = ModelChannel.get_assignable_group_names()
            arg['match_source_info'] = Task.get_match_source_info()
            arg['play_profile_list'] = []
            epg_meta = _load_epg_meta()
            arg['basic_epg_xml_url'] = P.ModelSetting.get('basic_epg_xml_url') or ''
            arg['basic_epg_last_fetch_time'] = P.ModelSetting.get('basic_epg_last_fetch_time') or epg_meta.get('fetched_at', '')
            arg['basic_epg_channel_count'] = P.ModelSetting.get('basic_epg_channel_count') or str(epg_meta.get('channel_count', 0))
            arg['basic_epg_programme_count'] = P.ModelSetting.get('basic_epg_programme_count') or str(epg_meta.get('programme_count', 0))
            arg['basic_epg_icon_count'] = P.ModelSetting.get('basic_epg_icon_count') or str(epg_meta.get('icon_count', 0))
            arg['basic_epg_file_size'] = P.ModelSetting.get('basic_epg_file_size') or str(epg_meta.get('file_size', 0))
            arg['epg_sample_channels'] = epg_meta.get('sample_channels', []) or []
            arg['epg_cache_exists'] = os.path.exists(_epg_cache_xml_path())
            arg['basic_epg_dlive_enabled'] = (
                'True'
                if str(P.ModelSetting.get('basic_epg_dlive_enabled') or 'False').strip().lower() in ['true', 'on', '1', 'yes', 'y']
                else 'False'
            )
            arg['basic_epg_dlive_channel_name'] = P.ModelSetting.get('basic_epg_dlive_channel_name') or '지역채널'
            arg['basic_epg_dlive_channel_id'] = P.ModelSetting.get('basic_epg_dlive_channel_id') or 'DLIVE_SONGPA'
            arg['basic_epg_dlive_schedule_url'] = P.ModelSetting.get('basic_epg_dlive_schedule_url') or DEFAULT_DLIVE_SCHEDULE_URL
            epg_provider_state = _build_epg_provider_rows(
                P.ModelSetting.get('basic_epg_provider_enabled') or '',
                P.ModelSetting.get('basic_epg_provider_priority') or '',
            )
            arg['basic_epg_provider_priority'] = epg_provider_state.get('priority_csv', '')
            arg['basic_epg_provider_enabled'] = epg_provider_state.get('enabled_csv', '')
            arg['epg_provider_rows'] = epg_provider_state.get('rows', [])
            arg['epg_provider_enabled_labels'] = epg_provider_state.get('enabled_labels', [])
            arg['epg_provider_disabled_labels'] = epg_provider_state.get('disabled_labels', [])
            arg['epg_detected_provider_rows'] = epg_meta.get('provider_rows', []) or []
            arg['epg_provider_mode'] = epg_meta.get('provider_mode', 'unknown') or 'unknown'

            if sub == 'm3u':
                try:
                    profile_ret = Task.get_play_profiles()
                    if isinstance(profile_ret, dict):
                        arg['play_profile_list'] = profile_ret.get('profiles', []) or []
                except Exception as e:
                    logger.warning(f'[ff_tvh_m3u] process_menu get_play_profiles failed: {str(e)}')

                current_profile = str(arg.get('basic_tvh_stream_profile', '')).strip()
                if current_profile and current_profile not in arg['play_profile_list']:
                    arg['play_profile_list'].insert(0, current_profile)

                arg['group_profile_map'] = ModelGroupProfile.get_map()
                arg['channel_profile_map'] = ModelChannelProfile.get_map()

            arg['basic_tvh_use_verify_ssl'] = (
                'True'
                if str(arg.get('basic_tvh_use_verify_ssl', '')).strip().lower() in ['true', 'on', '1', 'yes', 'y']
                else 'False'
            )

            if sub == 'logo':
                logo_query = str(req.args.get('logo_q') or '').strip()
                logo_filter = str(req.args.get('logo_filter') or 'all').strip().lower()
                try:
                    base_url = request.host_url.rstrip('/')
                except Exception:
                    base_url = ''
                arg['logo_query'] = logo_query
                arg['logo_filter'] = logo_filter
                arg['logo_preview_rows'] = Task.get_logo_preview_rows(
                    base_url=base_url,
                    query=logo_query,
                    filter_mode=logo_filter,
                )
                arg['logo_preview_count'] = len(arg['logo_preview_rows'])
            elif sub == 'addlogo':
                arg['custom_logo_asset_dir'] = Task.get_custom_logo_asset_dir()

            arg['basic_tvh_include_auth_in_url'] = (
                'True'
                if str(arg.get('basic_tvh_include_auth_in_url', '')).strip().lower() in ['true', 'on', '1', 'yes', 'y']
                else 'False'
            )

            return render_template(
                f'{P.package_name}_{self.name}_setting.html',
                arg=arg
            )

        except Exception as e:
            logger.exception(f'[ff_tvh_m3u] process_menu exception: {str(e)}')
            return render_template('sample.html', title=f'{P.package_name} - {sub}')

    def process_ajax(self, sub, req):
        try:
            gate = _check_sjva_or_block('json')
            if gate is not None:
                return gate

            logger.debug(f'[ff_tvh_m3u] process_ajax sub={sub}')

            if sub == 'setting_save':
                save_info = _save_runtime_settings(req)
                if save_info.get('saved'):
                    return jsonify({'ret': 'success', 'msg': '설정을 저장했습니다.', 'save_info': save_info})
                return jsonify({'ret': 'danger', 'msg': '설정 저장 실패'})

            elif sub == 'test_connection':
                _save_runtime_settings(req)
                return jsonify(Task.test_connection())

            elif sub == 'test_admin_login':
                _save_runtime_settings(req)
                return jsonify(Task.test_admin_login())

            elif sub == 'test_play_login':
                _save_runtime_settings(req)
                return jsonify(Task.test_play_login())

            elif sub == 'load_play_profiles':
                _save_runtime_settings(req)
                return jsonify(Task.get_play_profiles())

            elif sub == 'save_logo_priority':
                raw_priority = request.form.get('logo_priority', '')
                state = _build_logo_priority_state(raw_priority)
                try:
                    P.ModelSetting.set('basic_logo_priority', state.get('text', ''))
                    return jsonify({
                        'ret': 'success',
                        'msg': '로고 우선순위를 저장했습니다.',
                        'logo_priority_text': state.get('text', ''),
                        'logo_priority_items': state.get('items', []),
                    })
                except Exception as e:
                    logger.exception(f'[ff_tvh_m3u] save_logo_priority failed: {str(e)}')
                    return jsonify({'ret': 'danger', 'msg': f'로고 우선순위 저장 실패: {str(e)}'})


            elif sub == 'logo_preview_select':
                return jsonify({
                    'ret': 'warning',
                    'msg': '로고 선택 저장 기능은 아직 지원하지 않습니다.',
                })

            elif sub == 'logo_preview_clear':
                return jsonify({
                    'ret': 'warning',
                    'msg': '로고 선택 초기화 기능은 아직 지원하지 않습니다.',
                })

            elif sub == 'upload_custom_logo':
                return jsonify(handle_custom_logo_upload(req))

            elif sub == 'epg_status':
                return jsonify(_get_epg_status_payload())

            elif sub == 'epg_fetch':
                _save_runtime_settings(req)
                epg_url = (P.ModelSetting.get('basic_epg_xml_url') or request.form.get('basic_epg_xml_url') or '').strip()
                if not epg_url:
                    return jsonify({'ret': 'warning', 'msg': 'myepg XML 주소를 먼저 입력하세요.'})
                try:
                    verify_ssl = str(P.ModelSetting.get('basic_tvh_use_verify_ssl') or 'False').strip().lower() in ['true', 'on', '1', 'yes', 'y']
                    meta = _fetch_epg_and_build_meta(epg_url, verify_ssl=verify_ssl)
                    P.ModelSetting.set('basic_epg_last_fetch_time', meta.get('fetched_at', ''))
                    P.ModelSetting.set('basic_epg_channel_count', str(meta.get('channel_count', 0)))
                    P.ModelSetting.set('basic_epg_programme_count', str(meta.get('programme_count', 0)))
                    P.ModelSetting.set('basic_epg_icon_count', str(meta.get('icon_count', 0)))
                    P.ModelSetting.set('basic_epg_file_size', str(meta.get('file_size', 0)))
                    provider_rows = meta.get('provider_rows', []) or []
                    if meta.get('provider_mode') == 'integrated':
                        provider_msg = ' / 통합 EPG 감지: ' + ', '.join([f"{row.get('label')}({row.get('count')})" for row in provider_rows])
                    elif meta.get('provider_mode') == 'single' and provider_rows:
                        row = provider_rows[0]
                        provider_msg = f" / 단일 EPG 감지: {row.get('label')}({row.get('count')}) - 우선순위 설정과 무관하게 계속 진행 가능"
                    else:
                        provider_msg = ''
                    return jsonify({
                        'ret': 'success',
                        'msg': f"EPG 원본 불러오기 완료 / 채널 {meta.get('channel_count', 0)} / 편성 {meta.get('programme_count', 0)}{provider_msg}",
                        **meta,
                    })
                except Exception as e:
                    logger.exception(f'[ff_tvh_m3u] epg_fetch failed: {str(e)}')
                    return jsonify({'ret': 'danger', 'msg': f'EPG 원본 불러오기 실패: {str(e)}'})

            elif sub == 'sync_channels':
                _save_runtime_settings(req)
                sync_ret = Task.sync_channels()
                if sync_ret.get('ret') != 'success':
                    return jsonify(sync_ret)

                match_ret = Task.apply_db_rules()
                final_ret = 'success'
                if match_ret.get('ret') == 'danger':
                    final_ret = 'danger'
                elif match_ret.get('ret') == 'warning':
                    final_ret = 'warning'

                return jsonify({
                    'ret': final_ret,
                    'msg': f"{sync_ret.get('msg', '')} / {match_ret.get('msg', '')}",
                    'sync': sync_ret,
                    'match': match_ret,
                })

            elif sub == 'apply_db_rules':
                _save_runtime_settings(req)
                return jsonify(Task.apply_db_rules())

            elif sub == 'reset_plugin_db':
                return jsonify(Task.reset_plugin_db())

            elif sub == 'search_match_channel':
                keyword = request.form.get('keyword')
                limit = request.form.get('limit', 30)
                return jsonify(Task.search_master_channels(keyword, limit=limit))

            elif sub == 'add_db_match_channel':
                channel_uuid = request.form.get('channel_uuid')
                channel_id = request.form.get('channel_id')
                return jsonify(Task.add_db_match_channel(channel_uuid, channel_id))

            elif sub == 'move_group':
                group_name = request.form.get('group_name')
                direction = request.form.get('direction')
                return jsonify(Task.move_group(group_name, direction))

            elif sub == 'assign_channels_to_group':
                channel_uuids = request.form.getlist('channel_uuids')
                new_group_name = request.form.get('new_group_name')
                target_group_name = request.form.get('target_group_name')
                return jsonify(Task.assign_channels_to_group(new_group_name, target_group_name, channel_uuids))

            elif sub == 'clear_manual_group':
                channel_uuids = request.form.getlist('channel_uuids')
                return jsonify(Task.clear_manual_group(channel_uuids))

            elif sub == 'save_group_profile':
                group_name = request.form.get('group_name')
                profile = request.form.get('profile')
                return jsonify(Task.save_group_profile(group_name, profile))

            elif sub == 'save_channel_profile':
                channel_uuid = request.form.get('channel_uuid')
                profile = request.form.get('profile')
                return jsonify(Task.save_channel_profile(channel_uuid, profile))

            elif sub == 'preview_m3u':
                _save_runtime_settings(req)
                target = (request.form.get('target') or 'tivimate').strip().lower()
                if target not in ['tvh', 'tivimate']:
                    target = 'tivimate'
                text = Task.build_m3u(target=target)
                preview = '\n'.join(text.splitlines()[:1000])
                return jsonify({'ret': 'success', 'preview': preview, 'msg': f'M3U 미리보기 생성 완료 ({target})'})

            return jsonify({'ret': 'warning', 'msg': f'알 수 없는 요청: {sub}'})

        except Exception as e:
            logger.exception(f'[ff_tvh_m3u] process_ajax exception: {str(e)}')
            return jsonify({'ret': 'danger', 'msg': str(e)})

    def process_normal(self, sub, req):
        try:
            if sub == 'custom_logo_mirror':
                return jsonify(handle_custom_logo_mirror(req))
            return jsonify({'ret': 'warning', 'msg': 'unknown normal request'})
        except Exception as e:
            logger.exception(f'[ff_tvh_m3u] process_normal exception: {str(e)}')
            return jsonify({'ret': 'danger', 'msg': str(e)})

    def process_api(self, sub, req):
        try:
            gate = _check_sjva_or_block('api')
            if gate is not None:
                return gate

            if sub == 'm3u':
                text = Task.build_m3u(target='tivimate')
                return Response(
                    text,
                    mimetype='audio/x-mpegurl',
                    headers={'Content-Disposition': 'inline; filename=tivimate_channels.m3u'}
                )

            elif sub == 'm3u_tvh':
                text = Task.build_m3u(target='tvh')
                return Response(
                    text,
                    mimetype='audio/x-mpegurl',
                    headers={'Content-Disposition': 'inline; filename=tvh_channels.m3u'}
                )

            elif sub == 'm3u_tivimate':
                text = Task.build_m3u(target='tivimate')
                return Response(
                    text,
                    mimetype='audio/x-mpegurl',
                    headers={'Content-Disposition': 'inline; filename=tivimate_channels.m3u'}
                )

            elif sub == 'custom_logo_mirror':
                return jsonify(handle_custom_logo_mirror(req))

            elif sub == 'epg_raw':
                xml_path = _epg_cache_xml_path()
                if not os.path.exists(xml_path):
                    return Response('EPG cache not found', status=404, mimetype='text/plain')
                with open(xml_path, 'rb') as f:
                    data = f.read()
                return Response(
                    data,
                    mimetype='application/xml',
                    headers={'Content-Disposition': 'inline; filename=myepg_raw.xml'}
                )

            elif sub == 'epg_tvh':
                xml_path = _epg_cache_tvh_xml_path()
                if _epg_tvh_cache_needs_rebuild():
                    tvh_summary = _build_epg_tvh_cache(_epg_cache_xml_path())
                    meta = _load_epg_meta()
                    meta.update(tvh_summary)
                    _save_epg_meta(meta)

                if not os.path.exists(xml_path):
                    return Response('EPG cache not found', status=404, mimetype='text/plain')
                return Response(
                    _iter_file_chunks(xml_path),
                    mimetype='application/xml',
                    headers={'Content-Disposition': 'inline; filename=tvh_epg.xml'}
                )

            elif sub == 'epg_tivimate':
                xml_path = _epg_cache_tvh_xml_path()
                if _epg_tvh_cache_needs_rebuild():
                    tvh_summary = _build_epg_tvh_cache(_epg_cache_xml_path())
                    meta = _load_epg_meta()
                    meta.update(tvh_summary)
                    _save_epg_meta(meta)

                if not os.path.exists(xml_path):
                    return Response('EPG cache not found', status=404, mimetype='text/plain')
                return Response(
                    _iter_file_chunks(xml_path),
                    mimetype='application/xml',
                    headers={'Content-Disposition': 'inline; filename=tivimate_epg.xml'}
                )

            return jsonify({'ret': 'warning', 'msg': 'unknown api'})

        except Exception as e:
            logger.exception(f'[ff_tvh_m3u] process_api exception: {str(e)}')
            return jsonify({'ret': 'danger', 'msg': str(e)})
