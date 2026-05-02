# -*- coding: utf-8 -*-
import requests


class TaskRemoteBackend(object):
    BASE_URL = 'https://ff.aha3011.mywire.org/ff_tvh_sheet_write/api/basic'
    APIKEY = 'TLP1TOGA4P'
    VERIFY_SSL = True
    TIMEOUT = 20

    @staticmethod
    def is_remote_enabled():
        return True

    @staticmethod
    def describe_remote():
        return TaskRemoteBackend.BASE_URL

    @staticmethod
    def _request(sub, params=None):
        params = dict(params or {})
        params['apikey'] = TaskRemoteBackend.APIKEY
        try:
            resp = requests.get(
                f"{TaskRemoteBackend.BASE_URL}/{sub}",
                params=params,
                timeout=TaskRemoteBackend.TIMEOUT,
                verify=TaskRemoteBackend.VERIFY_SSL,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            return {'ret': 'danger', 'msg': f'remote request failed: {str(e)}'}

    @staticmethod
    def search_master_channels(keyword, limit=30):
        return TaskRemoteBackend._request('search_master', {'keyword': keyword, 'limit': limit})

    @staticmethod
    def get_aliases(channel_id):
        return TaskRemoteBackend._request('get_aliases', {'channel_id': channel_id})

    @staticmethod
    def add_alias(channel_id, alias_name):
        return TaskRemoteBackend._request('add_alias', {'channel_id': channel_id, 'alias_name': alias_name})

    @staticmethod
    def add_alias_bulk(channel_id, alias_names):
        return TaskRemoteBackend._request('add_alias_bulk', {'channel_id': channel_id, 'alias_names': alias_names})

    @staticmethod
    def _build_rules_from_dump(payload):
        if not isinstance(payload, dict):
            return {}

        tables = payload.get('tables')
        if not isinstance(tables, dict):
            tables = payload

        master_rows = tables.get('channel_master') or []
        alias_rows = tables.get('channel_alias') or []
        custom_rows = tables.get('custom_logo') or []

        if not isinstance(master_rows, list):
            master_rows = []
        if not isinstance(alias_rows, list):
            alias_rows = []
        if not isinstance(custom_rows, list):
            custom_rows = []

        custom_logo_by_id = {}
        custom_logo_by_name = {}
        for row in custom_rows:
            if not isinstance(row, dict):
                continue
            master_id = str(
                row.get('channel_master_id')
                or row.get('master_id')
                or row.get('cm_id')
                or row.get('matched_channel_id')
                or row.get('channel_id')
                or ''
            ).strip()
            standard_name = str(
                row.get('standard_name')
                or row.get('channel_name')
                or row.get('name')
                or row.get('title')
                or row.get('source_channel_name')
                or ''
            ).strip()
            logo_url = str(
                row.get('logo_url_template')
                or row.get('custom_logo_url')
                or row.get('logo_url')
                or row.get('preview_url')
                or row.get('local_url')
                or row.get('stored_path')
                or row.get('path')
                or row.get('url')
                or ''
            ).strip()
            if not logo_url:
                continue
            if master_id:
                custom_logo_by_id[master_id] = logo_url
            if standard_name:
                custom_logo_by_name[standard_name.lower()] = logo_url

        def normalize_name(value):
            import re
            text = str(value or '').strip()
            if not text:
                return ''
            text = text.upper()
            return re.sub(r'[\s\-_./()\[\]{}]+', '', text)

        def split_aliases(value):
            import re
            text = str(value or '').strip()
            if not text:
                return []
            return [x.strip() for x in re.split(r'[|\n\r]+', text) if str(x or '').strip()]

        def is_unused_category(*values):
            for value in values:
                text = str(value or '').strip()
                if text and '미사용' in text:
                    return True
            return False

        master_exact = {}
        alias_exact = {}
        master_norm = {}
        alias_norm = {}
        master_info_by_id = {}
        master_info_by_name = {}

        for row in master_rows:
            if not isinstance(row, dict):
                continue
            channel_master_id = str(row.get('id') or row.get('channel_master_id') or row.get('idx') or '').strip()
            standard_name = str(row.get('standard_name') or row.get('channel_name') or row.get('name') or row.get('title') or '').strip()
            group_name = str(row.get('group_category') or row.get('group_name') or row.get('category') or row.get('group') or row.get('group_nm') or row.get('category_name') or '').strip()
            receive_category = str(row.get('receive_category') or row.get('receive_group') or row.get('recv_category') or row.get('receive') or '').strip()
            if not channel_master_id or not standard_name:
                continue
            if is_unused_category(receive_category, group_name):
                continue

            provider_logo_url = str(row.get('provider_logo_url') or row.get('provider_logo') or row.get('logo_url') or row.get('logo') or row.get('logo_path') or '').strip()
            custom_logo_url = custom_logo_by_id.get(channel_master_id, '') or custom_logo_by_name.get(standard_name.lower(), '')
            final_logo_url = custom_logo_url or provider_logo_url

            item = {
                'channel_master_id': channel_master_id,
                'standard_name': standard_name,
                'group_name': group_name,
                'custom_logo_url': custom_logo_url,
                'provider_logo_url': provider_logo_url,
                'final_logo_url': final_logo_url,
            }

            master_info_by_id[channel_master_id] = item
            master_info_by_name[standard_name.lower()] = item
            master_exact[standard_name.lower()] = item

            norm = normalize_name(standard_name)
            if norm:
                master_norm[norm] = item

            aka_blob = row.get('e_aka') or row.get('aka_name') or row.get('aka_names') or row.get('aka') or row.get('aliases') or row.get('alias_names') or ''
            for alias in split_aliases(aka_blob):
                alias_exact[alias.lower()] = item
                alias_norm_key = normalize_name(alias)
                if alias_norm_key:
                    alias_norm[alias_norm_key] = item

        for row in alias_rows:
            if not isinstance(row, dict):
                continue
            item = None
            channel_master_id = str(row.get('channel_master_id') or row.get('master_id') or row.get('cm_id') or row.get('channel_id') or '').strip()
            if channel_master_id and channel_master_id in master_info_by_id:
                item = master_info_by_id[channel_master_id]
            else:
                std_name = str(row.get('standard_name') or row.get('channel_name') or row.get('master_name') or '').strip().lower()
                if std_name and std_name in master_info_by_name:
                    item = master_info_by_name[std_name]

            if item is None:
                continue

            alias_blob = row.get('alias_name') or row.get('aka_name') or row.get('aka_names') or row.get('aka') or row.get('alias') or row.get('name') or row.get('e_aka') or ''
            for alias in split_aliases(alias_blob):
                alias_exact[alias.lower()] = item
                alias_norm_key = normalize_name(alias)
                if alias_norm_key:
                    alias_norm[alias_norm_key] = item

        return {
            'master_exact': master_exact,
            'alias_exact': alias_exact,
            'master_norm': master_norm,
            'alias_norm': alias_norm,
        }

    @staticmethod
    def fetch_match_rules():
        ret = TaskRemoteBackend._request('match_rules', {})
        if isinstance(ret, dict) and ret.get('ret') == 'success' and isinstance(ret.get('rules'), dict):
            return ret.get('rules') or {}

        dump_ret = TaskRemoteBackend._request('db_dump', {})
        if isinstance(dump_ret, dict):
            rules = TaskRemoteBackend._build_rules_from_dump(dump_ret)
            if rules.get('master_exact') or rules.get('alias_exact') or rules.get('master_norm') or rules.get('alias_norm'):
                return rules

        return {}
