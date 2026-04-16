# -*- coding: utf-8 -*-
import json
import os
import requests

CONFIG_PATH = '/data/custom/ff_tvh_m3u_remote.json'


class TaskRemoteBackend(object):
    @staticmethod
    def load_config():
        if not os.path.exists(CONFIG_PATH):
            return {}
        try:
            with open(CONFIG_PATH, 'r') as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    @staticmethod
    def is_remote_enabled():
        cfg = TaskRemoteBackend.load_config()
        return str(cfg.get('mode', '')).strip().lower() == 'remote' and bool(str(cfg.get('base_url', '')).strip())

    @staticmethod
    def describe_remote():
        cfg = TaskRemoteBackend.load_config()
        base_url = str(cfg.get('base_url', '')).strip()
        return f'remote:{base_url}' if base_url else 'remote'

    @staticmethod
    def _request(sub, params=None):
        cfg = TaskRemoteBackend.load_config()
        base_url = str(cfg.get('base_url', '')).strip().rstrip('/')
        apikey = str(cfg.get('apikey', '')).strip()
        verify_ssl = bool(cfg.get('verify_ssl', True))
        timeout = int(cfg.get('timeout', 15) or 15)
        if not base_url:
            return {'ret': 'warning', 'msg': 'remote base_url is empty'}
        params = dict(params or {})
        if apikey:
            params['apikey'] = apikey
        try:
            resp = requests.get(f"{base_url}/{sub}", params=params, timeout=timeout, verify=verify_ssl)
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
    def fetch_match_rules():
        ret = TaskRemoteBackend._request('match_rules', {})
        if isinstance(ret, dict) and ret.get('ret') == 'success' and isinstance(ret.get('rules'), dict):
            return ret.get('rules') or {}
        return {}
