# -*- coding: utf-8 -*-
from urllib.parse import quote, urlsplit, urlunsplit, parse_qsl, urlencode

import requests

from .setup import *


class TaskBase:
    @staticmethod
    def _build_session(username='', password=''):
        session = requests.Session()

        verify_ssl = False
        try:
            verify_ssl = P.ModelSetting.get_bool('basic_tvh_use_verify_ssl')
        except Exception:
            verify_ssl = False

        session.verify = verify_ssl

        if username and password:
            session.auth = (username, password)

        return session

    @staticmethod
    def get_session():
        username = P.ModelSetting.get('basic_tvh_admin_username')
        password = P.ModelSetting.get('basic_tvh_admin_password')
        return TaskBase._build_session(username, password)

    @staticmethod
    def get_play_session():
        username = P.ModelSetting.get('basic_tvh_play_username')
        password = P.ModelSetting.get('basic_tvh_play_password')
        return TaskBase._build_session(username, password)

    @staticmethod
    def get_api_base():
        value = P.ModelSetting.get('basic_tvh_api_base')
        return (value or '').rstrip('/')

    @staticmethod
    def get_stream_base():
        value = P.ModelSetting.get('basic_tvh_stream_base')
        return (value or '').rstrip('/')

    @staticmethod
    def get_play_username():
        return P.ModelSetting.get('basic_tvh_play_username') or ''

    @staticmethod
    def get_play_password():
        return P.ModelSetting.get('basic_tvh_play_password') or ''

    @staticmethod
    def get_stream_profile():
        return P.ModelSetting.get('basic_tvh_stream_profile') or ''

    @staticmethod
    def get_include_auth_in_url():
        try:
            value = P.ModelSetting.get('basic_tvh_include_auth_in_url')
            if value is None:
                return False
            value = str(value).strip().lower()
            return value in ['true', 'on', '1', 'yes', 'y']
        except Exception:
            return False

    @staticmethod
    def inject_auth_to_url(base_url, username, password):
        if not username:
            return base_url

        parts = urlsplit(base_url)
        hostname = parts.hostname or ''
        port = f':{parts.port}' if parts.port else ''
        userinfo = f'{quote(username)}:{quote(password)}@'
        new_netloc = f'{userinfo}{hostname}{port}'
        return urlunsplit((parts.scheme, new_netloc, parts.path, parts.query, parts.fragment))

    @staticmethod
    def remove_auth_from_url(base_url):
        parts = urlsplit(base_url)
        hostname = parts.hostname or ''
        port = f':{parts.port}' if parts.port else ''
        new_netloc = f'{hostname}{port}'
        return urlunsplit((parts.scheme, new_netloc, parts.path, parts.query, parts.fragment))

    @staticmethod
    def set_query_param(url, key, value):
        parts = urlsplit(url)
        query = dict(parse_qsl(parts.query, keep_blank_values=True))
        if value:
            query[key] = value
        else:
            query.pop(key, None)
        new_query = urlencode(query)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))