"""Read-only, allowlisted view of the public channel/logo catalog.

No private FF API key, stream URL, FF settings or write handler is used here.
"""
import json
import os
import re
import sqlite3
from contextlib import closing
from urllib.parse import quote, urlsplit, parse_qsl

from flask import Response

CATALOG_DB = '/data/db/ff_tvh_sheet_write.db'
MAX_ROWS = 50000
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
RESOURCES = frozenset({'health', 'match_rules', 'db_dump', 'search_master', 'get_aliases'})
TABLE_FIELDS = {
    'channel_master': ('id', 'standard_name', 'channel_name', 'name', 'group_category',
        'group_name', 'receive_category', 'e_aka', 'aka_name', 'aka_names', 'aliases',
        'provider_logo_url', 'provider_logo', 'logo_url', 'logo_path', 'custom_logo_url'),
    'channel_alias': ('id', 'channel_id', 'channel_master_id', 'master_id', 'alias_name',
        'alias_norm', 'standard_name', 'source', 'created_at'),
    'provider_logo_local': ('id', 'provider', 'provider_name', 'provider_type',
        'matched_channel_id', 'channel_id', 'channel_master_id', 'standard_name',
        'source_channel_name', 'provider_channel_name', 'stored_filename', 'filename',
        'logo_url_template', 'logo_url', 'final_url', 'local_url', 'original_logo_url'),
    'custom_logo': ('id', 'provider', 'matched_channel_id', 'channel_id',
        'channel_master_id', 'standard_name', 'source_channel_name', 'provider_channel_name',
        'stored_filename', 'filename', 'logo_url_template', 'custom_logo_url', 'logo_url',
        'final_url', 'original_logo_url'),
}


def _value(field, value):
    if value is None or isinstance(value, (int, float)):
        return value
    text = str(value)[:4096]
    if 'url' in field or field == 'logo_path':
        try:
            parts = urlsplit(text.replace('__FF_BASE_URL__', 'https://catalog.invalid'))
            if (parts.username or parts.password
                    or any(k.lower() in {'apikey', 'password', 'token', 'key', 'auth'}
                           for k, _ in parse_qsl(parts.query))):
                return ''
            if not (parts.scheme in {'http', 'https'}
                    or parts.path.startswith(('/customlogo/', '/tvh_m3u_plugin/docs/assets/'))):
                return ''
        except ValueError:
            return ''
    return text


def _dump(con):
    tables = {}
    for table, permitted in TABLE_FIELDS.items():
        columns = {r[1] for r in con.execute('PRAGMA table_info(' + table + ')')}
        selected = [c for c in permitted if c in columns]
        if not selected:
            tables[table] = []
            continue
        rows = con.execute('SELECT ' + ','.join(selected) + ' FROM ' + table + ' LIMIT ?',
                           (MAX_ROWS + 1,)).fetchall()
        if len(rows) > MAX_ROWS:
            raise ValueError('catalog too large')
        tables[table] = [{k: _value(k, row[k]) for k in selected} for row in rows]
    return tables


def read_catalog(resource, params, db_path=CATALOG_DB):
    if resource not in RESOURCES:
        raise ValueError('unknown resource')
    if resource == 'health':
        return {'ret': 'success', 'scope': 'catalog-read', 'api_version': 1}
    uri = 'file:' + quote(os.path.abspath(db_path), safe='/') + '?mode=ro'
    with closing(sqlite3.connect(uri, uri=True, timeout=3)) as con:
        con.execute('PRAGMA query_only=ON')
        con.row_factory = sqlite3.Row
        if resource in {'match_rules', 'db_dump'}:
            tables = _dump(con)
            if resource == 'db_dump':
                return {'ret': 'success', 'tables': tables}
            from .task_remote_backend import TaskRemoteBackend
            rules = TaskRemoteBackend._build_rules_from_dump({'tables': tables})
            return {'ret': 'success', 'rules': rules,
                    'counts': {k: len(v) for k, v in rules.items()}}
        if resource == 'search_master':
            keyword = str(params.get('keyword') or '').strip()
            if not keyword or len(keyword) > 256:
                raise ValueError('invalid keyword')
            try:
                limit = max(1, min(int(params.get('limit', 30)), 100))
            except (TypeError, ValueError):
                raise ValueError('invalid limit')
            rows = con.execute('''SELECT DISTINCT CAST(m.id AS TEXT) AS channel_id,
                m.standard_name, m.group_category AS group_name, m.receive_category
                FROM channel_master m LEFT JOIN channel_alias a
                ON CAST(a.channel_id AS TEXT)=CAST(m.id AS TEXT)
                WHERE m.standard_name LIKE ? OR a.alias_name LIKE ?
                ORDER BY CASE WHEN m.standard_name=? THEN 0 ELSE 1 END, m.standard_name
                LIMIT ?''', ('%' + keyword + '%', '%' + keyword + '%', keyword, limit))
        else:
            identity = str(params.get('channel_id') or '').strip()
            if not re.fullmatch(r'[A-Za-z0-9_-]{1,64}', identity):
                raise ValueError('invalid channel')
            rows = con.execute('''SELECT id,CAST(channel_id AS TEXT) AS channel_id,
                alias_name,alias_norm,source,created_at FROM channel_alias
                WHERE CAST(channel_id AS TEXT)=? ORDER BY alias_name LIMIT 1000''', (identity,))
        return {'ret': 'success', 'list': [dict(r) for r in rows]}


def catalog_response(req, reader=read_catalog):
    resource = str(req.args.get('resource') or '')
    if resource not in RESOURCES:
        return _response({'ret': 'warning', 'msg': 'not found'}, 404)
    # Ignore all caller-selected table/path/URL/key/command parameters.
    params = {k: req.args.get(k) for k in ('keyword', 'limit', 'channel_id') if k in req.args}
    try:
        return _response(reader(resource, params))
    except ValueError:
        return _response({'ret': 'warning', 'msg': 'invalid catalog request'}, 400)
    except Exception:
        return _response({'ret': 'warning', 'msg': 'catalog unavailable'}, 503)


def _response(payload, status=200):
    data = json.dumps(payload, ensure_ascii=False).encode('utf-8')
    if len(data) > MAX_RESPONSE_BYTES:
        data = b'{"ret":"warning","msg":"catalog too large"}'
        status = 503
    return Response(data, status=status, mimetype='application/json',
                    headers={'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff'})
