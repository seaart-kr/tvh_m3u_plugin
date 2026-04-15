# -*- coding: utf-8 -*-
import re
import sqlite3
from datetime import datetime

from .setup import logger
from .model import DB_PATH
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


WRITE_DB_PATH = '/data/db/ff_tvh_sheet_write.db'


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
        con = sqlite3.connect(WRITE_DB_PATH)
        con.row_factory = sqlite3.Row
        return con

    @staticmethod
    def _connect_plugin_db():
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        return con

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
    def search_master_channels(keyword, limit=30):
        keyword = str(keyword or '').strip()
        if keyword == '':
            return {
                'ret': 'warning',
                'msg': '검색어를 입력하세요.',
                'list': []
            }

        try:
            try:
                limit = int(limit)
            except Exception:
                limit = 30
            limit = max(1, min(limit, 100))

            con = Task._connect_write_db()
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
                'list': items
            }
        except Exception as e:
            logger.exception(f'[tvh_m3u_plugin] search_master_channels exception: {str(e)}')
            return {
                'ret': 'danger',
                'msg': f'표준 채널 검색 실패: {str(e)}',
                'list': []
            }

    @staticmethod
    def add_db_match_channel(channel_uuid, channel_id):
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

            con = Task._connect_write_db()
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
            }

        except Exception as e:
            logger.exception(f'[tvh_m3u_plugin] add_db_match_channel exception: {str(e)}')
            return {
                'ret': 'danger',
                'msg': f'DB에 매칭채널 추가 실패: {str(e)}'
            }
