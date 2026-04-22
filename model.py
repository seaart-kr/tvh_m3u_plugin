# -*- coding: utf-8 -*-
from datetime import datetime
import json
import os
import re
import sqlite3

from .setup import *


DB_PATH = f'/data/db/{P.package_name}.db'


def get_conn():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def now_str():
    return str(datetime.now())[:19]


def normalize_channel_name(value):
    text = str(value or '').strip()
    if not text:
        return ''
    text = text.upper()
    text = re.sub(r'[\s\-_./()\[\]{}]+', '', text)
    return text


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS ff_tvh_m3u_tag (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tag_uuid TEXT UNIQUE NOT NULL,
        name TEXT NOT NULL,
        idx INTEGER DEFAULT 0,
        created_time TEXT,
        updated_time TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS ff_tvh_m3u_group_order (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        group_name TEXT UNIQUE NOT NULL,
        sort_order INTEGER DEFAULT 0,
        created_time TEXT,
        updated_time TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS ff_tvh_m3u_channel (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        channel_uuid TEXT UNIQUE NOT NULL,
        number INTEGER DEFAULT 0,
        name TEXT NOT NULL,
        enabled INTEGER DEFAULT 1,
        raw_tags TEXT DEFAULT '[]',
        group_name TEXT DEFAULT '그룹 없음',
        manual_group_name TEXT DEFAULT '',
        sheet_group_name TEXT DEFAULT '',
        sheet_channel_id TEXT DEFAULT '',
        sheet_logo_url TEXT DEFAULT '',
        raw_data TEXT DEFAULT '{}',
        created_time TEXT,
        updated_time TEXT
    )
    """)

    # schema migration
    cur.execute("PRAGMA table_info(ff_tvh_m3u_channel)")
    channel_columns = [row[1] for row in cur.fetchall()]
    if 'manual_group_name' not in channel_columns:
        cur.execute("ALTER TABLE ff_tvh_m3u_channel ADD COLUMN manual_group_name TEXT DEFAULT ''")
    if 'sheet_group_name' not in channel_columns:
        cur.execute("ALTER TABLE ff_tvh_m3u_channel ADD COLUMN sheet_group_name TEXT DEFAULT ''")
    if 'sheet_channel_id' not in channel_columns:
        cur.execute("ALTER TABLE ff_tvh_m3u_channel ADD COLUMN sheet_channel_id TEXT DEFAULT ''")
    if 'sheet_logo_url' not in channel_columns:
        cur.execute("ALTER TABLE ff_tvh_m3u_channel ADD COLUMN sheet_logo_url TEXT DEFAULT ''")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS ff_tvh_m3u_sheet_rule (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        channel_name TEXT DEFAULT '',
        aka_name TEXT DEFAULT '',
        group_name TEXT DEFAULT '',
        logo_url TEXT DEFAULT '',
        normalized_name TEXT DEFAULT '',
        normalized_aka TEXT DEFAULT '',
        enabled INTEGER DEFAULT 1,
        created_time TEXT,
        updated_time TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS ff_tvh_m3u_group_profile (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        group_name TEXT UNIQUE NOT NULL,
        profile TEXT DEFAULT '',
        created_time TEXT,
        updated_time TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS ff_tvh_m3u_channel_profile (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        channel_uuid TEXT UNIQUE NOT NULL,
        profile TEXT DEFAULT '',
        created_time TEXT,
        updated_time TEXT
    )
    """)


    cur.execute("""
    CREATE TABLE IF NOT EXISTS ff_tvh_m3u_logo_override (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        channel_uuid TEXT UNIQUE NOT NULL,
        channel_name TEXT DEFAULT '',
        channel_name_norm TEXT DEFAULT '',
        selected_provider TEXT DEFAULT '',
        logo_url_template TEXT DEFAULT '',
        created_time TEXT,
        updated_time TEXT
    )
    """)

    conn.commit()
    conn.close()


class ModelLogoOverride:
    def __init__(self, channel_uuid='', channel_name='', channel_name_norm='', selected_provider='', logo_url_template='', created_time=None, updated_time=None, id=None):
        self.id = id
        self.channel_uuid = channel_uuid
        self.channel_name = channel_name
        self.channel_name_norm = channel_name_norm
        self.selected_provider = selected_provider
        self.logo_url_template = logo_url_template
        self.created_time = created_time
        self.updated_time = updated_time

    @staticmethod
    def from_row(row):
        return ModelLogoOverride(
            id=row['id'],
            channel_uuid=row['channel_uuid'],
            channel_name=row['channel_name'],
            channel_name_norm=row['channel_name_norm'],
            selected_provider=row['selected_provider'],
            logo_url_template=row['logo_url_template'],
            created_time=row['created_time'],
            updated_time=row['updated_time'],
        )

    @staticmethod
    def get_all():
        init_db()
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM ff_tvh_m3u_logo_override ORDER BY channel_name ASC, channel_uuid ASC")
        rows = [ModelLogoOverride.from_row(row) for row in cur.fetchall()]
        conn.close()
        return rows

    @staticmethod
    def get_maps():
        rows = ModelLogoOverride.get_all()
        uuid_map = {}
        name_map = {}
        for row in rows:
            item = {
                'channel_uuid': row.channel_uuid,
                'channel_name': row.channel_name,
                'channel_name_norm': row.channel_name_norm,
                'selected_provider': row.selected_provider,
                'logo_url_template': row.logo_url_template,
                'updated_time': row.updated_time,
            }
            if row.channel_uuid:
                uuid_map[row.channel_uuid] = item
            if row.channel_name_norm:
                name_map[row.channel_name_norm] = item
        return {'uuid_map': uuid_map, 'name_map': name_map}

    @staticmethod
    def save(channel_uuid, channel_name, selected_provider, logo_url_template):
        init_db()
        channel_uuid = str(channel_uuid or '').strip()
        channel_name = str(channel_name or '').strip()
        channel_name_norm = normalize_channel_name(channel_name)
        selected_provider = str(selected_provider or '').strip().lower()
        logo_url_template = str(logo_url_template or '').strip()
        if not channel_uuid:
            return False
        now = now_str()
        conn = get_conn()
        cur = conn.cursor()
        row = cur.execute("SELECT id, created_time FROM ff_tvh_m3u_logo_override WHERE channel_uuid = ? LIMIT 1", (channel_uuid,)).fetchone()
        if row:
            created_time = row['created_time'] or now
            cur.execute(
                "UPDATE ff_tvh_m3u_logo_override SET channel_name=?, channel_name_norm=?, selected_provider=?, logo_url_template=?, updated_time=? WHERE channel_uuid=?",
                (channel_name, channel_name_norm, selected_provider, logo_url_template, now, channel_uuid),
            )
        else:
            cur.execute(
                "INSERT INTO ff_tvh_m3u_logo_override (channel_uuid, channel_name, channel_name_norm, selected_provider, logo_url_template, created_time, updated_time) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (channel_uuid, channel_name, channel_name_norm, selected_provider, logo_url_template, now, now),
            )
        conn.commit()
        conn.close()
        return True

    @staticmethod
    def delete(channel_uuid):
        init_db()
        channel_uuid = str(channel_uuid or '').strip()
        if not channel_uuid:
            return 0
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM ff_tvh_m3u_logo_override WHERE channel_uuid = ?", (channel_uuid,))
        changed = cur.rowcount
        conn.commit()
        conn.close()
        return changed


class ModelTag:
    def __init__(self, tag_uuid='', name='', index=0, created_time=None, updated_time=None, id=None):
        self.id = id
        self.tag_uuid = tag_uuid
        self.name = name
        self.index = index
        self.created_time = created_time
        self.updated_time = updated_time

    def as_dict(self):
        return {
            'id': self.id,
            'tag_uuid': self.tag_uuid,
            'name': self.name,
            'index': self.index,
            'created_time': self.created_time,
            'updated_time': self.updated_time,
        }

    @staticmethod
    def from_row(row):
        return ModelTag(
            id=row['id'],
            tag_uuid=row['tag_uuid'],
            name=row['name'],
            index=row['idx'],
            created_time=row['created_time'],
            updated_time=row['updated_time'],
        )

    @staticmethod
    def get_all():
        init_db()
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM ff_tvh_m3u_tag ORDER BY idx ASC, name ASC")
        rows = [ModelTag.from_row(row) for row in cur.fetchall()]
        conn.close()
        return rows

    @staticmethod
    def clear_all():
        init_db()
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM ff_tvh_m3u_tag")
        conn.commit()
        conn.close()

    @staticmethod
    def bulk_insert(items):
        init_db()
        now = now_str()
        conn = get_conn()
        cur = conn.cursor()

        for idx, item in enumerate(items):
            tag_uuid = str(item.get('uuid', '')).strip()
            name = str(item.get('name', '')).strip() or f'태그-{idx}'
            if not tag_uuid:
                continue

            cur.execute("""
            INSERT INTO ff_tvh_m3u_tag
            (tag_uuid, name, idx, created_time, updated_time)
            VALUES (?, ?, ?, ?, ?)
            """, (tag_uuid, name, idx, now, now))

        conn.commit()
        conn.close()


class ModelGroupOrder:
    def __init__(self, group_name='', sort_order=0, created_time=None, updated_time=None, id=None):
        self.id = id
        self.group_name = group_name
        self.sort_order = sort_order
        self.created_time = created_time
        self.updated_time = updated_time

    def as_dict(self):
        return {
            'id': self.id,
            'group_name': self.group_name,
            'sort_order': self.sort_order,
            'created_time': self.created_time,
            'updated_time': self.updated_time,
        }

    @staticmethod
    def from_row(row):
        return ModelGroupOrder(
            id=row['id'],
            group_name=row['group_name'],
            sort_order=row['sort_order'],
            created_time=row['created_time'],
            updated_time=row['updated_time'],
        )

    @staticmethod
    def get_all():
        init_db()
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM ff_tvh_m3u_group_order ORDER BY sort_order ASC, group_name ASC")
        rows = [ModelGroupOrder.from_row(row) for row in cur.fetchall()]
        conn.close()
        return rows

    @staticmethod
    def get_priority_list():
        return [
            '지상파', '종합편성/종편', '연예/오락', '뉴스/경제', '드라마', '여성/패션',
            '스포츠', '영화', '음악', '만화', '어린이', '교양', '다큐', '교육',
            '레저', '공공', '종교', '홈쇼핑', '해외위성', '라디오', '기타',
        ]

    @staticmethod
    def normalize_group_name(group_name):
        name = str(group_name or '').strip()
        compact = name.replace(' ', '')
        if compact in ['종편', '종합편성', '종합편성/종편']:
            return '종합편성/종편'
        if compact in ['어린이', '어린이/애니']:
            return '어린이'
        if compact in ['애니', '만화']:
            return '만화'
        return name

    @staticmethod
    def reorder_by_priority():
        rows = ModelGroupOrder.get_all()
        if not rows:
            return

        priority_list = ModelGroupOrder.get_priority_list()

        def sort_key(row):
            original_name = row.group_name
            normalized = ModelGroupOrder.normalize_group_name(original_name)
            if normalized == '기타':
                return (2, 9999, row.sort_order, original_name)
            if normalized in priority_list:
                return (0, priority_list.index(normalized), row.sort_order, original_name)
            return (1, 9998, row.sort_order, original_name)

        ordered_rows = sorted(rows, key=sort_key)

        conn = get_conn()
        cur = conn.cursor()
        now = now_str()
        changed = False
        for idx, row in enumerate(ordered_rows):
            if row.sort_order != idx:
                cur.execute(
                    "UPDATE ff_tvh_m3u_group_order SET sort_order = ?, updated_time = ? WHERE id = ?",
                    (idx, now, row.id),
                )
                changed = True
        if changed:
            conn.commit()
        conn.close()

    @staticmethod
    def normalize_orders():
        rows = ModelGroupOrder.get_all()
        conn = get_conn()
        cur = conn.cursor()
        now = now_str()
        changed = False
        for idx, row in enumerate(rows):
            if row.sort_order != idx:
                cur.execute(
                    "UPDATE ff_tvh_m3u_group_order SET sort_order = ?, updated_time = ? WHERE id = ?",
                    (idx, now, row.id),
                )
                changed = True
        if changed:
            conn.commit()
        conn.close()

    @staticmethod
    def sync_from_group_names(group_names):
        init_db()
        group_names = [str(x).strip() for x in (group_names or []) if str(x).strip()]
        current_rows = ModelGroupOrder.get_all()
        current_map = {row.group_name: row for row in current_rows}
        next_order = max([row.sort_order for row in current_rows], default=-1) + 1

        conn = get_conn()
        cur = conn.cursor()
        now = now_str()
        changed = False

        for name in group_names:
            if name not in current_map:
                cur.execute(
                    "INSERT INTO ff_tvh_m3u_group_order (group_name, sort_order, created_time, updated_time) VALUES (?, ?, ?, ?)",
                    (name, next_order, now, now),
                )
                next_order += 1
                changed = True

        existing_names = set(group_names)
        for row in current_rows:
            if row.group_name not in existing_names:
                cur.execute("DELETE FROM ff_tvh_m3u_group_order WHERE id = ?", (row.id,))
                changed = True

        if changed:
            conn.commit()
        conn.close()

        if changed:
            ModelGroupOrder.normalize_orders()
        if not current_rows:
            ModelGroupOrder.reorder_by_priority()

    @staticmethod
    def move(group_name, direction):
        rows = ModelGroupOrder.get_all()
        idx = -1
        for i, row in enumerate(rows):
            if row.group_name == group_name:
                idx = i
                break
        if idx < 0:
            return False, '대상 그룹을 찾지 못했습니다.'

        if direction == 'up':
            target_idx = idx - 1
        elif direction == 'down':
            target_idx = idx + 1
        else:
            return False, '이동 방향이 올바르지 않습니다.'

        if target_idx < 0 or target_idx >= len(rows):
            return False, '더 이상 이동할 수 없습니다.'

        current_row = rows[idx]
        target_row = rows[target_idx]
        conn = get_conn()
        cur = conn.cursor()
        now = now_str()
        cur.execute(
            "UPDATE ff_tvh_m3u_group_order SET sort_order = ?, updated_time = ? WHERE id = ?",
            (target_row.sort_order, now, current_row.id),
        )
        cur.execute(
            "UPDATE ff_tvh_m3u_group_order SET sort_order = ?, updated_time = ? WHERE id = ?",
            (current_row.sort_order, now, target_row.id),
        )
        conn.commit()
        conn.close()
        ModelGroupOrder.normalize_orders()
        return True, '그룹 순서를 저장했습니다.'

    @staticmethod
    def get_ordered_group_names(fallback_group_names=None):
        fallback_group_names = [str(x).strip() for x in (fallback_group_names or []) if str(x).strip()]
        saved_rows = ModelGroupOrder.get_all()
        ordered = []
        used = set()
        for row in saved_rows:
            if row.group_name not in used:
                ordered.append(row.group_name)
                used.add(row.group_name)
        for name in sorted(fallback_group_names):
            if name not in used:
                ordered.append(name)
                used.add(name)
        return ordered



class ModelChannel:
    def __init__(
        self,
        channel_uuid='',
        number=0,
        name='',
        enabled=True,
        raw_tags='[]',
        group_name='그룹 없음',
        manual_group_name='',
        sheet_group_name='',
        sheet_channel_id='',
        sheet_logo_url='',
        raw_data='{}',
        created_time=None,
        updated_time=None,
        id=None,
    ):
        self.id = id
        self.channel_uuid = channel_uuid
        self.number = number
        self.name = name
        self.enabled = enabled
        self.raw_tags = raw_tags
        self.group_name = group_name
        self.manual_group_name = manual_group_name
        self.sheet_group_name = sheet_group_name
        self.sheet_channel_id = sheet_channel_id
        self.sheet_logo_url = sheet_logo_url
        self.raw_data = raw_data
        self.created_time = created_time
        self.updated_time = updated_time

    def as_dict(self):
        data = {
            'id': self.id,
            'channel_uuid': self.channel_uuid,
            'number': self.number,
            'name': self.name,
            'enabled': bool(self.enabled),
            'raw_tags': self.raw_tags,
            'group_name': self.group_name,
            'manual_group_name': self.manual_group_name,
            'sheet_group_name': self.sheet_group_name,
            'sheet_channel_id': self.sheet_channel_id,
            'sheet_logo_url': self.sheet_logo_url,
            'effective_group_name': self.get_effective_group_name(),
            'raw_data': self.raw_data,
            'created_time': self.created_time,
            'updated_time': self.updated_time,
        }
        try:
            data['raw_tags_list'] = json.loads(self.raw_tags or '[]')
        except Exception:
            data['raw_tags_list'] = []
        return data

    def get_effective_group_name(self):
        manual = str(self.manual_group_name or '').strip()
        if manual:
            return manual
        sheet_group = str(self.sheet_group_name or '').strip()
        if sheet_group:
            return sheet_group
        return str(self.group_name or '').strip() or '그룹 없음'

    @staticmethod
    def from_row(row):
        keys = row.keys()
        return ModelChannel(
            id=row['id'],
            channel_uuid=row['channel_uuid'],
            number=row['number'],
            name=row['name'],
            enabled=bool(row['enabled']),
            raw_tags=row['raw_tags'],
            group_name=row['group_name'],
            manual_group_name=(row['manual_group_name'] if 'manual_group_name' in keys else ''),
            sheet_group_name=(row['sheet_group_name'] if 'sheet_group_name' in keys else ''),
            sheet_channel_id=(row['sheet_channel_id'] if 'sheet_channel_id' in keys else ''),
            sheet_logo_url=(row['sheet_logo_url'] if 'sheet_logo_url' in keys else ''),
            raw_data=row['raw_data'],
            created_time=row['created_time'],
            updated_time=row['updated_time'],
        )

    @staticmethod
    def get_all():
        init_db()
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM ff_tvh_m3u_channel "
            "ORDER BY COALESCE(NULLIF(manual_group_name, ''), NULLIF(sheet_group_name, ''), group_name) ASC, number ASC, name ASC"
        )
        rows = [ModelChannel.from_row(row) for row in cur.fetchall()]
        conn.close()
        return rows

    @staticmethod
    def get_channel_map():
        return {row.channel_uuid: row for row in ModelChannel.get_all() if row.channel_uuid}

    @staticmethod
    def get_grouped():
        rows = ModelChannel.get_all()
        grouped = {}
        for row in rows:
            group_name = row.get_effective_group_name()
            grouped.setdefault(group_name, [])
            grouped[group_name].append(row.as_dict())

        group_names = list(grouped.keys())
        ordered_group_names = ModelGroupOrder.get_ordered_group_names(group_names)
        ordered_grouped = {}
        for group_name in ordered_group_names:
            if group_name in grouped:
                ordered_grouped[group_name] = grouped[group_name]
        return ordered_grouped

    @staticmethod
    def clear_all():
        init_db()
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM ff_tvh_m3u_channel")
        cur.execute("DELETE FROM ff_tvh_m3u_tag")
        conn.commit()
        conn.close()

    @staticmethod
    def bulk_insert(items):
        init_db()
        now = now_str()
        conn = get_conn()
        cur = conn.cursor()
        for item in items:
            cur.execute(
                """
                INSERT INTO ff_tvh_m3u_channel
                (channel_uuid, number, name, enabled, raw_tags, group_name, manual_group_name, sheet_group_name, sheet_channel_id, sheet_logo_url, raw_data, created_time, updated_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item.get('channel_uuid', ''),
                    item.get('number', 0),
                    item.get('name', ''),
                    1 if item.get('enabled', True) else 0,
                    item.get('raw_tags', '[]'),
                    item.get('group_name', '그룹 없음'),
                    item.get('manual_group_name', ''),
                    item.get('sheet_group_name', ''),
                    item.get('sheet_channel_id', ''),
                    item.get('sheet_logo_url', ''),
                    item.get('raw_data', '{}'),
                    now,
                    now,
                ),
            )
        conn.commit()
        conn.close()

    @staticmethod
    def get_manual_group_map():
        init_db()
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT channel_uuid, manual_group_name FROM ff_tvh_m3u_channel WHERE COALESCE(manual_group_name, '') != ''")
        rows = cur.fetchall()
        conn.close()
        return {row['channel_uuid']: row['manual_group_name'] for row in rows if row['channel_uuid']}

    @staticmethod
    def get_effective_group_names():
        return list(ModelChannel.get_grouped().keys())

    @staticmethod
    def get_assignable_group_names():
        names = ModelSheetRule.get_group_names()
        if names:
            return names
        names = []
        for name in ModelChannel.get_effective_group_names():
            name = str(name or '').strip()
            if not name or name == '그룹 없음':
                continue
            if name not in names:
                names.append(name)
        return names

    @staticmethod
    def get_ungrouped():
        rows = ModelChannel.get_all()
        result = []
        for row in rows:
            effective_group_name = str(row.get_effective_group_name() or '').strip() or '그룹 없음'
            if effective_group_name == '그룹 없음':
                result.append(row.as_dict())
        return result

    @staticmethod
    def assign_manual_group(channel_uuids, group_name):
        init_db()
        channel_uuids = [str(x).strip() for x in (channel_uuids or []) if str(x).strip()]
        group_name = str(group_name or '').strip()
        if not channel_uuids or not group_name:
            return 0

        conn = get_conn()
        cur = conn.cursor()
        now = now_str()
        changed = 0
        for channel_uuid in channel_uuids:
            cur.execute(
                "UPDATE ff_tvh_m3u_channel SET manual_group_name = ?, updated_time = ? WHERE channel_uuid = ?",
                (group_name, now, channel_uuid),
            )
            changed += cur.rowcount
        conn.commit()
        conn.close()
        return changed

    @staticmethod
    def clear_manual_group(channel_uuids):
        init_db()
        channel_uuids = [str(x).strip() for x in (channel_uuids or []) if str(x).strip()]
        if not channel_uuids:
            return 0
        conn = get_conn()
        cur = conn.cursor()
        now = now_str()
        changed = 0
        for channel_uuid in channel_uuids:
            cur.execute(
                "UPDATE ff_tvh_m3u_channel SET manual_group_name = '', updated_time = ? WHERE channel_uuid = ?",
                (now, channel_uuid),
            )
            changed += cur.rowcount
        conn.commit()
        conn.close()
        return changed

    @staticmethod
    def replace_sheet_matches(items):
        init_db()
        items = items or []
        conn = get_conn()
        cur = conn.cursor()
        now = now_str()

        cur.execute(
            "UPDATE ff_tvh_m3u_channel SET sheet_group_name = '', sheet_channel_id = '', sheet_logo_url = '', updated_time = ?",
            (now,),
        )

        changed = 0
        for item in items:
            channel_uuid = str(item.get('channel_uuid') or '').strip()
            if not channel_uuid:
                continue
            sheet_group_name = str(item.get('sheet_group_name') or '').strip()
            sheet_channel_id = str(item.get('sheet_channel_id') or '').strip()
            sheet_logo_url = str(item.get('sheet_logo_url') or '').strip()
            cur.execute(
                "UPDATE ff_tvh_m3u_channel SET sheet_group_name = ?, sheet_channel_id = ?, sheet_logo_url = ?, updated_time = ? WHERE channel_uuid = ?",
                (sheet_group_name, sheet_channel_id, sheet_logo_url, now, channel_uuid),
            )
            changed += cur.rowcount

        conn.commit()
        conn.close()
        return changed


def clear_sheet_matches():
        init_db()
        conn = get_conn()
        cur = conn.cursor()
        now = now_str()
        cur.execute(
            "UPDATE ff_tvh_m3u_channel SET sheet_group_name = '', sheet_channel_id = '', sheet_logo_url = '', updated_time = ? WHERE COALESCE(sheet_group_name, '') != '' OR COALESCE(sheet_channel_id, '') != '' OR COALESCE(sheet_logo_url, '') != ''",
            (now,),
        )
        changed = cur.rowcount
        conn.commit()
        conn.close()
        return changed


class ModelSheetRule:
    def __init__(
        self,
        channel_name='',
        aka_name='',
        group_name='',
        logo_url='',
        normalized_name='',
        normalized_aka='',
        enabled=True,
        created_time=None,
        updated_time=None,
        id=None,
    ):
        self.id = id
        self.channel_name = channel_name
        self.aka_name = aka_name
        self.group_name = group_name
        self.logo_url = logo_url
        self.normalized_name = normalized_name
        self.normalized_aka = normalized_aka
        self.enabled = enabled
        self.created_time = created_time
        self.updated_time = updated_time

    @staticmethod
    def from_row(row):
        return ModelSheetRule(
            id=row['id'],
            channel_name=row['channel_name'],
            aka_name=row['aka_name'],
            group_name=row['group_name'],
            logo_url=row['logo_url'],
            normalized_name=row['normalized_name'],
            normalized_aka=row['normalized_aka'],
            enabled=bool(row['enabled']),
            created_time=row['created_time'],
            updated_time=row['updated_time'],
        )

    @staticmethod
    def get_all():
        init_db()
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM ff_tvh_m3u_sheet_rule WHERE enabled = 1 ORDER BY group_name ASC, channel_name ASC")
        rows = [ModelSheetRule.from_row(row) for row in cur.fetchall()]
        conn.close()
        return rows

    @staticmethod
    def get_group_names():
        init_db()
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT group_name FROM ff_tvh_m3u_sheet_rule WHERE enabled = 1 AND COALESCE(group_name, '') != '' ORDER BY group_name ASC")
        names = [row['group_name'] for row in cur.fetchall() if str(row['group_name'] or '').strip()]
        conn.close()
        return names

    @staticmethod
    def get_count():
        init_db()
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS cnt FROM ff_tvh_m3u_sheet_rule")
        row = cur.fetchone()
        conn.close()
        return int(row['cnt']) if row else 0

    @staticmethod
    def replace_all(items):
        init_db()
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM ff_tvh_m3u_sheet_rule")
        now = now_str()
        for item in items or []:
            channel_name = str(item.get('channel_name', '') or '').strip()
            aka_name = str(item.get('aka_name', '') or '').strip()
            group_name = str(item.get('group_name', '') or '').strip()
            logo_url = str(item.get('logo_url', '') or '').strip()
            if not channel_name and not aka_name:
                continue
            cur.execute(
                """
                INSERT INTO ff_tvh_m3u_sheet_rule
                (channel_name, aka_name, group_name, logo_url, normalized_name, normalized_aka, enabled, created_time, updated_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    channel_name,
                    aka_name,
                    group_name,
                    logo_url,
                    normalize_channel_name(channel_name),
                    normalize_channel_name(aka_name),
                    1,
                    now,
                    now,
                ),
            )
        conn.commit()
        conn.close()

    @staticmethod
    def find_match(channel_name):
        channel_norm = normalize_channel_name(channel_name)
        if not channel_norm:
            return None
        rules = ModelSheetRule.get_all()
        # 1차 exact match
        for row in rules:
            if channel_norm and channel_norm == row.normalized_name:
                return row
            if channel_norm and row.normalized_aka and channel_norm == row.normalized_aka:
                return row
        # 2차 contains match
        for row in rules:
            for candidate in [row.normalized_name, row.normalized_aka]:
                if candidate and len(candidate) >= 3 and (candidate in channel_norm or channel_norm in candidate):
                    return row
        return None


class ModelGroupProfile:
    def __init__(self, group_name='', profile='', created_time=None, updated_time=None, id=None):
        self.id = id
        self.group_name = group_name
        self.profile = profile
        self.created_time = created_time
        self.updated_time = updated_time

    @staticmethod
    def from_row(row):
        return ModelGroupProfile(
            id=row['id'],
            group_name=row['group_name'],
            profile=row['profile'],
            created_time=row['created_time'],
            updated_time=row['updated_time'],
        )

    @staticmethod
    def get_all():
        init_db()
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM ff_tvh_m3u_group_profile ORDER BY group_name ASC")
        rows = [ModelGroupProfile.from_row(row) for row in cur.fetchall()]
        conn.close()
        return rows

    @staticmethod
    def get_map():
        return {row.group_name: row.profile for row in ModelGroupProfile.get_all() if row.group_name}

    @staticmethod
    def get_profile(group_name):
        group_name = str(group_name or '').strip()
        if not group_name:
            return ''
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT profile FROM ff_tvh_m3u_group_profile WHERE group_name = ?", (group_name,))
        row = cur.fetchone()
        conn.close()
        return (row['profile'] if row else '') or ''

    @staticmethod
    def upsert(group_name, profile):
        init_db()
        group_name = str(group_name or '').strip()
        profile = str(profile or '').strip()
        if not group_name:
            return False
        conn = get_conn()
        cur = conn.cursor()
        now = now_str()
        cur.execute("SELECT id FROM ff_tvh_m3u_group_profile WHERE group_name = ?", (group_name,))
        row = cur.fetchone()
        if profile == '':
            if row:
                cur.execute("DELETE FROM ff_tvh_m3u_group_profile WHERE group_name = ?", (group_name,))
                conn.commit()
            conn.close()
            return True
        if row:
            cur.execute(
                "UPDATE ff_tvh_m3u_group_profile SET profile = ?, updated_time = ? WHERE group_name = ?",
                (profile, now, group_name),
            )
        else:
            cur.execute(
                "INSERT INTO ff_tvh_m3u_group_profile (group_name, profile, created_time, updated_time) VALUES (?, ?, ?, ?)",
                (group_name, profile, now, now),
            )
        conn.commit()
        conn.close()
        return True

    @staticmethod
    def cleanup_by_group_names(group_names):
        init_db()
        valid = set([str(x).strip() for x in (group_names or []) if str(x).strip()])
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT group_name FROM ff_tvh_m3u_group_profile")
        rows = cur.fetchall()
        deleted = 0
        for row in rows:
            if row['group_name'] not in valid:
                cur.execute("DELETE FROM ff_tvh_m3u_group_profile WHERE group_name = ?", (row['group_name'],))
                deleted += 1
        if deleted:
            conn.commit()
        conn.close()
        return deleted


class ModelChannelProfile:
    def __init__(self, channel_uuid='', profile='', created_time=None, updated_time=None, id=None):
        self.id = id
        self.channel_uuid = channel_uuid
        self.profile = profile
        self.created_time = created_time
        self.updated_time = updated_time

    @staticmethod
    def from_row(row):
        return ModelChannelProfile(
            id=row['id'],
            channel_uuid=row['channel_uuid'],
            profile=row['profile'],
            created_time=row['created_time'],
            updated_time=row['updated_time'],
        )

    @staticmethod
    def get_all():
        init_db()
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM ff_tvh_m3u_channel_profile ORDER BY channel_uuid ASC")
        rows = [ModelChannelProfile.from_row(row) for row in cur.fetchall()]
        conn.close()
        return rows

    @staticmethod
    def get_map():
        return {row.channel_uuid: row.profile for row in ModelChannelProfile.get_all() if row.channel_uuid}

    @staticmethod
    def get_profile(channel_uuid):
        channel_uuid = str(channel_uuid or '').strip()
        if not channel_uuid:
            return ''
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT profile FROM ff_tvh_m3u_channel_profile WHERE channel_uuid = ?", (channel_uuid,))
        row = cur.fetchone()
        conn.close()
        return (row['profile'] if row else '') or ''

    @staticmethod
    def upsert(channel_uuid, profile):
        init_db()
        channel_uuid = str(channel_uuid or '').strip()
        profile = str(profile or '').strip()
        if not channel_uuid:
            return False
        conn = get_conn()
        cur = conn.cursor()
        now = now_str()
        cur.execute("SELECT id FROM ff_tvh_m3u_channel_profile WHERE channel_uuid = ?", (channel_uuid,))
        row = cur.fetchone()
        if profile == '':
            if row:
                cur.execute("DELETE FROM ff_tvh_m3u_channel_profile WHERE channel_uuid = ?", (channel_uuid,))
                conn.commit()
            conn.close()
            return True
        if row:
            cur.execute(
                "UPDATE ff_tvh_m3u_channel_profile SET profile = ?, updated_time = ? WHERE channel_uuid = ?",
                (profile, now, channel_uuid),
            )
        else:
            cur.execute(
                "INSERT INTO ff_tvh_m3u_channel_profile (channel_uuid, profile, created_time, updated_time) VALUES (?, ?, ?, ?)",
                (channel_uuid, profile, now, now),
            )
        conn.commit()
        conn.close()
        return True

    @staticmethod
    def cleanup_by_channel_uuids(channel_uuids):
        init_db()
        valid = set([str(x).strip() for x in (channel_uuids or []) if str(x).strip()])
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT channel_uuid FROM ff_tvh_m3u_channel_profile")
        rows = cur.fetchall()
        deleted = 0
        for row in rows:
            if row['channel_uuid'] not in valid:
                cur.execute("DELETE FROM ff_tvh_m3u_channel_profile WHERE channel_uuid = ?", (row['channel_uuid'],))
                deleted += 1
        if deleted:
            conn.commit()
        conn.close()
        return deleted


init_db()