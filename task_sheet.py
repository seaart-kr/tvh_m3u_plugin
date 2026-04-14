# -*- coding: utf-8 -*-
import os
import re
import sqlite3
from datetime import datetime

from .setup import P, logger
from .model import ModelChannel, ModelGroupOrder, ModelGroupProfile
from .task_base import TaskBase


WRITE_DB_PATH = '/data/db/ff_tvh_sheet_write.db'


class TaskSheet(TaskBase):
    @staticmethod
    def _now():
        return str(datetime.now())[:19]

    @staticmethod
    def normalize_name(value):
        text = str(value or '').strip()
        if not text:
            return ''
        text = text.upper()
        text = re.sub(r'[\s\-_./()\[\]{}]+', '', text)
        return text

    @staticmethod
    def get_match_source_info():
        return {
            'ret': 'success',
            'source': 'ff_tvh_sheet_write.db',
            'db_path': WRITE_DB_PATH,
            'exists': os.path.exists(WRITE_DB_PATH),
        }

    @staticmethod
    def _connect_write_db():
        if not os.path.exists(WRITE_DB_PATH):
            raise RuntimeError(f'기준 DB 파일이 없습니다: {WRITE_DB_PATH}')
        conn = sqlite3.connect(WRITE_DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _table_exists(conn, table_name):
        cur = conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,)
        )
        return cur.fetchone() is not None

    @staticmethod
    def _get_columns(conn, table_name):
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({table_name})")
        rows = cur.fetchall()
        return [row['name'] if isinstance(row, sqlite3.Row) else row[1] for row in rows]

    @staticmethod
    def _pick_column(columns, candidates):
        lower_map = {str(col).lower(): col for col in columns}
        for candidate in candidates:
            if candidate.lower() in lower_map:
                return lower_map[candidate.lower()]
        return None

    @staticmethod
    def _fetch_rows(conn, table_name, select_columns):
        cur = conn.cursor()
        sql = f"SELECT {', '.join(select_columns)} FROM {table_name}"
        cur.execute(sql)
        return cur.fetchall()

    @staticmethod
    def load_db_rules():
        conn = TaskSheet._connect_write_db()

        if not TaskSheet._table_exists(conn, 'channel_master'):
            conn.close()
            raise RuntimeError('channel_master 테이블이 없습니다.')

        master_cols = TaskSheet._get_columns(conn, 'channel_master')
        master_id_col = TaskSheet._pick_column(master_cols, ['id', 'channel_master_id', 'idx'])
        master_name_col = TaskSheet._pick_column(master_cols, ['standard_name', 'channel_name', 'name', 'title'])
        master_group_col = TaskSheet._pick_column(master_cols, ['group_name', 'group_category', 'category', 'group', 'group_nm'])
        master_provider_logo_col = TaskSheet._pick_column(master_cols, ['provider_logo_url', 'provider_logo', 'logo_url', 'logo', 'logo_path'])

        if not master_id_col or not master_name_col:
            conn.close()
            raise RuntimeError('channel_master 필수 컬럼(id/name)을 찾지 못했습니다.')

        select_cols = [
            f"{master_id_col} AS channel_master_id",
            f"{master_name_col} AS standard_name",
        ]
        if master_group_col:
            select_cols.append(f"{master_group_col} AS group_name")
        else:
            select_cols.append("'' AS group_name")

        if master_provider_logo_col:
            select_cols.append(f"{master_provider_logo_col} AS provider_logo_url")
        else:
            select_cols.append("'' AS provider_logo_url")

        master_rows = TaskSheet._fetch_rows(conn, 'channel_master', select_cols)

        custom_logo_by_master_id = {}
        custom_logo_by_name = {}

        if TaskSheet._table_exists(conn, 'custom_logo'):
            custom_cols = TaskSheet._get_columns(conn, 'custom_logo')
            custom_fk_col = TaskSheet._pick_column(custom_cols, ['channel_master_id', 'master_id', 'cm_id'])
            custom_name_col = TaskSheet._pick_column(custom_cols, ['standard_name', 'channel_name', 'name', 'title'])
            custom_logo_col = TaskSheet._pick_column(custom_cols, ['logo_url', 'custom_logo_url', 'url', 'path', 'logo'])

            custom_select_cols = []
            custom_select_cols.append(f"{custom_fk_col} AS channel_master_id" if custom_fk_col else "NULL AS channel_master_id")
            custom_select_cols.append(f"{custom_name_col} AS standard_name" if custom_name_col else "'' AS standard_name")
            custom_select_cols.append(f"{custom_logo_col} AS custom_logo_url" if custom_logo_col else "'' AS custom_logo_url")

            custom_rows = TaskSheet._fetch_rows(conn, 'custom_logo', custom_select_cols)
            for row in custom_rows:
                logo_url = str(row['custom_logo_url'] or '').strip()
                if not logo_url:
                    continue

                if row['channel_master_id'] is not None:
                    custom_logo_by_master_id[row['channel_master_id']] = logo_url

                std_name = str(row['standard_name'] or '').strip().lower()
                if std_name:
                    custom_logo_by_name[std_name] = logo_url

        master_exact = {}
        alias_exact = {}
        master_norm = {}
        alias_norm = {}
        master_info_by_id = {}
        master_info_by_name = {}

        for row in master_rows:
            standard_name = str(row['standard_name'] or '').strip()
            if not standard_name:
                continue

            channel_master_id = row['channel_master_id']
            group_name = str(row['group_name'] or '').strip()
            provider_logo_url = str(row['provider_logo_url'] or '').strip()
            custom_logo_url = custom_logo_by_master_id.get(channel_master_id, '') or custom_logo_by_name.get(standard_name.lower(), '')
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

            norm = TaskSheet.normalize_name(standard_name)
            if norm:
                master_norm[norm] = item

        if TaskSheet._table_exists(conn, 'channel_alias'):
            alias_cols = TaskSheet._get_columns(conn, 'channel_alias')
            alias_fk_col = TaskSheet._pick_column(alias_cols, ['channel_master_id', 'master_id', 'cm_id'])
            alias_name_col = TaskSheet._pick_column(alias_cols, ['alias_name', 'aka_name', 'alias', 'aka', 'name'])
            alias_standard_name_col = TaskSheet._pick_column(alias_cols, ['standard_name', 'channel_name', 'master_name'])

            if alias_name_col:
                alias_select_cols = [
                    f"{alias_name_col} AS alias_name",
                    f"{alias_fk_col} AS channel_master_id" if alias_fk_col else "NULL AS channel_master_id",
                    f"{alias_standard_name_col} AS standard_name" if alias_standard_name_col else "'' AS standard_name",
                ]

                alias_rows = TaskSheet._fetch_rows(conn, 'channel_alias', alias_select_cols)
                for row in alias_rows:
                    alias_name = str(row['alias_name'] or '').strip()
                    if not alias_name:
                        continue

                    item = None
                    if row['channel_master_id'] in master_info_by_id:
                        item = master_info_by_id[row['channel_master_id']]
                    else:
                        std_name = str(row['standard_name'] or '').strip().lower()
                        if std_name and std_name in master_info_by_name:
                            item = master_info_by_name[std_name]

                    if item is None:
                        continue

                    alias_exact[alias_name.lower()] = item
                    norm = TaskSheet.normalize_name(alias_name)
                    if norm:
                        alias_norm[norm] = item

        conn.close()

        return {
            'master_exact': master_exact,
            'alias_exact': alias_exact,
            'master_norm': master_norm,
            'alias_norm': alias_norm,
        }

    @staticmethod
    def match_channel(channel_name, rules):
        name = str(channel_name or '').strip()
        if not name:
            return None, None

        key = name.lower()
        norm = TaskSheet.normalize_name(name)

        if key in rules['master_exact']:
            return rules['master_exact'][key], 'master_exact'
        if key in rules['alias_exact']:
            return rules['alias_exact'][key], 'alias_exact'
        if norm in rules['master_norm']:
            return rules['master_norm'][norm], 'master_norm'
        if norm in rules['alias_norm']:
            return rules['alias_norm'][norm], 'alias_norm'
        return None, None

    @staticmethod
    def apply_db_rules():
        try:
            channels = ModelChannel.get_all()
            if not channels:
                return {'ret': 'warning', 'msg': '먼저 채널 동기화를 실행하세요.'}

            rules = TaskSheet.load_db_rules()

            updates = []
            matched_count = 0
            unmatched_count = 0

            for row in channels:
                info, _match_type = TaskSheet.match_channel(row.name, rules)
                if info is None:
                    unmatched_count += 1
                    continue

                matched_count += 1

                manual_group_name = str(getattr(row, 'manual_group_name', '') or '').strip()
                sheet_group_name = ''
                if not manual_group_name:
                    sheet_group_name = str(info.get('group_name') or '').strip()

                custom_logo_url = str(info.get('custom_logo_url') or '').strip()
                provider_logo_url = str(info.get('provider_logo_url') or '').strip()
                final_logo_url = str(info.get('final_logo_url') or '').strip()

                updates.append({
                    'channel_uuid': row.channel_uuid,
                    'sheet_group_name': sheet_group_name,
                    'sheet_logo_url': final_logo_url,
                    'sheet_logo_wave1': provider_logo_url,
                    'sheet_logo_wave2': provider_logo_url,
                    'sheet_logo_custom': custom_logo_url,
                })

            ModelChannel.replace_sheet_matches(updates)

            group_names = ModelChannel.get_effective_group_names()
            ModelGroupOrder.sync_from_group_names(group_names)
            ModelGroupProfile.cleanup_by_group_names(group_names)

            P.ModelSetting.set('basic_match_last_run_time', TaskSheet._now())
            P.ModelSetting.set('basic_match_last_count', str(matched_count))
            P.ModelSetting.set('basic_match_last_unmatched_count', str(unmatched_count))
            P.ModelSetting.set('basic_match_source', WRITE_DB_PATH)

            logger.info(
                f'[ff_tvh_m3u] apply_db_rules done matched={matched_count} '
                f'unmatched={unmatched_count} db={WRITE_DB_PATH}'
            )

            return {
                'ret': 'success',
                'msg': f'기준 DB 매칭 완료: 매칭 {matched_count} / 미매칭 {unmatched_count}',
                'matched_count': matched_count,
                'unmatched_count': unmatched_count,
                'db_path': WRITE_DB_PATH,
            }
        except Exception as e:
            logger.exception(f'[ff_tvh_m3u] apply_db_rules exception: {str(e)}')
            return {'ret': 'danger', 'msg': f'기준 DB 매칭 실패: {str(e)}'}

    @staticmethod
    def clear_sheet_matches(reason='manual'):
        try:
            cleared_count = ModelChannel.clear_sheet_matches()
            group_names = ModelChannel.get_effective_group_names()
            ModelGroupOrder.sync_from_group_names(group_names)
            ModelGroupProfile.cleanup_by_group_names(group_names)
            P.ModelSetting.set('basic_match_last_count', '0')
            P.ModelSetting.set('basic_match_last_unmatched_count', '0')

            if reason == 'manual':
                msg = f'기준 DB 매칭 결과 {cleared_count}건을 초기화했습니다.'
            else:
                msg = f'매칭 결과 {cleared_count}건을 초기화했습니다.'

            return {
                'ret': 'success',
                'msg': msg,
                'cleared_count': cleared_count,
            }
        except Exception as e:
            logger.exception(f'[ff_tvh_m3u] clear_sheet_matches exception: {str(e)}')
            return {'ret': 'danger', 'msg': f'매칭 초기화 실패: {str(e)}'}

    @staticmethod
    def get_sheet_group_names():
        return ModelChannel.get_assignable_group_names()
