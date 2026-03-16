# -*- coding: utf-8 -*-
import json
from datetime import datetime

from .setup import P, logger
from .model import ModelTag, ModelChannel, ModelGroupOrder, ModelGroupProfile, ModelChannelProfile, DB_PATH
from .task_base import TaskBase


class TaskSync(TaskBase):
    @staticmethod
    def sync_channels():
        try:
            api_base = TaskSync.get_api_base()
            if not api_base:
                return {'ret': 'danger', 'msg': 'TVH API 주소가 비어 있습니다.'}

            logger.info('[ff_tvh_m3u] sync_channels start')
            session = TaskSync.get_session()

            tag_url = f'{api_base}/api/channeltag/grid?start=0&limit=99999'
            channel_url = f'{api_base}/api/channel/grid?start=0&limit=99999'

            logger.debug(f'[ff_tvh_m3u] tag_url={tag_url}')
            logger.debug(f'[ff_tvh_m3u] channel_url={channel_url}')
            logger.debug(f'[ff_tvh_m3u] sync db path = {DB_PATH}')

            manual_group_map = ModelChannel.get_manual_group_map()
            logger.debug(f'[ff_tvh_m3u] preserved manual groups count={len(manual_group_map)}')

            tag_resp = session.get(tag_url, timeout=20)
            tag_resp.raise_for_status()
            tag_json = tag_resp.json()

            channel_resp = session.get(channel_url, timeout=30)
            channel_resp.raise_for_status()
            channel_json = channel_resp.json()

            entries_tag = tag_json.get('entries', []) or []
            entries_channel = channel_json.get('entries', []) or []

            logger.info(f'[ff_tvh_m3u] sync_channels fetched tags={len(entries_tag)} channels={len(entries_channel)}')

            ModelChannel.clear_all()
            ModelTag.clear_all()
            logger.info('[ff_tvh_m3u] sync_channels cleared local tables')

            ModelTag.bulk_insert(entries_tag)

            tag_map = {row.tag_uuid: row.name for row in ModelTag.get_all()}
            channel_count = 0
            grouped_preview = {}
            channel_rows = []

            for item in entries_channel:
                channel_uuid = str(item.get('uuid', '')).strip()
                if not channel_uuid:
                    continue

                raw_tags = item.get('tags', []) or []
                if not isinstance(raw_tags, list):
                    raw_tags = []

                tag_names = []
                for tag_uuid in raw_tags:
                    tag_uuid_str = str(tag_uuid).strip()
                    if tag_uuid_str in tag_map:
                        tag_names.append(tag_map[tag_uuid_str])

                tvh_group_name = tag_names[0] if tag_names else '그룹 없음'
                channel_name = str(item.get('name', '')).strip() or channel_uuid
                manual_group_name = str(manual_group_map.get(channel_uuid, '') or '').strip()
                effective_group_name = manual_group_name or tvh_group_name or '그룹 없음'

                channel_rows.append({
                    'channel_uuid': channel_uuid,
                    'number': item.get('number') or 0,
                    'name': channel_name,
                    'enabled': bool(item.get('enabled', True)),
                    'raw_tags': json.dumps(raw_tags, ensure_ascii=False),
                    'group_name': tvh_group_name,
                    'manual_group_name': manual_group_name,
                    'sheet_group_name': '',
                    'sheet_logo_url': '',
                    'raw_data': json.dumps(item, ensure_ascii=False),
                })
                channel_count += 1

                grouped_preview.setdefault(effective_group_name, 0)
                grouped_preview[effective_group_name] += 1

            ModelChannel.bulk_insert(channel_rows)
            ModelGroupOrder.sync_from_group_names(list(grouped_preview.keys()))
            ModelGroupProfile.cleanup_by_group_names(ModelChannel.get_effective_group_names())
            ModelChannelProfile.cleanup_by_channel_uuids([row['channel_uuid'] for row in channel_rows])

            preview_items = sorted(grouped_preview.items(), key=lambda x: x[0])
            preview_text = ', '.join([f'{k}:{v}' for k, v in preview_items[:20]])
            if len(preview_items) > 20:
                preview_text += f' ... (+{len(preview_items) - 20} groups)'

            synced_at = str(datetime.now())[:19]
            P.ModelSetting.set('basic_last_sync_count', str(channel_count))
            P.ModelSetting.set('basic_last_sync_time', synced_at)

            logger.info(f'[ff_tvh_m3u] sync_channels groups={preview_text}')
            logger.info(f'[ff_tvh_m3u] sync_channels done channels={channel_count} groups={len(grouped_preview)} synced_at={synced_at}')

            return {
                'ret': 'success',
                'msg': f'채널 동기화 완료: TVH 채널 {channel_count}개 / 그룹 {len(grouped_preview)}개',
                'count': channel_count,
                'group_count': len(grouped_preview),
                'last_sync_time': synced_at,
            }

        except Exception as e:
            logger.exception(f'[ff_tvh_m3u] sync_channels exception: {str(e)}')
            return {'ret': 'danger', 'msg': f'동기화 실패: {str(e)}'}
