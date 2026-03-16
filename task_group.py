# -*- coding: utf-8 -*-
from .setup import logger
from .model import ModelChannel, ModelGroupOrder, ModelSheetRule
from .task_base import TaskBase


class TaskGroup(TaskBase):
    @staticmethod
    def get_sheet_group_names():
        names = ModelSheetRule.get_group_names()
        if names:
            return names
        return ModelChannel.get_assignable_group_names()

    @staticmethod
    def move_group(group_name, direction):
        try:
            group_name = str(group_name or '').strip()
            direction = str(direction or '').strip().lower()

            if not group_name:
                return {'ret': 'warning', 'msg': '그룹명이 비어 있습니다.'}

            ok, msg = ModelGroupOrder.move(group_name, direction)
            if not ok:
                logger.warning(f'[ff_tvh_m3u] move_group failed group_name={group_name} direction={direction} msg={msg}')
                return {'ret': 'warning', 'msg': msg}

            logger.info(f'[ff_tvh_m3u] move_group success group_name={group_name} direction={direction}')
            return {'ret': 'success', 'msg': msg, 'grouped_channels': ModelChannel.get_grouped()}

        except Exception as e:
            logger.exception(f'[ff_tvh_m3u] move_group exception: {str(e)}')
            return {'ret': 'danger', 'msg': f'그룹 이동 실패: {str(e)}'}

    @staticmethod
    def assign_channels_to_group(new_group_name='', target_group_name='', channel_uuids=None):
        try:
            channel_uuids = [str(x).strip() for x in (channel_uuids or []) if str(x).strip()]
            new_group_name = str(new_group_name or '').strip()
            target_group_name = str(target_group_name or '').strip()

            use_manual_new_group = bool(new_group_name)
            group_name = new_group_name if use_manual_new_group else target_group_name

            if not channel_uuids:
                return {'ret': 'warning', 'msg': '선택된 채널이 없습니다.'}
            if not group_name:
                return {'ret': 'warning', 'msg': '이동할 그룹을 선택하거나 새 그룹명을 입력하세요.'}
            if group_name == '그룹 없음':
                return {'ret': 'warning', 'msg': '그룹 없음으로 수동 배정할 수 없습니다.'}

            # 새 그룹명 직접 입력은 항상 허용한다.
            # 자동 채널 매칭 이후에도 수동 그룹으로 덮어쓸 수 있어야 한다.
            if not use_manual_new_group:
                valid_groups = TaskGroup.get_sheet_group_names()
                if valid_groups and group_name not in valid_groups:
                    return {
                        'ret': 'warning',
                        'msg': '시트 그룹 목록에 없는 이름입니다. 새 그룹을 만들려면 직접 입력을 사용하세요.'
                    }

            changed_count = ModelChannel.assign_manual_group(channel_uuids, group_name)
            ModelGroupOrder.sync_from_group_names(ModelChannel.get_effective_group_names())

            logger.info(
                f'[ff_tvh_m3u] assign_channels_to_group success '
                f'group_name={group_name} manual_new={use_manual_new_group} changed={changed_count}'
            )
            return {
                'ret': 'success',
                'msg': f'{changed_count}개 채널을 [{group_name}] 그룹으로 저장했습니다.',
                'group_name': group_name,
                'changed_count': changed_count,
                'is_new_group': use_manual_new_group,
                'grouped_channels': ModelChannel.get_grouped(),
                'ungrouped_channels': ModelChannel.get_ungrouped(),
            }

        except Exception as e:
            logger.exception(f'[ff_tvh_m3u] assign_channels_to_group exception: {str(e)}')
            return {'ret': 'danger', 'msg': f'채널 그룹 지정 실패: {str(e)}'}

    @staticmethod
    def clear_manual_group(channel_uuids=None):
        try:
            channel_uuids = [str(x).strip() for x in (channel_uuids or []) if str(x).strip()]
            if not channel_uuids:
                return {'ret': 'warning', 'msg': '선택된 채널이 없습니다.'}

            manual_map = ModelChannel.get_manual_group_map()
            target_uuids = [uuid for uuid in channel_uuids if manual_map.get(uuid)]
            if not target_uuids:
                return {
                    'ret': 'warning',
                    'msg': '선택한 채널에 해제할 수동 그룹이 없습니다. 수동지정 해제는 [수동] 배지가 있는 채널에만 적용됩니다.'
                }

            changed_count = ModelChannel.clear_manual_group(target_uuids)
            ModelGroupOrder.sync_from_group_names(ModelChannel.get_effective_group_names())

            logger.info(f'[ff_tvh_m3u] clear_manual_group success changed={changed_count}')
            return {
                'ret': 'success',
                'msg': f'{changed_count}개 채널의 수동 그룹 지정을 해제했습니다. 시트 매칭이 있는 채널은 해당 시트 그룹으로 다시 표시됩니다.',
                'changed_count': changed_count,
                'grouped_channels': ModelChannel.get_grouped(),
                'ungrouped_channels': ModelChannel.get_ungrouped(),
            }
        except Exception as e:
            logger.exception(f'[ff_tvh_m3u] clear_manual_group exception: {str(e)}')
            return {'ret': 'danger', 'msg': f'수동 그룹 해제 실패: {str(e)}'}
