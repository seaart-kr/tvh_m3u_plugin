# -*- coding: utf-8 -*-
from .setup import logger
from .model import ModelChannel, ModelGroupProfile, ModelChannelProfile
from .task_base import TaskBase


class TaskProfile(TaskBase):
    @staticmethod
    def get_group_profile_map():
        return ModelGroupProfile.get_map()

    @staticmethod
    def get_channel_profile_map():
        return ModelChannelProfile.get_map()

    @staticmethod
    def save_group_profile(group_name='', profile=''):
        try:
            group_name = str(group_name or '').strip()
            profile = str(profile or '').strip()

            if not group_name:
                return {'ret': 'warning', 'msg': '그룹명이 비어 있습니다.'}

            valid_groups = set(ModelChannel.get_effective_group_names())
            if group_name not in valid_groups:
                return {'ret': 'warning', 'msg': '존재하지 않는 그룹입니다.'}

            ok = ModelGroupProfile.upsert(group_name, profile)
            if not ok:
                return {'ret': 'warning', 'msg': '그룹 프로필 저장 실패'}

            logger.info(
                f'[ff_tvh_m3u] save_group_profile success group_name={group_name} profile={profile or "<default>"}'
            )
            return {
                'ret': 'success',
                'msg': f'[{group_name}] 그룹 프로필을 저장했습니다.' if profile else f'[{group_name}] 그룹 프로필을 기본값으로 되돌렸습니다.',
                'group_name': group_name,
                'profile': profile,
            }
        except Exception as e:
            logger.exception(f'[ff_tvh_m3u] save_group_profile exception: {str(e)}')
            return {'ret': 'danger', 'msg': f'그룹 프로필 저장 실패: {str(e)}'}

    @staticmethod
    def save_channel_profile(channel_uuid='', profile=''):
        try:
            channel_uuid = str(channel_uuid or '').strip()
            profile = str(profile or '').strip()

            if not channel_uuid:
                return {'ret': 'warning', 'msg': '채널 UUID가 비어 있습니다.'}

            channel_map = {row.channel_uuid: row for row in ModelChannel.get_all()}
            row = channel_map.get(channel_uuid)
            if row is None:
                return {'ret': 'warning', 'msg': '존재하지 않는 채널입니다.'}

            ok = ModelChannelProfile.upsert(channel_uuid, profile)
            if not ok:
                return {'ret': 'warning', 'msg': '채널 프로필 저장 실패'}

            logger.info(
                f'[ff_tvh_m3u] save_channel_profile success channel_uuid={channel_uuid} profile={profile or "<default>"}'
            )
            return {
                'ret': 'success',
                'msg': f'[{row.name}] 채널 프로필을 저장했습니다.' if profile else f'[{row.name}] 채널 프로필을 기본값으로 되돌렸습니다.',
                'channel_uuid': channel_uuid,
                'profile': profile,
                'channel_name': row.name,
            }
        except Exception as e:
            logger.exception(f'[ff_tvh_m3u] save_channel_profile exception: {str(e)}')
            return {'ret': 'danger', 'msg': f'채널 프로필 저장 실패: {str(e)}'}
