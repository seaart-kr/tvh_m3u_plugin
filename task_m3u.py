# -*- coding: utf-8 -*-
import re
from urllib.parse import urlsplit, urlunsplit

from .setup import P, logger
from .model import ModelChannel, ModelGroupProfile, ModelChannelProfile
from .task_base import TaskBase


class TaskM3U(TaskBase):
    @staticmethod
    def fetch_playlist_map():
        """
        TVH 원본 /playlist/channels 에서
        tvg-id(UUID) -> 실제 재생 URL 맵을 만든다.
        """
        try:
            api_base = TaskM3U.get_api_base()
            if not api_base:
                return {}

            session = TaskM3U.get_session()
            playlist_url = f'{api_base}/playlist/channels'
            logger.debug(f'[ff_tvh_m3u] playlist_url={playlist_url}')

            resp = session.get(playlist_url, timeout=30)
            resp.raise_for_status()
            text = resp.text or ''

            mapping = {}
            current_uuid = None

            for raw_line in text.splitlines():
                line = raw_line.strip()
                if not line:
                    continue

                if line.startswith('#EXTINF'):
                    match = re.search(r'tvg-id="([^"]+)"', line)
                    current_uuid = match.group(1).strip() if match else None
                    continue

                if line.startswith('http://') or line.startswith('https://'):
                    if current_uuid:
                        mapping[current_uuid] = line
                        current_uuid = None

            logger.debug(f'[ff_tvh_m3u] playlist map count={len(mapping)}')
            return mapping

        except Exception as e:
            logger.exception(f'[ff_tvh_m3u] fetch_playlist_map exception: {str(e)}')
            return {}

    @staticmethod
    def normalize_stream_url(source_url, profile=''):
        """
        TVH 원본 playlist URL의 path/query는 유지하고,
        호스트/포트/스킴은 플러그인 설정의 TVH 스트림 주소를 기준으로 강제한다.
        이후 재생 계정/프로필 옵션을 반영한다.
        """
        if not source_url:
            return ''

        stream_base = TaskM3U.get_stream_base()
        if not stream_base:
            return ''

        source_parts = urlsplit(source_url)
        base_parts = urlsplit(stream_base)

        url = urlunsplit((
            base_parts.scheme,
            base_parts.netloc,
            source_parts.path,
            source_parts.query,
            source_parts.fragment
        ))

        if TaskM3U.get_include_auth_in_url():
            username = TaskM3U.get_play_username()
            password = TaskM3U.get_play_password()
            url = TaskM3U.remove_auth_from_url(url)
            url = TaskM3U.inject_auth_to_url(url, username, password)
        else:
            url = TaskM3U.remove_auth_from_url(url)

        if profile:
            url = TaskM3U.set_query_param(url, 'profile', profile)
        else:
            url = TaskM3U.set_query_param(url, 'profile', '')

        return url

    @staticmethod
    def get_effective_profile(channel_uuid='', group_name=''):
        channel_uuid = str(channel_uuid or '').strip()
        group_name = str(group_name or '').strip()

        channel_profile = ModelChannelProfile.get_profile(channel_uuid)
        if channel_profile:
            return channel_profile

        group_profile = ModelGroupProfile.get_profile(group_name)
        if group_profile:
            return group_profile

        return TaskM3U.get_stream_profile()

    @staticmethod
    def _sanitize_attr(value):
        value = '' if value is None else str(value)
        return value.replace('&', '&amp;').replace('"', '&quot;').replace('\n', ' ').replace('\r', ' ').strip()

    @staticmethod
    def _sanitize_name(value):
        value = '' if value is None else str(value)
        return value.replace('\n', ' ').replace('\r', ' ').strip()

    @staticmethod
    def build_extinf(target, channel_uuid, tvg_name, tvg_chno, group_name, sheet_logo_url=''):
        target = str(target or 'tivimate').strip().lower()
        channel_uuid = TaskM3U._sanitize_attr(channel_uuid)
        tvg_name_attr = TaskM3U._sanitize_attr(tvg_name)
        tvg_name_text = TaskM3U._sanitize_name(tvg_name)
        tvg_chno = TaskM3U._sanitize_attr(tvg_chno)
        group_name = TaskM3U._sanitize_attr(group_name)
        sheet_logo_url = TaskM3U._sanitize_attr(sheet_logo_url)

        attrs = [
            f'tvg-id="{channel_uuid}"',
            f'tvg-name="{tvg_name_attr}"',
            f'tvg-chno="{tvg_chno}"',
            f'group-title="{group_name}"',
        ]

        if target == 'tivimate' and sheet_logo_url:
            attrs.append(f'tvg-logo="{sheet_logo_url}"')

        return f'#EXTINF:-1 {" ".join(attrs)},{tvg_name_text}'

    @staticmethod
    def build_m3u(target='tivimate'):
        try:
            target = str(target or 'tivimate').strip().lower()
            if target not in ['tvh', 'tivimate']:
                target = 'tivimate'

            logger.info(f'[ff_tvh_m3u] build_m3u start target={target}')

            grouped_rows = ModelChannel.get_grouped()
            total_groups = len(grouped_rows)
            total_channels = sum(len(x) for x in grouped_rows.values())
            logger.info(f'[ff_tvh_m3u] build_m3u source groups={total_groups} channels={total_channels}')

            lines = ['#EXTM3U']
            playlist_map = TaskM3U.fetch_playlist_map()
            playlist_map_count = len(playlist_map)
            logger.info(f'[ff_tvh_m3u] build_m3u playlist_map_count={playlist_map_count}')

            added_count = 0
            skipped_disabled = 0
            skipped_no_playlist = 0
            skipped_empty_url = 0

            for group_name, channels in grouped_rows.items():
                for ch in channels:
                    if not ch.get('enabled', True):
                        skipped_disabled += 1
                        continue

                    tvg_name = ch.get('name') or ''
                    tvg_chno = ch.get('number') or 0
                    channel_uuid = ch.get('channel_uuid')
                    sheet_logo_url = str(ch.get('sheet_logo_url') or '').strip()

                    source_url = playlist_map.get(channel_uuid, '')
                    if not source_url:
                        skipped_no_playlist += 1
                        continue

                    effective_profile = TaskM3U.get_effective_profile(channel_uuid, group_name)
                    stream_url = TaskM3U.normalize_stream_url(source_url, effective_profile)
                    if not stream_url:
                        skipped_empty_url += 1
                        continue

                    extinf = TaskM3U.build_extinf(
                        target=target,
                        channel_uuid=channel_uuid,
                        tvg_name=tvg_name,
                        tvg_chno=tvg_chno,
                        group_name=group_name,
                        sheet_logo_url=sheet_logo_url,
                    )
                    lines.append(extinf)
                    lines.append(stream_url)
                    added_count += 1

            logger.info(
                f'[ff_tvh_m3u] build_m3u done target={target} added={added_count} '
                f'skipped_disabled={skipped_disabled} '
                f'skipped_no_playlist={skipped_no_playlist} '
                f'skipped_empty_url={skipped_empty_url}'
            )

            return '\n'.join(lines) + '\n'

        except Exception as e:
            logger.exception(f'[ff_tvh_m3u] build_m3u exception: {str(e)}')
            return '#EXTM3U\n'

    @staticmethod
    def get_m3u_url(target='tivimate'):
        try:
            from flask import request
            base_url = request.host_url.rstrip('/')
            target = str(target or 'tivimate').strip().lower()
            if target == 'tvh':
                return f'{base_url}/{P.package_name}/api/m3u_tvh'
            if target == 'tivimate':
                return f'{base_url}/{P.package_name}/api/m3u_tivimate'
            return f'{base_url}/{P.package_name}/api/m3u'
        except Exception as e:
            logger.exception(f'[ff_tvh_m3u] get_m3u_url exception: {str(e)}')
            if str(target or '').strip().lower() == 'tvh':
                return f'/{P.package_name}/api/m3u_tvh'
            if str(target or '').strip().lower() == 'tivimate':
                return f'/{P.package_name}/api/m3u_tivimate'
            return f'/{P.package_name}/api/m3u'
