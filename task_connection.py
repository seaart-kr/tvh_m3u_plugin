# -*- coding: utf-8 -*-
from .setup import logger
from .task_base import TaskBase


class TaskConnection(TaskBase):
    @staticmethod
    def _safe_json(response):
        try:
            return response.json()
        except Exception:
            return {'text': response.text[:1000]}

    @staticmethod
    def _extract_profiles(data):
        if isinstance(data, dict):
            entries = data.get('entries', []) or []
        elif isinstance(data, list):
            entries = data
        else:
            entries = []

        profiles = []
        for item in entries:
            if not isinstance(item, dict):
                continue

            candidates = [
                item.get('val'),
                item.get('text'),
                item.get('name'),
                item.get('profile'),
                item.get('value'),
                item.get('key'),
            ]

            name = ''
            for candidate in candidates:
                candidate = str(candidate or '').strip()
                if candidate:
                    name = candidate
                    break

            if name and name not in profiles:
                profiles.append(name)

        return profiles, entries

    @staticmethod
    def get_play_profiles():
        try:
            api_base = TaskConnection.get_api_base()
            if not api_base:
                return {'ret': 'danger', 'msg': 'TVH API 주소가 비어 있습니다.', 'profiles': []}

            session = TaskConnection.get_play_session()
            url = f'{api_base}/api/profile/list'

            response = session.get(url, timeout=10)
            response.raise_for_status()

            data = TaskConnection._safe_json(response)
            profiles, entries = TaskConnection._extract_profiles(data)

            logger.info('[ff_tvh_m3u] get_play_profiles success')
            return {
                'ret': 'success',
                'msg': f'사용 가능한 프로필 {len(profiles)}개 확인',
                'profiles': profiles,
                'entries': entries,
            }

        except Exception as e:
            logger.warning(f'[ff_tvh_m3u] get_play_profiles failed: {str(e)}')
            return {
                'ret': 'warning',
                'msg': f'프로필 목록 조회 실패: {str(e)}',
                'profiles': [],
            }

    @staticmethod
    def test_admin_login():
        try:
            api_base = TaskConnection.get_api_base()
            if not api_base:
                return {'ret': 'danger', 'msg': 'TVH API 주소가 비어 있습니다.'}

            session = TaskConnection.get_session()

            serverinfo = None
            channel_sample = None

            url_serverinfo = f'{api_base}/api/serverinfo'
            resp = session.get(url_serverinfo, timeout=10)
            resp.raise_for_status()
            serverinfo = TaskConnection._safe_json(resp)

            url_channel = f'{api_base}/api/channel/grid?start=0&limit=1'
            resp = session.get(url_channel, timeout=10)
            resp.raise_for_status()
            channel_data = TaskConnection._safe_json(resp)
            entries = channel_data.get('entries', []) if isinstance(channel_data, dict) else []
            channel_sample = entries[0] if entries else None

            logger.info('[ff_tvh_m3u] test_admin_login success')
            return {
                'ret': 'success',
                'msg': '관리자 로그인 성공',
                'serverinfo': serverinfo,
                'channel_sample': channel_sample,
                'summary': {
                    'serverinfo_ok': True,
                    'channel_api_ok': True,
                }
            }

        except Exception as e:
            logger.warning(f'[ff_tvh_m3u] test_admin_login failed: {str(e)}')
            return {'ret': 'danger', 'msg': f'관리자 로그인 실패: {str(e)}'}

    @staticmethod
    def test_play_login():
        try:
            api_base = TaskConnection.get_api_base()
            if not api_base:
                return {'ret': 'danger', 'msg': 'TVH API 주소가 비어 있습니다.', 'profiles': []}

            session = TaskConnection.get_play_session()

            profiles = []
            profile_entries = []
            playlist_ok = False
            playlist_line_count = 0

            url_profile = f'{api_base}/api/profile/list'
            resp = session.get(url_profile, timeout=10)
            resp.raise_for_status()

            profile_data = TaskConnection._safe_json(resp)
            profiles, profile_entries = TaskConnection._extract_profiles(profile_data)

            try:
                url_playlist = f'{api_base}/playlist/channels'
                resp = session.get(url_playlist, timeout=15)
                resp.raise_for_status()
                playlist_text = resp.text or ''
                playlist_ok = '#EXTM3U' in playlist_text or '#EXTINF' in playlist_text
                playlist_line_count = len([x for x in playlist_text.splitlines() if x.strip()])
            except Exception as e:
                logger.debug(f'[ff_tvh_m3u] test_play_login playlist optional failed: {str(e)}')

            logger.info('[ff_tvh_m3u] test_play_login success')
            return {
                'ret': 'success',
                'msg': '재생 계정 로그인 성공',
                'profiles': profiles,
                'profile_entries': profile_entries,
                'summary': {
                    'profile_count': len(profiles),
                    'playlist_ok': playlist_ok,
                    'playlist_line_count': playlist_line_count,
                }
            }

        except Exception as e:
            logger.warning(f'[ff_tvh_m3u] test_play_login failed: {str(e)}')
            return {'ret': 'danger', 'msg': f'재생 계정 로그인 실패: {str(e)}', 'profiles': []}

    @staticmethod
    def test_connection():
        return TaskConnection.test_admin_login()
