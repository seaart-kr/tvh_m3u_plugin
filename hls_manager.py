# -*- coding: utf-8 -*-
import atexit
import base64
import hashlib
import os
import re
import shutil
import subprocess
import threading
import time

from .setup import logger


class HLSManager:
    CACHE_ROOT = '/data/tmp/tvh_m3u_plugin_hls'
    IDLE_TIMEOUT = 15
    START_TIMEOUT = 15
    CLEANUP_INTERVAL = 3
    PLAYLIST_MAX_AGE = 8
    LOCK_STALE_AGE = 90
    MAX_STREAMS = 0
    LIMIT_POLICY = 'reject'
    CONSUMER_MODE = 'shared'
    SWITCH_DELAY = 0.5
    SEGMENT_RE = re.compile(r'^segment_[0-9]+\.ts$')

    _lock = threading.RLock()
    _streams = {}
    _start_guard = threading.Lock()
    _cleanup_started = False
    _consumer_channels = {}

    @classmethod
    def configure(
        cls,
        max_streams=0,
        limit_policy='reject',
        consumer_mode='shared',
        idle_timeout=15,
        switch_delay=0.5,
    ):
        try:
            max_streams = max(0, min(int(max_streams), 64))
        except (TypeError, ValueError):
            max_streams = 0
        try:
            idle_timeout = max(5, min(int(idle_timeout), 600))
        except (TypeError, ValueError):
            idle_timeout = 15
        try:
            switch_delay = max(0.0, min(float(switch_delay), 5.0))
        except (TypeError, ValueError):
            switch_delay = 0.5

        limit_policy = str(limit_policy or '').strip().lower()
        if limit_policy not in ['reject', 'oldest']:
            limit_policy = 'reject'
        consumer_mode = str(consumer_mode or '').strip().lower()
        if consumer_mode not in ['shared', 'single']:
            consumer_mode = 'shared'

        with cls._lock:
            cls.MAX_STREAMS = max_streams
            cls.LIMIT_POLICY = limit_policy
            cls.CONSUMER_MODE = consumer_mode
            cls.IDLE_TIMEOUT = idle_timeout
            cls.SWITCH_DELAY = switch_delay
        return {
            'max_streams': max_streams,
            'limit_policy': limit_policy,
            'consumer_mode': consumer_mode,
            'idle_timeout': idle_timeout,
            'switch_delay': switch_delay,
        }

    @classmethod
    def _normalize_consumer_id(cls, consumer_id):
        value = str(consumer_id or '').strip()
        if not value or len(value) > 64:
            return ''
        if not re.fullmatch(r'[A-Za-z0-9_-]+', value):
            return ''
        return value

    @classmethod
    def _detach_stream_locked(cls, channel_uuid):
        channel_uuid = str(channel_uuid or '').strip()
        state = cls._streams.pop(channel_uuid, None)
        for consumer_id, mapped_channel in list(cls._consumer_channels.items()):
            if mapped_channel == channel_uuid:
                cls._consumer_channels.pop(consumer_id, None)
        return state

    @classmethod
    def _prepare_request_locked(cls, channel_uuid, consumer_id):
        consumer_id = cls._normalize_consumer_id(consumer_id)
        states_to_stop = []

        for existing_uuid, existing_state in list(cls._streams.items()):
            process = existing_state.get('process')
            if process is None or process.poll() is not None:
                detached = cls._detach_stream_locked(existing_uuid)
                if detached:
                    states_to_stop.append(detached)

        state = cls._streams.get(channel_uuid)
        if cls.CONSUMER_MODE == 'single' and consumer_id:
            previous_uuid = cls._consumer_channels.get(consumer_id)
            if previous_uuid and previous_uuid != channel_uuid:
                previous_state = cls._streams.get(previous_uuid)
                if previous_state is not None:
                    previous_state.setdefault('consumers', set()).discard(consumer_id)
                    if not previous_state.get('consumers'):
                        detached = cls._detach_stream_locked(previous_uuid)
                        if detached:
                            states_to_stop.append(detached)
                cls._consumer_channels.pop(consumer_id, None)
            cls._consumer_channels[consumer_id] = channel_uuid

        if state is not None:
            if consumer_id:
                state.setdefault('consumers', set()).add(consumer_id)
            return state, states_to_stop, None

        live_states = list(cls._streams.items())
        if cls.MAX_STREAMS > 0 and len(live_states) >= cls.MAX_STREAMS:
            if cls.LIMIT_POLICY == 'reject':
                if consumer_id and cls._consumer_channels.get(consumer_id) == channel_uuid:
                    cls._consumer_channels.pop(consumer_id, None)
                return None, states_to_stop, {
                    'ret': 'warning',
                    'status': 429,
                    'msg': 'HLS stream limit reached',
                }
            oldest_uuid, _oldest_state = min(
                live_states,
                key=lambda item: item[1].get('last_access', 0),
            )
            detached = cls._detach_stream_locked(oldest_uuid)
            if detached:
                states_to_stop.append(detached)

        return None, states_to_stop, None

    @classmethod
    def _consumer_may_read_locked(cls, channel_uuid, consumer_id):
        consumer_id = cls._normalize_consumer_id(consumer_id)
        if cls.CONSUMER_MODE != 'single' or not consumer_id:
            return True
        current_channel = cls._consumer_channels.get(consumer_id)
        return not current_channel or current_channel == channel_uuid

    @classmethod
    def _stream_key(cls, channel_uuid):
        return hashlib.sha256(str(channel_uuid).encode('utf-8')).hexdigest()[:32]

    @classmethod
    def _stream_dir(cls, channel_uuid):
        return os.path.join(cls.CACHE_ROOT, cls._stream_key(channel_uuid))

    @classmethod
    def _start_lock_path(cls, channel_uuid):
        return os.path.join(cls.CACHE_ROOT, f'.{cls._stream_key(channel_uuid)}.lock')

    @classmethod
    def _touch_shared_access(cls, channel_uuid, directory):
        try:
            os.makedirs(directory, exist_ok=True)
            access_path = os.path.join(directory, '.access')
            with open(access_path, 'a', encoding='utf-8'):
                os.utime(access_path, None)
            lock_path = cls._start_lock_path(channel_uuid)
            if os.path.exists(lock_path):
                os.utime(lock_path, None)
        except OSError:
            pass

    @classmethod
    def _read_ready_playlist(cls, directory):
        playlist_path = os.path.join(directory, 'index.m3u8')
        try:
            if time.time() - os.path.getmtime(playlist_path) > cls.PLAYLIST_MAX_AGE:
                return ''
            with open(playlist_path, 'r', encoding='utf-8') as handle:
                playlist = handle.read()
            if '#EXTM3U' in playlist and '#EXTINF:' in playlist:
                return playlist
        except OSError:
            pass
        return ''

    @classmethod
    def _acquire_start_lock(cls, channel_uuid):
        os.makedirs(cls.CACHE_ROOT, exist_ok=True)
        lock_path = cls._start_lock_path(channel_uuid)
        for _attempt in range(2):
            try:
                fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                with os.fdopen(fd, 'w', encoding='ascii') as handle:
                    handle.write(str(os.getpid()))
                return lock_path
            except FileExistsError:
                try:
                    if time.time() - os.path.getmtime(lock_path) > cls.LOCK_STALE_AGE:
                        os.unlink(lock_path)
                        continue
                except OSError:
                    continue
                return ''
        return ''

    @classmethod
    def _safe_clear_directory(cls, directory):
        root = os.path.realpath(cls.CACHE_ROOT)
        target = os.path.realpath(directory)
        if target == root or not target.startswith(root + os.sep):
            raise ValueError('unsafe HLS cache path')
        if os.path.isdir(target):
            shutil.rmtree(target)

    @classmethod
    def _stop_state(cls, state, clear=True):
        process = state.get('process') if state else None
        if process is not None and process.poll() is None:
            try:
                process.terminate()
                process.wait(timeout=3)
            except Exception:
                try:
                    process.kill()
                    process.wait(timeout=2)
                except Exception:
                    pass
        if clear and state and state.get('directory'):
            try:
                cls._safe_clear_directory(state['directory'])
            except Exception as e:
                logger.warning(f'[tvh_m3u] HLS cache cleanup failed: {str(e)}')
        lock_path = state.get('lock_path') if state else ''
        if lock_path:
            try:
                os.unlink(lock_path)
            except OSError:
                pass

    @classmethod
    def _start_cleanup_thread(cls):
        with cls._lock:
            if cls._cleanup_started:
                return
            cls._cleanup_started = True
        thread = threading.Thread(
            target=cls._cleanup_loop,
            name='tvh_m3u_hls_cleanup',
            daemon=True,
        )
        thread.start()

    @classmethod
    def _cleanup_loop(cls):
        while True:
            time.sleep(cls.CLEANUP_INTERVAL)
            now = time.monotonic()
            stale = []
            with cls._lock:
                for channel_uuid, state in list(cls._streams.items()):
                    process = state.get('process')
                    is_dead = process is None or process.poll() is not None
                    last_access = state.get('last_access', now)
                    try:
                        access_path = os.path.join(state.get('directory') or '', '.access')
                        shared_idle = time.time() - os.path.getmtime(access_path)
                        is_idle = shared_idle > cls.IDLE_TIMEOUT
                    except OSError:
                        is_idle = now - last_access > cls.IDLE_TIMEOUT
                    if is_dead or is_idle:
                        detached = cls._detach_stream_locked(channel_uuid)
                        if detached:
                            stale.append(detached)
            for state in stale:
                cls._stop_state(state, clear=True)

    @classmethod
    def _build_command(cls, upstream_url, directory, username='', password=''):
        ffmpeg_path = shutil.which('ffmpeg')
        if not ffmpeg_path:
            raise RuntimeError('FFmpeg is not installed')

        command = [
            ffmpeg_path,
            '-hide_banner',
            '-loglevel', 'warning',
            '-nostdin',
            '-user_agent', 'tvh_m3u_plugin/hls-proxy',
        ]
        if username and password:
            token = base64.b64encode(
                f'{username}:{password}'.encode('utf-8')
            ).decode('ascii')
            command.extend(['-headers', f'Authorization: Basic {token}\r\n'])

        command.extend([
            '-i', upstream_url,
            '-map', '0:v:0?',
            '-map', '0:a:0?',
            '-c', 'copy',
            '-f', 'hls',
            '-hls_time', '2',
            '-hls_list_size', '6',
            '-hls_delete_threshold', '2',
            '-hls_flags', 'delete_segments+omit_endlist+independent_segments+temp_file',
            '-hls_segment_filename', os.path.join(directory, 'segment_%09d.ts'),
            os.path.join(directory, 'index.m3u8'),
        ])
        return command

    @classmethod
    def ensure_stream(cls, channel_uuid, upstream_url, username='', password='', consumer_id=''):
        channel_uuid = str(channel_uuid or '').strip()
        upstream_url = str(upstream_url or '').strip()
        if not channel_uuid or not upstream_url:
            return {'ret': 'warning', 'status': 400, 'msg': 'invalid stream'}

        cls._start_cleanup_thread()
        now = time.monotonic()
        directory = cls._stream_dir(channel_uuid)
        consumer_id = cls._normalize_consumer_id(consumer_id)

        with cls._start_guard:
            with cls._lock:
                _prepared, states_to_stop, limit_error = cls._prepare_request_locked(
                    channel_uuid,
                    consumer_id,
                )
            for stale_state in states_to_stop:
                cls._stop_state(stale_state, clear=True)
            if states_to_stop and cls.SWITCH_DELAY > 0:
                time.sleep(cls.SWITCH_DELAY)
        if limit_error:
            return limit_error

        with cls._lock:
            state = cls._streams.get(channel_uuid)
            if state and state.get('process') and state['process'].poll() is None:
                state['last_access'] = now
                if consumer_id:
                    state.setdefault('consumers', set()).add(consumer_id)
                cls._touch_shared_access(channel_uuid, state.get('directory') or directory)
            else:
                if state:
                    cls._detach_stream_locked(channel_uuid)
                    cls._stop_state(state, clear=True)

                shared_playlist = cls._read_ready_playlist(directory)
                if shared_playlist:
                    cls._touch_shared_access(channel_uuid, directory)
                    return {
                        'ret': 'success',
                        'playlist': shared_playlist,
                        'directory': directory,
                    }

                lock_path = cls._acquire_start_lock(channel_uuid)
                if not lock_path:
                    state = None
                else:
                    state = {'lock_path': lock_path}

            if state is None:
                process = None
            elif not state.get('process'):
                cls._safe_clear_directory(directory)
                os.makedirs(directory, exist_ok=True)
                try:
                    process = subprocess.Popen(
                        cls._build_command(
                            upstream_url,
                            directory,
                            username=username,
                            password=password,
                        ),
                        stdin=subprocess.DEVNULL,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        close_fds=True,
                    )
                except Exception as e:
                    cls._safe_clear_directory(directory)
                    try:
                        os.unlink(state.get('lock_path') or '')
                    except OSError:
                        pass
                    logger.warning(
                        f'[tvh_m3u] HLS process start failed channel_uuid={channel_uuid}: {str(e)}'
                    )
                    return {'ret': 'warning', 'status': 503, 'msg': 'HLS is unavailable'}

                state = {
                    'channel_uuid': channel_uuid,
                    'process': process,
                    'directory': directory,
                    'playlist': os.path.join(directory, 'index.m3u8'),
                    'last_access': now,
                    'lock_path': state.get('lock_path'),
                    'consumers': {consumer_id} if consumer_id else set(),
                }
                cls._streams[channel_uuid] = state
                cls._touch_shared_access(channel_uuid, directory)

        deadline = time.monotonic() + cls.START_TIMEOUT
        while time.monotonic() < deadline:
            if state is None:
                playlist = cls._read_ready_playlist(directory)
                if playlist:
                    cls._touch_shared_access(channel_uuid, directory)
                    return {
                        'ret': 'success',
                        'playlist': playlist,
                        'directory': directory,
                    }
                time.sleep(0.1)
                continue
            process = state.get('process')
            if process is None or process.poll() is not None:
                break
            playlist_path = state.get('playlist')
            if playlist_path and os.path.isfile(playlist_path):
                try:
                    with open(playlist_path, 'r', encoding='utf-8') as handle:
                        playlist = handle.read()
                    if '#EXTM3U' in playlist and '#EXTINF:' in playlist:
                        cls.touch(channel_uuid, consumer_id=consumer_id)
                        return {
                            'ret': 'success',
                            'playlist': playlist,
                            'directory': state.get('directory'),
                        }
                except OSError:
                    pass
            time.sleep(0.1)

        if state is not None:
            with cls._lock:
                current = cls._streams.get(channel_uuid)
                if current is state:
                    cls._detach_stream_locked(channel_uuid)
            cls._stop_state(state, clear=True)
        logger.warning(f'[tvh_m3u] HLS playlist unavailable channel_uuid={channel_uuid}')
        return {'ret': 'warning', 'status': 502, 'msg': 'HLS stream unavailable'}

    @classmethod
    def get_playlist(cls, channel_uuid, consumer_id=''):
        channel_uuid = str(channel_uuid or '').strip()
        if not channel_uuid or len(channel_uuid) > 128:
            return ''
        with cls._lock:
            if not cls._consumer_may_read_locked(channel_uuid, consumer_id):
                return ''
        directory = cls._stream_dir(channel_uuid)
        playlist = cls._read_ready_playlist(directory)
        if playlist:
            cls.touch(channel_uuid, consumer_id=consumer_id)
            cls._touch_shared_access(channel_uuid, directory)
        return playlist

    @classmethod
    def touch(cls, channel_uuid, consumer_id=''):
        channel_uuid = str(channel_uuid or '').strip()
        consumer_id = cls._normalize_consumer_id(consumer_id)
        with cls._lock:
            if not cls._consumer_may_read_locked(channel_uuid, consumer_id):
                return None
            if cls.CONSUMER_MODE == 'single' and consumer_id:
                cls._consumer_channels[consumer_id] = channel_uuid
            state = cls._streams.get(channel_uuid)
            if state:
                state['last_access'] = time.monotonic()
                cls._touch_shared_access(channel_uuid, state.get('directory') or '')
                if consumer_id:
                    state.setdefault('consumers', set()).add(consumer_id)
            return state

    @classmethod
    def read_segment(cls, channel_uuid, filename, consumer_id=''):
        channel_uuid = str(channel_uuid or '').strip()
        filename = str(filename or '').strip()
        if not channel_uuid or len(channel_uuid) > 128:
            return None
        if not cls.SEGMENT_RE.fullmatch(filename):
            return None
        with cls._lock:
            if not cls._consumer_may_read_locked(channel_uuid, consumer_id):
                return None
        state = cls.touch(channel_uuid, consumer_id=consumer_id)
        directory = os.path.realpath(
            (state or {}).get('directory') or cls._stream_dir(channel_uuid)
        )
        cls._touch_shared_access(channel_uuid, directory)
        path = os.path.realpath(os.path.join(directory, filename))
        if not directory or not path.startswith(directory + os.sep):
            return None
        try:
            with open(path, 'rb') as handle:
                return handle.read()
        except OSError:
            return None

    @classmethod
    def shutdown_all(cls):
        with cls._lock:
            states = list(cls._streams.values())
            cls._streams.clear()
            cls._consumer_channels.clear()
        for state in states:
            cls._stop_state(state, clear=True)


atexit.register(HLSManager.shutdown_all)
