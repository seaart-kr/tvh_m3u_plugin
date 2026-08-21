import importlib.util
import os
import sys
import tempfile
import time
import types
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
PACKAGE = '_tvh_m3u_hls_manager_test'


class _Logger:
    def __getattr__(self, _name):
        return lambda *_args, **_kwargs: None


def _load_module():
    package = types.ModuleType(PACKAGE)
    package.__path__ = [str(ROOT)]
    sys.modules[PACKAGE] = package

    setup = types.ModuleType(f'{PACKAGE}.setup')
    setup.logger = _Logger()
    sys.modules[setup.__name__] = setup

    name = f'{PACKAGE}.hls_manager'
    spec = importlib.util.spec_from_file_location(name, ROOT / 'hls_manager.py')
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


HLS_MODULE = _load_module()


class HLSManagerTest(unittest.TestCase):
    def setUp(self):
        self.manager = HLS_MODULE.HLSManager
        self.temp = tempfile.TemporaryDirectory()
        self.original_root = self.manager.CACHE_ROOT
        self.manager.CACHE_ROOT = self.temp.name
        self.manager._streams = {}
        self.manager._consumer_channels = {}
        self.manager.configure(
            max_streams=0,
            limit_policy='reject',
            consumer_mode='shared',
            idle_timeout=15,
            switch_delay=0,
        )

    def tearDown(self):
        self.manager.shutdown_all()
        self.manager.CACHE_ROOT = self.original_root
        self.temp.cleanup()

    def test_ffmpeg_command_copies_stream_into_live_hls(self):
        directory = os.path.join(self.temp.name, 'channel')
        with mock.patch.object(HLS_MODULE.shutil, 'which', return_value='/usr/bin/ffmpeg'):
            command = self.manager._build_command(
                'https://tvh.example/stream/channel/uuid?profile=pass',
                directory,
                username='viewer',
                password='secret',
            )

        joined = ' '.join(command)
        self.assertIn('-c copy', joined)
        self.assertIn('-f hls', joined)
        self.assertIn('-hls_time 2', joined)
        self.assertIn('delete_segments+omit_endlist+independent_segments+temp_file', joined)
        self.assertIn('Authorization: Basic ', joined)
        self.assertNotIn('viewer:secret@', joined)
        self.assertNotIn(' secret ', f' {joined} ')

    def test_segment_reader_allows_only_managed_ts_files(self):
        directory = self.manager._stream_dir('channel-uuid')
        os.makedirs(directory, exist_ok=True)
        segment_path = os.path.join(directory, 'segment_000000001.ts')
        Path(segment_path).write_bytes(b'video-data')
        self.assertEqual(
            self.manager.read_segment('channel-uuid', 'segment_000000001.ts'),
            b'video-data',
        )
        self.assertIsNone(self.manager.read_segment('channel-uuid', '../secret.ts'))
        self.assertIsNone(self.manager.read_segment('channel-uuid', 'index.m3u8'))

    def test_cache_cleanup_rejects_root_and_outside_paths(self):
        with self.assertRaises(ValueError):
            self.manager._safe_clear_directory(self.temp.name)
        with self.assertRaises(ValueError):
            self.manager._safe_clear_directory(str(Path(self.temp.name).parent / 'outside'))

    def test_start_lock_prevents_duplicate_channel_processes(self):
        first = self.manager._acquire_start_lock('channel-uuid')
        second = self.manager._acquire_start_lock('channel-uuid')

        self.assertTrue(first.endswith('.lock'))
        self.assertEqual(second, '')

    def test_fresh_shared_playlist_is_reused_without_local_process(self):
        directory = self.manager._stream_dir('channel-uuid')
        os.makedirs(directory, exist_ok=True)
        playlist = '#EXTM3U\n#EXTINF:2.0,\nsegment_000000001.ts\n'
        Path(directory, 'index.m3u8').write_text(playlist, encoding='utf-8')

        self.assertEqual(self.manager.get_playlist('channel-uuid'), playlist)
        self.assertEqual(self.manager._streams, {})

    def test_cached_playlist_registers_single_consumer_channel(self):
        self.manager.configure(consumer_mode='single', switch_delay=0)
        directory = self.manager._stream_dir('channel-uuid')
        os.makedirs(directory, exist_ok=True)
        playlist = '#EXTM3U\n#EXTINF:2.0,\nsegment_000000001.ts\n'
        Path(directory, 'index.m3u8').write_text(playlist, encoding='utf-8')

        self.assertEqual(
            self.manager.get_playlist('channel-uuid', consumer_id='living-room'),
            playlist,
        )
        self.assertEqual(self.manager._consumer_channels['living-room'], 'channel-uuid')

    @staticmethod
    def _live_state(channel_uuid, last_access=0, consumers=None):
        process = mock.Mock()
        process.poll.return_value = None
        process.wait.return_value = 0
        return {
            'channel_uuid': channel_uuid,
            'process': process,
            'directory': '',
            'last_access': last_access,
            'consumers': set(consumers or []),
        }

    def test_configure_validates_runtime_limits(self):
        configured = self.manager.configure(
            max_streams='999',
            limit_policy='invalid',
            consumer_mode='invalid',
            idle_timeout='1',
            switch_delay='99',
        )

        self.assertEqual(configured['max_streams'], 64)
        self.assertEqual(configured['limit_policy'], 'reject')
        self.assertEqual(configured['consumer_mode'], 'shared')
        self.assertEqual(configured['idle_timeout'], 5)
        self.assertEqual(configured['switch_delay'], 5.0)

    def test_single_consumer_switch_detaches_previous_channel(self):
        self.manager.configure(
            max_streams=1,
            limit_policy='oldest',
            consumer_mode='single',
            switch_delay=0,
        )
        previous = self._live_state('old-channel', consumers={'living-room'})
        self.manager._streams = {'old-channel': previous}
        self.manager._consumer_channels = {'living-room': 'old-channel'}

        _state, states_to_stop, error = self.manager._prepare_request_locked(
            'new-channel',
            'living-room',
        )

        self.assertIsNone(error)
        self.assertEqual(states_to_stop, [previous])
        self.assertNotIn('old-channel', self.manager._streams)
        self.assertEqual(self.manager._consumer_channels['living-room'], 'new-channel')

    def test_single_consumer_switch_preserves_other_viewers(self):
        self.manager.configure(consumer_mode='single', switch_delay=0)
        previous = self._live_state(
            'shared-channel',
            consumers={'living-room', 'bedroom'},
        )
        self.manager._streams = {'shared-channel': previous}
        self.manager._consumer_channels = {
            'living-room': 'shared-channel',
            'bedroom': 'shared-channel',
        }

        _state, states_to_stop, error = self.manager._prepare_request_locked(
            'new-channel',
            'living-room',
        )

        self.assertIsNone(error)
        self.assertEqual(states_to_stop, [])
        self.assertIn('shared-channel', self.manager._streams)
        self.assertEqual(previous['consumers'], {'bedroom'})
        self.assertEqual(self.manager._consumer_channels['bedroom'], 'shared-channel')

    def test_stream_limit_can_reject_or_evict_oldest(self):
        first = self._live_state('first', last_access=1)
        second = self._live_state('second', last_access=2)
        self.manager._streams = {'first': first, 'second': second}
        self.manager.configure(max_streams=2, limit_policy='reject', switch_delay=0)

        _state, states_to_stop, error = self.manager._prepare_request_locked('third', '')
        self.assertEqual(states_to_stop, [])
        self.assertEqual(error['status'], 429)

        self.manager.configure(max_streams=2, limit_policy='oldest', switch_delay=0)
        _state, states_to_stop, error = self.manager._prepare_request_locked('third', '')
        self.assertIsNone(error)
        self.assertEqual(states_to_stop, [first])
        self.assertNotIn('first', self.manager._streams)

    def test_stale_single_consumer_segment_is_rejected(self):
        self.manager.configure(consumer_mode='single', switch_delay=0)
        self.manager._consumer_channels = {'living-room': 'new-channel'}
        directory = self.manager._stream_dir('old-channel')
        os.makedirs(directory, exist_ok=True)
        Path(directory, 'segment_000000001.ts').write_bytes(b'old-video')

        self.assertIsNone(
            self.manager.read_segment(
                'old-channel',
                'segment_000000001.ts',
                consumer_id='living-room',
            )
        )


if __name__ == '__main__':
    unittest.main()
