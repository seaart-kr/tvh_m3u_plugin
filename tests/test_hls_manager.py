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


if __name__ == '__main__':
    unittest.main()
