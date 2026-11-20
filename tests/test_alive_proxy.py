import ast
import types
import unittest
from pathlib import Path

from test_logo_regressions import TASK_M3U_MODULE


ROOT = Path(__file__).resolve().parents[1]


class AliveProxyTest(unittest.TestCase):
    def test_proxy_url_uses_plugin_key_without_tvh_credentials(self):
        task = TASK_M3U_MODULE.TaskM3U
        original_package = getattr(TASK_M3U_MODULE.P, 'package_name', None)
        TASK_M3U_MODULE.P.package_name = 'tvh_m3u_plugin'
        try:
            url = task.build_alive_stream_url(
                'https://oracle.example',
                'PUBLICKEY1',
                'channel-uuid',
            )
            session_url = task.build_alive_stream_url(
                'https://oracle.example',
                'PUBLICKEY1',
                'channel-uuid',
                consumer_id='living-room',
            )
            raw_url = task.build_proxy_stream_url(
                'https://oracle.example',
                'PUBLICKEY1',
                'channel-uuid',
                use_hls=False,
            )
        finally:
            if original_package is None:
                delattr(TASK_M3U_MODULE.P, 'package_name')
            else:
                TASK_M3U_MODULE.P.package_name = original_package

        self.assertIn('/normal/url.m3u8?play_key=', url)
        self.assertNotIn('apikey=', url)
        self.assertNotIn('PUBLICKEY1', url)
        self.assertNotIn('@', url)
        self.assertIn('&client=living-room', session_url)
        self.assertNotIn('client=', raw_url)
        self.assertIn('/normal/stream.ts?', raw_url)
        self.assertNotIn('@', raw_url)

    def test_resolver_validates_channel_and_never_embeds_auth(self):
        task = TASK_M3U_MODULE.TaskM3U
        channel = types.SimpleNamespace(
            enabled=True,
            name='KBS1',
            get_effective_group_name=lambda: '지상파',
        )
        originals = {
            'get_channel_map': getattr(TASK_M3U_MODULE.ModelChannel, 'get_channel_map', None),
            'fetch_playlist_map': task.fetch_playlist_map,
            'get_effective_profile': task.get_effective_profile,
            'normalize_stream_url': task.normalize_stream_url,
        }
        calls = []
        try:
            TASK_M3U_MODULE.ModelChannel.get_channel_map = staticmethod(
                lambda: {'channel-uuid': channel}
            )
            task.fetch_playlist_map = staticmethod(
                lambda: {'channel-uuid': 'http://tvh.example/stream/channel/channel-uuid'}
            )
            task.get_effective_profile = staticmethod(lambda *_args: 'pass')

            def normalize(source_url, profile='', include_auth=None):
                calls.append((source_url, profile, include_auth))
                return 'http://tvh.example/stream/channel/channel-uuid?profile=pass'

            task.normalize_stream_url = staticmethod(normalize)
            result = task.resolve_alive_stream('channel-uuid')
        finally:
            if originals['get_channel_map'] is None:
                delattr(TASK_M3U_MODULE.ModelChannel, 'get_channel_map')
            else:
                TASK_M3U_MODULE.ModelChannel.get_channel_map = originals['get_channel_map']
            task.fetch_playlist_map = originals['fetch_playlist_map']
            task.get_effective_profile = originals['get_effective_profile']
            task.normalize_stream_url = originals['normalize_stream_url']

        self.assertEqual(result['ret'], 'success')
        self.assertEqual(calls[0][2], False)
        self.assertNotIn('@', result['upstream_url'])

    def test_api_has_alive_playlist_and_streaming_proxy_guards(self):
        source = (ROOT / 'mod_basic.py').read_text(encoding='utf-8-sig')
        tree = ast.parse(source)
        sub_values = {
            comparator.value
            for node in ast.walk(tree)
            if isinstance(node, ast.Compare)
            and isinstance(node.left, ast.Name)
            and node.left.id == 'sub'
            for comparator in node.comparators
            if isinstance(comparator, ast.Constant)
        }
        self.assertIn('m3u_shyni', sub_values)
        self.assertNotIn('m3u_alive', source)
        self.assertIn('stream.ts', sub_values)
        self.assertIn('url.m3u8', sub_values)
        self.assertIn('hls_segment.ts', sub_values)
        self.assertIn('HLSManager.ensure_stream(', source)
        self.assertIn('HLSManager.read_segment(', source)
        self.assertIn('_configure_hls_manager()', source)
        self.assertIn('consumer_id=consumer_id', source)
        self.assertIn('filename=os.path.basename(line), consumer_id=consumer_id', source)
        self.assertIn("content_type='video/mp2t'", source)
        self.assertIn("allow_redirects=False", source)
        self.assertIn("response.headers['X-Accel-Buffering'] = 'no'", source)
        self.assertIn('stream_with_context(generate_stream())', source)

    def test_tivimate_uses_signed_ts_and_shyni_uses_signed_hls(self):
        source = (ROOT / 'task_m3u.py').read_text(encoding='utf-8-sig')
        build_m3u_source = source[source.index('    def build_m3u('):source.index('    def build_alive_fix_url_yaml(')]

        self.assertNotIn('include_auth=True', build_m3u_source)
        self.assertIn("use_hls=(target == 'shyni')", build_m3u_source)
        self.assertIn('TaskM3U.build_proxy_stream_url(', build_m3u_source)
        self.assertNotIn("use_hls=(target == 'tivimate')", build_m3u_source)

    def test_proxy_playlist_apis_use_the_installation_signer(self):
        source = (ROOT / 'mod_basic.py').read_text(encoding='utf-8-sig')
        api_source = source[source.index('    def process_api('):]

        for sub in ['m3u_tvh', 'm3u_shyni']:
            marker = f"sub == '{sub}'"
            start = api_source.index(marker)
            block = api_source[start:start + 700]
            self.assertIn("proxy_base_url=request.host_url.rstrip('/')", block)
            self.assertIn("proxy_apikey=_playback_secret()", block)

    def test_tivimate_and_shyni_have_separate_published_addresses(self):
        module_source = (ROOT / 'mod_basic.py').read_text(encoding='utf-8-sig')
        api_template = (ROOT / 'templates' / 'tvh_m3u_basic_api.html').read_text(encoding='utf-8-sig')

        self.assertIn("sub == 'm3u_shyni'", module_source)
        self.assertIn("arg['m3u_shyni_url']", api_template)
        self.assertIn("arg['m3u_tivimate_url']", api_template)
        self.assertIn('TiviMate용', api_template)
        self.assertIn('Shyni HLS용', api_template)

    def test_alive_fixed_url_yaml_contains_proxy_channels(self):
        task = TASK_M3U_MODULE.TaskM3U
        original_package = getattr(TASK_M3U_MODULE.P, 'package_name', None)
        originals = {
            'get_grouped': getattr(TASK_M3U_MODULE.ModelChannel, 'get_grouped', None),
            'fetch_playlist_map': task.fetch_playlist_map,
            'get_effective_logo_url': task.get_effective_logo_url,
        }
        try:
            TASK_M3U_MODULE.P.package_name = 'tvh_m3u_plugin'
            TASK_M3U_MODULE.ModelChannel.get_grouped = staticmethod(lambda: {
                '지상파': [{
                    'enabled': True,
                    'channel_uuid': 'channel-uuid',
                    'name': 'KBS1: 서울',
                    'sheet_logo_url': '',
                }],
            })
            task.fetch_playlist_map = staticmethod(lambda: {
                'channel-uuid': 'http://tvh.example/stream/channel/channel-uuid',
            })
            task.get_effective_logo_url = staticmethod(
                lambda **_kwargs: 'https://oracle.example/logo/kbs1.png'
            )

            text = task.build_alive_fix_url_yaml(
                'https://oracle.example',
                'PUBLICKEY1',
            )
        finally:
            if original_package is None:
                delattr(TASK_M3U_MODULE.P, 'package_name')
            else:
                TASK_M3U_MODULE.P.package_name = original_package
            if originals['get_grouped'] is None:
                delattr(TASK_M3U_MODULE.ModelChannel, 'get_grouped')
            else:
                TASK_M3U_MODULE.ModelChannel.get_grouped = originals['get_grouped']
            task.fetch_playlist_map = originals['fetch_playlist_map']
            task.get_effective_logo_url = originals['get_effective_logo_url']

        self.assertTrue(text.startswith('channel_source:\n  fix_url:\n'))
        self.assertIn('channel_source:', text)
        self.assertIn('  fix_url:', text)
        self.assertIn('      name: "KBS1: 서울"', text)
        self.assertIn('      icon: "https://oracle.example/logo/kbs1.png"', text)
        self.assertIn('      url: "https://oracle.example/tvh_m3u_plugin/normal/url.m3u8?play_key=', text)
        self.assertIn('&i=channel-uuid', text)
        self.assertNotIn('PUBLICKEY1', text)
        self.assertNotIn('@', text)

    def test_fixed_url_ui_supports_output_and_copy(self):
        module_source = (ROOT / 'mod_basic.py').read_text(encoding='utf-8-sig')
        api_template = (ROOT / 'templates' / 'tvh_m3u_basic_api.html').read_text(encoding='utf-8-sig')
        script = (ROOT / 'templates' / 'tvh_m3u_basic_common_script.html').read_text(encoding='utf-8-sig')

        self.assertIn("sub == 'alive_fix_url.yaml'", module_source)
        self.assertIn('id="alive_fix_url_btn"', api_template)
        self.assertIn('id="alive_fix_url_output"', api_template)
        self.assertIn('id="alive_fix_url_copy_btn"', api_template)
        self.assertIn("navigator.clipboard.writeText(text)", script)
        self.assertIn("document.execCommand('copy')", script)

    def test_m3u_settings_explain_server_side_authentication(self):
        source = (ROOT / 'templates' / 'tvh_m3u_basic_m3u.html').read_text(encoding='utf-8-sig')

        self.assertIn('TVH 원본에 연결할 때 서버에서 사용합니다.', source)
        self.assertNotIn('ID:비밀번호@', source)
        self.assertIn('Shyni는 HLS 중계를 사용합니다', source)


if __name__ == '__main__':
    unittest.main()
