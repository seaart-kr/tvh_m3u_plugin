import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class SecurityRegressionTest(unittest.TestCase):
    def read(self, relative_path):
        return (ROOT / relative_path).read_text(encoding='utf-8')

    def test_custom_logo_upload_does_not_transmit_ff_apikey(self):
        source = self.read('task_custom_logo.py')
        self.assertNotIn('X-Source-FF-Apikey', source)
        self.assertNotIn('_source_ff_apikey', source)

    def test_remote_backend_public_key_is_read_only_by_design(self):
        source = self.read('task_remote_backend.py')
        self.assertIn("os.environ.get('TVH_M3U_REMOTE_APIKEY')", source)
        self.assertIn('PUBLIC_READ_APIKEY', source)
        self.assertNotIn('def add_alias(', source)
        self.assertNotIn('def add_alias_bulk(', source)

    def test_passwords_are_not_rendered_back_into_forms(self):
        source = self.read('mod_basic.py')
        self.assertIn("arg['basic_tvh_admin_password'] = ''", source)
        self.assertIn("arg['basic_tvh_play_password'] = ''", source)
        for template in ['templates/tvh_m3u_basic_sync.html', 'templates/tvh_m3u_basic_m3u.html']:
            self.assertIn('type="password"', self.read(template))

    def test_epg_download_and_decompression_have_size_limits(self):
        source = self.read('mod_basic.py')
        self.assertIn('TVH_M3U_EPG_MAX_DOWNLOAD_BYTES', source)
        self.assertIn('TVH_M3U_EPG_MAX_XML_BYTES', source)
        self.assertIn('_copy_limited(fr, fw, max_xml_bytes)', source)


if __name__ == '__main__':
    unittest.main()
