import importlib
import io
import sqlite3
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from flask import Flask, request
from PIL import Image
from werkzeug.datastructures import FileStorage

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = '_secure_logo_test'
package = types.ModuleType(PACKAGE)
package.__path__ = [str(ROOT)]
sys.modules[PACKAGE] = package
setup = types.ModuleType(PACKAGE + '.setup')
setup.P = types.SimpleNamespace(ModelSetting=types.SimpleNamespace(get=lambda _: None))
setup.logger = types.SimpleNamespace(warning=lambda *_: None)
sys.modules[setup.__name__] = setup
task = types.ModuleType(PACKAGE + '.task_m3u')
task.TaskM3U = types.SimpleNamespace(_build_logo_public_template=lambda f: '/customlogo/' + f,
    _load_logo_cache=lambda **_: None)
sys.modules[task.__name__] = task
logos = importlib.import_module(PACKAGE + '.task_custom_logo')
images = importlib.import_module(PACKAGE + '.custom_logo_image')
access = importlib.import_module(PACKAGE + '.public_access')


class SecureLogoUploadTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.temp.name) / 'logos.db')
        self.old_db = logos.WRITE_DB_PATH
        logos.WRITE_DB_PATH = self.db
        self.app = Flask(__name__)

    def tearDown(self):
        logos.WRITE_DB_PATH = self.old_db
        self.temp.cleanup()

    def png(self):
        data = io.BytesIO()
        Image.new('RGB', (16, 16), 'red').save(data, format='PNG')
        return data.getvalue()

    def test_old_decoder_is_rejected_before_image_open(self):
        with patch.object(Image, '__version__', '9.2.0'), patch.object(Image, 'open') as decoder:
            with self.assertRaises(images.LogoImageError):
                images.normalize_logo_image(self.png())
            decoder.assert_not_called()

    def test_only_allowed_decoders_are_selected_and_metadata_is_removed(self):
        with patch.object(Image, 'open', wraps=Image.open) as decoder:
            result = images.normalize_logo_image(self.png())
        self.assertEqual(set(decoder.call_args.kwargs['formats']), {'PNG', 'JPEG', 'GIF', 'WEBP'})
        with Image.open(io.BytesIO(result['data'])) as image:
            self.assertEqual(image.format, 'PNG')
            self.assertNotIn('exif', image.info)

    def test_file_read_is_bounded_before_size_validation(self):
        class Oversize:
            def read(self, size):
                self.requested = size
                return b'x' * size
        file = Oversize()
        with self.assertRaises(Exception):
            logos._save_file(file, self.temp.name)
        self.assertEqual(file.requested, images.MAX_INPUT_BYTES + 1)
        self.assertEqual(list(Path(self.temp.name).glob('*.png')), [])

    def test_request_body_guard_rejects_before_form_is_read(self):
        for path, kwargs in [('/upload', {'method': 'GET'}),
                             ('/upload', {'method': 'POST', 'content_type': 'multipart/form-data',
                                          'environ_overrides': {'CONTENT_LENGTH': str(7 * 1024 * 1024)}})]:
            with self.app.test_request_context(path, **kwargs):
                with self.assertRaises(ValueError):
                    access.check_upload_request(request)

    def test_invalid_image_does_not_consume_success_quota(self):
        req = types.SimpleNamespace(method='POST', content_length=300, mimetype='multipart/form-data',
            remote_addr='127.0.0.1', form={'source_channel_name': 'KBS'},
            files={'logo_file': FileStorage(io.BytesIO(b'invalid'), filename='bad.png')})
        with patch.object(logos, '_mirror_queue_dir', return_value=self.temp.name):
            with self.assertRaises(Exception):
                logos.handle_custom_logo_mirror(req)
        with sqlite3.connect(self.db) as con:
            self.assertEqual(con.execute('SELECT SUM(attempts) FROM custom_logo_attempt_rate').fetchone()[0], 1)
            self.assertIsNone(con.execute("SELECT name FROM sqlite_master WHERE name='custom_logo_upload_rate'").fetchone())

    def test_valid_submission_saves_png_and_preserves_existing_catalog_entry(self):
        def req():
            return types.SimpleNamespace(method='POST', content_length=1000, mimetype='multipart/form-data',
                remote_addr='127.0.0.1', form={'source_channel_name': 'KBS'},
                files={'logo_file': FileStorage(io.BytesIO(self.png()), filename='../../bad.svg')})
        with patch.object(logos, '_mirror_queue_dir', return_value=self.temp.name):
            first = logos.handle_custom_logo_mirror(req())
            with sqlite3.connect(self.db) as con:
                con.execute("UPDATE custom_logo SET stored_filename='owner.png'")
            logos.handle_custom_logo_mirror(req())
            alias_request = req()
            alias_request.form = {'source_channel_name': 'Another alias', 'standard_name': 'KBS'}
            logos.handle_custom_logo_mirror(alias_request)
        self.assertEqual(first['ret'], 'success')
        self.assertTrue((Path(self.temp.name) / first['stored_filename']).read_bytes().startswith(b'\x89PNG'))
        with sqlite3.connect(self.db) as con:
            self.assertEqual(con.execute('SELECT stored_filename FROM custom_logo').fetchone()[0], 'owner.png')

    def test_read_key_and_private_ff_key_cannot_submit_icons(self):
        submit_key = 'logo_submit_v1_' + 's' * 40
        for key in ['catalog_read_v1_' + 'x' * 40, 'FF-PRIVATE', '']:
            with self.app.test_request_context('/mirror', method='POST', headers={'X-Logo-Submit-Key': key}):
                result = access.authorize_public(request, enabled=True, configured_key=submit_key,
                    private_key='FF-PRIVATE', header='X-Logo-Submit-Key', prefix='logo_submit_v1_', method='POST')
                self.assertEqual(result[1], 403)
        with self.app.test_request_context('/mirror', method='POST', headers={'X-Logo-Submit-Key': submit_key}):
            self.assertIsNone(access.authorize_public(request, enabled=True, configured_key=submit_key,
                private_key='FF-PRIVATE', header='X-Logo-Submit-Key', prefix='logo_submit_v1_', method='POST'))


if __name__ == '__main__':
    unittest.main()
