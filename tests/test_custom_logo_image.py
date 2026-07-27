import importlib.util
import tempfile
import unittest
from io import BytesIO
from pathlib import Path

from PIL import Image


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('custom_logo_image', ROOT / 'custom_logo_image.py')
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class CustomLogoImageTest(unittest.TestCase):
    def image_bytes(self, image_format='JPEG', size=(32, 24)):
        output = BytesIO()
        Image.new('RGB', size, (20, 40, 60)).save(output, format=image_format)
        return output.getvalue()

    def test_normalizes_to_content_addressed_png(self):
        result = MODULE.normalize_logo_image(self.image_bytes())
        self.assertTrue(result['data'].startswith(b'\x89PNG\r\n\x1a\n'))
        self.assertEqual(result['filename'], f"custom_{result['sha256'][:24]}.png")
        self.assertEqual(result['file_size'], len(result['data']))

    def test_same_image_produces_same_filename(self):
        data = self.image_bytes()
        first = MODULE.normalize_logo_image(data)
        second = MODULE.normalize_logo_image(data)
        self.assertEqual(first['filename'], second['filename'])

    def test_rejects_svg_and_invalid_bytes(self):
        with self.assertRaises(MODULE.LogoImageError):
            MODULE.normalize_logo_image(b'<svg xmlns="http://www.w3.org/2000/svg"></svg>')
        with self.assertRaises(MODULE.LogoImageError):
            MODULE.normalize_logo_image(b'not an image')

    def test_rejects_too_many_pixels_before_decode(self):
        with self.assertRaises(MODULE.LogoImageError):
            MODULE.normalize_logo_image(self.image_bytes('PNG', (4097, 4096)))

    def test_atomic_write_replaces_complete_file(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / 'logo.png'
            MODULE.atomic_write(str(target), b'first')
            MODULE.atomic_write(str(target), b'second')
            self.assertEqual(target.read_bytes(), b'second')


if __name__ == '__main__':
    unittest.main()
