# -*- coding: utf-8 -*-
"""Validation and normalization helpers for untrusted custom logo uploads."""

import hashlib
import os
import re
import struct
import subprocess
import sys
import tempfile
import threading
from io import BytesIO


MAX_INPUT_BYTES = 5 * 1024 * 1024
MAX_OUTPUT_BYTES = 5 * 1024 * 1024
MAX_PIXELS = 4096 * 4096
ALLOWED_FORMATS = {'PNG', 'JPEG', 'GIF', 'WEBP'}
_DECODE_SLOTS = threading.BoundedSemaphore(2)


class LogoImageError(Exception):
    pass


def normalize_logo_image(data):
    """Decode an untrusted image and return a metadata-free, static PNG."""
    _check_input(data)
    if not _DECODE_SLOTS.acquire(blocking=False):
        raise LogoImageError('다른 로고를 처리 중입니다. 잠시 후 다시 시도하세요.')
    try:
        decoder = os.environ.get('TVH_M3U_LOGO_PYTHON')
        if decoder:
            return _decode_with_helper(decoder, data)
        return _decode_logo_image(data)
    finally:
        _DECODE_SLOTS.release()


def _check_input(data):
    if not data:
        raise LogoImageError('업로드한 파일이 비어 있습니다.')
    if len(data) > MAX_INPUT_BYTES:
        raise LogoImageError('로고 파일은 5MB 이하만 업로드할 수 있습니다.')


def _decode_with_helper(decoder, data):
    if not os.path.isabs(decoder):
        raise LogoImageError('로고 검증용 Python 경로는 절대 경로여야 합니다.')
    try:
        result = subprocess.run(
            [decoder, '-I', '-B', os.path.abspath(__file__)], input=data,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=15, env={},
        )
    except (OSError, subprocess.SubprocessError):
        raise LogoImageError('로고 검증용 Python을 실행할 수 없습니다.') from None
    png = result.stdout
    if (result.returncode != 0 or len(png) < 24 or len(png) > MAX_OUTPUT_BYTES
            or png[:16] != b'\x89PNG\r\n\x1a\n\0\0\0\rIHDR'):
        raise LogoImageError('정상적인 이미지 파일이 아닙니다. 로고 검증용 Python과 Pillow 버전도 확인하세요.')
    width, height = struct.unpack('>II', png[16:24])
    return _normalized_result(png, width, height)


def _decode_logo_image(data):
    try:
        from PIL import Image, UnidentifiedImageError, features
    except ImportError:
        raise LogoImageError('이미지 검증에 필요한 Pillow 패키지가 설치되어 있지 않습니다.')

    version = tuple(int(v) for v in re.findall(r'\d+', Image.__version__)[:3])
    if version < (13, 0, 0):
        raise LogoImageError('보안 수정판 Pillow 13.0.0 이상이 필요합니다. FF 이미지의 라이브러리를 업데이트하세요.')
    formats = sorted(ALLOWED_FORMATS)
    webp = features.version('webp')
    if not webp or tuple(int(v) for v in re.findall(r'\d+', webp)[:3]) < (1, 3, 2):
        formats.remove('WEBP')
    try:
        with Image.open(BytesIO(data), formats=formats) as source:
            if str(source.format or '').upper() not in ALLOWED_FORMATS:
                raise LogoImageError('PNG, JPG, GIF, WEBP 이미지만 업로드할 수 있습니다.')

            width, height = source.size
            if width < 1 or height < 1 or width > 4096 or height > 4096 or width * height > MAX_PIXELS:
                raise LogoImageError('이미지 크기는 최대 4096x4096 픽셀까지 허용됩니다.')

            source.seek(0)
            source.load()
            has_alpha = source.mode in ('RGBA', 'LA') or (
                source.mode == 'P' and 'transparency' in source.info
            )
            normalized = source.convert('RGBA' if has_alpha else 'RGB')
            normalized.info.clear()
            output = BytesIO()
            normalized.save(output, format='PNG', optimize=True)
            normalized.close()
    except LogoImageError:
        raise
    except (UnidentifiedImageError, OSError, ValueError, SyntaxError, EOFError, Image.DecompressionBombError):
        raise LogoImageError('정상적인 이미지 파일이 아닙니다.')

    return _normalized_result(output.getvalue(), width, height)


def _normalized_result(png_data, width, height):
    if len(png_data) > MAX_OUTPUT_BYTES:
        raise LogoImageError('변환된 PNG 파일은 5MB 이하만 저장할 수 있습니다.')

    sha256 = hashlib.sha256(png_data).hexdigest()
    return {
        'data': png_data,
        'filename': f'custom_{sha256[:24]}.png',
        'sha1': hashlib.sha1(png_data).hexdigest(),
        'sha256': sha256,
        'file_size': len(png_data),
        'width': width,
        'height': height,
    }


def atomic_write(path, data):
    """Atomically replace a file without exposing a partial upload."""
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    fd, temporary_path = tempfile.mkstemp(prefix='.custom_logo_', suffix='.tmp', dir=directory)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, path)
    except Exception:
        try:
            os.unlink(temporary_path)
        except OSError:
            pass
        raise


if __name__ == '__main__':
    try:
        incoming = sys.stdin.buffer.read(MAX_INPUT_BYTES + 1)
        _check_input(incoming)
        sys.stdout.buffer.write(_decode_logo_image(incoming)['data'])
    except LogoImageError:
        sys.exit(1)
