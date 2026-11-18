# -*- coding: utf-8 -*-
"""Validation and normalization helpers for untrusted custom logo uploads."""

import hashlib
import os
import re
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
    if not _DECODE_SLOTS.acquire(blocking=False):
        raise LogoImageError('다른 로고를 처리 중입니다. 잠시 후 다시 시도하세요.')
    try:
        return _decode_logo_image(data)
    finally:
        _DECODE_SLOTS.release()


def _decode_logo_image(data):
    if not data:
        raise LogoImageError('업로드한 파일이 비어 있습니다.')
    if len(data) > MAX_INPUT_BYTES:
        raise LogoImageError('로고 파일은 5MB 이하만 업로드할 수 있습니다.')

    try:
        from PIL import Image, UnidentifiedImageError, features
    except ImportError:
        raise LogoImageError('이미지 검증에 필요한 Pillow 패키지가 설치되어 있지 않습니다.')

    version = tuple(int(v) for v in re.findall(r'\d+', Image.__version__)[:3])
    if version < (12, 3, 0):
        raise LogoImageError('보안 수정판 Pillow 12.3.0 이상이 필요합니다. FF 이미지의 라이브러리를 업데이트하세요.')
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

    png_data = output.getvalue()
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
