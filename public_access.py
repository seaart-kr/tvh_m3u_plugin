"""Scoped public endpoints: these capabilities never authorize an FF API."""
import hmac
import re

from flask import jsonify

MAX_UPLOAD_REQUEST_BYTES = 6 * 1024 * 1024


def authorize_public(req, *, enabled, configured_key, private_key, header, prefix, method):
    if str(enabled).strip().lower() not in {'true', 'on', '1', 'yes'}:
        return jsonify({'ret': 'warning', 'msg': 'not found'}), 404
    if req.method != method:
        return jsonify({'ret': 'warning', 'msg': 'method not allowed'}), 405, {'Allow': method}
    key = str(configured_key or '')
    if (not key.startswith(prefix) or not re.fullmatch(r'[A-Za-z0-9_-]{32,128}', key)
            or hmac.compare_digest(key.encode(), str(private_key or '').encode())):
        return jsonify({'ret': 'warning', 'msg': 'endpoint unavailable'}), 503
    supplied = req.headers.get(header, '')
    if not supplied.isascii() or not hmac.compare_digest(supplied, key):
        return jsonify({'ret': 'warning', 'msg': 'forbidden'}), 403
    return None


def check_upload_request(req):
    """Reject large/unknown bodies before accessing form/files or decoding."""
    if req.method != 'POST':
        raise ValueError('로고 업로드는 POST 요청만 허용합니다.')
    if req.content_length is None:
        raise ValueError('파일 크기를 확인할 수 없는 업로드입니다.')
    if req.content_length > MAX_UPLOAD_REQUEST_BYTES:
        raise ValueError('업로드 요청은 6MB 이하만 허용합니다. 로고 파일은 5MB 이하입니다.')
    if req.mimetype != 'multipart/form-data':
        raise ValueError('multipart/form-data 로고 업로드만 허용합니다.')
