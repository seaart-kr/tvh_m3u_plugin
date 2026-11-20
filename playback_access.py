"""Scoped playback links independent of the public catalog/upload keys."""
import hashlib
import hmac
import os
import secrets
import tempfile
from pathlib import Path
import time
from urllib.parse import urlencode

FEEDS = frozenset({
    'm3u', 'm3u_tvh', 'm3u_tivimate', 'm3u_shyni', 'alive_fix_url.yaml',
    'epg_raw', 'epg_tvh', 'epg_tivimate',
})
CHANNELS = frozenset({'url.m3u8', 'stream.ts'})
SEGMENT = 'hls_segment.ts'
SEGMENT_TTL = 3600
NO_STORE = {'Cache-Control': 'no-store', 'Referrer-Policy': 'no-referrer'}


def installation_secret(ff_key, path='/data/db/tvh_m3u_playback.key'):
    if not ff_key:
        return ''
    target = Path(path)
    if not target.is_file():
        # Publish a complete private file; other workers may initialize it concurrently.
        fd, staged = tempfile.mkstemp(prefix='.tvh-playback-', dir=target.parent)
        try:
            with os.fdopen(fd, 'w', encoding='ascii') as out:
                out.write(secrets.token_hex(32))
            try:
                os.link(staged, target)
            except FileExistsError:
                pass
        finally:
            os.unlink(staged)
    instance_key = target.read_text(encoding='ascii')
    if len(instance_key) != 64:
        raise ValueError('Invalid playback signing key')
    return hmac.new(ff_key.encode(), instance_key.encode(), hashlib.sha256).hexdigest()


def _signature(secret, target, channel, filename, expiry):
    message = '\0'.join(('tvh-m3u-playback-v1', target, channel, filename, str(expiry)))
    return hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()


def _scope_valid(target, channel, filename):
    if target in FEEDS:
        return not (channel or filename)
    if not channel or len(channel) > 128 or '\0' in channel:
        return False
    if target in CHANNELS:
        return not filename
    return target == SEGMENT and 0 < len(filename) <= 80 and '\0' not in filename


def signed_url(base, package, secret, target, channel='', filename='', consumer_id=''):
    if not secret or not _scope_valid(target, channel, filename):
        return ''
    query = [('play_key', _sign(secret, target, channel, filename))]
    if channel:
        query.extend((('m', 'url'), ('s', 'tvh'), ('i', channel)))
    if filename:
        query.append(('f', filename))
    if consumer_id:
        query.append(('client', consumer_id))
    return f"{base.rstrip('/')}/{package}/normal/{target}?{urlencode(query)}"


def _sign(secret, target, channel, filename):
    expiry = int(time.time()) + SEGMENT_TTL if target == SEGMENT else 0
    return f'{expiry}.{_signature(secret, target, channel, filename, expiry)}'


def authorized(req, secret, target):
    if req.method not in ('GET', 'HEAD') or not secret:
        return False
    channel = req.args.get('i', '')
    filename = req.args.get('f', '')
    if not _scope_valid(target, channel, filename):
        return False
    supplied = req.args.get('play_key', '')
    expiry_text, _, signature = supplied.partition('.')
    if not (expiry_text.isascii() and expiry_text.isdecimal() and len(expiry_text) <= 12
            and signature.isascii() and len(signature) == 64):
        return False
    expiry = int(expiry_text)
    if target == SEGMENT:
        if expiry <= int(time.time()):
            return False
    elif expiry:
        return False
    return hmac.compare_digest(signature, _signature(secret, target, channel, filename, expiry))
