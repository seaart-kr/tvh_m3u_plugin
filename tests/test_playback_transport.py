import importlib
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import requests
from flask import Flask, request

from test_logo_regressions import PACKAGE

access = importlib.import_module(PACKAGE + '.playback_access')
transport = importlib.import_module(PACKAGE + '.upstream_transport')
slots = importlib.import_module(PACKAGE + '.stream_slots')


class PlaybackTransportTest(unittest.TestCase):
    def test_links_are_bound_to_scope_and_channel(self):
        app = Flask(__name__)
        url = access.signed_url('https://ff.example', 'plugin', 'test-signing-key',
                                'url.m3u8', channel='alpha')
        with app.test_request_context(url):
            self.assertTrue(access.authorized(request, 'test-signing-key', 'url.m3u8'))
            self.assertFalse(access.authorized(request, 'test-signing-key', 'stream.ts'))
            self.assertFalse(access.authorized(request, 'different-server-key', 'url.m3u8'))
        with app.test_request_context(url.replace('i=alpha', 'i=other')):
            self.assertFalse(access.authorized(request, 'test-signing-key', 'url.m3u8'))

    def test_segment_links_expire_and_include_filename(self):
        app = Flask(__name__)
        with patch.object(access.time, 'time', return_value=100):
            url = access.signed_url('https://ff.example', 'plugin', 'test-key', access.SEGMENT,
                                    channel='alpha', filename='segment_000000001.ts')
            with app.test_request_context(url):
                self.assertTrue(access.authorized(request, 'test-key', access.SEGMENT))
        with patch.object(access.time, 'time', return_value=100 + access.SEGMENT_TTL):
            with app.test_request_context(url):
                self.assertFalse(access.authorized(request, 'test-key', access.SEGMENT))

    def test_installation_secrets_are_distinct_and_rotation_revokes(self):
        with tempfile.TemporaryDirectory() as temp:
            key = Path(temp) / 'first'
            first = access.installation_secret('ff-key', key)
            self.assertEqual(access.installation_secret('ff-key', key), first)
            self.assertNotEqual(access.installation_secret('new-ff-key', key), first)
            self.assertNotEqual(access.installation_secret('ff-key', Path(temp) / 'second'), first)
            self.assertEqual(key.stat().st_mode & 0o777, 0o600)

    def test_upstream_connection_policy_is_applied_by_session(self):
        with patch.object(requests.Session, 'request') as http:
            with transport.VerifiedSession() as session:
                session.get('https://tvh.example/', verify=False, allow_redirects=True)
            self.assertTrue(http.call_args.kwargs['verify'])
            self.assertFalse(http.call_args.kwargs['allow_redirects'])
            self.assertEqual(http.call_args.kwargs['timeout'], transport.STREAM_TIMEOUT)
        with self.assertRaises(requests.exceptions.InvalidURL):
            transport.validate_upstream('http://8.8.8.8/')
        self.assertEqual(transport.validate_upstream('http://127.0.0.1/'), 'http://127.0.0.1/')

    def test_slots_release_when_connection_closes(self):
        with tempfile.TemporaryDirectory() as temp:
            handle = slots.acquire_slot(temp, 1)
            try:
                self.assertIsNone(slots.acquire_slot(temp, 1))
            finally:
                os.close(handle)
            os.close(slots.acquire_slot(temp, 1))
