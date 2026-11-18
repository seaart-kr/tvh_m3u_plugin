import importlib
import sqlite3
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch
from pathlib import Path

from flask import Flask, request

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = '_catalog_security_test'
package = types.ModuleType(PACKAGE)
package.__path__ = [str(ROOT)]
sys.modules[PACKAGE] = package
access = importlib.import_module(PACKAGE + '.public_access')
catalog = importlib.import_module(PACKAGE + '.catalog_api')
backend = importlib.import_module(PACKAGE + '.task_remote_backend')


class CatalogApiTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.temp.name) / 'catalog.db')
        with sqlite3.connect(self.path) as con:
            con.executescript('''
                CREATE TABLE channel_master (id INTEGER,standard_name TEXT,group_category TEXT,
                    receive_category TEXT,provider_logo_url TEXT,password TEXT,apikey TEXT);
                CREATE TABLE channel_alias (id INTEGER,channel_id TEXT,alias_name TEXT,
                    alias_norm TEXT,source TEXT,created_at TEXT);
                CREATE TABLE custom_logo (id INTEGER,source_channel_name TEXT,stored_filename TEXT,
                    logo_url_template TEXT,private_token TEXT);
                INSERT INTO channel_master VALUES (1,'KBS 1','지상파','방송',
                    'https://user:private@example.org/logo.png','DO-NOT-EXPORT','FF-PRIVATE');
                INSERT INTO channel_alias VALUES (1,'1','KBS1 HD','KBS1HD','test','2026-11-18');
                INSERT INTO custom_logo VALUES (1,'KBS 1','kbs.png','/customlogo/kbs.png','SECRET');
                CREATE TABLE ff_settings (apikey TEXT);
                INSERT INTO ff_settings VALUES ('FF-PRIVATE');
            ''')
        self.app = Flask(__name__)
        self.key = backend.TaskRemoteBackend.PUBLIC_READ_APIKEY
        self.reads = []

        @self.app.route('/catalog', methods=['GET', 'POST'])
        def endpoint():
            gate = access.authorize_public(request, enabled=True, configured_key=self.key,
                private_key='FF-PRIVATE', header='X-Catalog-Key', prefix='catalog_read_v1_', method='GET')
            if gate is not None:
                return gate
            def reader(resource, params):
                self.reads.append(resource)
                return catalog.read_catalog(resource, params, self.path)
            return catalog.catalog_response(request, reader)
        self.client = self.app.test_client()

    def tearDown(self):
        self.temp.cleanup()

    def get(self, resource, **query):
        return self.client.get('/catalog', query_string={'resource': resource, **query},
                               headers={'X-Catalog-Key': self.key})

    def test_no_key_private_ff_key_and_write_method_never_reach_reader(self):
        for headers in [{}, {'X-Catalog-Key': 'FF-PRIVATE'}]:
            self.assertEqual(self.client.get('/catalog?resource=db_dump', headers=headers).status_code, 403)
        self.assertEqual(self.client.post('/catalog?resource=db_dump',
            headers={'X-Catalog-Key': self.key}).status_code, 405)
        for resource in ['add_alias', 'add_alias_bulk', 'm3u_shyni', '../../system', 'db_delete']:
            self.assertEqual(self.get(resource).status_code, 404)
        self.assertEqual(self.reads, [])

    def test_dump_excludes_private_columns_tables_and_credential_urls(self):
        response = self.get('db_dump', table='ff_settings', command='add_alias', apikey='FF-PRIVATE')
        self.assertEqual(response.status_code, 200)
        text = response.get_data(as_text=True)
        for forbidden in ['DO-NOT-EXPORT', 'FF-PRIVATE', 'SECRET', 'user:private', 'ff_settings', 'private_token']:
            self.assertNotIn(forbidden, text)
        self.assertEqual(response.json['tables']['channel_master'][0]['provider_logo_url'], '')
        self.assertEqual(response.json['tables']['custom_logo'][0]['stored_filename'], 'kbs.png')
        with sqlite3.connect(self.path) as con:
            self.assertEqual(con.execute('SELECT COUNT(*) FROM channel_alias').fetchone()[0], 1)

    def test_match_rules_search_and_aliases_remain_compatible(self):
        rules = self.get('match_rules').json['rules']
        self.assertEqual(rules['alias_exact']['kbs1 hd']['group_name'], '지상파')
        self.assertEqual(self.get('search_master', keyword='KBS1').json['list'][0]['channel_id'], '1')
        self.assertEqual(self.get('get_aliases', channel_id='1').json['list'][0]['alias_name'], 'KBS1 HD')
        self.assertEqual(self.get('get_aliases', channel_id="1' OR 1=1").status_code, 400)
        self.assertEqual(self.get('search_master', keyword='x' * 257).status_code, 400)

    def test_reusing_private_key_as_server_capability_is_rejected(self):
        with self.app.test_request_context('/catalog', headers={'X-Catalog-Key': self.key}):
            result = access.authorize_public(request, enabled=True, configured_key=self.key,
                private_key=self.key, header='X-Catalog-Key', prefix='catalog_read_v1_', method='GET')
            self.assertEqual(result[1], 503)

    def test_client_sends_only_catalog_header_and_cannot_dispatch_writes(self):
        reply = Mock(status_code=200)
        reply.json.return_value = {'ret': 'success'}
        with patch.object(backend.requests, 'get', return_value=reply) as get:
            self.assertEqual(backend.TaskRemoteBackend._request('add_alias')['ret'], 'danger')
            get.assert_not_called()
            backend.TaskRemoteBackend._request('match_rules', {'apikey': 'FF-PRIVATE', 'resource': 'add_alias'})
            args = get.call_args.kwargs
            self.assertEqual(args['params'], {'resource': 'match_rules'})
            self.assertEqual(args['headers'], {'X-Catalog-Key': self.key})
            self.assertFalse(args['allow_redirects'])
            reply.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
