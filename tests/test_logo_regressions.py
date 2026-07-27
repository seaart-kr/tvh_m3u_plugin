import ast
import importlib.util
import sqlite3
import sys
import tempfile
import types
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PACKAGE = '_tvh_m3u_logo_regression_test'


class _Logger:
    def __getattr__(self, _name):
        return lambda *_args, **_kwargs: None


def _load_task_m3u():
    package = types.ModuleType(PACKAGE)
    package.__path__ = [str(ROOT)]
    sys.modules[PACKAGE] = package

    setup = types.ModuleType(f'{PACKAGE}.setup')
    setup.P = types.SimpleNamespace()
    setup.logger = _Logger()
    sys.modules[setup.__name__] = setup

    model = types.ModuleType(f'{PACKAGE}.model')
    model.ModelChannel = type('ModelChannel', (), {})
    model.ModelGroupProfile = type('ModelGroupProfile', (), {})
    model.ModelChannelProfile = type('ModelChannelProfile', (), {})
    model.ModelLogoOverride = type('ModelLogoOverride', (), {})
    sys.modules[model.__name__] = model

    task_base = types.ModuleType(f'{PACKAGE}.task_base')
    task_base.TaskBase = type('TaskBase', (), {})
    sys.modules[task_base.__name__] = task_base

    name = f'{PACKAGE}.task_m3u'
    spec = importlib.util.spec_from_file_location(name, ROOT / 'task_m3u.py')
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


TASK_M3U_MODULE = _load_task_m3u()


class LogoRegressionTest(unittest.TestCase):
    def test_remote_logo_cache_is_loaded_when_local_db_exists(self):
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / 'write.db'
            sqlite3.connect(str(db_path)).close()

            task = TASK_M3U_MODULE.TaskM3U
            task.WRITE_DB_PATH = str(db_path)
            task._logo_cache = {
                'db_mtime': None,
                'custom_name_map': {},
                'custom_id_map': {},
                'provider_name_map': {},
                'provider_id_map': {},
            }
            calls = []

            def load_remote(cache):
                calls.append(True)
                cache['provider_name_map']['kt'] = {
                    'remotechannel': {
                        'provider': 'kt',
                        'url_template': 'https://example.invalid/logo.png',
                        'original_url': '',
                    },
                }
                return cache

            original_loader = task._load_remote_logo_cache
            try:
                task._load_remote_logo_cache = staticmethod(load_remote)
                result = task._load_logo_cache(force=True)
            finally:
                task._load_remote_logo_cache = original_loader

            self.assertEqual(len(calls), 1)
            self.assertIn('remotechannel', result['provider_name_map']['kt'])

    def test_logo_ajax_routes_call_existing_override_methods(self):
        tree = ast.parse((ROOT / 'mod_basic.py').read_text(encoding='utf-8-sig'))

        def methods_called_by_branch(sub_value):
            for node in ast.walk(tree):
                if not isinstance(node, ast.If):
                    continue
                test = node.test
                if not (
                    isinstance(test, ast.Compare)
                    and isinstance(test.left, ast.Name)
                    and test.left.id == 'sub'
                    and len(test.ops) == 1
                    and isinstance(test.ops[0], ast.Eq)
                    and len(test.comparators) == 1
                    and isinstance(test.comparators[0], ast.Constant)
                    and test.comparators[0].value == sub_value
                ):
                    continue
                return {
                    call.func.attr
                    for statement in node.body
                    for call in ast.walk(statement)
                    if isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
                }
            return set()

        self.assertIn('save_logo_override', methods_called_by_branch('logo_preview_select'))
        self.assertIn('clear_logo_override', methods_called_by_branch('logo_preview_clear'))


if __name__ == '__main__':
    unittest.main()
