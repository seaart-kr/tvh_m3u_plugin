# -*- coding: utf-8 -*-
import os
import traceback
import logging
from logging.handlers import RotatingFileHandler

setting = {
    'filepath': __file__,
    'use_db': True,
    'use_default_setting': True,
    'home_module': 'basic',
    'menu': {
        'uri': __package__,
        'name': 'TVH M3U',
        'list': [
            {
                'uri': 'basic',
                'name': '기본',
                'list': [
                    {'uri': 'sync', 'name': '채널 동기화'},
                    {'uri': 'epg', 'name': 'EPG 설정'},
                    {'uri': 'logo', 'name': '로고 설정'},
                    {'uri': 'addlogo', 'name': '커스텀 로고 추가'},
                    {'uri': 'm3u', 'name': '재생 프로필 설정'},
                    {'uri': 'api', 'name': 'API'},
                ]
            },
            {'uri': 'log', 'name': '로그'},
        ]
    },
    'setting_menu': None,
    'default_route': 'normal',
}

from plugin import *
P = create_plugin_instance(setting)

bootstrap_logger = P.logger


def get_plugin_logger():
    log_name = P.package_name
    log = logging.getLogger(log_name)
    if log.handlers:
        return log

    log.setLevel(logging.INFO)
    log.propagate = False

    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f'{P.package_name}.log')

    formatter = logging.Formatter(
        '%(asctime)s  %(levelname)s %(name)s %(filename)s:%(lineno)d %(message)s'
    )

    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    log.addHandler(file_handler)
    return log


logger = get_plugin_logger()

try:
    from .mod_basic import ModuleBasic
    P.set_module_list([ModuleBasic])
    bootstrap_logger.info(f'[{P.package_name}] plugin module loaded')
except Exception as e:
    bootstrap_logger.error(f'[{P.package_name}] startup failed: {str(e)}')
    bootstrap_logger.error(traceback.format_exc())
