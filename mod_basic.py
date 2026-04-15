# -*- coding: utf-8 -*-
import importlib
import sys

from flask import request, render_template, jsonify, redirect, Response, render_template_string

from .setup import *
from .model import ModelTag, ModelChannel, ModelGroupOrder, ModelGroupProfile, ModelChannelProfile, DB_PATH
from .task import Task


def _is_sync_form(req):
    try:
        if req is None or not getattr(req, 'form', None):
            return False
        keys = set(req.form.keys())
        sync_markers = {
            'basic_tvh_api_base',
            'basic_tvh_stream_base',
            'basic_tvh_admin_username',
            'basic_tvh_admin_password',
            'basic_tvh_play_username',
            'basic_tvh_play_password',
            'basic_tvh_use_verify_ssl',
            'basic_tvh_include_auth_in_url',
            'basic_tvh_stream_profile',
        }
        return len(keys.intersection(sync_markers)) > 0
    except Exception:
        return False


def _save_runtime_settings(req):
    result = {
        'saved': False,
        'is_sync_form': False,
    }
    try:
        if req is None or not getattr(req, 'form', None):
            return result
        result['is_sync_form'] = _is_sync_form(req)
        P.ModelSetting.setting_save(req)
        result['saved'] = True
    except Exception as e:
        logger.warning(f'[ff_tvh_m3u] runtime setting_save skipped: {str(e)}')
    return result


def _load_sjva_module():
    for mod_name in ['sjva.setup', 'sjva']:
        if mod_name in sys.modules:
            return sys.modules.get(mod_name)
    for mod_name in ['sjva.setup', 'sjva']:
        try:
            return importlib.import_module(mod_name)
        except Exception:
            continue
    return None


def _is_truthy(value):
    if isinstance(value, bool):
        return value
    text = str(value or '').strip().lower()
    return text in ['true', '1', 'yes', 'y', 'ok', 'success', 'authenticated', 'auth']


def _is_sjva_auth_ok(raw):
    if raw is True:
        return True
    if raw in [None, False, '', 0]:
        return False

    deny_words = ['미인증', '인증필요', 'need auth', 'not auth', 'unauth', 'expired', 'fail', 'error']
    allow_words = ['인증되었습니다', '인증완료', 'authenticated', 'auth ok', 'success']

    if isinstance(raw, dict):
        for key in ['is_auth', 'auth', 'authenticated', 'success', 'ok']:
            if key in raw:
                return _is_truthy(raw.get(key))

        if 'ret' in raw and isinstance(raw.get('ret'), bool):
            return raw.get('ret') is True

        if 'ret' in raw:
            ret_text = str(raw.get('ret') or '').strip().lower()
            if ret_text in ['true', 'success', 'ok', 'authenticated']:
                return True
            if ret_text in ['false', 'warning', 'danger', 'error', 'fail']:
                return False

        joined = ' '.join([
            str(raw.get('msg', '') or ''),
            str(raw.get('desc', '') or ''),
            str(raw.get('status', '') or ''),
            str(raw.get('result', '') or ''),
            str(raw.get('ret', '') or ''),
            str(raw.get('sjva_id', '') or ''),
            str(raw.get('level', '') or ''),
            str(raw.get('point', '') or ''),
        ]).strip().lower()

        if any(word in joined for word in [w.lower() for w in deny_words]):
            return False
        if any(word in joined for word in [w.lower() for w in allow_words]):
            return True
        return False

    text = str(raw).strip().lower()
    if any(word in text for word in [w.lower() for w in deny_words]):
        return False
    if any(word in text for word in [w.lower() for w in allow_words]):
        return True
    return False


def _get_sjva_auth_info():
    mod = _load_sjva_module()
    if mod is None:
        return {
            'ok': False,
            'reason': 'SJVA 플러그인이 설치되어 있지 않습니다.',
            'raw': None,
        }

    getter = None

    try:
        plugin_obj = getattr(mod, 'P', None)
        if plugin_obj is not None:
            getter = getattr(plugin_obj, 'get_auth_status', None)
    except Exception:
        getter = None

    if getter is None:
        getter = getattr(mod, 'get_auth_status', None)

    if getter is None:
        return {
            'ok': False,
            'reason': 'SJVA 인증 상태 조회 함수를 찾지 못했습니다.',
            'raw': None,
        }

    try:
        raw = getter()
        ok = _is_sjva_auth_ok(raw)
        return {
            'ok': ok,
            'reason': '' if ok else 'SJVA 설정에서 APIKEY 확인 후 [인증하기]를 먼저 실행하세요.',
            'raw': raw,
        }
    except Exception as e:
        logger.exception(f'[ff_tvh_m3u] sjva get_auth_status exception: {str(e)}')
        return {
            'ok': False,
            'reason': f'SJVA 인증 상태 조회 실패: {str(e)}',
            'raw': None,
        }


def _render_sjva_auth_required_page(message):
    return render_template_string(
        """
        <div style="padding:24px; max-width:780px; margin:0 auto; font-family:Arial, sans-serif;">
          <div style="border:1px solid #f0d98c; background:#fffaf0; padding:18px 20px; border-radius:8px;">
            <h3 style="margin:0 0 10px 0;">SJVA 인증 필요</h3>
            <div style="margin-bottom:10px;">{{ message }}</div>
            <div style="color:#666;">SJVA 설정 화면에서 APIKEY를 확인하고 <strong>인증하기</strong>를 1회 실행한 뒤 다시 접속하세요.</div>
          </div>
        </div>
        """,
        message=message
    )


def _check_sjva_or_block(mode='html'):
    auth_info = _get_sjva_auth_info()
    if auth_info.get('ok'):
        return None

    logger.warning(f"[ff_tvh_m3u] sjva auth denied reason={auth_info.get('reason')} raw={auth_info.get('raw')}")
    message = auth_info.get('reason') or 'SJVA 인증이 필요합니다.'

    if mode == 'html':
        return _render_sjva_auth_required_page(message)

    if mode == 'json':
        return jsonify({
            'ret': 'warning',
            'msg': message,
            'sjva_auth_required': True,
        })

    return Response(message, status=403, mimetype='text/plain')


class ModuleBasic(PluginModuleBase):
    db_default = {
        'basic_tvh_api_base': '',
        'basic_tvh_stream_base': '',
        'basic_tvh_admin_username': '',
        'basic_tvh_admin_password': '',
        'basic_tvh_play_username': '',
        'basic_tvh_play_password': '',
        'basic_tvh_stream_profile': '',
        'basic_tvh_include_auth_in_url': 'False',
        'basic_tvh_use_verify_ssl': 'False',
        'basic_last_sync_time': '',
        'basic_last_sync_count': '0',
        'basic_match_last_run_time': '',
        'basic_match_last_count': '0',
        'basic_match_last_unmatched_count': '0',
        'basic_match_source': '/data/db/ff_tvh_sheet_write.db',
    }

    def __init__(self, P):
        super(ModuleBasic, self).__init__(P, name='basic', first_menu='sync')

    def process_menu(self, sub, req):
        try:
            gate = _check_sjva_or_block('html')
            if gate is not None:
                return gate

            sub = sub or 'sync'

            referrer = request.headers.get('Referer', '') or ''
            is_internal_referrer = f'/{P.package_name}/' in referrer

            if sub != 'sync' and not is_internal_referrer:
                return redirect(f'/{P.package_name}/basic/sync')

            logger.debug(f'[ff_tvh_m3u] process_menu sub={sub}')
            logger.debug(f'[ff_tvh_m3u] db engine url = {db.engine.url}')
            logger.debug(f'[ff_tvh_m3u] dedicated db path = {DB_PATH}')

            arg = P.ModelSetting.to_dict()
            arg['package_name'] = P.package_name
            arg['page_sub'] = sub
            arg['ajax_sub'] = self.name
            arg['m3u_url'] = ToolUtil.make_apikey_url(f"/{P.package_name}/api/m3u")
            arg['m3u_tvh_url'] = ToolUtil.make_apikey_url(f"/{P.package_name}/api/m3u_tvh")
            arg['m3u_tivimate_url'] = ToolUtil.make_apikey_url(f"/{P.package_name}/api/m3u_tivimate")
            arg['last_sync_time'] = P.ModelSetting.get('basic_last_sync_time')
            arg['last_sync_count'] = P.ModelSetting.get('basic_last_sync_count')
            arg['match_last_run_time'] = P.ModelSetting.get('basic_match_last_run_time')
            arg['match_last_count'] = P.ModelSetting.get('basic_match_last_count')
            arg['match_last_unmatched_count'] = P.ModelSetting.get('basic_match_last_unmatched_count')
            arg['match_source'] = P.ModelSetting.get('basic_match_source') or '/data/db/ff_tvh_sheet_write.db'
            arg['grouped_channels'] = ModelChannel.get_grouped()
            arg['tag_count'] = len(ModelTag.get_all())
            arg['channel_count'] = len(ModelChannel.get_all())
            arg['group_count'] = len(ModelGroupOrder.get_all())
            arg['ungrouped_channels'] = ModelChannel.get_ungrouped()
            arg['ungrouped_count'] = len(arg['ungrouped_channels'])
            arg['assignable_group_names'] = ModelChannel.get_assignable_group_names()
            arg['match_source_info'] = Task.get_match_source_info()
            arg['play_profile_list'] = []

            if sub == 'm3u':
                try:
                    profile_ret = Task.get_play_profiles()
                    if isinstance(profile_ret, dict):
                        arg['play_profile_list'] = profile_ret.get('profiles', []) or []
                except Exception as e:
                    logger.warning(f'[ff_tvh_m3u] process_menu get_play_profiles failed: {str(e)}')

                current_profile = str(arg.get('basic_tvh_stream_profile', '')).strip()
                if current_profile and current_profile not in arg['play_profile_list']:
                    arg['play_profile_list'].insert(0, current_profile)

                arg['group_profile_map'] = ModelGroupProfile.get_map()
                arg['channel_profile_map'] = ModelChannelProfile.get_map()

            arg['basic_tvh_use_verify_ssl'] = (
                'True'
                if str(arg.get('basic_tvh_use_verify_ssl', '')).strip().lower() in ['true', 'on', '1', 'yes', 'y']
                else 'False'
            )
            arg['basic_tvh_include_auth_in_url'] = (
                'True'
                if str(arg.get('basic_tvh_include_auth_in_url', '')).strip().lower() in ['true', 'on', '1', 'yes', 'y']
                else 'False'
            )

            return render_template(
                f'{P.package_name}_{self.name}_setting.html',
                arg=arg
            )

        except Exception as e:
            logger.exception(f'[ff_tvh_m3u] process_menu exception: {str(e)}')
            return render_template('sample.html', title=f'{P.package_name} - {sub}')

    def process_ajax(self, sub, req):
        try:
            gate = _check_sjva_or_block('json')
            if gate is not None:
                return gate

            logger.debug(f'[ff_tvh_m3u] process_ajax sub={sub}')

            if sub == 'setting_save':
                save_info = _save_runtime_settings(req)
                if save_info.get('saved'):
                    return jsonify({'ret': 'success', 'msg': '설정을 저장했습니다.', 'save_info': save_info})
                return jsonify({'ret': 'danger', 'msg': '설정 저장 실패'})

            elif sub == 'test_connection':
                _save_runtime_settings(req)
                return jsonify(Task.test_connection())

            elif sub == 'test_admin_login':
                _save_runtime_settings(req)
                return jsonify(Task.test_admin_login())

            elif sub == 'test_play_login':
                _save_runtime_settings(req)
                return jsonify(Task.test_play_login())

            elif sub == 'load_play_profiles':
                _save_runtime_settings(req)
                return jsonify(Task.get_play_profiles())

            elif sub == 'sync_channels':
                _save_runtime_settings(req)
                sync_ret = Task.sync_channels()
                if sync_ret.get('ret') != 'success':
                    return jsonify(sync_ret)

                match_ret = Task.apply_db_rules()
                final_ret = 'success'
                if match_ret.get('ret') == 'danger':
                    final_ret = 'danger'
                elif match_ret.get('ret') == 'warning':
                    final_ret = 'warning'

                return jsonify({
                    'ret': final_ret,
                    'msg': f"{sync_ret.get('msg', '')} / {match_ret.get('msg', '')}",
                    'sync': sync_ret,
                    'match': match_ret,
                })

            elif sub == 'apply_db_rules':
                _save_runtime_settings(req)
                return jsonify(Task.apply_db_rules())

            elif sub == 'reset_plugin_db':
                return jsonify(Task.reset_plugin_db())

            elif sub == 'search_match_channel':
                keyword = request.form.get('keyword')
                limit = request.form.get('limit', 30)
                return jsonify(Task.search_master_channels(keyword, limit=limit))

            elif sub == 'add_db_match_channel':
                channel_uuid = request.form.get('channel_uuid')
                channel_id = request.form.get('channel_id')
                return jsonify(Task.add_db_match_channel(channel_uuid, channel_id))

            elif sub == 'move_group':
                group_name = request.form.get('group_name')
                direction = request.form.get('direction')
                return jsonify(Task.move_group(group_name, direction))

            elif sub == 'assign_channels_to_group':
                channel_uuids = request.form.getlist('channel_uuids')
                new_group_name = request.form.get('new_group_name')
                target_group_name = request.form.get('target_group_name')
                return jsonify(Task.assign_channels_to_group(new_group_name, target_group_name, channel_uuids))

            elif sub == 'clear_manual_group':
                channel_uuids = request.form.getlist('channel_uuids')
                return jsonify(Task.clear_manual_group(channel_uuids))

            elif sub == 'save_group_profile':
                group_name = request.form.get('group_name')
                profile = request.form.get('profile')
                return jsonify(Task.save_group_profile(group_name, profile))

            elif sub == 'save_channel_profile':
                channel_uuid = request.form.get('channel_uuid')
                profile = request.form.get('profile')
                return jsonify(Task.save_channel_profile(channel_uuid, profile))

            elif sub == 'preview_m3u':
                _save_runtime_settings(req)
                target = (request.form.get('target') or 'tivimate').strip().lower()
                if target not in ['tvh', 'tivimate']:
                    target = 'tivimate'
                text = Task.build_m3u(target=target)
                preview = '\n'.join(text.splitlines()[:1000])
                return jsonify({'ret': 'success', 'preview': preview, 'msg': f'M3U 미리보기 생성 완료 ({target})'})

            return jsonify({'ret': 'warning', 'msg': f'알 수 없는 요청: {sub}'})

        except Exception as e:
            logger.exception(f'[ff_tvh_m3u] process_ajax exception: {str(e)}')
            return jsonify({'ret': 'danger', 'msg': str(e)})

    def process_api(self, sub, req):
        try:
            gate = _check_sjva_or_block('api')
            if gate is not None:
                return gate

            if sub == 'm3u':
                text = Task.build_m3u(target='tivimate')
                return Response(
                    text,
                    mimetype='audio/x-mpegurl',
                    headers={'Content-Disposition': 'inline; filename=tivimate_channels.m3u'}
                )

            elif sub == 'm3u_tvh':
                text = Task.build_m3u(target='tvh')
                return Response(
                    text,
                    mimetype='audio/x-mpegurl',
                    headers={'Content-Disposition': 'inline; filename=tvh_channels.m3u'}
                )

            elif sub == 'm3u_tivimate':
                text = Task.build_m3u(target='tivimate')
                return Response(
                    text,
                    mimetype='audio/x-mpegurl',
                    headers={'Content-Disposition': 'inline; filename=tivimate_channels.m3u'}
                )

            return jsonify({'ret': 'warning', 'msg': 'unknown api'})

        except Exception as e:
            logger.exception(f'[ff_tvh_m3u] process_api exception: {str(e)}')
            return jsonify({'ret': 'danger', 'msg': str(e)})
