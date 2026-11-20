"""Transport policy and bounded relays for TVHeadend and EPG."""
import ipaddress
import os
import socket
from urllib.parse import urlsplit

import requests

CONNECT_TIMEOUT = 10
READ_TIMEOUT = 20
STREAM_TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)


def validate_upstream(url):
    parts = urlsplit(url)
    host = parts.hostname
    if parts.scheme not in ('http', 'https') or not host or parts.username is not None:
        raise requests.exceptions.InvalidURL('TVH/EPG 주소에는 HTTP(S)와 별도 인증 설정을 사용하세요.')
    if parts.scheme == 'http':
        try:
            addresses = {ipaddress.ip_address(x[4][0].split('%')[0])
                         for x in socket.getaddrinfo(host, parts.port or 80, type=socket.SOCK_STREAM)}
        except (OSError, ValueError) as exc:
            raise requests.exceptions.InvalidURL('HTTP 원본 주소를 확인할 수 없습니다.') from exc
        if not addresses or any(not (ip.is_private or ip.is_loopback) for ip in addresses):
            raise requests.exceptions.InvalidURL('공용 네트워크의 TVH/EPG 원본에는 HTTPS가 필요합니다.')
    return url


class VerifiedSession(requests.Session):
    def __init__(self):
        super().__init__()
        self.trust_env = False

    def request(self, method, url, **kwargs):
        validate_upstream(url)
        kwargs['allow_redirects'] = False
        kwargs['verify'] = os.environ.get('TVH_M3U_CA_BUNDLE') or True
        kwargs.setdefault('timeout', STREAM_TIMEOUT)
        return super().request(method, url, **kwargs)
