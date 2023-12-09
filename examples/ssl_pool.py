import ssl
import urllib.parse
from http.client import HTTPResponse
from typing import Tuple

from generic_connection_pool.contrib.socket import SslSocketConnectionManager
from generic_connection_pool.threading import ConnectionPool

Hostname = str
Port = int
Endpoint = Tuple[Hostname, Port]
Connection = ssl.SSLSocket


http_pool = ConnectionPool[Endpoint, Connection](
    SslSocketConnectionManager(ssl.create_default_context()),
    idle_timeout=30.0,
    max_lifetime=600.0,
    min_idle=3,
    max_size=20,
    total_max_size=100,
    background_collector=True,
)


def fetch(url: str, timeout: float = 5.0) -> None:
    url = urllib.parse.urlsplit(url)
    port = url.port or 443 if url.scheme == 'https' else 80

    with http_pool.connection(endpoint=(url.hostname, port), timeout=timeout) as sock:
        request = (
            'GET {path} HTTP/1.1\r\n'
            'Host: {host}\r\n'
            '\r\n'
            '\r\n'
        ).format(host=url.hostname, path=url.path)

        sock.write(request.encode())

        response = HTTPResponse(sock)
        response.begin()
        status, body = response.getcode(), response.read(response.length)

        print(status)
        print(body)


try:
    fetch('https://en.wikipedia.org/wiki/HTTP')  # http connection opened
    fetch('https://en.wikipedia.org/wiki/Python_(programming_language)')  # http connection reused
finally:
    http_pool.close()
