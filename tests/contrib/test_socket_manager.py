import socketserver
import ssl
import threading
from ipaddress import IPv4Address
from pathlib import Path
from typing import Tuple

import pytest
import pytest_asyncio.plugin

from generic_connection_pool.contrib.socket import SslSocketConnectionManager, TcpSocketConnectionManager
from generic_connection_pool.threding import ConnectionPool


@pytest.fixture
async def tcp_server(request: pytest_asyncio.plugin.SubRequest, resource_dir: Path):
    params = getattr(request, 'param', {})
    user_ssl = params.get('use_ssl', False)

    hostname, addr, port = 'localhost', IPv4Address('127.0.0.1'),  10000

    class RequestHandler(socketserver.BaseRequestHandler):
        def handle(self):
            data = self.request.recv(1024)
            self.request.sendall(data)

    class TCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
        allow_reuse_address = True

    class SSLServer(TCPServer):
        def get_request(self):
            socket, addr = super().get_request()
            ssl_socket = ssl.wrap_socket(
                socket,
                server_side=True,
                keyfile=resource_dir / 'ssl.key',
                certfile=resource_dir / 'ssl.cert',
            )
            return ssl_socket, addr

    Server = SSLServer if user_ssl else TCPServer

    server = Server((hostname, port), RequestHandler)
    with server:
        server_thread = threading.Thread(target=server.serve_forever)
        server_thread.daemon = True
        server_thread.start()
        yield hostname, addr, port
        server.shutdown()


@pytest.mark.parametrize('tcp_server', [{'use_ssl': False}], indirect=['tcp_server'])
def test_tcp_socket_manager(tcp_server: Tuple[str, IPv4Address, int]):
    hostname, addr, port = tcp_server

    pool = ConnectionPool(TcpSocketConnectionManager())
    with pool.connection((addr, port)) as sock:
        request = b'test'
        sock.sendall(request)
        response = sock.recv(len(request))
        assert response == request

    pool.close()


@pytest.mark.parametrize('tcp_server', [{'use_ssl': True}], indirect=['tcp_server'])
def test_ssl_socket_manager(resource_dir: Path, tcp_server: Tuple[str, IPv4Address, int]):
    hostname, addr, port = tcp_server
    ssl_context = ssl.create_default_context(cafile=resource_dir / 'ssl.cert')

    pool = ConnectionPool(SslSocketConnectionManager(ssl_context))
    with pool.connection((hostname, port)) as sock:
        request = b'test'
        sock.sendall(request)
        response = sock.recv(len(request))
        assert response == request

    pool.close()
