import socket
import socketserver
import ssl
import threading
from ipaddress import IPv4Address
from pathlib import Path
from typing import Generator, Optional, Tuple

import pytest

from generic_connection_pool.contrib.socket import SslSocketConnectionManager, TcpSocketConnectionManager
from generic_connection_pool.threading import ConnectionPool


class EchoTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True

    class EchoRequestHandler(socketserver.BaseRequestHandler):
        def handle(self):
            data = self.request.recv(1500)
            self.request.sendall(data)

    def __init__(self, server_address: Tuple[str, int]):
        super().__init__(server_address, self.EchoRequestHandler)
        self._server_thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._server_thread = threading.Thread(target=self.serve_forever)
        self._server_thread.daemon = True
        self._server_thread.start()

    def stop(self) -> None:
        self.server_close()
        self.shutdown()
        if (server_thread := self._server_thread) is not None:
            server_thread.join()


class EchoSSLServer(EchoTCPServer):
    def __init__(self, server_address: Tuple[str, int], ssl_ctx: ssl.SSLContext):
        super().__init__(server_address)
        self._ssl_ctx = ssl_ctx

    def get_request(self):
        sock, addr = super().get_request()
        ssl_socket = self._ssl_ctx.wrap_socket(sock, server_side=True)
        return ssl_socket, addr


@pytest.fixture(scope='module')
def tcp_server(port_gen: Generator[int, None, None]) -> Generator[Tuple[IPv4Address, int], None, None]:
    addr, port = IPv4Address('127.0.0.1'), next(port_gen)

    server = EchoTCPServer((str(addr), port))
    server.start()
    yield addr, port
    server.stop()


@pytest.fixture(scope='module')
def ssl_server(resource_dir: Path, port_gen: Generator[int, None, None]) -> Generator[Tuple[str, int], None, None]:
    hostname, port = 'localhost', next(port_gen)

    ssl_ctx = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS_SERVER)
    ssl_ctx.load_cert_chain(keyfile=resource_dir / 'ssl.key', certfile=resource_dir / 'ssl.cert')

    server = EchoSSLServer((hostname, port), ssl_ctx=ssl_ctx)
    server.start()
    yield hostname, port
    server.stop()


@pytest.mark.timeout(5.0)
def test_tcp_socket_manager(tcp_server: Tuple[IPv4Address, int]):
    addr, port = tcp_server

    pool = ConnectionPool(TcpSocketConnectionManager())

    request = b'test'
    for _ in range(3):
        with pool.connection((addr, port)) as sock1:
            sock1.sendall(request)
            response = sock1.recv(len(request))
            assert response == request

            with pool.connection((addr, port)) as sock2:
                sock2.sendall(request)
                response = sock2.recv(len(request))
                assert response == request

    pool.close()


@pytest.mark.timeout(5.0)
def test_ssl_socket_manager(resource_dir: Path, ssl_server: Tuple[str, int]):
    hostname, port = ssl_server
    ssl_context = ssl.create_default_context(cafile=resource_dir / 'ssl.cert')

    pool = ConnectionPool(SslSocketConnectionManager(ssl_context))

    request = b'test'
    for _ in range(3):
        with pool.connection((hostname, port)) as sock1:
            sock1.sendall(request)
            response = sock1.recv(len(request))
            assert response == request

            with pool.connection((hostname, port)) as sock2:
                sock2.sendall(request)
                response = sock2.recv(len(request))
                assert response == request

    pool.close()


@pytest.mark.timeout(5.0)
def test_tcp_socket_manager_timeout(delay, port_gen: Generator[int, None, None]):
    addr, port = IPv4Address('127.0.0.1'), next(port_gen)

    server_sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    server_sock.bind((str(addr), port))
    server_sock.listen(1)

    pool = ConnectionPool(TcpSocketConnectionManager())
    with pytest.raises(TimeoutError):
        with pool.connection(endpoint=(addr, port), timeout=delay):
            with pool.connection(endpoint=(addr, port), timeout=delay):
                with pool.connection(endpoint=(addr, port), timeout=delay):
                    pass

    pool.close()
    server_sock.close()


@pytest.mark.timeout(5.0)
def test_ssl_socket_manager_timeout(delay, port_gen: Generator[int, None, None]):
    hostname, port = 'localhost', next(port_gen)

    server_sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    server_sock.bind((hostname, port))
    server_sock.listen(1)

    pool = ConnectionPool(SslSocketConnectionManager(ssl.create_default_context()))
    with pytest.raises(TimeoutError):
        with pool.connection(endpoint=(hostname, port), timeout=delay):
            with pool.connection(endpoint=(hostname, port), timeout=delay):
                with pool.connection(endpoint=(hostname, port), timeout=delay):
                    pass

    pool.close()
    server_sock.close()
