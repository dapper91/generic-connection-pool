import socket
import socketserver
import sys
import tempfile
import threading
from pathlib import Path
from typing import Generator, Optional

import pytest

from generic_connection_pool.contrib.unix import UnixSocketConnectionManager
from generic_connection_pool.threading import ConnectionPool
from tests.contrib.test_socket_manager import EchoTCPServer

if sys.platform not in ('linux', 'darwin', 'freebsd'):
    pytest.skip(reason="unix platform only", allow_module_level=True)


class EchoUnixServer(EchoTCPServer):
    address_family = socket.AF_UNIX

    def __init__(self, server_address: str):
        socketserver.TCPServer.__init__(self, server_address, self.EchoRequestHandler)
        self._server_thread: Optional[threading.Thread] = None


@pytest.fixture(scope='module')
def unix_socket_server() -> Generator[Path, None, None]:
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir) / 'unix-socket-server.sock'
        server = EchoUnixServer(str(path))
        server.start()
        yield path
        server.stop()


@pytest.mark.timeout(5.0)
def test_unix_socket_manager(unix_socket_server: Path):
    path = unix_socket_server

    pool = ConnectionPool[Path, socket.socket](UnixSocketConnectionManager())

    attempts = 3
    request = b'test'
    for _ in range(attempts):
        with pool.connection(path) as sock1:
            sock1.sendall(request)
            response = sock1.recv(len(request))
            assert response == request

            with pool.connection(path) as sock2:
                sock2.sendall(request)
                response = sock2.recv(len(request))
                assert response == request

    pool.close()
