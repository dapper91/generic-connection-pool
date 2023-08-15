"""
Unix specific functionality.
"""

import errno
import pathlib
import socket
import sys
from typing import Generic, Optional

from generic_connection_pool.contrib.socket import BaseConnectionManager
from generic_connection_pool.threading import EndpointT

from .socket import socket_timeout

if sys.platform not in ('linux', 'darwin', 'freebsd'):
    raise AssertionError('this module is supported only on unix platforms')


UnixSocketEndpoint = pathlib.Path


class CheckSocketAlivenessMixin(Generic[EndpointT]):
    """
    Socket aliveness checking mixin.
    """

    def check_aliveness(self, endpoint: EndpointT, conn: socket.socket, timeout: Optional[float] = None) -> bool:
        try:
            with socket_timeout(conn, timeout):
                resp = conn.recv(1, socket.MSG_PEEK | socket.MSG_DONTWAIT)
            if resp == b'':
                return False
        except BlockingIOError as exc:
            if exc.errno != errno.EAGAIN:
                raise
        except OSError:
            return False

        return True


class UnixSocketConnectionManager(
    CheckSocketAlivenessMixin[UnixSocketEndpoint],
    BaseConnectionManager[UnixSocketEndpoint, socket.socket],
):
    """
    Unix socket connection manager.
    """

    def create(self, endpoint: UnixSocketEndpoint, timeout: Optional[float] = None) -> socket.socket:
        sock = socket.socket(family=socket.AF_UNIX, type=socket.SOCK_STREAM)

        with socket_timeout(sock, timeout):
            sock.connect(str(endpoint))

        return sock

    def dispose(self, endpoint: UnixSocketEndpoint, conn: socket.socket, timeout: Optional[float] = None) -> None:
        try:
            conn.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

        conn.close()
