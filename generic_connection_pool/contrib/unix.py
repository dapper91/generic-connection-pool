"""
Unix specific functionality.
"""

import errno
import socket
import sys
from typing import Optional

from generic_connection_pool.contrib.socket import TcpEndpoint

from .socket import socket_timeout

if sys.platform not in ('linux', 'darwin'):
    raise AssertionError('this module is supported only on linux and darwin platform')


class CheckSocketAlivenessMixin:
    """
    Socket aliveness checking mixin.
    """

    def check_aliveness(self, endpoint: TcpEndpoint, conn: socket.socket, timeout: Optional[float] = None) -> bool:
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
