"""
Synchronous socket connection manager implementation.
"""

import contextlib
import socket
from ipaddress import IPv4Address, IPv6Address
from ssl import SSLContext, SSLSocket
from typing import Generator, Optional, Tuple, Union

from generic_connection_pool.threading import BaseConnectionManager

IpAddress = Union[IPv4Address, IPv6Address]
Hostname = str
Port = int
TcpEndpoint = Tuple[IpAddress, Port]


@contextlib.contextmanager
def socket_timeout(sock: socket.socket, timeout: Optional[float]) -> Generator[None, None, None]:
    if timeout is None:
        yield
        return

    orig_timeout = sock.gettimeout()
    sock.settimeout(max(timeout, 1e-6))  # if timeout is 0 set it a small value to prevent non-blocking socket mode
    try:
        yield
    except OSError as e:
        if 'timed out' in str(e):
            raise TimeoutError
        else:
            raise
    finally:
        sock.settimeout(orig_timeout)


class TcpSocketConnectionManager(BaseConnectionManager[TcpEndpoint, socket.socket]):
    """
    TCP socket connection manager.
    """

    def create(self, endpoint: TcpEndpoint, timeout: Optional[float] = None) -> socket.socket:
        addr, port = endpoint

        if addr.version == 4:
            family = socket.AF_INET
        elif addr.version == 6:
            family = socket.AF_INET6
        else:
            raise RuntimeError("unsupported address version type: %s", addr.version)

        sock = socket.socket(family=family, type=socket.SOCK_STREAM)

        with socket_timeout(sock, timeout):
            sock.connect((str(addr), port))

        return sock

    def dispose(self, endpoint: TcpEndpoint, conn: socket.socket, timeout: Optional[float] = None) -> None:
        try:
            conn.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

        conn.close()


SslEndpoint = Tuple[Hostname, Port]


class SslSocketConnectionManager(BaseConnectionManager[SslEndpoint, SSLSocket]):
    """
    SSL socket connection manager.
    """

    def __init__(self, ssl: SSLContext):
        self._ssl = ssl

    def create(self, endpoint: SslEndpoint, timeout: Optional[float] = None) -> SSLSocket:
        hostname, port = endpoint

        sock = self._ssl.wrap_socket(socket.socket(type=socket.SOCK_STREAM), server_hostname=hostname)

        with socket_timeout(sock, timeout):
            sock.connect((hostname, port))

        return sock

    def dispose(self, endpoint: SslEndpoint, conn: SSLSocket, timeout: Optional[float] = None) -> None:
        try:
            conn.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

        conn.close()
