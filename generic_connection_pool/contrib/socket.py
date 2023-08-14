"""
Synchronous socket connection manager implementation.
"""

import contextlib
import errno
import socket
from ipaddress import IPv4Address, IPv6Address
from ssl import SSLContext, SSLSocket
from typing import Generator, Generic, Optional, Tuple, Union

from generic_connection_pool.threading import BaseConnectionManager, EndpointT

IpAddress = Union[IPv4Address, IPv6Address]
Hostname = str
Port = int
TcpEndpoint = Tuple[IpAddress, Port]


@contextlib.contextmanager
def socket_nonblocking(sock: socket.socket) -> Generator[None, None, None]:
    orig_timeout = sock.gettimeout()
    sock.settimeout(0)
    try:
        yield
    finally:
        sock.settimeout(orig_timeout)


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


class SocketAlivenessCheckingMixin(Generic[EndpointT]):
    """
    Socket aliveness checking mix-in.
    """

    def check_aliveness(self, endpoint: EndpointT, conn: socket.socket, timeout: Optional[float] = None) -> bool:
        try:
            with socket_nonblocking(conn):
                if conn.recv(1, socket.MSG_PEEK) == b'':
                    return False
        except BlockingIOError as exc:
            if exc.errno != errno.EAGAIN:
                raise
        except OSError:
            return False

        return True


class TcpSocketConnectionManager(
    SocketAlivenessCheckingMixin[TcpEndpoint],
    BaseConnectionManager[TcpEndpoint, socket.socket],
):
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


class SslSocketAlivenessCheckingMixin(Generic[EndpointT]):
    """
    SSL socket aliveness checking mix-in.
    """

    def check_aliveness(self, endpoint: EndpointT, conn: SSLSocket, timeout: Optional[float] = None) -> bool:
        try:
            with socket_nonblocking(conn):
                # peek into the plain socket since ssl socket doesn't support flags
                if socket.socket.recv(conn, 1, socket.MSG_PEEK) == b'':
                    return False
        except BlockingIOError as exc:
            if exc.errno != errno.EAGAIN:
                raise
        except OSError:
            return False

        return True


SslEndpoint = Tuple[Hostname, Port]


class SslSocketConnectionManager(
    SslSocketAlivenessCheckingMixin[SslEndpoint],
    BaseConnectionManager[SslEndpoint, SSLSocket],
):
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
