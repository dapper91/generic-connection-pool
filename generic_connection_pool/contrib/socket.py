import socket
from ipaddress import IPv4Address, IPv6Address
from ssl import SSLContext, SSLSocket
from typing import Optional, Tuple, Union

from generic_connection_pool.threding import BaseConnectionManager

IpAddress = Union[IPv4Address, IPv6Address]
Hostname = str
Port = int
TcpEndpoint = Tuple[IpAddress, Port]


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
        sock.settimeout(timeout)
        sock.connect((str(addr), port))

        return sock

    def dispose(self, endpoint: TcpEndpoint, conn: socket.socket, timeout: Optional[float] = None) -> None:
        conn.settimeout(timeout)
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
        sock.settimeout(timeout)
        sock.connect((hostname, port))

        return sock

    def dispose(self, endpoint: SslEndpoint, conn: SSLSocket, timeout: Optional[float] = None) -> None:
        conn.settimeout(timeout)
        try:
            conn.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

        conn.close()
