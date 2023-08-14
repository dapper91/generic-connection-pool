"""
Asynchronous socket connection manager implementation.
"""

import asyncio
import errno
import socket
from ipaddress import IPv4Address, IPv6Address
from ssl import SSLContext
from typing import Generic, Tuple, Union

from generic_connection_pool.asyncio import BaseConnectionManager, EndpointT

IpAddress = Union[IPv4Address, IPv6Address]
Port = int
TcpEndpoint = Tuple[IpAddress, Port]


class SocketAlivenessCheckingMixin(Generic[EndpointT]):
    """
    Nonblocking socket aliveness checking mix-in.
    """

    async def check_aliveness(self, endpoint: EndpointT, conn: socket.socket) -> bool:
        try:
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

    async def create(self, endpoint: TcpEndpoint) -> socket.socket:
        loop = asyncio.get_running_loop()
        addr, port = endpoint

        if addr.version == 4:
            family = socket.AF_INET
        elif addr.version == 6:
            family = socket.AF_INET6
        else:
            raise RuntimeError("unsupported address version type: %s", addr.version)

        sock = socket.socket(family=family, type=socket.SOCK_STREAM)
        sock.setblocking(False)

        await loop.sock_connect(sock, address=(str(addr), port))

        return sock

    async def dispose(self, endpoint: TcpEndpoint, conn: socket.socket) -> None:
        conn.shutdown(socket.SHUT_RDWR)
        conn.close()


Hostname = str
TcpStreamEndpoint = Tuple[Hostname, Port]
Stream = Tuple[asyncio.StreamReader, asyncio.StreamWriter]


class StreamAlivenessCheckingMixin(Generic[EndpointT]):
    """
    Asynchronous stream aliveness checking mix-in.
    """

    async def check_aliveness(self, endpoint: EndpointT, conn: Stream) -> bool:
        reader, writer = conn

        try:
            await reader.read(0)
        except OSError:
            return False

        return not writer.is_closing() and not reader.at_eof()


class TcpStreamConnectionManager(
    StreamAlivenessCheckingMixin[TcpStreamEndpoint],
    BaseConnectionManager[TcpStreamEndpoint, Stream],
):
    """
    TCP stream connection manager.
    """

    def __init__(self, ssl: Union[None, bool, SSLContext] = None):
        self._ssl = ssl

    async def create(self, endpoint: TcpStreamEndpoint) -> Stream:
        hostname, port = endpoint
        server_hostname = hostname if self._ssl is not None else None

        reader, writer = await asyncio.open_connection(
            hostname,
            port,
            server_hostname=server_hostname,
            ssl=self._ssl,
        )

        return reader, writer

    async def dispose(self, endpoint: TcpStreamEndpoint, conn: Stream) -> None:
        reader, writer = conn
        if writer.can_write_eof():
            writer.write_eof()

        writer.close()
        await writer.wait_closed()
