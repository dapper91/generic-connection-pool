"""
Asynchronous unix specific functionality.
"""

import asyncio
import pathlib
import socket
import sys

from generic_connection_pool.contrib.socket_async import BaseConnectionManager, SocketAlivenessCheckingMixin, Stream
from generic_connection_pool.contrib.socket_async import StreamAlivenessCheckingMixin

if sys.platform not in ('linux', 'darwin', 'freebsd'):
    raise AssertionError('this module is supported only on unix platforms')


UnixSocketEndpoint = pathlib.Path


class UnixSocketConnectionManager(
    SocketAlivenessCheckingMixin[UnixSocketEndpoint],
    BaseConnectionManager[UnixSocketEndpoint, socket.socket],
):
    """
    Asynchronous unix socket connection manager.
    """

    async def create(self, endpoint: UnixSocketEndpoint) -> socket.socket:
        loop = asyncio.get_running_loop()

        sock = socket.socket(family=socket.AF_UNIX, type=socket.SOCK_STREAM)
        sock.setblocking(False)

        await loop.sock_connect(sock, address=(str(endpoint)))

        return sock

    async def dispose(self, endpoint: UnixSocketEndpoint, conn: socket.socket) -> None:
        try:
            conn.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

        conn.close()


class UnixSocketStreamConnectionManager(
    StreamAlivenessCheckingMixin[UnixSocketEndpoint],
    BaseConnectionManager[UnixSocketEndpoint, Stream],
):
    """
    Asynchronous unix socket stream connection manager.
    """

    async def create(self, endpoint: UnixSocketEndpoint) -> Stream:
        reader, writer = await asyncio.open_unix_connection(path=endpoint)

        return reader, writer

    async def dispose(self, endpoint: UnixSocketEndpoint, conn: Stream) -> None:
        reader, writer = conn
        if writer.can_write_eof():
            writer.write_eof()

        writer.close()
        await writer.wait_closed()
