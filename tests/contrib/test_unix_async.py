import asyncio
import socket
import sys
import tempfile
from pathlib import Path
from typing import AsyncGenerator, Generator, Optional

import pytest

from generic_connection_pool.asyncio import ConnectionPool
from generic_connection_pool.contrib.unix_async import Stream, UnixSocketConnectionManager
from generic_connection_pool.contrib.unix_async import UnixSocketStreamConnectionManager

if sys.platform not in ('linux', 'darwin', 'freebsd'):
    pytest.skip(reason="unix platform only", allow_module_level=True)


class EchoUnixServer:
    @staticmethod
    async def echo_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        while data := await reader.read(1024):
            writer.write(data)
            await writer.drain()

        writer.close()
        await writer.wait_closed()

    def __init__(self, path: Path):
        self._path = path
        self._server_task: Optional[asyncio.Task[None]] = None

    async def start(self) -> None:
        server = await asyncio.start_unix_server(
            self.echo_handler,
            path=self._path,
            start_serving=False,
        )
        self._server_task = asyncio.create_task(server.serve_forever())

    async def stop(self) -> None:
        if (server_task := self._server_task) is not None:
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass


@pytest.fixture
async def unix_socket_server(port_gen: Generator[int, None, None]) -> AsyncGenerator[Path, None]:
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir) / 'unix-socket-server-async.sock'
        server = EchoUnixServer(path)
        await server.start()
        yield path
        await server.stop()


@pytest.mark.timeout(5.0)
async def test_unix_socket_manager(unix_socket_server: Path):
    loop = asyncio.get_running_loop()
    path = unix_socket_server

    pool = ConnectionPool[Path, socket.socket](UnixSocketConnectionManager())

    attempts = 3
    request = b'test'
    for _ in range(attempts):
        async with pool.connection(path) as sock1:
            await loop.sock_sendall(sock1, request)
            response = await loop.sock_recv(sock1, len(request))
            assert response == request

            async with pool.connection(path) as sock2:
                await loop.sock_sendall(sock2, request)
                response = await loop.sock_recv(sock2, len(request))
                assert response == request

    await pool.close()


@pytest.mark.timeout(5.0)
async def test_unix_stream_manager(resource_dir: Path, unix_socket_server: Path):
    path = unix_socket_server

    pool = ConnectionPool[Path, Stream](
        UnixSocketStreamConnectionManager(),
    )

    attempts = 3
    request = b'test'
    for _ in range(attempts):
        async with pool.connection(path) as (reader1, writer1):
            writer1.write(request)
            await writer1.drain()
            response = await reader1.read(len(request))
            assert response == request

            async with pool.connection(path) as (reader2, writer2):
                writer2.write(request)
                await writer2.drain()
                response = await reader2.read(len(request))
                assert response == request

    await pool.close()
