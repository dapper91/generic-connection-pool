import asyncio
import ssl
from ipaddress import IPv4Address
from pathlib import Path
from typing import AsyncGenerator, Generator, Optional, Tuple

import pytest

from generic_connection_pool.asyncio import ConnectionPool
from generic_connection_pool.contrib.socket_async import TcpSocketConnectionManager, TcpStreamConnectionManager


class TCPServer:
    @staticmethod
    async def echo_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        data = await reader.read(1500)
        writer.write(data)
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    def __init__(self, hostname: str, port: int, ssl_ctx: Optional[ssl.SSLContext] = None):
        self._hostname = hostname
        self._port = port
        self._ssl_ctx = ssl_ctx
        self._server_task: Optional[asyncio.Task[None]] = None

    async def start(self) -> None:
        server = await asyncio.start_server(
            self.echo_handler,
            host=self._hostname,
            port=self._port,
            ssl=self._ssl_ctx,
            reuse_port=True,
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
async def tcp_server(port_gen: Generator[int, None, None]) -> AsyncGenerator[Tuple[IPv4Address, int], None]:
    addr, port = IPv4Address('127.0.0.1'), next(port_gen)
    server = TCPServer(str(addr), port)
    await server.start()
    yield addr, port
    await server.stop()


@pytest.fixture
async def ssl_server(resource_dir: Path, port_gen: Generator[int, None, None]) -> AsyncGenerator[Tuple[str, int], None]:
    hostname, port = 'localhost', next(port_gen)
    ssl_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ssl_ctx.load_cert_chain(resource_dir / 'ssl.cert', resource_dir / 'ssl.key')

    server = TCPServer(hostname, port, ssl_ctx=ssl_ctx)
    await server.start()
    yield hostname, port
    await server.stop()


async def test_tcp_socket_manager(tcp_server: Tuple[IPv4Address, int]):
    loop = asyncio.get_running_loop()
    addr, port = tcp_server

    pool = ConnectionPool(TcpSocketConnectionManager())
    async with pool.connection((addr, port)) as sock:
        request = b'test'
        await loop.sock_sendall(sock, request)
        response = await loop.sock_recv(sock, len(request))
        assert response == request

    await pool.close()


async def test_tcp_stream_manager(resource_dir: Path, tcp_server: Tuple[IPv4Address, int]):
    addr, port = tcp_server

    pool = ConnectionPool(TcpStreamConnectionManager(ssl=None))
    async with pool.connection((str(addr), port)) as (reader, writer):
        request = b'test'
        writer.write(request)
        await writer.drain()
        response = await reader.read()
        assert response == request

    await pool.close()


async def test_ssl_stream_manager(resource_dir: Path, ssl_server: Tuple[str, int]):
    hostname, port = ssl_server
    ssl_context = ssl.create_default_context(cafile=resource_dir / 'ssl.cert')

    pool = ConnectionPool(TcpStreamConnectionManager(ssl_context))
    async with pool.connection((hostname, port)) as (reader, writer):
        request = b'test'
        writer.write(request)
        await writer.drain()
        response = await reader.read()
        assert response == request

    await pool.close()
