import asyncio
import ssl
from ipaddress import IPv4Address
from pathlib import Path
from typing import Tuple

import pytest
import pytest_asyncio.plugin

from generic_connection_pool.asyncio import ConnectionPool
from generic_connection_pool.contrib.socket_async import TcpSocketConnectionManager, TcpStreamConnectionManager


@pytest.fixture
async def tcp_server(request: pytest_asyncio.plugin.SubRequest, resource_dir: Path):
    params = getattr(request, 'param', {})
    user_ssl = params.get('use_ssl', False)

    if user_ssl:
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.load_cert_chain(resource_dir / 'ssl.cert', resource_dir / 'ssl.key')
    else:
        context = None

    hostname, addr, port = 'localhost', IPv4Address('127.0.0.1'),  10000

    async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        data = await reader.read(1024)
        writer.write(data)
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    async with (server := await asyncio.start_server(echo, host=hostname, port=port, ssl=context, reuse_port=True)):
        server_task = asyncio.create_task(server.serve_forever())
        yield hostname, addr, port
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass


async def test_tcp_socket_manager(tcp_server: Tuple[str, IPv4Address, int]):
    loop = asyncio.get_running_loop()
    hostname, addr, port = tcp_server

    pool = ConnectionPool(TcpSocketConnectionManager())
    async with pool.connection((addr, port)) as sock:
        request = b'test'
        await loop.sock_sendall(sock, request)
        response = await loop.sock_recv(sock, len(request))
        assert response == request

    await pool.close()


@pytest.mark.parametrize(
    'use_ssl, tcp_server', [
        (True, {'use_ssl': True}),
        (False, {'use_ssl': False}),
    ],
    indirect=['tcp_server'],
)
async def test_ssl_stream_manager(resource_dir: Path, use_ssl: bool, tcp_server: Tuple[str, IPv4Address, int]):
    hostname, addr, port = tcp_server

    if use_ssl:
        ssl_context = ssl.create_default_context(cafile=resource_dir / 'ssl.cert')
    else:
        ssl_context = None

    pool = ConnectionPool(TcpStreamConnectionManager(ssl_context))
    async with pool.connection((hostname, port)) as (reader, writer):
        request = b'test'
        writer.write(request)
        await writer.drain()
        response = await reader.read()
        assert response == request

    await pool.close()
