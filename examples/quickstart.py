import asyncio
from typing import Tuple

from generic_connection_pool.asyncio import ConnectionPool
from generic_connection_pool.contrib.socket_async import TcpStreamConnectionManager

Hostname = str
Port = int
Endpoint = Tuple[Hostname, Port]
Connection = Tuple[asyncio.StreamReader, asyncio.StreamWriter]


async def main() -> None:
    pool = ConnectionPool[Endpoint, Connection](
        TcpStreamConnectionManager(ssl=True),
        idle_timeout=30.0,
        max_lifetime=600.0,
        min_idle=3,
        max_size=20,
        total_max_size=100,
        background_collector=True,
    )

    async with pool.connection(endpoint=('www.wikipedia.org', 443), timeout=5.0) as (reader, writer):
        request = (
            'GET / HTTP/1.0\n'
            'Host: www.wikipedia.org\n'
            '\n'
            '\n'
        )
        writer.write(request.encode())
        await writer.drain()
        response = await reader.read()

        print(response.decode())

    await pool.close()

asyncio.run(main())
