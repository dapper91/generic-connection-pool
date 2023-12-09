import asyncio
import urllib.parse
from typing import Tuple

from generic_connection_pool.asyncio import ConnectionPool
from generic_connection_pool.contrib.socket_async import TcpStreamConnectionManager

Hostname = str
Port = int
Endpoint = Tuple[Hostname, Port]
Connection = Tuple[asyncio.StreamReader, asyncio.StreamWriter]


async def main() -> None:
    http_pool = ConnectionPool[Endpoint, Connection](
        TcpStreamConnectionManager(ssl=True),
        idle_timeout=30.0,
        max_lifetime=600.0,
        min_idle=3,
        max_size=20,
        total_max_size=100,
        background_collector=True,
    )

    async def fetch(url: str, timeout: float = 5.0) -> None:
        url = urllib.parse.urlsplit(url)
        port = url.port or 443 if url.scheme == 'https' else 80

        async with http_pool.connection(endpoint=(url.hostname, port), timeout=timeout) as (reader, writer):
            request = (
                'GET {path} HTTP/1.1\r\n'
                'Host: {host}\r\n'
                '\r\n'
                '\r\n'
            ).format(host=url.hostname, path=url.path)

            writer.write(request.encode())
            await writer.drain()

            status_line = await reader.readuntil(b'\r\n')
            headers = await reader.readuntil(b'\r\n\r\n')
            headers = {
                pair[0].lower(): pair[1]
                for header in headers.split(b'\r\n')
                if len(pair := header.decode().split(':', maxsplit=1)) == 2
            }

            chunks = []
            content_length = int(headers['content-length'])
            while content_length:
                chunk = await reader.read(content_length)
                chunks.append(chunk)
                content_length -= len(chunk)

            print(status_line)
            print(b''.join(chunks))

    try:
        await fetch('https://en.wikipedia.org/wiki/HTTP')  # http connection opened
        await fetch('https://en.wikipedia.org/wiki/Python_(programming_language)')  # http connection reused
    finally:
        await http_pool.close()

asyncio.run(main())
