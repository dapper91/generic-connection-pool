=======================
generic-connection-pool
=======================

.. image:: https://static.pepy.tech/personalized-badge/generic-connection-pool?period=month&units=international_system&left_color=grey&right_color=orange&left_text=Downloads/month
    :target: https://pepy.tech/project/generic-connection-pool
    :alt: Downloads/month
.. image:: https://github.com/dapper91/generic-connection-pool/actions/workflows/test.yml/badge.svg?branch=master
    :target: https://github.com/dapper91/generic-connection-pool/actions/workflows/test.yml
    :alt: Build status
.. image:: https://img.shields.io/pypi/l/generic-connection-pool.svg
    :target: https://pypi.org/project/generic-connection-pool
    :alt: License
.. image:: https://img.shields.io/pypi/pyversions/generic-connection-pool.svg
    :target: https://pypi.org/project/generic-connection-pool
    :alt: Supported Python versions
.. image:: https://codecov.io/gh/dapper91/generic-connection-pool/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/dapper91/generic-connection-pool
    :alt: Code coverage


``generic-connection-pool`` is a connection pool that can be used for tcp, http, database connections.

Features:

- **generic nature**: can be used for any connection you desire (tcp, http, database)
- **runtime agnostic**: synchronous and asynchronous pool supported
- **flexibility**: flexable connection retention policy configuration
- **fully-typed**: mypy type-checker compatible


Installation
------------

You can install generic-connection-pool with pip:

.. code-block:: console

    $ pip install generic-connection-pool


Quickstart
----------

.. code-block:: python

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

    asyncio.run(main())
