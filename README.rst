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


``generic-connection-pool`` is a connection pool that can be used for TCP, http, database connections.

Features:

- **generic nature**: can be used for any connection you desire (TCP, http, database)
- **runtime agnostic**: synchronous and asynchronous pool supported
- **flexibility**: flexable connection retention and recycling policy
- **fully-typed**: mypy type-checker compatible


Installation
------------

You can install generic-connection-pool with pip:

.. code-block:: console

    $ pip install generic-connection-pool


Quickstart
----------

The following example illustrates how to create asynchronous ssl socket pool:

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


Configuration
-------------

Synchronous and asynchronous pools supports the following parameters:

* **connection_manager**: connection manager instance
* **acquire_timeout**: connection acquiring default timeout
* **dispose_batch_size**: number of connections to be disposed at once
  (if background collector is started the parameter is ignored)
* **dispose_timeout**: connection disposal timeout
* **background_collector**: start worker that disposes timed-out connections in background maintain provided pool state
  otherwise they will be disposed on each connection release
* **idle_timeout**: number of seconds after which a connection will be closed respecting min_idle parameter
  (the connection will be closed only if the connection number exceeds min_idle)
* **max_lifetime**: number of seconds after which a connection will be closed (min_idle parameter will be ignored)
* **min_idle**: minimum number of connections the pool tries to hold (for each endpoint)
* **max_size**: maximum number of connections (for each endpoint)
* **total_max_size**: maximum number of connections (for all endpoints)


Generic nature
--------------

Since the pool has generic nature is can be used for database connections as well:

.. code-block:: python

    import psycopg2.extensions

    from generic_connection_pool.contrib.psycopg2 import DbConnectionManager
    from generic_connection_pool.threading import ConnectionPool

    Endpoint = str
    Connection = psycopg2.extensions.connection


    def main() -> None:
        dsn_params = dict(dbname='postgres', user='postgres', password='secret')

        pool = ConnectionPool[Endpoint, Connection](
            DbConnectionManager(
                dsn_params={
                    'master': dict(dsn_params, host='db-master.local'),
                    'replica-1': dict(dsn_params, host='db-replica-1.local'),
                    'replica-2': dict(dsn_params, host='db-replica-2.local'),
                },
            ),
            acquire_timeout=2.0,
            idle_timeout=60.0,
            max_lifetime=600.0,
            min_idle=3,
            max_size=10,
            total_max_size=15,
            background_collector=True,
        )

        with pool.connection(endpoint='master') as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM pg_stats;")
            print(cur.fetchone())

        with pool.connection(endpoint='replica-1') as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM pg_stats;")
            print(cur.fetchone())

        pool.close()


    main()


Extendability
-------------

If built-in connection managers are not suitable for your task the one can be easily created by yourself:

.. code-block:: python

    import socket
    from ssl import SSLContext, SSLSocket
    from typing import Optional, Tuple

    from generic_connection_pool.threading import BaseConnectionManager, ConnectionPool

    Hostname = str
    Port = int
    SslEndpoint = Tuple[Hostname, Port]
    Connection = SSLSocket


    class SslSocketConnectionManager(BaseConnectionManager[SslEndpoint, Connection]):
        """
        SSL socket connection manager.
        """

        def __init__(self, ssl: SSLContext):
            self._ssl = ssl

        def create(self, endpoint: SslEndpoint, timeout: Optional[float] = None) -> Connection:
            hostname, port = endpoint

            sock = self._ssl.wrap_socket(socket.socket(type=socket.SOCK_STREAM), server_hostname=hostname)
            sock.settimeout(timeout)
            sock.connect((hostname, port))

            return sock

        def dispose(self, endpoint: SslEndpoint, conn: Connection, timeout: Optional[float] = None) -> None:
            conn.settimeout(timeout)
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

            conn.close()


    def main() -> None:
        pool = ConnectionPool[SslEndpoint, Connection](
            SslSocketConnectionManager(ssl=SSLContext()),
            idle_timeout=30.0,
            max_lifetime=600.0,
            min_idle=3,
            max_size=20,
            total_max_size=100,
            background_collector=True,
        )

        with pool.connection(endpoint=('www.wikipedia.org', 443), timeout=5.0) as sock:
            request = (
                'GET / HTTP/1.0\n'
                'Host: www.wikipedia.org\n'
                '\n'
                '\n'
            )
            sock.write(request.encode())
            response = []
            while chunk := sock.recv():
                response.append(chunk)

            print(b''.join(response).decode())

        pool.close()


    main()
