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
.. image:: https://readthedocs.org/projects/generic-connection-pool/badge/?version=stable&style=flat
   :alt: ReadTheDocs status
   :target: https://generic-connection-pool.readthedocs.io/en/stable/


``generic-connection-pool`` is an extensible connection pool agnostic to the connection type it is managing.
It can be used for TCP, http, database or ssh connections.

Features
--------

- **generic nature**: can be used for any connection you desire (TCP, http, database, ssh, etc.)
- **runtime agnostic**: synchronous and asynchronous runtime supported
- **flexibility**: flexable connection retention and recycling policy
- **fully-typed**: mypy type-checker compatible


Getting started
---------------

Connection pool supports the following configurations:

* **background_collector**: if ``True`` starts a background worker that disposes expired and idle connections
  maintaining requested pool state. If ``False`` the connections will be disposed on each connection release.
* **dispose_batch_size**: maximum number of expired and idle connections to be disposed on connection release
  (if background collector is started the parameter is ignored).
* **idle_timeout**: inactivity time (in seconds) after which an extra connection will be disposed
  (a connection considered as extra if the number of endpoint connection exceeds ``min_idle``).
* **max_lifetime**: number of seconds after which any connection will be disposed.
* **min_idle**: minimum number of connections in each endpoint the pool tries to hold. Connections that exceed
  that number will be considered as extra and disposed after ``idle_timeout`` seconds of inactivity.
* **max_size**: maximum number of endpoint connections.
* **total_max_size**: maximum number of all connections in the pool.


The following example illustrates how to create https pool:

.. code-block:: python

    import socket
    import ssl
    import urllib.parse
    from http.client import HTTPResponse
    from typing import Tuple

    from generic_connection_pool.contrib.socket import SslSocketConnectionManager
    from generic_connection_pool.threading import ConnectionPool

    Hostname = str
    Port = int
    Endpoint = Tuple[Hostname, Port]
    Connection = socket.socket


    http_pool = ConnectionPool[Endpoint, Connection](
        SslSocketConnectionManager(ssl.create_default_context()),
        idle_timeout=30.0,
        max_lifetime=600.0,
        min_idle=3,
        max_size=20,
        total_max_size=100,
        background_collector=True,
    )


    def fetch(url: str, timeout: float = 5.0) -> None:
        url = urllib.parse.urlsplit(url)
        port = url.port or 443 if url.scheme == 'https' else 80

        with http_pool.connection(endpoint=(url.hostname, port), timeout=timeout) as sock:
            request = (
                'GET {path} HTTP/1.1\r\n'
                'Host: {host}\r\n'
                '\r\n'
                '\r\n'
            ).format(host=url.hostname, path=url.path)

            sock.write(request.encode())

            response = HTTPResponse(sock)
            response.begin()
            status, body = response.getcode(), response.read(response.length)

            print(status)
            print(body)


    try:
        fetch('https://en.wikipedia.org/wiki/HTTP')  # http connection is opened
        fetch('https://en.wikipedia.org/wiki/Python_(programming_language)')  # http connection is reused
    finally:
        http_pool.close()

... or database one

.. code-block:: python

    import psycopg2.extensions

    from generic_connection_pool.contrib.psycopg2 import DbConnectionManager
    from generic_connection_pool.threading import ConnectionPool

    Endpoint = str
    Connection = psycopg2.extensions.connection


    dsn_params = dict(dbname='postgres', user='postgres', password='secret')

    pg_pool = ConnectionPool[Endpoint, Connection](
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

    try:
        # connection is opened
        with pg_pool.connection(endpoint='master') as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM pg_stats;")
            print(cur.fetchone())

        # connection is opened
        with pg_pool.connection(endpoint='replica-1') as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM pg_stats;")
            print(cur.fetchone())

        # connection is reused
        with pg_pool.connection(endpoint='master') as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM pg_stats;")
            print(cur.fetchone())

    finally:
        pg_pool.close()


See `documentation <https://generic-connection-pool.readthedocs.io/en/latest/>`_ for more details.
