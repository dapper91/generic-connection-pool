.. _quickstart:


Quickstart
~~~~~~~~~~

Runtime
_______

``generic-connection-pool`` supports synchronous and asynchronous connection pools. To instantiate one
create :py:class:`~generic_connection_pool.threading.ConnectionPool` for threading runtime or
:py:class:`~generic_connection_pool.asyncio.ConnectionPool` for asynchronous runtime:

.. code-block:: python

    import socket
    from ipaddress import IPv4Address
    from typing import Tuple

    from generic_connection_pool.contrib.socket import TcpSocketConnectionManager
    from generic_connection_pool.threading import ConnectionPool

    Port = int
    Endpoint = Tuple[IPv4Address, Port]
    Connection = socket.socket

    pool = ConnectionPool[Endpoint, Connection](
        TcpSocketConnectionManager(),
        idle_timeout=30.0,
        max_lifetime=600.0,
        min_idle=3,
        max_size=20,
        total_max_size=100,
        background_collector=True,
    )


Connection manager
__________________

Connection pool implements logic common for any connection. Logic specific to a particular
connection type is implemented by a connection manager. It defines connection lifecycle
(how to create, dispose a connection or check its aliveness).
For more information see :py:class:`~generic_connection_pool.threading.BaseConnectionManager`
or :py:class:`~generic_connection_pool.asyncio.BaseConnectionManager`:

.. code-block:: python

    IpAddress = IPv4Address
    Port = int
    TcpEndpoint = Tuple[IpAddress, Port]

    class TcpSocketConnectionManager(BaseConnectionManager[TcpEndpoint, socket.socket]):
        def create(self, endpoint: TcpEndpoint, timeout: Optional[float] = None) -> socket.socket:
            addr, port = endpoint

            sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
            sock.connect((str(addr), port))

            return sock

        def dispose(self, endpoint: TcpEndpoint, conn: socket.socket, timeout: Optional[float] = None) -> None:
            conn.shutdown(socket.SHUT_RDWR)
            conn.close()


You can use one of the :ref:`predefined <pages/api:Contrib>` connection managers or implement your own one.
How to implement custom connection manager :ref:`see <pages/quickstart:Custom connection manager>`


Connection acquiring
____________________

After the pool is instantiated a connection can be acquired using :py:meth:`~generic_connection_pool.threading.ConnectionPool.acquire`
and released using :py:meth:`~generic_connection_pool.threading.ConnectionPool.release`.
To get rid of boilerplate code the connection manager supports automated acquiring using
:py:meth:`~generic_connection_pool.threading.ConnectionPool.connection` returning a context manager:


.. code-block:: python

    with pool.connection(endpoint=(addr, port), timeout=5.0) as sock:
        sock.sendall(...)
        response = sock.recv(...)


Connection aliveness checks
___________________________

Connection manager provides an api for connection aliveness checks.
To implement that override method :py:meth:`~generic_connection_pool.threading.BaseConnectionManager.check_aliveness`.
The method must return ``True`` if connection is alive and ``False`` otherwise:

.. code-block:: python

    DbEndpoint = str
    Connection = psycopg2.extensions.connection

    class DbConnectionManager(BaseConnectionManager[DbEndpoint, Connection]):
        ...

        def check_aliveness(self, endpoint: DbEndpoint, conn: Connection, timeout: Optional[float] = None) -> bool:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
                    cur.fetchone()
            except (psycopg2.Error, OSError):
                return False

            return True



Custom connection manager
_________________________

To implement a custom connection manager you must override two methods:
:py:meth:`~generic_connection_pool.threading.BaseConnectionManager.create` and
:py:meth:`~generic_connection_pool.threading.BaseConnectionManager.dispose`.
The other methods are optional.

The following example illustrate how to implement a custom connection manager for redis.

.. literalinclude:: ../../../examples/custom_manager.py
  :language: python


Connection manager allows to define methods to be called on connection acquire, release or when
a connection determined to be dead. That helps to log pool actions or collect metrics.
The following examples illustrate how to collect pool metrics and export them to prometheus.

.. literalinclude:: ../../../examples/manager_hooks.py
  :language: python


Examples
________


TCP connection pool
...................

The following example illustrate how to use synchronous tcp connection pool.

.. literalinclude:: ../../../examples/tcp_pool.py
  :language: python


SSL connection pool
...................

The library also provide ssl based connection manager.
The following example illustrate how to create ssl connection pool.

.. literalinclude:: ../../../examples/ssl_pool.py
  :language: python


Asynchronous connection pool
............................

Asynchronous connection pool api looks pretty much the same:

.. literalinclude:: ../../../examples/async_pool.py
  :language: python


DB connection pool
..................

Connection pool can manage any connection including database ones. The library provides
connection manager for postgres :py:class:`~generic_connection_pool.contrib.psycopg2.DbConnectionManager`.

.. literalinclude:: ../../../examples/pg_pool.py
  :language: python
