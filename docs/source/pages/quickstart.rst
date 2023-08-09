.. _quickstart:


Quickstart
~~~~~~~~~~

Runtime
_______

``generic-connection-pool`` supports synchronous and asynchronous connection pools. To instantiate one
create :py:class:`generic_connection_pool.threading.ConnectionPool` for threading runtime or
:py:class:`generic_connection_pool.asyncio.ConnectionPool` for asynchronous runtime.


Connection manager
__________________

Connection pool accepts connection manager as a parameter. It define connection lifecycle
(how to create, dispose a connection or check its aliveness).
For more information see :py:class:`generic_connection_pool.threading.BaseConnectionManager`
or :py:class:`generic_connection_pool.asyncio.BaseConnectionManager`.

You can use one of the :ref:`predefined <pages/api:Contrib>` connection managers or implement your own one.
How to implement custom connection manager :ref:`see <pages/quickstart:Custom connection manager>`


Connection acquiring
____________________

After the pool is instantiated a connection can be acquired using :py:meth:`generic_connection_pool.threading.ConnectionPool.acquire`
or released using :py:meth:`generic_connection_pool.threading.ConnectionPool.release`.
To get rid of boilerplate code connection manager supports automated acquiring using context manager
:py:meth:`generic_connection_pool.threading.ConnectionPool.connection`.


TCP connection pool
___________________

The following example illustrate how to use synchronous tcp connection pool.

.. literalinclude:: ../../../examples/tcp_pool.py
  :language: python


SSL connection pool
___________________

The library also provide ssl based connection manager.
The following example illustrate how to create ssl connection pool.

.. literalinclude:: ../../../examples/ssl_pool.py
  :language: python

Connection aliveness checks
___________________________

Connection manager provides an api for connection aliveness checks.
To implement that override method :py:meth:`generic_connection_pool.threading.BaseConnectionManager.check_aliveness`.
The method must return ``True`` if connection is alive and ``False`` otherwise.


Asynchronous connection pool
____________________________

Asynchronous connection pool api looks pretty much the same:

.. literalinclude:: ../../../examples/async_pool.py
  :language: python


DB connection pool
__________________

Connection pool can manage any connection including database ones. The library provides
connection manager for postgres :py:class:`generic_connection_pool.contrib.psycopg2.DbConnectionManager`.

.. literalinclude:: ../../../examples/pg_pool.py
  :language: python


Custom connection manager
_________________________

To implement a custom connection manager you must override two methods:
:py:meth:`generic_connection_pool.threading.BaseConnectionManager.create` and
:py:meth:`generic_connection_pool.threading.BaseConnectionManager.dispose`.
The other methods are optional.

The following example illustrate how to implement custom connection manager for redis.

.. literalinclude:: ../../../examples/custom_manager.py
  :language: python
