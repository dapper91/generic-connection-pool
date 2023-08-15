import ssl
import time
from ssl import SSLSocket
from typing import Any, Dict, Tuple

import prometheus_client as prom

from generic_connection_pool.contrib.socket import SslSocketConnectionManager
from generic_connection_pool.threading import ConnectionPool

Hostname = str
Port = int
Endpoint = Tuple[Hostname, Port]

acquire_latency_hist = prom.Histogram('acquire_latency', 'Connections acquire latency', labelnames=['hostname'])
acquire_total = prom.Counter('acquire_total', 'Connections acquire count', labelnames=['hostname'])
dead_conn_total = prom.Counter('dead_conn_total', 'Dead connections count', labelnames=['hostname'])


class ObservableConnectionManager(SslSocketConnectionManager):

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._acquires: Dict[SSLSocket, float] = {}

    def on_acquire(self, endpoint: Endpoint, conn: SSLSocket) -> None:
        hostname, port = endpoint

        acquire_total.labels(hostname).inc()
        self._acquires[conn] = time.time()

    def on_release(self, endpoint: Endpoint, conn: SSLSocket) -> None:
        hostname, port = endpoint

        acquired_at = self._acquires.pop(conn)
        acquire_latency_hist.labels(hostname).observe(time.time() - acquired_at)

    def on_connection_dead(self, endpoint: Endpoint, conn: SSLSocket) -> None:
        hostname, port = endpoint

        dead_conn_total.labels(hostname).inc()


http_pool = ConnectionPool[Endpoint, SSLSocket](
    ObservableConnectionManager(ssl.create_default_context()),
)
