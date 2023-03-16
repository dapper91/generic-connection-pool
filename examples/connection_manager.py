import socket
from ssl import SSLContext, SSLSocket
from typing import Optional, Tuple

from generic_connection_pool.threding import BaseConnectionManager, ConnectionPool

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

        orig_timeout = sock.gettimeout()
        sock.settimeout(timeout)
        try:
            sock.connect((hostname, port))
        finally:
            sock.settimeout(orig_timeout)

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
