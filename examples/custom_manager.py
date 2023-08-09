import socket
from ipaddress import IPv4Address
from typing import Optional, Tuple

from generic_connection_pool.threading import BaseConnectionManager, ConnectionPool

Port = int
Endpoint = Tuple[IPv4Address, Port]
Connection = socket.socket


class RedisConnectionManager(BaseConnectionManager[Endpoint, socket.socket]):
    def __init__(self, username: str, password: str):
        self._username = username
        self._password = password

    def create(self, endpoint: Endpoint, timeout: Optional[float] = None) -> socket.socket:
        addr, port = endpoint

        sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
        sock.connect((str(addr), port))

        if (resp := self.cmd(sock, f'AUTH {self._username} {self._password}')) != '+OK':
            raise RuntimeError(f"authentication failed: {resp}")

        return sock

    def cmd(self, sock: socket.socket, cmd: str) -> str:
        sock.sendall(f'{cmd}\r\n'.encode())

        response = sock.recv(1024)
        return response.rstrip(b'\r\n').decode()

    def dispose(self, endpoint: Endpoint, conn: socket.socket, timeout: Optional[float] = None) -> None:
        try:
            conn.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

        conn.close()

    def check_aliveness(self, endpoint: Endpoint, conn: socket.socket, timeout: Optional[float] = None) -> bool:
        try:
            if self.cmd(conn, 'ping') != '+PONG':
                return False
        except OSError:
            return False

        return True


redis_pool = ConnectionPool[Endpoint, Connection](
    RedisConnectionManager('', 'secret'),
    idle_timeout=30.0,
    max_lifetime=600.0,
    min_idle=3,
    max_size=20,
    total_max_size=100,
    background_collector=True,
)


def command(addr: IPv4Address, port: int, cmd: str) -> None:
    with redis_pool.connection(endpoint=(addr, port), timeout=5.0) as sock:
        sock.sendall(cmd.encode() + b'\r\n')
        response = sock.recv(1024)

        print(response.decode())


try:
    command(IPv4Address('127.0.0.1'), 6379, 'CLIENT ID')  # tcp connection is opened
    command(IPv4Address('127.0.0.1'), 6379, 'CLIENT INFO')  # tcp connection is reused
finally:
    redis_pool.close()
