import socket
from ipaddress import IPv4Address, IPv6Address
from typing import Tuple, Union

from generic_connection_pool.contrib.socket import TcpSocketConnectionManager
from generic_connection_pool.threading import ConnectionPool

Port = int
IpAddress = Union[IPv4Address, IPv6Address]
Endpoint = Tuple[IpAddress, Port]
Connection = socket.socket


redis_pool = ConnectionPool[Endpoint, Connection](
    TcpSocketConnectionManager(),
    idle_timeout=30.0,
    max_lifetime=600.0,
    min_idle=3,
    max_size=20,
    total_max_size=100,
    background_collector=True,
)


def command(addr: IpAddress, port: int, cmd: str) -> None:
    with redis_pool.connection(endpoint=(addr, port), timeout=5.0) as sock:
        sock.sendall(cmd.encode() + b'\n')
        response = sock.recv(1024)

        print(response.decode())


try:
    command(IPv4Address('127.0.0.1'), 6379, 'CLIENT ID')  # tcp connection opened
    command(IPv4Address('127.0.0.1'), 6379, 'INFO')  # tcp connection reused
finally:
    redis_pool.close()
