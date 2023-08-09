"""
Postgres connection manager implementation.
"""

from typing import Mapping, Optional

import psycopg2.extensions

from generic_connection_pool.threading import BaseConnectionManager

DbEndpoint = str
Connection = psycopg2.extensions.connection
DsnParameters = Mapping[DbEndpoint, Mapping[str, str]]


class DbConnectionManager(BaseConnectionManager[DbEndpoint, Connection]):
    """
    Psycopg2 based postgres connection manager.

    :param dsn_params: databases dsn parameters
    """

    def __init__(self, dsn_params: DsnParameters):
        self._dsn_params = dsn_params

    def create(self, endpoint: DbEndpoint, timeout: Optional[float] = None) -> Connection:
        return psycopg2.connect(**self._dsn_params[endpoint])  # type: ignore[call-overload]

    def dispose(self, endpoint: DbEndpoint, conn: Connection, timeout: Optional[float] = None) -> None:
        conn.close()

    def check_aliveness(self, endpoint: DbEndpoint, conn: Connection, timeout: Optional[float] = None) -> bool:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
        except (psycopg2.Error, OSError):
            return False

        return True

    def on_acquire(self, endpoint: DbEndpoint, conn: Connection) -> None:
        self._rollback_uncommitted(conn)

    def on_release(self, endpoint: DbEndpoint, conn: Connection) -> None:
        self._rollback_uncommitted(conn)

    def _rollback_uncommitted(self, conn: Connection) -> None:
        if conn.status == psycopg2.extensions.STATUS_IN_TRANSACTION:
            conn.rollback()
