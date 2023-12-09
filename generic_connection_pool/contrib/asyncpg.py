"""
Postgres asyncpg connection manager implementation.
"""

from typing import Generic, Mapping, Optional, TypeVar

import asyncpg

from generic_connection_pool.asyncio import BaseConnectionManager

DbEndpoint = str
Connection = asyncpg.Connection
DsnParameters = Mapping[DbEndpoint, Mapping[str, str]]

RecordT = TypeVar('RecordT', bound=asyncpg.Record)


class DbConnectionManager(BaseConnectionManager[DbEndpoint, 'Connection[RecordT]'], Generic[RecordT]):
    """
    Psycopg2 based postgres connection manager.

    :param dsn_params: databases dsn parameters
    """

    def __init__(self, dsn_params: DsnParameters):
        self._dsn_params = dsn_params

    async def create(
            self,
            endpoint: DbEndpoint,
            timeout: Optional[float] = None,
    ) -> 'Connection[RecordT]':
        return await asyncpg.connect(**self._dsn_params[endpoint])  # type: ignore[call-overload]

    async def dispose(
            self,
            endpoint: DbEndpoint,
            conn: 'Connection[RecordT]',
            timeout: Optional[float] = None,
    ) -> None:
        await conn.close(timeout=timeout)

    async def check_aliveness(
            self,
            endpoint: DbEndpoint,
            conn: 'Connection[RecordT]',
            timeout: Optional[float] = None,
    ) -> bool:
        return conn.is_closed()

    async def on_acquire(self, endpoint: DbEndpoint, conn: 'Connection[RecordT]') -> None:
        await self._rollback_uncommitted(conn)

    async def on_release(self, endpoint: DbEndpoint, conn: 'Connection[RecordT]') -> None:
        await self._rollback_uncommitted(conn)

    async def _rollback_uncommitted(self, conn: 'Connection[RecordT]') -> None:
        if conn.is_in_transaction():
            await conn.execute('ROLLBACK')
