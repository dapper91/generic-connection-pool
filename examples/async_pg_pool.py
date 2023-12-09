import asyncio

import asyncpg

from generic_connection_pool.asyncio import ConnectionPool
from generic_connection_pool.contrib.asyncpg import DbConnectionManager

Endpoint = str
Connection = asyncpg.Connection


async def main() -> None:
    dsn_params = dict(database='postgres', user='postgres', password='secret')

    pg_pool = ConnectionPool[Endpoint, 'Connection[asyncpg.Record]'](
        DbConnectionManager[asyncpg.Record](
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
        # connection opened
        async with pg_pool.connection(endpoint='master') as conn:
            result = await conn.fetchval("SELECT inet_server_addr()")
            print(result)

        # connection opened
        async with pg_pool.connection(endpoint='replica-1') as conn:
            result = await conn.fetchval("SELECT inet_server_addr()")
            print(result)

        # connection reused
        async with pg_pool.connection(endpoint='master') as conn:
            result = await conn.fetchval("SELECT inet_server_addr()")
            print(result)

    finally:
        await pg_pool.close()

asyncio.run(main())
