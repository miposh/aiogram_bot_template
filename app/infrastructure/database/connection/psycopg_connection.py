from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from psycopg import AsyncCursor
from psycopg.rows import DictRow, dict_row
from psycopg_pool import AsyncConnectionPool

from app.infrastructure.database.connection.base import BaseConnection
from app.infrastructure.database.query.results import (
    MultipleQueryResult,
    SingleQueryResult,
)


class PsycopgConnection(BaseConnection):
    """Runs every statement on a connection borrowed from the pool.

    The connection goes back to the pool as soon as the statement completes
    (committed on success, rolled back on error), so handlers never keep a
    connection and an open transaction while awaiting Telegram API calls.
    """

    def __init__(self, pool: AsyncConnectionPool) -> None:
        self._pool = pool

    @asynccontextmanager
    async def _cursor(self) -> AsyncIterator[AsyncCursor[DictRow]]:
        async with self._pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                yield cur

    @staticmethod
    async def _fetch_all_results(cur: AsyncCursor[DictRow]) -> list[DictRow]:
        rows: list[DictRow] = []
        while True:
            rows.extend(await cur.fetchall())
            if not cur.nextset():
                return rows

    async def execute(
        self,
        sql: str,
        params: tuple[Any, ...] | list[tuple[Any, ...]] | None = None,
        connection: Any | None = None,
    ) -> None:
        async with self._cursor() as cur:
            await cur.execute(sql, params)

    async def fetchone(
        self,
        sql: str,
        params: tuple[Any, ...] | list[tuple[Any, ...]] | None = None,
        connection: Any | None = None,
    ) -> SingleQueryResult:
        async with self._cursor() as cur:
            await cur.execute(sql, params)
            return SingleQueryResult(await cur.fetchone())

    async def fetchmany(
        self,
        sql: str,
        params: tuple[Any, ...] | list[tuple[Any, ...]] | None = None,
        connection: Any | None = None,
    ) -> MultipleQueryResult:
        async with self._cursor() as cur:
            await cur.execute(sql, params)
            return MultipleQueryResult(await cur.fetchall())

    async def insert_and_fetchone(
        self,
        sql: str,
        params: tuple[Any, ...],
        connection: Any | None = None,
    ) -> SingleQueryResult:
        return await self.fetchone(sql, params)

    async def insert_and_fetchmany(
        self,
        sql: str,
        params: list[tuple[Any, ...]],
        connection: Any | None = None,
    ) -> MultipleQueryResult:
        async with self._cursor() as cur:
            # Without returning=True psycopg discards RETURNING rows of executemany().
            await cur.executemany(sql, params, returning=True)
            return MultipleQueryResult(await self._fetch_all_results(cur))

    async def update_and_fetchone(
        self,
        sql: str,
        params: tuple[Any, ...],
        connection: Any | None = None,
    ) -> SingleQueryResult:
        return await self.fetchone(sql, params)

    async def update_and_fetchmany(
        self,
        sql: str,
        params: list[tuple[Any, ...]],
        connection: Any | None = None,
    ) -> MultipleQueryResult:
        return await self.insert_and_fetchmany(sql, params)
