"""
Database repository for the Consumer Service.

Handles the schema creation, idempotent inserts of processed events, and
retrieval helpers. Uses asyncpg for fully async PostgreSQL access.
"""
import json
import logging
import os
from typing import Any, Dict, Optional

import asyncpg

logger = logging.getLogger("consumer_service.db_repository")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "processed_data")
DB_USER = os.getenv("DB_USER", "user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS processed_events (
    id SERIAL PRIMARY KEY,
    message_id UUID UNIQUE NOT NULL,
    original_timestamp TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL,
    event_type TEXT NOT NULL,
    processed_data JSONB NOT NULL
);
"""

INSERT_SQL = """
INSERT INTO processed_events (message_id, original_timestamp, processed_at, event_type, processed_data)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (message_id) DO NOTHING
RETURNING id;
"""

EXISTS_SQL = "SELECT 1 FROM processed_events WHERE message_id = $1;"

LIST_SQL = "SELECT message_id, original_timestamp, processed_at, event_type, processed_data FROM processed_events ORDER BY id DESC LIMIT $1;"


class DBRepository:
    def __init__(self) -> None:
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        if self._pool is not None:
            return
        logger.info("Connecting to database", extra={"host": DB_HOST, "db": DB_NAME})
        self._pool = await asyncpg.create_pool(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            min_size=1,
            max_size=5,
        )
        async with self._pool.acquire() as conn:
            await conn.execute(CREATE_TABLE_SQL)
        logger.info("Database connected and schema ensured")

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def message_exists(self, message_id: str) -> bool:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(EXISTS_SQL, message_id)
            return row is not None

    async def save_processed_data(
        self,
        message_id: str,
        original_timestamp: str,
        processed_at: str,
        event_type: str,
        processed_data: Dict[str, Any],
    ) -> bool:
        """
        Idempotently insert a processed event.

        Returns True if a new row was inserted, False if the message_id
        already existed (i.e. this was a duplicate delivery).
        """
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    INSERT_SQL,
                    message_id,
                    original_timestamp,
                    processed_at,
                    event_type,
                    json.dumps(processed_data),
                )
        inserted = row is not None
        if not inserted:
            logger.warning(
                "Duplicate message skipped (idempotency)",
                extra={"message_id": message_id},
            )
        return inserted

    async def list_recent(self, limit: int = 50):
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(LIST_SQL, limit)
            return [dict(row) for row in rows]


repository = DBRepository()
