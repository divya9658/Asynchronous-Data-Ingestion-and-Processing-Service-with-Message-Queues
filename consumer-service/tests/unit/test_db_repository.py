import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from services.db_repository import DBRepository  # noqa: E402


class _FakeConnection:
    def __init__(self, fetchrow_return=None):
        self._fetchrow_return = fetchrow_return
        self.executed = []

    async def execute(self, *args, **kwargs):
        self.executed.append(args)

    async def fetchrow(self, *args, **kwargs):
        return self._fetchrow_return

    async def fetch(self, *args, **kwargs):
        return []

    def transaction(self):
        return _FakeTransaction()


class _FakeTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _FakePoolAcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class _FakePool:
    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        return _FakePoolAcquireCtx(self._conn)


@pytest.mark.asyncio
async def test_save_processed_data_returns_true_when_inserted():
    fake_conn = _FakeConnection(fetchrow_return={"id": 1})
    repo = DBRepository()
    repo._pool = _FakePool(fake_conn)

    inserted = await repo.save_processed_data(
        message_id="id-1",
        original_timestamp="2026-01-01T00:00:00Z",
        processed_at="2026-01-01T00:00:01Z",
        event_type="page_view",
        processed_data={"user_id": "ABC"},
    )
    assert inserted is True


@pytest.mark.asyncio
async def test_save_processed_data_returns_false_on_conflict():
    fake_conn = _FakeConnection(fetchrow_return=None)  # ON CONFLICT DO NOTHING -> no row
    repo = DBRepository()
    repo._pool = _FakePool(fake_conn)

    inserted = await repo.save_processed_data(
        message_id="id-1",
        original_timestamp="2026-01-01T00:00:00Z",
        processed_at="2026-01-01T00:00:01Z",
        event_type="page_view",
        processed_data={"user_id": "ABC"},
    )
    assert inserted is False


@pytest.mark.asyncio
async def test_message_exists_true_when_row_found():
    fake_conn = _FakeConnection(fetchrow_return={"1": 1})
    repo = DBRepository()
    repo._pool = _FakePool(fake_conn)

    exists = await repo.message_exists("id-1")
    assert exists is True


@pytest.mark.asyncio
async def test_message_exists_false_when_no_row():
    fake_conn = _FakeConnection(fetchrow_return=None)
    repo = DBRepository()
    repo._pool = _FakePool(fake_conn)

    exists = await repo.message_exists("id-1")
    assert exists is False
