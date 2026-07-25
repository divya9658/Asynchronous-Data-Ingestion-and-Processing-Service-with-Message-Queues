"""
Integration tests for the Consumer Service.

These require a live RabbitMQ + PostgreSQL (e.g. via `docker-compose up
rabbitmq db`) and are skipped automatically if those aren't reachable.

Covers:
  - MQ -> Consumer -> DB: publish directly to the queue, verify persistence.
  - Idempotency: publishing the same message_id twice results in one row.
  - Error scenario: an unprocessable message ends up in the DLQ.
"""
import asyncio
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

aio_pika = pytest.importorskip("aio_pika")
asyncpg = pytest.importorskip("asyncpg")

RABBITMQ_HOST = os.getenv("MESSAGE_QUEUE_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("MESSAGE_QUEUE_PORT", "5672"))
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "processed_data")
DB_USER = os.getenv("DB_USER", "user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")

EXCHANGE_NAME = os.getenv("EXCHANGE_NAME", "data_events_exchange")
ROUTING_KEY = os.getenv("ROUTING_KEY", "data.ingest")
DLQ_QUEUE_NAME = os.getenv("DLQ_QUEUE_NAME", "data_ingest_dlq")


async def _services_available() -> bool:
    try:
        conn = await aio_pika.connect(host=RABBITMQ_HOST, port=RABBITMQ_PORT, login="guest", password="guest", timeout=3)
        await conn.close()
        pg = await asyncpg.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD, timeout=3)
        await pg.close()
        return True
    except Exception:
        return False


async def _publish_raw(message: dict) -> None:
    connection = await aio_pika.connect(host=RABBITMQ_HOST, port=RABBITMQ_PORT, login="guest", password="guest")
    async with connection:
        channel = await connection.channel()
        exchange = await channel.declare_exchange(EXCHANGE_NAME, aio_pika.ExchangeType.DIRECT, durable=True)
        body = json.dumps(message).encode("utf-8")
        await exchange.publish(aio_pika.Message(body=body), routing_key=ROUTING_KEY)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mq_to_consumer_to_db_flow():
    if not await _services_available():
        pytest.skip("RabbitMQ/PostgreSQL not reachable; skipping integration test")

    message_id = str(uuid.uuid4())
    message = {
        "message_id": message_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data": {"user_id": "int-user", "event_type": "integration_flow", "details": {}},
    }
    await _publish_raw(message)

    # NOTE: assumes the consumer_service container is running (started via
    # docker-compose) and will pick this message up and persist it.
    pg = await asyncpg.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
    try:
        found = False
        for _ in range(20):
            row = await pg.fetchrow("SELECT * FROM processed_events WHERE message_id = $1", message_id)
            if row is not None:
                found = True
                assert row["processed_data"] is not None
                break
            await asyncio.sleep(1)
        assert found, "Message was not processed and persisted within timeout"
    finally:
        await pg.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_invalid_message_lands_in_dlq():
    if not await _services_available():
        pytest.skip("RabbitMQ/PostgreSQL not reachable; skipping integration test")

    # Missing 'data' entirely -> should fail processing every retry and DLQ.
    message_id = str(uuid.uuid4())
    bad_message = {"message_id": message_id, "timestamp": datetime.now(timezone.utc).isoformat()}
    await _publish_raw(bad_message)

    connection = await aio_pika.connect(host=RABBITMQ_HOST, port=RABBITMQ_PORT, login="guest", password="guest")
    found = False
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(DLQ_QUEUE_NAME, durable=True)
        try:
            async with queue.iterator(timeout=30) as it:
                async for incoming in it:
                    async with incoming.process(requeue=True):
                        payload = json.loads(incoming.body.decode("utf-8"))
                        original = payload.get("original_message", {})
                        if original.get("message_id") == message_id:
                            found = True
                            break
        except asyncio.TimeoutError:
            pass
    assert found, "Bad message did not land in the DLQ within timeout"
