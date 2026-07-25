"""
Integration test for the Producer API.

This test spins up the FastAPI app in-process (using httpx's ASGITransport)
and verifies that a POST to /api/data/ingest results in a message being
published to RabbitMQ. It requires a reachable RabbitMQ instance (e.g. via
`docker-compose up rabbitmq`) and will be skipped automatically otherwise.

Run with: pytest tests/integration -m integration
"""
import json
import os
import sys
from pathlib import Path

import pytest
import pytest_asyncio

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

aio_pika = pytest.importorskip("aio_pika")
httpx = pytest.importorskip("httpx")

RABBITMQ_HOST = os.getenv("MESSAGE_QUEUE_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("MESSAGE_QUEUE_PORT", "5672"))


async def _rabbitmq_available() -> bool:
    try:
        connection = await aio_pika.connect(
            host=RABBITMQ_HOST, port=RABBITMQ_PORT, login="guest", password="guest",
            timeout=3,
        )
        await connection.close()
        return True
    except Exception:
        return False


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ingest_endpoint_publishes_to_queue():
    if not await _rabbitmq_available():
        pytest.skip("RabbitMQ is not reachable; skipping integration test")

    from app import app
    from services.message_publisher import QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        payload = {"user_id": "int-test-user", "event_type": "integration_test", "details": {"k": "v"}}
        response = await client.post("/api/data/ingest", json=payload)

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "accepted"
    message_id = body["message_id"]

    # Now consume directly from the queue to verify the message landed there.
    connection = await aio_pika.connect(
        host=RABBITMQ_HOST, port=RABBITMQ_PORT, login="guest", password="guest"
    )
    async with connection:
        channel = await connection.channel()
        queue = await channel.get_queue(QUEUE_NAME)

        found = False
        async with queue.iterator(timeout=10) as queue_iter:
            async for msg in queue_iter:
                async with msg.process():
                    data = json.loads(msg.body.decode("utf-8"))
                    if data.get("message_id") == message_id:
                        found = True
                        break
        assert found, "Published message was not found in the queue"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ingest_endpoint_rejects_invalid_payload():
    from app import app

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/api/data/ingest", json={"event_type": "missing_user_id"})

    assert response.status_code == 422  # FastAPI/Pydantic validation error
