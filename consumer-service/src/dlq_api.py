"""
A small FastAPI app exposing GET /api/dead-letters to inspect messages
that landed in the Dead-Letter Queue. It reads (peeks) messages from the
DLQ without consuming/acking them by default, so operators can inspect and
later re-drive them manually.
"""
import json
import logging
import os
from typing import List

import aio_pika
from fastapi import FastAPI, Query

logger = logging.getLogger("consumer_service.dlq_api")

MESSAGE_QUEUE_HOST = os.getenv("MESSAGE_QUEUE_HOST", "localhost")
MESSAGE_QUEUE_PORT = int(os.getenv("MESSAGE_QUEUE_PORT", "5672"))
MESSAGE_QUEUE_USER = os.getenv("MESSAGE_QUEUE_USER", "guest")
MESSAGE_QUEUE_PASSWORD = os.getenv("MESSAGE_QUEUE_PASSWORD", "guest")
DLQ_QUEUE_NAME = os.getenv("DLQ_QUEUE_NAME", "data_ingest_dlq")

app = FastAPI(title="Consumer Service - Dead Letter API", version="1.0.0")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/api/dead-letters")
async def get_dead_letters(limit: int = Query(default=10, ge=1, le=100)) -> List[dict]:
    """
    Peek up to `limit` messages currently sitting in the DLQ, without
    removing them (messages are requeued after inspection).
    """
    connection = await aio_pika.connect(
        host=MESSAGE_QUEUE_HOST,
        port=MESSAGE_QUEUE_PORT,
        login=MESSAGE_QUEUE_USER,
        password=MESSAGE_QUEUE_PASSWORD,
    )
    results = []
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=limit)
        queue = await channel.declare_queue(DLQ_QUEUE_NAME, durable=True)

        fetched_messages = []
        for _ in range(limit):
            incoming = await queue.get(fail=False, timeout=2)
            if incoming is None:
                break
            fetched_messages.append(incoming)
            try:
                results.append(json.loads(incoming.body.decode("utf-8")))
            except json.JSONDecodeError:
                results.append({"raw_body": incoming.body.decode("utf-8", "replace")})

        # Requeue (nack) everything we peeked so this endpoint is non-destructive.
        for incoming in fetched_messages:
            await incoming.nack(requeue=True)

    return results
