"""
RabbitMQ listener for the Consumer Service.

Subscribes to the main data-ingest queue, processes each message with
retry + exponential backoff, and routes permanently-failed messages to a
Dead-Letter Queue (DLQ) that includes the original message and error
details for later manual inspection.
"""
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict

import aio_pika
from aio_pika import ExchangeType, Message, DeliveryMode
from aio_pika.abc import AbstractIncomingMessage

from services.data_processor import process_data_message
from services.db_repository import repository
from utils.retry_logic import process_with_retry

logger = logging.getLogger("consumer_service.message_consumer")

MESSAGE_QUEUE_HOST = os.getenv("MESSAGE_QUEUE_HOST", "localhost")
MESSAGE_QUEUE_PORT = int(os.getenv("MESSAGE_QUEUE_PORT", "5672"))
MESSAGE_QUEUE_USER = os.getenv("MESSAGE_QUEUE_USER", "guest")
MESSAGE_QUEUE_PASSWORD = os.getenv("MESSAGE_QUEUE_PASSWORD", "guest")

EXCHANGE_NAME = os.getenv("EXCHANGE_NAME", "data_events_exchange")
ROUTING_KEY = os.getenv("ROUTING_KEY", "data.ingest")
QUEUE_NAME = os.getenv("QUEUE_NAME", "data_ingest_queue")
DLQ_EXCHANGE_NAME = os.getenv("DLQ_EXCHANGE_NAME", "data_events_dlx")
DLQ_QUEUE_NAME = os.getenv("DLQ_QUEUE_NAME", "data_ingest_dlq")

PREFETCH_COUNT = int(os.getenv("PREFETCH_COUNT", "10"))

_dlq_exchange = None  # module-level handle set up during connect


async def _publish_to_dlq(channel: aio_pika.Channel, message: Dict[str, Any], error: str) -> None:
    global _dlq_exchange
    dlq_payload = {
        "original_message": message,
        "error": error,
        "failed_at": datetime.now(timezone.utc).isoformat(),
    }
    body = json.dumps(dlq_payload).encode("utf-8")
    amqp_message = Message(
        body=body,
        delivery_mode=DeliveryMode.PERSISTENT,
        content_type="application/json",
    )
    await _dlq_exchange.publish(amqp_message, routing_key=DLQ_QUEUE_NAME)
    logger.warning(
        "Message routed to DLQ",
        extra={"message_id": message.get("message_id"), "error": error},
    )


async def _on_message(channel: aio_pika.Channel, incoming: AbstractIncomingMessage) -> None:
    async with incoming.process(ignore_processed=True):
        try:
            message = json.loads(incoming.body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            logger.error("Failed to decode message body", extra={"error": str(exc)})
            await _publish_to_dlq(channel, {"raw_body": incoming.body.decode("utf-8", "replace")}, f"JSON decode error: {exc}")
            return

        async def _publish_dlq_wrapper(msg: Dict[str, Any], error: str) -> None:
            await _publish_to_dlq(channel, msg, error)

        await process_with_retry(message, process_data_message, _publish_dlq_wrapper)


async def start_consuming() -> None:
    global _dlq_exchange

    await repository.connect()

    connection = await aio_pika.connect_robust(
        host=MESSAGE_QUEUE_HOST,
        port=MESSAGE_QUEUE_PORT,
        login=MESSAGE_QUEUE_USER,
        password=MESSAGE_QUEUE_PASSWORD,
    )

    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=PREFETCH_COUNT)

        exchange = await channel.declare_exchange(EXCHANGE_NAME, ExchangeType.DIRECT, durable=True)

        _dlq_exchange = await channel.declare_exchange(DLQ_EXCHANGE_NAME, ExchangeType.DIRECT, durable=True)
        dlq = await channel.declare_queue(DLQ_QUEUE_NAME, durable=True)
        await dlq.bind(_dlq_exchange, routing_key=DLQ_QUEUE_NAME)

        queue = await channel.declare_queue(
            QUEUE_NAME,
            durable=True,
            arguments={
                "x-dead-letter-exchange": DLQ_EXCHANGE_NAME,
                "x-dead-letter-routing-key": DLQ_QUEUE_NAME,
            },
        )
        await queue.bind(exchange, routing_key=ROUTING_KEY)

        logger.info("Consumer service listening for messages", extra={"queue": QUEUE_NAME})

        async with queue.iterator() as queue_iter:
            async for incoming in queue_iter:
                await _on_message(channel, incoming)
