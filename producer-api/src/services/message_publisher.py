"""
Handles the connection to RabbitMQ and publishing of messages.

Uses aio-pika for fully asynchronous, non-blocking publishing so the
Producer API never performs blocking I/O on the request path.
"""
import json
import logging
import os
from typing import Optional

import aio_pika
from aio_pika import ExchangeType, Message, DeliveryMode
from aio_pika.abc import AbstractChannel, AbstractConnection, AbstractExchange

logger = logging.getLogger("producer_api.message_publisher")

MESSAGE_QUEUE_HOST = os.getenv("MESSAGE_QUEUE_HOST", "localhost")
MESSAGE_QUEUE_PORT = int(os.getenv("MESSAGE_QUEUE_PORT", "5672"))
MESSAGE_QUEUE_USER = os.getenv("MESSAGE_QUEUE_USER", "guest")
MESSAGE_QUEUE_PASSWORD = os.getenv("MESSAGE_QUEUE_PASSWORD", "guest")
EXCHANGE_NAME = os.getenv("EXCHANGE_NAME", "data_events_exchange")
ROUTING_KEY = os.getenv("ROUTING_KEY", "data.ingest")
QUEUE_NAME = os.getenv("QUEUE_NAME", "data_ingest_queue")
DLQ_EXCHANGE_NAME = os.getenv("DLQ_EXCHANGE_NAME", "data_events_dlx")
DLQ_QUEUE_NAME = os.getenv("DLQ_QUEUE_NAME", "data_ingest_dlq")


class MessagePublisher:
    """A small wrapper that lazily creates and reuses a RabbitMQ connection."""

    def __init__(self) -> None:
        self._connection: Optional[AbstractConnection] = None
        self._channel: Optional[AbstractChannel] = None
        self._exchange: Optional[AbstractExchange] = None

    async def connect(self) -> None:
        if self._connection and not self._connection.is_closed:
            return

        logger.info(
            "Connecting to RabbitMQ",
            extra={"host": MESSAGE_QUEUE_HOST, "port": MESSAGE_QUEUE_PORT},
        )
        self._connection = await aio_pika.connect_robust(
            host=MESSAGE_QUEUE_HOST,
            port=MESSAGE_QUEUE_PORT,
            login=MESSAGE_QUEUE_USER,
            password=MESSAGE_QUEUE_PASSWORD,
        )
        self._channel = await self._connection.channel(publisher_confirms=True)

        # Main exchange + queue (durable) that the consumer subscribes to.
        self._exchange = await self._channel.declare_exchange(
            EXCHANGE_NAME, ExchangeType.DIRECT, durable=True
        )

        # Dead-letter exchange + queue, declared here too so producer-only
        # deployments (and tests) can inspect/publish to it if needed.
        dlx = await self._channel.declare_exchange(
            DLQ_EXCHANGE_NAME, ExchangeType.DIRECT, durable=True
        )
        dlq = await self._channel.declare_queue(DLQ_QUEUE_NAME, durable=True)
        await dlq.bind(dlx, routing_key=DLQ_QUEUE_NAME)

        # Main queue is configured to dead-letter into the DLX when messages
        # are rejected/nacked without requeue by the consumer.
        main_queue = await self._channel.declare_queue(
            QUEUE_NAME,
            durable=True,
            arguments={
                "x-dead-letter-exchange": DLQ_EXCHANGE_NAME,
                "x-dead-letter-routing-key": DLQ_QUEUE_NAME,
            },
        )
        await main_queue.bind(self._exchange, routing_key=ROUTING_KEY)

        logger.info("RabbitMQ connection and topology established")

    async def close(self) -> None:
        if self._connection and not self._connection.is_closed:
            await self._connection.close()
            logger.info("RabbitMQ connection closed")

    async def publish_message(self, message: dict) -> None:
        """Publish a JSON-serializable message to the main exchange."""
        if self._exchange is None:
            await self.connect()

        body = json.dumps(message).encode("utf-8")
        amqp_message = Message(
            body=body,
            delivery_mode=DeliveryMode.PERSISTENT,
            content_type="application/json",
            message_id=message.get("message_id"),
        )
        assert self._exchange is not None
        await self._exchange.publish(amqp_message, routing_key=ROUTING_KEY)
        logger.info(
            "Published message",
            extra={"message_id": message.get("message_id"), "routing_key": ROUTING_KEY},
        )


# Module-level singleton used by the FastAPI app.
publisher = MessagePublisher()


async def publish_message(message: dict) -> None:
    await publisher.publish_message(message)
