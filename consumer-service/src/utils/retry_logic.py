"""
Retry wrapper with exponential backoff for message processing, and helper
to publish a message (with error context) to the Dead-Letter Queue once
retries are exhausted.
"""
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict

from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)

from services.data_processor import ProcessingError

logger = logging.getLogger("consumer_service.retry_logic")

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
BACKOFF_BASE_SECONDS = float(os.getenv("BACKOFF_BASE_SECONDS", "1"))

DLQ_EXCHANGE_NAME = os.getenv("DLQ_EXCHANGE_NAME", "data_events_dlx")
DLQ_QUEUE_NAME = os.getenv("DLQ_QUEUE_NAME", "data_ingest_dlq")


class RetriesExhaustedError(Exception):
    """Raised (internally) once all retry attempts have failed."""

    def __init__(self, original_error: Exception):
        self.original_error = original_error
        super().__init__(str(original_error))


async def process_with_retry(
    message: Dict[str, Any],
    processor: Callable[[Dict[str, Any]], Awaitable[bool]],
    publish_to_dlq: Callable[[Dict[str, Any], str], Awaitable[None]],
) -> bool:
    """
    Run `processor(message)` with retries (exponential backoff: 1s, 2s, 4s
    by default). If all attempts fail, publish the message + error details
    to the DLQ via `publish_to_dlq` and swallow the exception (the message
    should still be acknowledged off the main queue).
    """
    last_error: Exception | None = None
    try:
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(MAX_RETRIES),
            wait=wait_exponential(multiplier=BACKOFF_BASE_SECONDS, min=BACKOFF_BASE_SECONDS),
            retry=retry_if_exception_type(Exception),
            reraise=True,
        ):
            with attempt:
                return await processor(message)
    except Exception as exc:  # noqa: BLE001
        last_error = exc
        logger.error(
            "All retry attempts exhausted; sending to DLQ",
            extra={"message_id": message.get("message_id"), "error": str(exc)},
        )
        await publish_to_dlq(message, str(last_error))
        return False

    return False
