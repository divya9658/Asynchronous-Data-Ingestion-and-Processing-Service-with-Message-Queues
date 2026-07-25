"""
Business logic that transforms an incoming raw message into the shape that
is persisted to the database, with idempotency enforced at the repository
layer (UNIQUE message_id + ON CONFLICT DO NOTHING).
"""
import logging
from datetime import datetime, timezone
from typing import Any, Dict

from services.db_repository import repository

logger = logging.getLogger("consumer_service.data_processor")


class ProcessingError(Exception):
    """Raised when a message cannot be processed (used to trigger retries)."""


def transform(message: Dict[str, Any]) -> Dict[str, Any]:
    """Pure transformation function -- easy to unit test in isolation."""
    data = message.get("data")
    if not isinstance(data, dict):
        raise ProcessingError(f"Message {message.get('message_id')} has no valid 'data' object")

    if "user_id" not in data or "event_type" not in data:
        raise ProcessingError(
            f"Message {message.get('message_id')} is missing required fields (user_id/event_type)"
        )

    transformed = dict(data)
    transformed["processed_at"] = datetime.now(timezone.utc).isoformat()
    transformed["user_id"] = str(transformed["user_id"]).upper()
    return transformed


async def process_data_message(message: Dict[str, Any]) -> bool:
    """
    Process a single message: transform it and persist idempotently.

    Returns True if a new record was written, False if it was a duplicate
    (already processed) message. Raises ProcessingError on failure so the
    caller's retry/DLQ logic can take over.
    """
    message_id = message.get("message_id")
    if not message_id:
        raise ProcessingError("Message is missing 'message_id'")

    logger.info("Processing message", extra={"message_id": message_id})

    try:
        transformed_data = transform(message)
    except ProcessingError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise ProcessingError(f"Transformation failed for {message_id}: {exc}") from exc

    try:
        inserted = await repository.save_processed_data(
            message_id=message_id,
            original_timestamp=message.get("timestamp"),
            processed_at=transformed_data["processed_at"],
            event_type=message.get("data", {}).get("event_type", "unknown"),
            processed_data=transformed_data,
        )
    except Exception as exc:  # noqa: BLE001
        raise ProcessingError(f"Persistence failed for {message_id}: {exc}") from exc

    if inserted:
        logger.info("Message processed and saved", extra={"message_id": message_id})
    else:
        logger.info("Message already processed (idempotent skip)", extra={"message_id": message_id})

    return inserted
