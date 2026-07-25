import logging
from datetime import datetime, timezone
from uuid import uuid4

from fastapi import APIRouter, HTTPException

from schemas.data_ingest import DataIngestPayload, IngestResponse
from services.message_publisher import publish_message

logger = logging.getLogger("producer_api.ingest")

router = APIRouter()


@router.post("/data/ingest", status_code=202, response_model=IngestResponse)
async def ingest_data(payload: DataIngestPayload) -> IngestResponse:
    message_id = str(uuid4())
    message = {
        "message_id": message_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data": payload.model_dump(),
    }

    logger.info("Received ingest request", extra={"message_id": message_id})

    try:
        await publish_message(message)
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "Failed to publish message",
            extra={"message_id": message_id, "error": str(exc)},
        )
        raise HTTPException(status_code=500, detail=f"Failed to publish message: {exc}") from exc

    return IngestResponse(status="accepted", message_id=message_id)
