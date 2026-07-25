import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from schemas.data_ingest import DataIngestPayload  # noqa: E402
from services.message_publisher import MessagePublisher  # noqa: E402


# ---------------------------------------------------------------------------
# Schema validation tests
# ---------------------------------------------------------------------------
def test_valid_payload_parses_successfully():
    payload = DataIngestPayload(
        user_id="abc123", event_type="page_view", details={"path": "/home"}
    )
    assert payload.user_id == "abc123"
    assert payload.event_type == "page_view"
    assert payload.details == {"path": "/home"}


def test_payload_defaults_details_to_empty_dict():
    payload = DataIngestPayload(user_id="abc123", event_type="page_view")
    assert payload.details == {}


@pytest.mark.parametrize("field", ["user_id", "event_type"])
def test_missing_required_field_raises(field):
    data = {"user_id": "abc123", "event_type": "page_view", "details": {}}
    del data[field]
    with pytest.raises(Exception):
        DataIngestPayload(**data)


def test_blank_user_id_raises():
    with pytest.raises(Exception):
        DataIngestPayload(user_id="   ", event_type="page_view", details={})


# ---------------------------------------------------------------------------
# Message publisher tests (RabbitMQ interactions mocked)
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_publish_message_calls_exchange_publish():
    mock_exchange = AsyncMock()
    publisher = MessagePublisher()
    publisher._exchange = mock_exchange  # bypass connect()

    message = {"message_id": "id-1", "timestamp": "2026-01-01T00:00:00Z", "data": {}}
    await publisher.publish_message(message)

    mock_exchange.publish.assert_awaited_once()
    args, kwargs = mock_exchange.publish.call_args
    published_message = args[0]
    body = json.loads(published_message.body.decode("utf-8"))
    assert body["message_id"] == "id-1"


@pytest.mark.asyncio
async def test_publish_message_connects_if_not_connected():
    publisher = MessagePublisher()
    publisher.connect = AsyncMock()

    async def fake_connect():
        publisher._exchange = AsyncMock()

    publisher.connect.side_effect = fake_connect

    message = {"message_id": "id-2", "timestamp": "2026-01-01T00:00:00Z", "data": {}}
    await publisher.publish_message(message)

    publisher.connect.assert_awaited_once()
