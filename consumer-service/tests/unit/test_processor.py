import sys
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from services.data_processor import transform, process_data_message, ProcessingError  # noqa: E402
from utils.retry_logic import process_with_retry  # noqa: E402


# ---------------------------------------------------------------------------
# transform() - pure function tests
# ---------------------------------------------------------------------------
def test_transform_uppercases_user_id_and_adds_processed_at():
    message = {
        "message_id": "id-1",
        "timestamp": "2026-01-01T00:00:00Z",
        "data": {"user_id": "abc", "event_type": "page_view", "details": {}},
    }
    result = transform(message)
    assert result["user_id"] == "ABC"
    assert "processed_at" in result


def test_transform_raises_when_data_missing():
    message = {"message_id": "id-2", "timestamp": "2026-01-01T00:00:00Z"}
    with pytest.raises(ProcessingError):
        transform(message)


def test_transform_raises_when_required_fields_missing():
    message = {"message_id": "id-3", "timestamp": "t", "data": {"details": {}}}
    with pytest.raises(ProcessingError):
        transform(message)


# ---------------------------------------------------------------------------
# process_data_message() - mocking the DB repository
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_process_data_message_saves_new_record():
    message = {
        "message_id": "id-4",
        "timestamp": "2026-01-01T00:00:00Z",
        "data": {"user_id": "abc", "event_type": "page_view", "details": {}},
    }
    with patch("services.data_processor.repository") as mock_repo:
        mock_repo.save_processed_data = AsyncMock(return_value=True)
        inserted = await process_data_message(message)

    assert inserted is True
    mock_repo.save_processed_data.assert_awaited_once()
    _, kwargs = mock_repo.save_processed_data.call_args
    assert kwargs["message_id"] == "id-4"
    assert kwargs["processed_data"]["user_id"] == "ABC"


@pytest.mark.asyncio
async def test_process_data_message_idempotent_duplicate_returns_false():
    message = {
        "message_id": "id-5",
        "timestamp": "2026-01-01T00:00:00Z",
        "data": {"user_id": "abc", "event_type": "page_view", "details": {}},
    }
    with patch("services.data_processor.repository") as mock_repo:
        mock_repo.save_processed_data = AsyncMock(return_value=False)
        inserted = await process_data_message(message)

    assert inserted is False


@pytest.mark.asyncio
async def test_process_data_message_raises_processing_error_when_db_fails():
    message = {
        "message_id": "id-6",
        "timestamp": "2026-01-01T00:00:00Z",
        "data": {"user_id": "abc", "event_type": "page_view", "details": {}},
    }
    with patch("services.data_processor.repository") as mock_repo:
        mock_repo.save_processed_data = AsyncMock(side_effect=RuntimeError("db down"))
        with pytest.raises(ProcessingError):
            await process_data_message(message)


@pytest.mark.asyncio
async def test_process_data_message_missing_message_id_raises():
    with pytest.raises(ProcessingError):
        await process_data_message({"timestamp": "t", "data": {}})


# ---------------------------------------------------------------------------
# retry logic tests
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_process_with_retry_succeeds_first_try():
    processor = AsyncMock(return_value=True)
    dlq_publisher = AsyncMock()

    result = await process_with_retry({"message_id": "ok"}, processor, dlq_publisher)

    assert result is True
    processor.assert_awaited_once()
    dlq_publisher.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_with_retry_eventually_succeeds():
    processor = AsyncMock(side_effect=[RuntimeError("transient"), True])
    dlq_publisher = AsyncMock()

    result = await process_with_retry({"message_id": "flaky"}, processor, dlq_publisher)

    assert result is True
    assert processor.await_count == 2
    dlq_publisher.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_with_retry_sends_to_dlq_after_exhausting_retries():
    processor = AsyncMock(side_effect=RuntimeError("permanent failure"))
    dlq_publisher = AsyncMock()

    result = await process_with_retry({"message_id": "bad"}, processor, dlq_publisher)

    assert result is False
    dlq_publisher.assert_awaited_once()
    args, _ = dlq_publisher.call_args
    assert args[0]["message_id"] == "bad"
    assert "permanent failure" in args[1]
