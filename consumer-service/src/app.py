"""
Entry point for the Consumer Service.

Runs two things concurrently inside a single process:
  1. The RabbitMQ message listener (background asyncio task).
  2. A lightweight FastAPI app exposing /health and /api/dead-letters.

This keeps the service simple to containerize (single process, single
port) while still satisfying the requirement of the Consumer Service
being a long-running background process plus the DLQ retrieval API.
"""
import asyncio
import logging
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pythonjsonlogger import jsonlogger

from dlq_api import app as dlq_app
from services.db_repository import repository
from services.message_consumer import start_consuming


def configure_logging() -> None:
    handler = logging.StreamHandler(sys.stdout)
    formatter = jsonlogger.JsonFormatter("%(asctime)s %(name)s %(levelname)s %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.handlers = [handler]
    root_logger.setLevel(logging.INFO)


configure_logging()
logger = logging.getLogger("consumer_service")

_consumer_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _consumer_task
    logger.info("Starting Consumer Service")
    await repository.connect()
    _consumer_task = asyncio.create_task(_run_consumer_forever())
    yield
    logger.info("Shutting down Consumer Service")
    if _consumer_task:
        _consumer_task.cancel()
    await repository.close()


async def _run_consumer_forever() -> None:
    """Wrap start_consuming with a restart loop so transient MQ hiccups
    (e.g. RabbitMQ still starting up) don't kill the consumer permanently."""
    while True:
        try:
            await start_consuming()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.error("Consumer loop crashed, restarting in 5s", extra={"error": str(exc)})
            await asyncio.sleep(5)


app = dlq_app
app.router.lifespan_context = lifespan


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001)
