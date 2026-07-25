import logging
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pythonjsonlogger import jsonlogger

from routes.ingest import router as ingest_router
from services.message_publisher import publisher


def configure_logging() -> None:
    handler = logging.StreamHandler(sys.stdout)
    formatter = jsonlogger.JsonFormatter(
        "%(asctime)s %(name)s %(levelname)s %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.handlers = [handler]
    root_logger.setLevel(logging.INFO)


configure_logging()
logger = logging.getLogger("producer_api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Producer API, connecting to message queue")
    await publisher.connect()
    yield
    logger.info("Shutting down Producer API")
    await publisher.close()


app = FastAPI(
    title="Data Ingestion Producer API",
    description="Asynchronously ingests data and publishes it to a message queue",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(ingest_router, prefix="/api")


@app.get("/health")
async def health():
    return {"status": "ok"}
