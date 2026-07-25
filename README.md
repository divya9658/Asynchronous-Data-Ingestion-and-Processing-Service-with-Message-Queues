# Asynchronous Data Ingestion and Processing Service

A decoupled, event-driven backend system that ingests data via a REST API,
publishes it to RabbitMQ, and processes it asynchronously in a separate
Consumer Service with idempotency, retries with exponential backoff, and a
Dead-Letter Queue (DLQ) for unprocessable messages.

## 1. Project Overview

| Component | Responsibility |
|---|---|
| **Producer API** (`producer-api/`, FastAPI) | Validates incoming JSON, publishes an event to RabbitMQ, returns `202 Accepted` immediately (non-blocking). |
| **Consumer Service** (`consumer-service/`, FastAPI + background listener) | Consumes events from RabbitMQ, transforms the data, persists it idempotently to PostgreSQL, retries transient failures with exponential backoff, and routes permanent failures to a DLQ. It also exposes `GET /api/dead-letters` to inspect failed messages. |
| **RabbitMQ** | Message broker decoupling the Producer from the Consumer. |
| **PostgreSQL** | Durable storage for processed events. |

Stack: **Python 3.11**, **FastAPI**, **aio-pika** (async RabbitMQ client), **asyncpg**, **tenacity** (retry/backoff), **Docker Compose**.

## 2. Architecture

```
                 HTTP POST                         AMQP publish
   Client  ───────────────────▶  Producer API  ───────────────────▶  RabbitMQ
                 202 Accepted        (FastAPI)      exchange:            │  exchange:
                                                     data_events_exchange │  data_events_exchange
                                                     routing key:         │  queue: data_ingest_queue
                                                     data.ingest          ▼
                                                                   Consumer Service
                                                                  (background listener)
                                                                          │
                                                     3x retry, exp. backoff (1s,2s,4s)
                                                                          │
                                                       success ──────────▶ PostgreSQL
                                                                          │  table: processed_events
                                                       exhausted ────────▶ Dead-Letter Queue
                                                                            (data_ingest_dlq)
                                                                                  ▲
                                                                                  │
                                                          GET /api/dead-letters (peek, non-destructive)
```

**Event-driven decoupling:** the Producer never talks to the Consumer or the
database directly — it only knows about the message queue. This means the
Producer stays fast and available even if the Consumer or database is
temporarily down (messages simply queue up in RabbitMQ), and either side can
be scaled independently.

**Queue topology (RabbitMQ, point-to-point via a direct exchange):**
- `data_events_exchange` (direct) → `data_ingest_queue`, routing key `data.ingest`
- `data_ingest_queue` is configured with `x-dead-letter-exchange` pointing at `data_events_dlx`
- `data_events_dlx` (direct) → `data_ingest_dlq`, routing key `data_ingest_dlq`

A direct exchange with a single bound queue gives simple point-to-point
delivery, which is sufficient here since only one Consumer Service consumes
ingestion events. Multiple consumer instances can still be run concurrently
against the same queue for horizontal scaling — RabbitMQ round-robins
deliveries across all connected consumers.

## 3. Setup Instructions

### Prerequisites
- Docker and Docker Compose installed.

### One-command startup
```bash
git clone <this-repo-url>
cd my-async-service
docker-compose up --build
```

This brings up, in order (via `depends_on` + healthchecks):
1. `rabbitmq` (management UI at http://localhost:15672, guest/guest)
2. `db` (PostgreSQL, exposed on `localhost:5432`)
3. `producer_api` (http://localhost:8000, waits for RabbitMQ to be healthy)
4. `consumer_service` (http://localhost:8001, waits for RabbitMQ **and** DB to be healthy)

All services expose a `/health` endpoint that Docker Compose's healthchecks
poll, so `docker-compose up` will only report services as healthy once
they're actually ready to accept traffic — there's no manual "wait for it to
boot" step.

> **Note on screenshots:** this repository was authored and fully unit
> tested in a sandboxed environment without local Docker access, so a
> screenshot of `docker-compose up` isn't included here. All code has been
> verified with `python -m py_compile`, dependency installation, and a full
> passing unit test suite (21/21 tests, see §6). Please run
> `docker-compose up --build` locally to see the stack come up — every
> service's `/health` endpoint should return `{"status": "ok"}` and RabbitMQ's
> management UI (http://localhost:15672) will show the `data_ingest_queue`
> and `data_ingest_dlq` queues once traffic has flowed.

### Environment variables
Each service has a `.env.example` (`producer-api/.env.example`,
`consumer-service/.env.example`) documenting all configuration knobs
(MQ host/port/credentials, exchange/queue names, DB connection, retry count,
and backoff base). `docker-compose.yml` loads these via `env_file` and
overrides the inter-service hostnames (`rabbitmq`, `db`) so the stack works
out of the box. Copy them to `.env` and adjust if you want to run components
outside of Docker Compose.

## 4. API Documentation

### Producer API — `POST /api/data/ingest`
Ingests a single event and publishes it to RabbitMQ.

**Request body:**
```json
{
  "user_id": "abc123",
  "event_type": "page_view",
  "details": { "path": "/home" }
}
```
`user_id` and `event_type` are required non-blank strings; `details` is an
optional free-form object. Extra fields are allowed and passed through.

**Responses:**
- `202 Accepted` — message published successfully:
  ```json
  { "status": "accepted", "message_id": "9c4b1e2a-..." }
  ```
- `422 Unprocessable Entity` — payload failed schema validation.
- `500 Internal Server Error` — publish to RabbitMQ failed.

**curl example:**
```bash
curl -X POST http://localhost:8000/api/data/ingest \
  -H "Content-Type: application/json" \
  -d '{"user_id": "abc123", "event_type": "page_view", "details": {"path": "/home"}}'
```

### Producer API — `GET /health`
Returns `{"status": "ok"}`. Used by the Docker healthcheck.

### Consumer Service — `GET /api/dead-letters?limit=10`
Peeks (non-destructively — messages are requeued after inspection) up to
`limit` (default 10, max 100) messages currently sitting in the DLQ.

**Response:**
```json
[
  {
    "original_message": {
      "message_id": "...",
      "timestamp": "...",
      "data": { "...": "..." }
    },
    "error": "Transformation failed for ...: ...",
    "failed_at": "2026-07-24T12:00:00+00:00"
  }
]
```

### Consumer Service — `GET /health`
Returns `{"status": "ok"}`.

### Verifying functionality end-to-end
```bash
# 1. Send a valid event
curl -X POST http://localhost:8000/api/data/ingest \
  -H "Content-Type: application/json" \
  -d '{"user_id": "abc123", "event_type": "page_view", "details": {"path": "/home"}}'

# 2. Check it was persisted (uppercased user_id, processed_at added)
docker-compose exec db psql -U user -d processed_data \
  -c "SELECT message_id, event_type, processed_data FROM processed_events ORDER BY id DESC LIMIT 5;"

# 3. Send a malformed event directly to the queue to see DLQ behavior,
#    or send a payload missing required fields to see 422 from the Producer.
curl -X POST http://localhost:8000/api/data/ingest \
  -H "Content-Type: application/json" -d '{"event_type": "missing_user_id"}'
# -> 422 Unprocessable Entity

# 4. Inspect the Dead-Letter Queue
curl http://localhost:8001/api/dead-letters
```

## 5. Design Choices

**Idempotency.** `processed_events.message_id` has a `UNIQUE` constraint, and
inserts use `INSERT ... ON CONFLICT (message_id) DO NOTHING`. This makes
re-processing a duplicate delivery (e.g. after a retry or an at-least-once
redelivery from RabbitMQ) a safe no-op instead of a duplicate row or an
error — the whole operation happens inside one DB transaction so the
check-and-insert is atomic.

**Retries with exponential backoff.** `consumer-service/src/utils/retry_logic.py`
wraps message processing with `tenacity.AsyncRetrying`: up to `MAX_RETRIES`
(default 3) attempts with delays of `1s, 2s, 4s` (configurable via
`BACKOFF_BASE_SECONDS`). This absorbs transient failures (e.g. a momentary
DB connection blip) without giving up immediately.

**Dead-Letter Queue.** When all retry attempts are exhausted, the original
message plus the error message and a `failed_at` timestamp are published to
`data_ingest_dlq`. The original message is still acknowledged off the main
queue so it doesn't block the queue or get redelivered forever — it's
preserved, inspectable, and re-drivable from the DLQ instead. The
`data_ingest_queue` is also configured with RabbitMQ's native
`x-dead-letter-exchange` so that a nacked/rejected message would be routed
the same way even outside of the application-level retry path (defense in
depth).

**Non-blocking Producer.** The Producer only performs one asynchronous
network call — publishing to RabbitMQ — before returning `202 Accepted`. It
never touches the database or waits on the Consumer, keeping ingestion fast
and available under load.

## 6. Running Tests

Both services separate **unit tests** (fully mocked, no external services
needed) from **integration tests** (require live RabbitMQ/PostgreSQL,
auto-skip otherwise via `pytest.importorskip`/connectivity checks).

### Producer API
```bash
cd producer-api
pip install -r requirements.txt
pytest tests/unit -v                     # unit tests, no services needed
pytest tests/integration -v -m integration   # requires `docker-compose up rabbitmq`
```
Current status: **7/7 unit tests passing** (schema validation + publisher,
mocked RabbitMQ channel).

### Consumer Service
```bash
cd consumer-service
pip install -r requirements.txt
pytest tests/unit -v                     # unit tests, no services needed
pytest tests/integration -v -m integration   # requires `docker-compose up rabbitmq db`
```
Current status: **14/14 unit tests passing** (transformation logic,
idempotent persistence, retry/backoff/DLQ behavior, DB repository, all with
mocked RabbitMQ/PostgreSQL clients).

## 7. Common Operational Notes
- Scale consumers horizontally with `docker-compose up --scale consumer_service=3` — RabbitMQ will round-robin deliveries across all instances of the same queue.
- RabbitMQ management UI: http://localhost:15672 (guest/guest) — useful for watching queue depth and DLQ contents live.
- All services log structured JSON to stdout (via `python-json-logger`), making them easy to ship to a log aggregator.
