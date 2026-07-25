-- Initial schema for the processed_data database.
-- This runs automatically on first container startup (mounted into
-- /docker-entrypoint-initdb.d). The Consumer Service also ensures this
-- table exists at runtime (CREATE TABLE IF NOT EXISTS), so this script is
-- a convenience for immediate visibility/tools and is safe to be redundant.

CREATE TABLE IF NOT EXISTS processed_events (
    id SERIAL PRIMARY KEY,
    message_id UUID UNIQUE NOT NULL,
    original_timestamp TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL,
    event_type TEXT NOT NULL,
    processed_data JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_processed_events_event_type ON processed_events (event_type);
