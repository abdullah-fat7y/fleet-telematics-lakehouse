-- Source database of the "fleet company". Every line the ingestion API receives is stored here,
-- exactly as received, together with the result of validation (nothing is dropped).
--
--   telemetry_live     real-time feed from vehicles; captured by Debezium (CDC) into Kafka
--   telemetry_history  bulk uploads (e.g. a vehicle's stored log after being offline);
--                      read hour by hour by the batch extractor
--   ingest_dlq         view of every rejected line in both tables, with the reason
--   api_ingest_log     one row per API request (received / valid / rejected / duration)

CREATE TABLE telemetry_live (
    id             BIGSERIAL PRIMARY KEY,
    raw            TEXT        NOT NULL,              -- the line exactly as received
    vehicle_id     TEXT,                              -- parsed when readable, else NULL
    event_time     TIMESTAMPTZ,                       -- parsed when readable, else NULL
    slice_hour     TIMESTAMPTZ NOT NULL,              -- event hour (neighbouring line's hour if unreadable)
    is_valid       BOOLEAN     NOT NULL,
    reject_reason  TEXT,                              -- e.g. 'missing_field: rpm'
    run_id         TEXT,
    sent_at        TEXT,                              -- when the generator sent the request
    received_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE telemetry_history (LIKE telemetry_live INCLUDING ALL);
CREATE INDEX telemetry_history_slice_hour ON telemetry_history (slice_hour);

-- Debezium needs the old row only for updates/deletes; the tables are insert-only, so DEFAULT is enough.
ALTER TABLE telemetry_live REPLICA IDENTITY DEFAULT;

CREATE VIEW ingest_dlq AS
    SELECT 'live' AS channel, id, reject_reason, raw, run_id, received_at FROM telemetry_live WHERE NOT is_valid
    UNION ALL
    SELECT 'history', id, reject_reason, raw, run_id, received_at FROM telemetry_history WHERE NOT is_valid;

CREATE TABLE api_ingest_log (
    request_id     BIGSERIAL PRIMARY KEY,
    channel        TEXT        NOT NULL,
    run_id         TEXT,
    lines          INT         NOT NULL,
    valid          INT         NOT NULL,
    rejected       INT         NOT NULL,
    duration_ms    DOUBLE PRECISION NOT NULL,
    received_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- Insert-only source tables: the API role may insert and read, never update or delete.
REVOKE UPDATE, DELETE, TRUNCATE ON telemetry_live, telemetry_history FROM PUBLIC;
