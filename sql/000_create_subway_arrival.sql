-- Baseline matching the existing PostgreSQL subway_arrival schema.
-- BIGSERIAL creates the bigint sequence/default and its ownership dependency.
-- Run automatically only on an empty Docker data volume; do not apply to production.
BEGIN;

CREATE TABLE IF NOT EXISTS public.subway_arrival (
    id BIGSERIAL PRIMARY KEY,
    subway_id VARCHAR(20),
    station_id VARCHAR(20),
    station_name VARCHAR(100),
    direction VARCHAR(20),
    train_line VARCHAR(100),
    train_no VARCHAR(20),
    arrival_seconds INTEGER,
    arrival_message VARCHAR(100),
    current_station VARCHAR(100),
    received_at TIMESTAMP WITHOUT TIME ZONE
);

COMMIT;
