-- Apply only after approval; load.py never executes this DDL automatically.
-- The Airflow CLI keeps legacy loading until this table exists.
-- No historical files are marked as loaded by this migration.
-- Confirm ownership/permissions for the pipeline role before deployment.
BEGIN;

CREATE TABLE IF NOT EXISTS public.pipeline_load_history (
    batch_id TEXT PRIMARY KEY,
    source_file TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    row_count BIGINT NOT NULL CHECK (row_count > 0)
);

COMMENT ON TABLE public.pipeline_load_history IS
    'Committed processed-file batches; reserved in the same transaction as arrival inserts.';

COMMIT;
