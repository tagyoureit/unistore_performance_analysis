-- =============================================================================
-- TEST_LOGS: Per-test log events (captured during execution)
-- =============================================================================
-- Stored separately from TEST_RESULTS for queryability and easier analysis.
--
-- Notes:
-- - Inserted by the app in batches (see backend/core/results_store.py).
-- - Ordered within a test by SEQ (monotonic per test).

USE DATABASE UNISTORE_BENCHMARK;
USE SCHEMA TEST_RESULTS;

CREATE TABLE IF NOT EXISTS TEST_LOGS (
    LOG_ID VARCHAR(36) PRIMARY KEY,
    TEST_ID VARCHAR(36) NOT NULL,
    WORKER_ID VARCHAR(100),

    SEQ INTEGER NOT NULL,
    TIMESTAMP TIMESTAMP_NTZ NOT NULL,
    LEVEL VARCHAR(20) NOT NULL,
    LOGGER VARCHAR(500),
    MESSAGE TEXT,
    EXCEPTION TEXT,

    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

ALTER TABLE TEST_LOGS ADD COLUMN IF NOT EXISTS WORKER_ID VARCHAR(100);




