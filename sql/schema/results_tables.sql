-- =============================================================================
-- Unistore Benchmark - Results Storage Schema
-- =============================================================================
-- This schema stores test results, metrics, and configurations for 
-- Snowflake/Postgres performance benchmarking.
--
-- Database: UNISTORE_BENCHMARK
-- Schema: TEST_RESULTS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Bootstrap (declarative + rerunnable)
-- -----------------------------------------------------------------------------
-- Uses CREATE OR ALTER for declarative, idempotent DDL.
-- Reference: https://docs.snowflake.com/en/sql-reference/sql/create-or-alter
CREATE OR ALTER DATABASE UNISTORE_BENCHMARK;

CREATE OR ALTER SCHEMA UNISTORE_BENCHMARK.TEST_RESULTS;

USE DATABASE UNISTORE_BENCHMARK;
USE SCHEMA TEST_RESULTS;

-- =============================================================================
-- TEST_RESULTS: Store individual test execution results
-- =============================================================================
CREATE OR ALTER TABLE TEST_RESULTS (
    -- Identification
    test_id VARCHAR(36),
    run_id VARCHAR(36),
    test_name VARCHAR(500) NOT NULL,
    scenario_name VARCHAR(500) NOT NULL,
    
    -- Test configuration summary
    table_name VARCHAR(500) NOT NULL,
    table_type VARCHAR(50) NOT NULL,
    warehouse VARCHAR(500),
    warehouse_size VARCHAR(50),
    
    -- Execution metadata
    status VARCHAR(50) NOT NULL,
    start_time TIMESTAMP_NTZ NOT NULL,
    end_time TIMESTAMP_NTZ,
    duration_seconds FLOAT,
    
    -- Workload summary
    concurrent_connections INTEGER NOT NULL,
    total_operations INTEGER DEFAULT 0,
    read_operations INTEGER DEFAULT 0,
    write_operations INTEGER DEFAULT 0,
    failed_operations INTEGER DEFAULT 0,
    
    -- Performance metrics (queries/second)
    qps FLOAT DEFAULT 0.0,
    reads_per_second FLOAT DEFAULT 0.0,
    writes_per_second FLOAT DEFAULT 0.0,
    
    -- Latency metrics (milliseconds)
    avg_latency_ms FLOAT DEFAULT 0.0,
    p50_latency_ms FLOAT DEFAULT 0.0,
    p90_latency_ms FLOAT DEFAULT 0.0,
    p95_latency_ms FLOAT DEFAULT 0.0,
    p99_latency_ms FLOAT DEFAULT 0.0,
    max_latency_ms FLOAT DEFAULT 0.0,
    min_latency_ms FLOAT DEFAULT 0.0,
    
    -- Throughput metrics
    bytes_read BIGINT DEFAULT 0,
    bytes_written BIGINT DEFAULT 0,
    rows_read BIGINT DEFAULT 0,
    rows_written BIGINT DEFAULT 0,
    
    -- Resource utilization
    warehouse_credits_used FLOAT,
    avg_cpu_percent FLOAT,
    avg_memory_mb FLOAT,
    
    -- Errors and issues
    error_count INTEGER DEFAULT 0,
    error_rate FLOAT DEFAULT 0.0,
    failure_reason TEXT,  -- Reason for test failure (setup/validation errors)
    errors VARIANT,
    
    -- Detailed data (JSON/VARIANT)
    query_executions VARIANT,
    metrics_snapshots VARIANT,
    test_config VARIANT,
    custom_metrics VARIANT,
    
    -- Tags and metadata
    tags VARIANT,
    notes TEXT,
    
    -- Audit fields
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- Read vs write percentiles (end-to-end)
    read_p50_latency_ms FLOAT,
    read_p95_latency_ms FLOAT,
    read_p99_latency_ms FLOAT,
    read_min_latency_ms FLOAT,
    read_max_latency_ms FLOAT,

    write_p50_latency_ms FLOAT,
    write_p95_latency_ms FLOAT,
    write_p99_latency_ms FLOAT,
    write_min_latency_ms FLOAT,
    write_max_latency_ms FLOAT,

    -- Per query kind (end-to-end)
    point_lookup_p50_latency_ms FLOAT,
    point_lookup_p95_latency_ms FLOAT,
    point_lookup_p99_latency_ms FLOAT,
    point_lookup_min_latency_ms FLOAT,
    point_lookup_max_latency_ms FLOAT,

    range_scan_p50_latency_ms FLOAT,
    range_scan_p95_latency_ms FLOAT,
    range_scan_p99_latency_ms FLOAT,
    range_scan_min_latency_ms FLOAT,
    range_scan_max_latency_ms FLOAT,

    insert_p50_latency_ms FLOAT,
    insert_p95_latency_ms FLOAT,
    insert_p99_latency_ms FLOAT,
    insert_min_latency_ms FLOAT,
    insert_max_latency_ms FLOAT,

    update_p50_latency_ms FLOAT,
    update_p95_latency_ms FLOAT,
    update_p99_latency_ms FLOAT,
    update_min_latency_ms FLOAT,
    update_max_latency_ms FLOAT,

    -- Overhead summaries (derived)
    app_overhead_p50_ms FLOAT,
    app_overhead_p95_ms FLOAT,
    app_overhead_p99_ms FLOAT,

    -- Warehouse configuration snapshot at test start (MCW settings, scaling policy, etc.)
    -- Captured via SHOW WAREHOUSES at test start to track configuration that may change.
    warehouse_config_snapshot VARIANT,

    -- Per-test query tag used to filter INFORMATION_SCHEMA.QUERY_HISTORY for this test.
    -- Format: "unistore_benchmark:test_id={test_id}"
    query_tag VARCHAR(200),

    -- FIND_MAX_CONCURRENCY mode results (step history, best concurrency, etc.)
    find_max_result VARIANT,

    -- Post-processing enrichment status (separate from test execution status)
    -- Values: NULL (legacy), PENDING, COMPLETED, FAILED, SKIPPED
    enrichment_status VARCHAR(20),
    enrichment_error TEXT
);

-- =============================================================================
-- METRICS_SNAPSHOTS: Time-series metrics data
-- =============================================================================
CREATE OR ALTER TABLE METRICS_SNAPSHOTS (
    snapshot_id VARCHAR(36),
    test_id VARCHAR(36) NOT NULL,
    
    -- Timing
    timestamp TIMESTAMP_NTZ NOT NULL,
    elapsed_seconds FLOAT NOT NULL,
    
    -- Core metrics
    total_queries INTEGER NOT NULL,
    qps FLOAT NOT NULL,
    
    -- Latency metrics (milliseconds)
    p50_latency_ms FLOAT NOT NULL,
    p95_latency_ms FLOAT NOT NULL,
    p99_latency_ms FLOAT NOT NULL,
    avg_latency_ms FLOAT NOT NULL,
    
    -- Operation breakdown
    read_count INTEGER DEFAULT 0,
    write_count INTEGER DEFAULT 0,
    error_count INTEGER DEFAULT 0,
    
    -- Throughput
    bytes_per_second FLOAT DEFAULT 0.0,
    rows_per_second FLOAT DEFAULT 0.0,
    
    -- Connection pool
    active_connections INTEGER DEFAULT 0,
    
    -- Additional metrics (JSON)
    custom_metrics VARIANT,
    
    -- Audit
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================================================
-- QUERY_EXECUTIONS: Detailed query execution history
-- =============================================================================
CREATE OR ALTER TABLE QUERY_EXECUTIONS (
    execution_id VARCHAR(36),
    test_id VARCHAR(36) NOT NULL,
    query_id VARCHAR(500) NOT NULL,
    
    -- Query details
    query_text TEXT NOT NULL,
    start_time TIMESTAMP_NTZ NOT NULL,
    end_time TIMESTAMP_NTZ NOT NULL,
    duration_ms FLOAT NOT NULL,
    
    -- Results
    rows_affected INTEGER,
    bytes_scanned BIGINT,
    warehouse VARCHAR(500),
    success BOOLEAN NOT NULL,
    error TEXT,
    
    -- Metadata
    connection_id INTEGER,
    custom_metadata VARIANT,
    
    -- Audit
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- Per-operation log
    query_kind VARCHAR(50),
    worker_id INTEGER,
    warmup BOOLEAN DEFAULT FALSE,

    -- End-to-end latency measured by the app (what customers experience)
    app_elapsed_ms FLOAT,

    -- Snowflake timings (from INFORMATION_SCHEMA.QUERY_HISTORY)
    sf_total_elapsed_ms FLOAT,
    sf_execution_ms FLOAT,
    sf_compilation_ms FLOAT,
    sf_queued_overload_ms FLOAT,
    sf_queued_provisioning_ms FLOAT,
    sf_tx_blocked_ms FLOAT,
    sf_bytes_scanned BIGINT,
    sf_rows_produced BIGINT,
    sf_rows_inserted BIGINT,
    sf_rows_updated BIGINT,
    sf_rows_deleted BIGINT,

    -- Derived overhead estimate
    app_overhead_ms FLOAT,

    -- Warehouse / caching details
    sf_cluster_number INTEGER,
    sf_pct_scanned_from_cache FLOAT
);

-- =============================================================================
-- Note: Indexes removed - Snowflake Standard Tables use automatic clustering
-- instead of traditional indexes. For better query performance, consider:
--   - Using clustering keys on frequently filtered columns
--   - Converting to Hybrid Tables if transactional semantics needed
--   - Using search optimization service for point lookups
-- =============================================================================

-- =============================================================================
-- Views for common queries
-- =============================================================================

-- Latest test results summary
CREATE OR REPLACE VIEW V_LATEST_TEST_RESULTS AS
SELECT 
    test_id,
    test_name,
    scenario_name,
    table_name,
    table_type,
    status,
    start_time,
    duration_seconds,
    qps,
    p95_latency_ms,
    error_rate,
    created_at
FROM TEST_RESULTS
WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY start_time DESC;

-- Metrics snapshots aggregated by minute
CREATE OR REPLACE VIEW V_METRICS_BY_MINUTE AS
SELECT 
    test_id,
    DATE_TRUNC('minute', timestamp) AS minute,
    AVG(qps) AS avg_qps,
    MAX(qps) AS max_qps,
    AVG(p95_latency_ms) AS avg_p95_latency_ms,
    MAX(p95_latency_ms) AS max_p95_latency_ms,
    SUM(error_count) AS total_errors
FROM METRICS_SNAPSHOTS
GROUP BY test_id, DATE_TRUNC('minute', timestamp)
ORDER BY test_id, minute;

-- -----------------------------------------------------------------------------
-- Warehouse queue / multi-cluster breakdown views (post-processing)
-- -----------------------------------------------------------------------------

-- Test-level summary for warehouse queueing + MCW behavior.
CREATE OR REPLACE VIEW V_WAREHOUSE_METRICS AS
SELECT 
    qe.TEST_ID,
    tr.TEST_NAME,
    tr.WAREHOUSE,
    tr.START_TIME AS TEST_START,
    tr.END_TIME AS TEST_END,
    tr.CONCURRENT_CONNECTIONS,
    
    -- Cluster distribution
    COUNT(DISTINCT qe.SF_CLUSTER_NUMBER) AS CLUSTERS_USED,
    MIN(qe.SF_CLUSTER_NUMBER) AS MIN_CLUSTER,
    MAX(qe.SF_CLUSTER_NUMBER) AS MAX_CLUSTER,
    
    -- Query counts
    COUNT(*) AS TOTAL_QUERIES,
    COUNT(CASE WHEN qe.WARMUP = FALSE THEN 1 END) AS NON_WARMUP_QUERIES,
    
    -- Queue time stats (ms)
    SUM(qe.SF_QUEUED_OVERLOAD_MS) AS TOTAL_QUEUED_OVERLOAD_MS,
    SUM(qe.SF_QUEUED_PROVISIONING_MS) AS TOTAL_QUEUED_PROVISIONING_MS,
    AVG(qe.SF_QUEUED_OVERLOAD_MS) AS AVG_QUEUED_OVERLOAD_MS,
    AVG(qe.SF_QUEUED_PROVISIONING_MS) AS AVG_QUEUED_PROVISIONING_MS,
    MAX(qe.SF_QUEUED_OVERLOAD_MS) AS MAX_QUEUED_OVERLOAD_MS,
    MAX(qe.SF_QUEUED_PROVISIONING_MS) AS MAX_QUEUED_PROVISIONING_MS,
    
    -- Queries that experienced queueing
    COUNT(CASE WHEN qe.SF_QUEUED_OVERLOAD_MS > 0 THEN 1 END) AS QUERIES_WITH_OVERLOAD_QUEUE,
    COUNT(CASE WHEN qe.SF_QUEUED_PROVISIONING_MS > 0 THEN 1 END) AS QUERIES_WITH_PROVISIONING_QUEUE,
    
    -- Cache hit analysis (reads with 0 bytes scanned = result cache hit)
    COUNT(CASE WHEN qe.SF_BYTES_SCANNED = 0 AND qe.QUERY_KIND IN ('POINT_LOOKUP', 'RANGE_SCAN') THEN 1 END) AS READ_CACHE_HITS,
    ROUND(100.0 * COUNT(CASE WHEN qe.SF_BYTES_SCANNED = 0 AND qe.QUERY_KIND IN ('POINT_LOOKUP', 'RANGE_SCAN') THEN 1 END) 
          / NULLIF(COUNT(CASE WHEN qe.QUERY_KIND IN ('POINT_LOOKUP', 'RANGE_SCAN') THEN 1 END), 0), 1) AS READ_CACHE_HIT_PCT
    
FROM QUERY_EXECUTIONS qe
JOIN TEST_RESULTS tr ON qe.TEST_ID = tr.TEST_ID
GROUP BY qe.TEST_ID, tr.TEST_NAME, tr.WAREHOUSE, tr.START_TIME, tr.END_TIME, tr.CONCURRENT_CONNECTIONS;

-- Per-cluster breakdown for MCW tests.
CREATE OR REPLACE VIEW V_CLUSTER_BREAKDOWN AS
SELECT
    TEST_ID,
    SF_CLUSTER_NUMBER AS CLUSTER_NUMBER,
    COUNT(*) AS QUERY_COUNT,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY SF_EXECUTION_MS) AS P50_EXEC_MS,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY SF_EXECUTION_MS) AS P95_EXEC_MS,
    MAX(SF_EXECUTION_MS) AS MAX_EXEC_MS,
    AVG(SF_QUEUED_OVERLOAD_MS) AS AVG_QUEUED_OVERLOAD_MS,
    AVG(SF_QUEUED_PROVISIONING_MS) AS AVG_QUEUED_PROVISIONING_MS,
    SUM(IFF(QUERY_KIND = 'POINT_LOOKUP', 1, 0)) AS POINT_LOOKUPS,
    SUM(IFF(QUERY_KIND = 'RANGE_SCAN', 1, 0)) AS RANGE_SCANS,
    SUM(IFF(QUERY_KIND = 'INSERT', 1, 0)) AS INSERTS,
    SUM(IFF(QUERY_KIND = 'UPDATE', 1, 0)) AS UPDATES
FROM QUERY_EXECUTIONS
WHERE COALESCE(WARMUP, FALSE) = FALSE
  AND SUCCESS = TRUE
  AND SF_CLUSTER_NUMBER IS NOT NULL
  AND SF_EXECUTION_MS IS NOT NULL
GROUP BY TEST_ID, SF_CLUSTER_NUMBER;

-- Per-second time-series for graphing warehouse metrics during a test.
CREATE OR REPLACE VIEW V_WAREHOUSE_TIMESERIES AS
SELECT 
    TEST_ID,
    DATE_TRUNC('second', START_TIME) AS SECOND,
    
    -- Cluster activity
    COUNT(DISTINCT SF_CLUSTER_NUMBER) AS ACTIVE_CLUSTERS,
    ARRAY_AGG(DISTINCT SF_CLUSTER_NUMBER) AS CLUSTER_IDS,
    
    -- Query counts
    COUNT(*) AS QUERIES_STARTED,
    COUNT(CASE WHEN QUERY_KIND = 'POINT_LOOKUP' THEN 1 END) AS POINT_LOOKUPS,
    COUNT(CASE WHEN QUERY_KIND = 'RANGE_SCAN' THEN 1 END) AS RANGE_SCANS,
    COUNT(CASE WHEN QUERY_KIND IN ('INSERT', 'UPDATE', 'DELETE') THEN 1 END) AS WRITES,
    
    -- Queue metrics (for queries starting this second)
    SUM(SF_QUEUED_OVERLOAD_MS) AS TOTAL_QUEUE_OVERLOAD_MS,
    SUM(SF_QUEUED_PROVISIONING_MS) AS TOTAL_QUEUE_PROVISIONING_MS,
    AVG(SF_QUEUED_OVERLOAD_MS) AS AVG_QUEUE_OVERLOAD_MS,
    MAX(SF_QUEUED_OVERLOAD_MS) AS MAX_QUEUE_OVERLOAD_MS,
    COUNT(CASE WHEN SF_QUEUED_OVERLOAD_MS > 0 THEN 1 END) AS QUERIES_QUEUED,
    AVG(SF_QUEUED_PROVISIONING_MS) AS AVG_QUEUE_PROVISIONING_MS,
    MAX(SF_QUEUED_PROVISIONING_MS) AS MAX_QUEUE_PROVISIONING_MS,
    COUNT(CASE WHEN SF_QUEUED_PROVISIONING_MS > 0 THEN 1 END) AS QUERIES_QUEUED_PROVISIONING,
    
    -- Latency (for queries starting this second)
    AVG(SF_EXECUTION_MS) AS AVG_EXEC_MS,
    APPROX_PERCENTILE(SF_EXECUTION_MS, 0.95) AS P95_EXEC_MS,
    MAX(SF_EXECUTION_MS) AS MAX_EXEC_MS
    
FROM QUERY_EXECUTIONS
WHERE WARMUP = FALSE
GROUP BY TEST_ID, DATE_TRUNC('second', START_TIME);

-- =============================================================================
-- Complete
-- =============================================================================

SELECT 'Schema setup complete' AS status;
