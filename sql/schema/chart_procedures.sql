-- =============================================================================
-- Unistore Benchmark - Chart Procedures for Cortex Agent
-- =============================================================================
-- These stored procedures return chart-ready JSON data for visualization.
-- They are designed to be called by the Cortex Agent as tools.
--
-- Database: UNISTORE_BENCHMARK
-- Schema: TEST_RESULTS
-- =============================================================================

USE DATABASE UNISTORE_BENCHMARK;
USE SCHEMA TEST_RESULTS;

-- =============================================================================
-- GET_METRICS_TIMESERIES: Time-series metrics for dashboard charts
-- =============================================================================
-- Returns aggregated time-series data for QPS, latency, and connections.
-- Supports configurable time bucketing for different chart resolutions.
-- =============================================================================

CREATE OR REPLACE PROCEDURE GET_METRICS_TIMESERIES(
    p_test_id VARCHAR,
    p_bucket_seconds INTEGER DEFAULT 1
)
RETURNS VARIANT
LANGUAGE SQL
COMMENT = 'Get time-series metrics for a test. Returns JSON with QPS, latency percentiles, and connection counts bucketed by time.'
AS
$$
DECLARE
    result VARIANT;
    run_id_val VARCHAR;
    bucket_size INTEGER;
BEGIN
    -- Validate bucket size (minimum 1 second)
    bucket_size := GREATEST(COALESCE(p_bucket_seconds, 1), 1);
    
    -- Get the run_id for this test
    SELECT run_id INTO run_id_val
    FROM TEST_RESULTS
    WHERE test_id = p_test_id
    LIMIT 1;
    
    IF (run_id_val IS NULL) THEN
        RETURN OBJECT_CONSTRUCT(
            'error', 'Test not found',
            'test_id', p_test_id
        );
    END IF;
    
    -- Aggregate metrics by time bucket
    SELECT OBJECT_CONSTRUCT(
        'test_id', p_test_id,
        'run_id', run_id_val,
        'bucket_seconds', bucket_size,
        'metadata', OBJECT_CONSTRUCT(
            'start_time', MIN(timestamp),
            'end_time', MAX(timestamp),
            'total_snapshots', COUNT(*),
            'total_buckets', COUNT(DISTINCT time_bucket)
        ),
        'data', ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'time_bucket', time_bucket,
                'elapsed_seconds', bucket_elapsed,
                'qps', avg_qps,
                'p50_latency_ms', avg_p50,
                'p95_latency_ms', avg_p95,
                'p99_latency_ms', avg_p99,
                'avg_latency_ms', avg_latency,
                'active_connections', sum_connections,
                'target_connections', max_target_connections,
                'error_count', sum_errors,
                'total_queries', sum_queries,
                'worker_count', worker_count
            ) ORDER BY time_bucket
        )
    ) INTO result
    FROM (
        SELECT
            FLOOR(elapsed_seconds / :bucket_size) * :bucket_size AS time_bucket,
            MIN(elapsed_seconds) AS bucket_elapsed,
            AVG(qps) AS avg_qps,
            AVG(p50_latency_ms) AS avg_p50,
            AVG(p95_latency_ms) AS avg_p95,
            AVG(p99_latency_ms) AS avg_p99,
            AVG(avg_latency_ms) AS avg_latency,
            SUM(active_connections) AS sum_connections,
            MAX(target_connections) AS max_target_connections,
            SUM(error_count) AS sum_errors,
            SUM(total_queries) AS sum_queries,
            COUNT(DISTINCT worker_id) AS worker_count,
            MIN(timestamp) AS timestamp
        FROM WORKER_METRICS_SNAPSHOTS
        WHERE run_id = run_id_val
          AND phase = 'MEASUREMENT'
        GROUP BY FLOOR(elapsed_seconds / :bucket_size)
    );
    
    RETURN result;
END;
$$;

-- =============================================================================
-- GET_ERROR_TIMELINE: Error distribution over time
-- =============================================================================
-- Returns error counts bucketed by time for error visualization.
-- =============================================================================

CREATE OR REPLACE PROCEDURE GET_ERROR_TIMELINE(
    p_test_id VARCHAR,
    p_bucket_seconds INTEGER DEFAULT 5
)
RETURNS VARIANT
LANGUAGE SQL
COMMENT = 'Get error timeline for a test. Returns JSON with error counts and types over time.'
AS
$$
DECLARE
    result VARIANT;
    bucket_size INTEGER;
BEGIN
    -- Validate bucket size (minimum 1 second)
    bucket_size := GREATEST(COALESCE(p_bucket_seconds, 1), 1);
    
    -- Aggregate errors by time bucket
    SELECT OBJECT_CONSTRUCT(
        'test_id', p_test_id,
        'bucket_seconds', bucket_size,
        'metadata', OBJECT_CONSTRUCT(
            'total_errors', SUM(error_count),
            'total_queries', COUNT(*),
            'error_rate_pct', ROUND(100.0 * SUM(CASE WHEN success = FALSE THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2),
            'first_error_time', MIN(CASE WHEN success = FALSE THEN start_time END),
            'last_error_time', MAX(CASE WHEN success = FALSE THEN start_time END)
        ),
        'data', ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'time_bucket', time_bucket,
                'error_count', error_count,
                'total_queries', total_queries,
                'error_rate_pct', error_rate_pct,
                'error_types', error_types
            ) ORDER BY time_bucket
        )
    ) INTO result
    FROM (
        SELECT
            FLOOR(TIMESTAMPDIFF('SECOND', 
                (SELECT MIN(start_time) FROM QUERY_EXECUTIONS WHERE test_id = p_test_id), 
                start_time
            ) / :bucket_size) * :bucket_size AS time_bucket,
            SUM(CASE WHEN success = FALSE THEN 1 ELSE 0 END) AS error_count,
            COUNT(*) AS total_queries,
            ROUND(100.0 * SUM(CASE WHEN success = FALSE THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS error_rate_pct,
            ARRAY_AGG(DISTINCT error) WITHIN GROUP (ORDER BY error) AS error_types
        FROM QUERY_EXECUTIONS
        WHERE test_id = p_test_id
          AND warmup = FALSE
        GROUP BY FLOOR(TIMESTAMPDIFF('SECOND', 
            (SELECT MIN(start_time) FROM QUERY_EXECUTIONS WHERE test_id = p_test_id), 
            start_time
        ) / :bucket_size)
    )
    WHERE error_count > 0 OR total_queries > 0;
    
    IF (result IS NULL) THEN
        RETURN OBJECT_CONSTRUCT(
            'test_id', p_test_id,
            'error', 'No query executions found for this test',
            'data', ARRAY_CONSTRUCT()
        );
    END IF;
    
    RETURN result;
END;
$$;

-- =============================================================================
-- GET_LATENCY_BREAKDOWN: Latency by operation type
-- =============================================================================
-- Returns latency percentiles broken down by query kind (reads vs writes,
-- point lookups vs range scans, etc.)
-- =============================================================================

CREATE OR REPLACE PROCEDURE GET_LATENCY_BREAKDOWN(
    p_test_id VARCHAR
)
RETURNS VARIANT
LANGUAGE SQL
COMMENT = 'Get latency breakdown by operation type. Returns JSON with percentiles for each query kind.'
AS
$$
DECLARE
    result VARIANT;
BEGIN
    SELECT OBJECT_CONSTRUCT(
        'test_id', p_test_id,
        'metadata', OBJECT_CONSTRUCT(
            'total_queries', SUM(query_count),
            'total_errors', SUM(error_count),
            'query_kinds', COUNT(DISTINCT query_kind)
        ),
        'by_query_kind', OBJECT_AGG(
            query_kind,
            OBJECT_CONSTRUCT(
                'count', query_count,
                'error_count', error_count,
                'error_rate_pct', error_rate_pct,
                'p50_ms', p50_ms,
                'p95_ms', p95_ms,
                'p99_ms', p99_ms,
                'avg_ms', avg_ms,
                'min_ms', min_ms,
                'max_ms', max_ms,
                'sf_execution_p50_ms', sf_exec_p50,
                'sf_execution_p95_ms', sf_exec_p95,
                'queue_overload_avg_ms', avg_queue_overload,
                'bytes_scanned_total', total_bytes_scanned
            )
        ),
        'by_operation_type', OBJECT_CONSTRUCT(
            'reads', reads_summary,
            'writes', writes_summary
        ),
        'overall', overall_summary
    ) INTO result
    FROM (
        -- By query kind aggregation
        SELECT
            query_kind,
            COUNT(*) AS query_count,
            SUM(CASE WHEN success = FALSE THEN 1 ELSE 0 END) AS error_count,
            ROUND(100.0 * SUM(CASE WHEN success = FALSE THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS error_rate_pct,
            ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY app_elapsed_ms), 2) AS p50_ms,
            ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY app_elapsed_ms), 2) AS p95_ms,
            ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY app_elapsed_ms), 2) AS p99_ms,
            ROUND(AVG(app_elapsed_ms), 2) AS avg_ms,
            ROUND(MIN(app_elapsed_ms), 2) AS min_ms,
            ROUND(MAX(app_elapsed_ms), 2) AS max_ms,
            ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY sf_execution_ms), 2) AS sf_exec_p50,
            ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY sf_execution_ms), 2) AS sf_exec_p95,
            ROUND(AVG(sf_queued_overload_ms), 2) AS avg_queue_overload,
            SUM(sf_bytes_scanned) AS total_bytes_scanned
        FROM QUERY_EXECUTIONS
        WHERE test_id = p_test_id
          AND warmup = FALSE
          AND success = TRUE
        GROUP BY query_kind
    ),
    (
        -- Reads summary (POINT_LOOKUP + RANGE_SCAN)
        SELECT OBJECT_CONSTRUCT(
            'count', COUNT(*),
            'p50_ms', ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY app_elapsed_ms), 2),
            'p95_ms', ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY app_elapsed_ms), 2),
            'p99_ms', ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY app_elapsed_ms), 2),
            'avg_ms', ROUND(AVG(app_elapsed_ms), 2)
        ) AS reads_summary
        FROM QUERY_EXECUTIONS
        WHERE test_id = p_test_id
          AND warmup = FALSE
          AND success = TRUE
          AND query_kind IN ('POINT_LOOKUP', 'RANGE_SCAN')
    ),
    (
        -- Writes summary (INSERT + UPDATE + DELETE)
        SELECT OBJECT_CONSTRUCT(
            'count', COUNT(*),
            'p50_ms', ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY app_elapsed_ms), 2),
            'p95_ms', ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY app_elapsed_ms), 2),
            'p99_ms', ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY app_elapsed_ms), 2),
            'avg_ms', ROUND(AVG(app_elapsed_ms), 2)
        ) AS writes_summary
        FROM QUERY_EXECUTIONS
        WHERE test_id = p_test_id
          AND warmup = FALSE
          AND success = TRUE
          AND query_kind IN ('INSERT', 'UPDATE', 'DELETE')
    ),
    (
        -- Overall summary
        SELECT OBJECT_CONSTRUCT(
            'count', COUNT(*),
            'p50_ms', ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY app_elapsed_ms), 2),
            'p95_ms', ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY app_elapsed_ms), 2),
            'p99_ms', ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY app_elapsed_ms), 2),
            'avg_ms', ROUND(AVG(app_elapsed_ms), 2),
            'min_ms', ROUND(MIN(app_elapsed_ms), 2),
            'max_ms', ROUND(MAX(app_elapsed_ms), 2)
        ) AS overall_summary
        FROM QUERY_EXECUTIONS
        WHERE test_id = p_test_id
          AND warmup = FALSE
          AND success = TRUE
    );
    
    IF (result IS NULL OR result:by_query_kind IS NULL) THEN
        RETURN OBJECT_CONSTRUCT(
            'test_id', p_test_id,
            'error', 'No query executions found for this test',
            'by_query_kind', OBJECT_CONSTRUCT(),
            'by_operation_type', OBJECT_CONSTRUCT(),
            'overall', OBJECT_CONSTRUCT()
        );
    END IF;
    
    RETURN result;
END;
$$;

-- =============================================================================
-- GET_WAREHOUSE_TIMESERIES: Multi-cluster warehouse metrics over time
-- =============================================================================
-- Returns warehouse scaling metrics (cluster counts, queue times) over time.
-- Useful for understanding MCW behavior during tests.
-- =============================================================================

CREATE OR REPLACE PROCEDURE GET_WAREHOUSE_TIMESERIES(
    p_test_id VARCHAR,
    p_bucket_seconds INTEGER DEFAULT 1
)
RETURNS VARIANT
LANGUAGE SQL
COMMENT = 'Get warehouse scaling metrics over time. Returns JSON with cluster counts and queue metrics.'
AS
$$
DECLARE
    result VARIANT;
    run_id_val VARCHAR;
    bucket_size INTEGER;
BEGIN
    bucket_size := GREATEST(COALESCE(p_bucket_seconds, 1), 1);
    
    -- Get the run_id for this test
    SELECT run_id INTO run_id_val
    FROM TEST_RESULTS
    WHERE test_id = p_test_id
    LIMIT 1;
    
    IF (run_id_val IS NULL) THEN
        RETURN OBJECT_CONSTRUCT(
            'error', 'Test not found',
            'test_id', p_test_id
        );
    END IF;
    
    -- Check if we have warehouse poll data
    SELECT OBJECT_CONSTRUCT(
        'test_id', p_test_id,
        'run_id', run_id_val,
        'bucket_seconds', bucket_size,
        'source', 'WAREHOUSE_POLL_SNAPSHOTS',
        'data', COALESCE(ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'elapsed_seconds', elapsed_seconds,
                'started_clusters', started_clusters,
                'running', running,
                'queued', queued,
                'min_cluster_count', min_cluster_count,
                'max_cluster_count', max_cluster_count,
                'scaling_policy', scaling_policy
            ) ORDER BY elapsed_seconds
        ), ARRAY_CONSTRUCT())
    ) INTO result
    FROM WAREHOUSE_POLL_SNAPSHOTS
    WHERE run_id = run_id_val;
    
    -- If no warehouse poll data, try to derive from query executions
    IF (result:data = ARRAY_CONSTRUCT() OR ARRAY_SIZE(result:data) = 0) THEN
        SELECT OBJECT_CONSTRUCT(
            'test_id', p_test_id,
            'bucket_seconds', bucket_size,
            'source', 'QUERY_EXECUTIONS',
            'data', ARRAY_AGG(
                OBJECT_CONSTRUCT(
                    'time_bucket', time_bucket,
                    'active_clusters', active_clusters,
                    'cluster_ids', cluster_ids,
                    'queries_started', queries_started,
                    'avg_queue_overload_ms', avg_queue_overload,
                    'max_queue_overload_ms', max_queue_overload,
                    'queries_queued', queries_queued
                ) ORDER BY time_bucket
            )
        ) INTO result
        FROM (
            SELECT
                FLOOR(TIMESTAMPDIFF('SECOND', 
                    (SELECT MIN(start_time) FROM QUERY_EXECUTIONS WHERE test_id = p_test_id), 
                    start_time
                ) / :bucket_size) * :bucket_size AS time_bucket,
                COUNT(DISTINCT sf_cluster_number) AS active_clusters,
                ARRAY_AGG(DISTINCT sf_cluster_number) AS cluster_ids,
                COUNT(*) AS queries_started,
                ROUND(AVG(sf_queued_overload_ms), 2) AS avg_queue_overload,
                MAX(sf_queued_overload_ms) AS max_queue_overload,
                SUM(CASE WHEN sf_queued_overload_ms > 0 THEN 1 ELSE 0 END) AS queries_queued
            FROM QUERY_EXECUTIONS
            WHERE test_id = p_test_id
              AND warmup = FALSE
            GROUP BY FLOOR(TIMESTAMPDIFF('SECOND', 
                (SELECT MIN(start_time) FROM QUERY_EXECUTIONS WHERE test_id = p_test_id), 
                start_time
            ) / :bucket_size)
        );
    END IF;
    
    RETURN result;
END;
$$;

-- =============================================================================
-- GET_STEP_HISTORY: FIND_MAX step progression
-- =============================================================================
-- Returns step-by-step data for FIND_MAX_CONCURRENCY tests.
-- =============================================================================

CREATE OR REPLACE PROCEDURE GET_STEP_HISTORY(
    p_test_id VARCHAR
)
RETURNS VARIANT
LANGUAGE SQL
COMMENT = 'Get FIND_MAX step history. Returns JSON with step-by-step progression data.'
AS
$$
DECLARE
    result VARIANT;
    run_id_val VARCHAR;
BEGIN
    -- Get the run_id for this test
    SELECT run_id INTO run_id_val
    FROM TEST_RESULTS
    WHERE test_id = p_test_id
    LIMIT 1;
    
    IF (run_id_val IS NULL) THEN
        RETURN OBJECT_CONSTRUCT(
            'error', 'Test not found',
            'test_id', p_test_id
        );
    END IF;
    
    SELECT OBJECT_CONSTRUCT(
        'test_id', p_test_id,
        'run_id', run_id_val,
        'metadata', OBJECT_CONSTRUCT(
            'total_steps', COUNT(*),
            'best_step', MAX(CASE WHEN outcome = 'STABLE' THEN step_number END),
            'final_outcome', MAX_BY(outcome, step_number),
            'final_reason', MAX_BY(stop_reason, step_number)
        ),
        'steps', ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'step_number', step_number,
                'target_workers', target_workers,
                'qps', ROUND(qps, 1),
                'p50_latency_ms', ROUND(p50_latency_ms, 1),
                'p95_latency_ms', ROUND(p95_latency_ms, 1),
                'p99_latency_ms', ROUND(p99_latency_ms, 1),
                'error_count', error_count,
                'error_rate', ROUND(error_rate, 2),
                'qps_vs_prior_pct', ROUND(qps_vs_prior_pct, 1),
                'p95_vs_baseline_pct', ROUND(p95_vs_baseline_pct, 1),
                'queue_detected', queue_detected,
                'outcome', outcome,
                'stop_reason', stop_reason,
                'duration_seconds', ROUND(step_duration_seconds, 1)
            ) ORDER BY step_number
        )
    ) INTO result
    FROM CONTROLLER_STEP_HISTORY
    WHERE run_id = run_id_val
      AND step_type = 'FIND_MAX';
    
    IF (result IS NULL OR result:steps IS NULL OR ARRAY_SIZE(result:steps) = 0) THEN
        -- Try to get from find_max_result in TEST_RESULTS
        SELECT OBJECT_CONSTRUCT(
            'test_id', p_test_id,
            'run_id', run_id_val,
            'source', 'TEST_RESULTS.find_max_result',
            'find_max_result', find_max_result
        ) INTO result
        FROM TEST_RESULTS
        WHERE test_id = p_test_id
          AND find_max_result IS NOT NULL;
    END IF;
    
    IF (result IS NULL) THEN
        RETURN OBJECT_CONSTRUCT(
            'test_id', p_test_id,
            'error', 'No step history found. This test may not be a FIND_MAX_CONCURRENCY test.',
            'steps', ARRAY_CONSTRUCT()
        );
    END IF;
    
    RETURN result;
END;
$$;

-- =============================================================================
-- Validation: Show created procedures
-- =============================================================================
SHOW PROCEDURES LIKE 'GET_%' IN SCHEMA UNISTORE_BENCHMARK.TEST_RESULTS;
