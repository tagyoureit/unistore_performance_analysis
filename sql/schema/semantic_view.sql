-- =============================================================================
-- Unistore Benchmark - Semantic View for Cortex Agent
-- =============================================================================
-- This semantic view provides a governed, queryable interface for benchmark
-- analytics data, enabling natural language queries via Cortex Agent.
--
-- Database: UNISTORE_BENCHMARK
-- Schema: TEST_RESULTS
-- =============================================================================

USE DATABASE UNISTORE_BENCHMARK;
USE SCHEMA TEST_RESULTS;

-- =============================================================================
-- BENCHMARK_ANALYTICS: Semantic View for Benchmark Analysis
-- =============================================================================
-- This semantic view exposes test results, worker metrics, and query executions
-- with logical naming, synonyms, and AI-friendly descriptions.
-- =============================================================================

CREATE OR REPLACE SEMANTIC VIEW BENCHMARK_ANALYTICS
  COMMENT = 'Semantic view for benchmark performance analytics - enables natural language queries via Cortex Agent'

  -- -------------------------------------------------------------------------
  -- TABLES: Define logical tables with primary keys
  -- -------------------------------------------------------------------------
  TABLES (
    -- Main test results table
    test_results AS UNISTORE_BENCHMARK.TEST_RESULTS.TEST_RESULTS
      PRIMARY KEY (test_id)
      WITH SYNONYMS ('tests', 'benchmark results', 'test runs', 'benchmarks')
      COMMENT = 'Main test results with configuration, status, and aggregate metrics. One row per test execution.',

    -- Per-worker time-series metrics
    worker_metrics AS UNISTORE_BENCHMARK.TEST_RESULTS.WORKER_METRICS_SNAPSHOTS
      PRIMARY KEY (snapshot_id)
      WITH SYNONYMS ('worker snapshots', 'time series', 'metrics over time')
      COMMENT = 'Time-series metrics snapshots per worker. Multiple rows per test, one per worker per time interval.',

    -- Individual query executions
    query_executions AS UNISTORE_BENCHMARK.TEST_RESULTS.QUERY_EXECUTIONS
      PRIMARY KEY (execution_id)
      WITH SYNONYMS ('queries', 'operations', 'query log')
      COMMENT = 'Individual query execution records with timing and results. Many rows per test.',

    -- Controller step history for FIND_MAX mode
    step_history AS UNISTORE_BENCHMARK.TEST_RESULTS.CONTROLLER_STEP_HISTORY
      PRIMARY KEY (step_id)
      WITH SYNONYMS ('find max steps', 'scaling steps', 'concurrency steps')
      COMMENT = 'Step-by-step history for FIND_MAX_CONCURRENCY tests showing how concurrency was incremented.'
  )

  -- -------------------------------------------------------------------------
  -- RELATIONSHIPS: Define foreign key relationships between tables
  -- -------------------------------------------------------------------------
  RELATIONSHIPS (
    -- Worker metrics belong to a test
    worker_metrics_to_test AS worker_metrics(test_id) REFERENCES test_results(test_id)
      COMMENT = 'Each worker metrics snapshot belongs to a test',

    -- Query executions belong to a test
    query_executions_to_test AS query_executions(test_id) REFERENCES test_results(test_id)
      COMMENT = 'Each query execution belongs to a test',

    -- Step history belongs to a run (which maps to test via run_id)
    step_history_to_test AS step_history(run_id) REFERENCES test_results(run_id)
      COMMENT = 'Each step history entry belongs to a test run'
  )

  -- -------------------------------------------------------------------------
  -- FACTS: Numeric measures at row level (before aggregation)
  -- -------------------------------------------------------------------------
  FACTS (
    -- Test Results Facts
    test_results.concurrent_connections AS concurrent_connections
      WITH SYNONYMS ('concurrency', 'workers', 'threads', 'connections')
      COMMENT = 'Number of concurrent workers or connections for the test',

    test_results.total_operations AS total_operations
      WITH SYNONYMS ('total ops', 'operation count', 'query count')
      COMMENT = 'Total number of operations executed during the test',

    test_results.read_operations AS read_operations
      WITH SYNONYMS ('reads', 'read count', 'select count')
      COMMENT = 'Number of read operations (point lookups and range scans)',

    test_results.write_operations AS write_operations
      WITH SYNONYMS ('writes', 'write count', 'insert count', 'update count')
      COMMENT = 'Number of write operations (inserts, updates, deletes)',

    test_results.failed_operations AS failed_operations
      WITH SYNONYMS ('failures', 'errors', 'failed count')
      COMMENT = 'Number of operations that failed with errors',

    test_results.qps AS throughput_qps
      WITH SYNONYMS ('qps', 'queries per second', 'ops per second', 'throughput')
      COMMENT = 'Sustained throughput in queries per second',

    test_results.duration_seconds AS duration_seconds
      WITH SYNONYMS ('duration', 'test duration', 'runtime')
      COMMENT = 'Total test duration in seconds',

    test_results.p50_latency_ms AS p50_latency_ms
      WITH SYNONYMS ('p50', 'median latency', '50th percentile')
      COMMENT = 'Median (50th percentile) latency in milliseconds',

    test_results.p95_latency_ms AS p95_latency_ms
      WITH SYNONYMS ('p95', '95th percentile latency')
      COMMENT = '95th percentile latency in milliseconds',

    test_results.p99_latency_ms AS p99_latency_ms
      WITH SYNONYMS ('p99', '99th percentile latency')
      COMMENT = '99th percentile latency in milliseconds',

    test_results.avg_latency_ms AS avg_latency_ms
      WITH SYNONYMS ('average latency', 'mean latency')
      COMMENT = 'Average latency in milliseconds',

    test_results.max_latency_ms AS max_latency_ms
      WITH SYNONYMS ('max latency', 'peak latency', 'worst latency')
      COMMENT = 'Maximum latency observed in milliseconds',

    test_results.error_rate AS error_rate
      WITH SYNONYMS ('error percentage', 'failure rate')
      COMMENT = 'Percentage of operations that failed (0.0 to 100.0)',

    test_results.warehouse_credits_used AS credits_used
      WITH SYNONYMS ('credits', 'cost', 'warehouse credits')
      COMMENT = 'Snowflake warehouse credits consumed during the test',

    -- Worker Metrics Facts
    worker_metrics.qps AS worker_qps
      WITH SYNONYMS ('worker throughput', 'instantaneous qps')
      COMMENT = 'Queries per second for this worker at this snapshot',

    worker_metrics.p50_latency_ms AS worker_p50_latency_ms
      COMMENT = 'Worker-level 50th percentile latency at snapshot',

    worker_metrics.p95_latency_ms AS worker_p95_latency_ms
      COMMENT = 'Worker-level 95th percentile latency at snapshot',

    worker_metrics.p99_latency_ms AS worker_p99_latency_ms
      COMMENT = 'Worker-level 99th percentile latency at snapshot',

    worker_metrics.active_connections AS active_connections
      WITH SYNONYMS ('active threads', 'current connections')
      COMMENT = 'Number of active connections at this snapshot',

    worker_metrics.error_count AS snapshot_error_count
      COMMENT = 'Cumulative error count at this snapshot',

    worker_metrics.elapsed_seconds AS elapsed_seconds
      COMMENT = 'Seconds elapsed since test start',

    -- Query Execution Facts
    query_executions.duration_ms AS query_duration_ms
      WITH SYNONYMS ('query time', 'execution time')
      COMMENT = 'End-to-end query duration in milliseconds',

    query_executions.app_elapsed_ms AS app_elapsed_ms
      WITH SYNONYMS ('client latency', 'end to end latency')
      COMMENT = 'Application-measured end-to-end latency in milliseconds',

    query_executions.sf_execution_ms AS snowflake_execution_ms
      WITH SYNONYMS ('server time', 'sf execution')
      COMMENT = 'Snowflake server-side execution time in milliseconds',

    query_executions.sf_compilation_ms AS snowflake_compilation_ms
      COMMENT = 'Snowflake query compilation time in milliseconds',

    query_executions.sf_queued_overload_ms AS queue_overload_ms
      WITH SYNONYMS ('queue time', 'overload queue')
      COMMENT = 'Time spent queued due to warehouse overload in milliseconds',

    query_executions.sf_queued_provisioning_ms AS queue_provisioning_ms
      COMMENT = 'Time spent queued while warehouse was provisioning in milliseconds',

    query_executions.app_overhead_ms AS app_overhead_ms
      WITH SYNONYMS ('network overhead', 'client overhead')
      COMMENT = 'Estimated application and network overhead in milliseconds',

    query_executions.sf_bytes_scanned AS bytes_scanned
      COMMENT = 'Bytes scanned by Snowflake for this query',

    query_executions.sf_rows_produced AS rows_produced
      COMMENT = 'Number of rows produced by the query',

    -- Step History Facts
    step_history.target_workers AS step_target_workers
      COMMENT = 'Target worker count for this step',

    step_history.qps AS step_qps
      WITH SYNONYMS ('step throughput')
      COMMENT = 'Achieved QPS during this step',

    step_history.p95_latency_ms AS step_p95_latency_ms
      COMMENT = 'P95 latency during this step',

    step_history.error_rate AS step_error_rate
      COMMENT = 'Error rate during this step'
  )

  -- -------------------------------------------------------------------------
  -- DIMENSIONS: Categorical and temporal attributes for filtering/grouping
  -- -------------------------------------------------------------------------
  DIMENSIONS (
    -- Test Results Dimensions
    test_results.test_id AS test_id
      COMMENT = 'Unique identifier for the test',

    test_results.run_id AS run_id
      COMMENT = 'Run identifier (same as test_id for single-worker tests)',

    test_results.test_name AS test_name
      WITH SYNONYMS ('name', 'benchmark name')
      COMMENT = 'Human-readable name of the test',

    test_results.scenario_name AS scenario_name
      WITH SYNONYMS ('scenario', 'test scenario')
      COMMENT = 'Name of the test scenario',

    test_results.table_name AS table_name
      WITH SYNONYMS ('target table')
      COMMENT = 'Name of the table being benchmarked',

    test_results.table_type AS table_type
      WITH SYNONYMS ('storage type', 'table kind')
      COMMENT = 'Type of table: HYBRID, STANDARD, ICEBERG, POSTGRES',

    test_results.warehouse AS warehouse_name
      WITH SYNONYMS ('warehouse', 'compute')
      COMMENT = 'Name of the Snowflake warehouse or Postgres instance',

    test_results.warehouse_size AS warehouse_size
      WITH SYNONYMS ('size', 'compute size')
      COMMENT = 'Size of the warehouse: XSMALL, SMALL, MEDIUM, LARGE, etc.',

    test_results.status AS test_status
      WITH SYNONYMS ('status', 'result status')
      COMMENT = 'Test status: COMPLETED, FAILED, CANCELLED',

    test_results.start_time AS start_time
      WITH SYNONYMS ('started', 'test start')
      COMMENT = 'When the test started',

    test_results.end_time AS end_time
      WITH SYNONYMS ('ended', 'test end')
      COMMENT = 'When the test ended',

    test_results.created_at AS created_at
      WITH SYNONYMS ('created', 'recorded')
      COMMENT = 'When the test record was created',

    -- Worker Metrics Dimensions
    worker_metrics.test_id AS metrics_test_id
      COMMENT = 'Test ID for this metrics snapshot',

    worker_metrics.worker_id AS worker_id
      WITH SYNONYMS ('worker', 'thread id')
      COMMENT = 'Unique identifier for the worker',

    worker_metrics.phase AS test_phase
      WITH SYNONYMS ('phase')
      COMMENT = 'Test phase: WARMUP, MEASUREMENT, COOLDOWN',

    worker_metrics.timestamp AS snapshot_timestamp
      WITH SYNONYMS ('time', 'snapshot time')
      COMMENT = 'Timestamp of this metrics snapshot',

    -- Query Execution Dimensions
    query_executions.test_id AS query_test_id
      COMMENT = 'Test ID for this query execution',

    query_executions.query_kind AS query_kind
      WITH SYNONYMS ('operation type', 'query type')
      COMMENT = 'Type of query: POINT_LOOKUP, RANGE_SCAN, INSERT, UPDATE, DELETE',

    query_executions.success AS query_success
      WITH SYNONYMS ('succeeded', 'passed')
      COMMENT = 'Whether the query succeeded (true/false)',

    query_executions.warmup AS is_warmup
      WITH SYNONYMS ('warmup query', 'warmup phase')
      COMMENT = 'Whether this query was during warmup phase',

    query_executions.start_time AS query_start_time
      COMMENT = 'When the query started executing',

    query_executions.sf_cluster_number AS cluster_number
      WITH SYNONYMS ('cluster', 'mcw cluster')
      COMMENT = 'Multi-cluster warehouse cluster number that executed this query',

    -- Step History Dimensions
    step_history.step_number AS step_number
      WITH SYNONYMS ('step')
      COMMENT = 'Step number in the FIND_MAX progression',

    step_history.outcome AS step_outcome
      WITH SYNONYMS ('step result', 'stability')
      COMMENT = 'Outcome of the step: STABLE, DEGRADED, ERROR_THRESHOLD'
  )

  -- -------------------------------------------------------------------------
  -- METRICS: Pre-defined aggregations
  -- -------------------------------------------------------------------------
  METRICS (
    -- Test count metrics
    test_results.test_count AS COUNT(DISTINCT test_id)
      WITH SYNONYMS ('number of tests', 'total tests')
      COMMENT = 'Count of distinct tests',

    -- Throughput metrics
    test_results.avg_qps AS AVG(qps)
      WITH SYNONYMS ('average throughput', 'mean qps')
      COMMENT = 'Average QPS across tests',

    test_results.max_qps AS MAX(qps)
      WITH SYNONYMS ('peak throughput', 'highest qps')
      COMMENT = 'Maximum QPS achieved',

    test_results.total_operations_sum AS SUM(total_operations)
      WITH SYNONYMS ('total ops sum')
      COMMENT = 'Sum of all operations across tests',

    -- Latency metrics
    test_results.avg_p50_latency AS AVG(p50_latency_ms)
      WITH SYNONYMS ('average median latency')
      COMMENT = 'Average of P50 latencies across tests',

    test_results.avg_p95_latency AS AVG(p95_latency_ms)
      WITH SYNONYMS ('average p95')
      COMMENT = 'Average of P95 latencies across tests',

    test_results.avg_p99_latency AS AVG(p99_latency_ms)
      WITH SYNONYMS ('average p99')
      COMMENT = 'Average of P99 latencies across tests',

    test_results.max_p99_latency AS MAX(p99_latency_ms)
      WITH SYNONYMS ('worst p99', 'peak p99')
      COMMENT = 'Maximum P99 latency across tests',

    -- Error metrics
    test_results.total_errors AS SUM(failed_operations)
      WITH SYNONYMS ('total failures', 'error sum')
      COMMENT = 'Total failed operations across tests',

    test_results.avg_error_rate AS AVG(error_rate)
      WITH SYNONYMS ('average error rate', 'mean error rate')
      COMMENT = 'Average error rate across tests',

    -- Cost metrics
    test_results.total_credits AS SUM(warehouse_credits_used)
      WITH SYNONYMS ('total cost', 'credits sum')
      COMMENT = 'Total warehouse credits used across tests',

    -- Query execution metrics
    query_executions.query_count AS COUNT(execution_id)
      WITH SYNONYMS ('operation count')
      COMMENT = 'Count of query executions',

    query_executions.avg_query_duration AS AVG(duration_ms)
      WITH SYNONYMS ('average query time')
      COMMENT = 'Average query duration in milliseconds',

    query_executions.avg_queue_time AS AVG(sf_queued_overload_ms)
      WITH SYNONYMS ('average queue time')
      COMMENT = 'Average time spent in queue due to overload'
  )

  -- -------------------------------------------------------------------------
  -- AI_SQL_GENERATION: Instructions for Cortex Analyst
  -- -------------------------------------------------------------------------
  AI_SQL_GENERATION (
    INSTRUCTIONS = $$
You are analyzing Snowflake and PostgreSQL benchmark test results. This semantic view contains performance data from the FlakeBench benchmarking tool.

KEY CONCEPTS:
- table_type: HYBRID (Snowflake Unistore), STANDARD (Snowflake standard tables), ICEBERG (Iceberg tables), POSTGRES (PostgreSQL)
- warehouse_size: XSMALL, SMALL, MEDIUM, LARGE, XLARGE, etc.
- query_kind: POINT_LOOKUP (single row by PK), RANGE_SCAN (range queries), INSERT, UPDATE, DELETE
- test_phase: WARMUP (initial load), MEASUREMENT (actual test period), COOLDOWN (cleanup)

LOAD MODES (derived from test_config):
- CONCURRENCY: Fixed number of workers running for set duration
- QPS: Auto-scaling to achieve target queries per second
- FIND_MAX_CONCURRENCY: Incrementally increases workers to find maximum sustainable load

COMMON QUERIES:
- Compare table types: GROUP BY table_type
- Compare warehouse sizes: GROUP BY warehouse_size
- Filter by status: WHERE test_status = 'COMPLETED'
- Recent tests: WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
- Exclude warmup: WHERE is_warmup = FALSE or test_phase = 'MEASUREMENT'

LATENCY INTERPRETATION:
- p50 (median): typical user experience
- p95: 95% of requests are faster than this
- p99: tail latency, important for SLOs
- Good OLTP: p95 < 50ms for point lookups
- Concerning: p99/p50 ratio > 5x indicates high variance

THROUGHPUT INTERPRETATION:
- QPS varies significantly by table type and warehouse size
- HYBRID tables optimized for transactional workloads
- Compare same table_type and warehouse_size for meaningful comparisons

When asked about a specific test, use test_id or test_name to filter.
When comparing performance, ensure you control for table_type and warehouse_size.
$$
  );

-- =============================================================================
-- Post-Creation Validation
-- =============================================================================
-- Run these commands to verify the semantic view was created correctly

-- Show the semantic view
SHOW SEMANTIC VIEWS LIKE 'BENCHMARK_ANALYTICS';

-- Show dimensions
SHOW SEMANTIC DIMENSIONS IN SEMANTIC VIEW BENCHMARK_ANALYTICS;

-- Show metrics
SHOW SEMANTIC METRICS IN SEMANTIC VIEW BENCHMARK_ANALYTICS;

-- Show facts
SHOW SEMANTIC FACTS IN SEMANTIC VIEW BENCHMARK_ANALYTICS;

-- Get the DDL
SELECT GET_DDL('SEMANTIC_VIEW', 'UNISTORE_BENCHMARK.TEST_RESULTS.BENCHMARK_ANALYTICS');
