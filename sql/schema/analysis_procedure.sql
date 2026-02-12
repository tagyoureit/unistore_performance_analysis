-- =============================================================================
-- Unistore Benchmark - AI Analysis Procedure for Cortex Agent
-- =============================================================================
-- This stored procedure wraps AI_COMPLETE with mode-specific prompts for
-- comprehensive benchmark analysis. It mirrors the Python prompt builders.
--
-- Database: UNISTORE_BENCHMARK
-- Schema: TEST_RESULTS
-- =============================================================================

USE DATABASE UNISTORE_BENCHMARK;
USE SCHEMA TEST_RESULTS;

-- =============================================================================
-- ANALYZE_BENCHMARK: AI-powered benchmark analysis
-- =============================================================================
-- Analyzes a benchmark test using AI_COMPLETE with mode-specific prompts.
-- Automatically detects the load mode (CONCURRENCY, QPS, FIND_MAX) from config.
-- =============================================================================

CREATE OR REPLACE PROCEDURE ANALYZE_BENCHMARK(
    p_test_id VARCHAR,
    p_mode VARCHAR DEFAULT NULL,  -- Auto-detect from test_config if NULL
    p_model VARCHAR DEFAULT 'claude-4-sonnet'
)
RETURNS VARIANT
LANGUAGE SQL
COMMENT = 'AI-powered analysis of benchmark tests. Auto-detects mode and provides graded analysis with recommendations.'
AS
$$
DECLARE
    result VARIANT;
    test_data VARIANT;
    load_mode VARCHAR;
    prompt TEXT;
    ai_response VARCHAR;
    
    -- Test metadata
    v_test_name VARCHAR;
    v_test_status VARCHAR;
    v_table_type VARCHAR;
    v_warehouse VARCHAR;
    v_warehouse_size VARCHAR;
    v_duration_seconds FLOAT;
    v_total_ops INTEGER;
    v_read_ops INTEGER;
    v_write_ops INTEGER;
    v_failed_ops INTEGER;
    v_qps FLOAT;
    v_p50 FLOAT;
    v_p95 FLOAT;
    v_p99 FLOAT;
    v_error_rate FLOAT;
    v_concurrent_connections INTEGER;
    v_credits_used FLOAT;
    v_test_config VARIANT;
    v_find_max_result VARIANT;
BEGIN
    -- Fetch test data
    SELECT 
        test_name,
        status,
        table_type,
        warehouse,
        warehouse_size,
        duration_seconds,
        total_operations,
        read_operations,
        write_operations,
        failed_operations,
        qps,
        p50_latency_ms,
        p95_latency_ms,
        p99_latency_ms,
        error_rate,
        concurrent_connections,
        warehouse_credits_used,
        test_config,
        find_max_result
    INTO 
        v_test_name,
        v_test_status,
        v_table_type,
        v_warehouse,
        v_warehouse_size,
        v_duration_seconds,
        v_total_ops,
        v_read_ops,
        v_write_ops,
        v_failed_ops,
        v_qps,
        v_p50,
        v_p95,
        v_p99,
        v_error_rate,
        v_concurrent_connections,
        v_credits_used,
        v_test_config,
        v_find_max_result
    FROM TEST_RESULTS
    WHERE test_id = p_test_id
    LIMIT 1;
    
    IF (v_test_name IS NULL) THEN
        RETURN OBJECT_CONSTRUCT(
            'error', 'Test not found',
            'test_id', p_test_id
        );
    END IF;
    
    -- Determine load mode
    IF (p_mode IS NOT NULL) THEN
        load_mode := UPPER(p_mode);
    ELSE
        -- Auto-detect from test_config
        load_mode := COALESCE(
            UPPER(v_test_config:template_config:load_mode::VARCHAR),
            UPPER(v_test_config:load_mode::VARCHAR),
            'CONCURRENCY'
        );
    END IF;
    
    -- Build mode-specific prompt
    CASE load_mode
        WHEN 'FIND_MAX_CONCURRENCY' THEN
            prompt := BUILD_FIND_MAX_PROMPT(
                p_test_id, v_test_name, v_test_status, v_table_type, v_warehouse, v_warehouse_size,
                v_duration_seconds, v_total_ops, v_read_ops, v_write_ops, v_failed_ops,
                v_qps, v_p50, v_p95, v_p99, v_error_rate, v_concurrent_connections,
                v_credits_used, v_test_config, v_find_max_result
            );
        WHEN 'QPS' THEN
            prompt := BUILD_QPS_PROMPT(
                p_test_id, v_test_name, v_test_status, v_table_type, v_warehouse, v_warehouse_size,
                v_duration_seconds, v_total_ops, v_read_ops, v_write_ops, v_failed_ops,
                v_qps, v_p50, v_p95, v_p99, v_error_rate, v_concurrent_connections,
                v_credits_used, v_test_config
            );
        ELSE  -- CONCURRENCY (default)
            prompt := BUILD_CONCURRENCY_PROMPT(
                p_test_id, v_test_name, v_test_status, v_table_type, v_warehouse, v_warehouse_size,
                v_duration_seconds, v_total_ops, v_read_ops, v_write_ops, v_failed_ops,
                v_qps, v_p50, v_p95, v_p99, v_error_rate, v_concurrent_connections,
                v_credits_used, v_test_config
            );
    END CASE;
    
    -- Call AI_COMPLETE
    SELECT AI_COMPLETE(
        model => p_model,
        prompt => prompt,
        model_parameters => PARSE_JSON('{"temperature": 0.3, "max_tokens": 4000}')
    ) INTO ai_response;
    
    -- Build result
    result := OBJECT_CONSTRUCT(
        'test_id', p_test_id,
        'test_name', v_test_name,
        'load_mode', load_mode,
        'model', p_model,
        'analysis', ai_response,
        'metadata', OBJECT_CONSTRUCT(
            'table_type', v_table_type,
            'warehouse', v_warehouse,
            'warehouse_size', v_warehouse_size,
            'qps', v_qps,
            'p95_latency_ms', v_p95,
            'error_rate', v_error_rate,
            'duration_seconds', v_duration_seconds
        )
    );
    
    RETURN result;
END;
$$;

-- =============================================================================
-- BUILD_CONCURRENCY_PROMPT: Prompt builder for CONCURRENCY mode
-- =============================================================================

CREATE OR REPLACE FUNCTION BUILD_CONCURRENCY_PROMPT(
    p_test_id VARCHAR,
    p_test_name VARCHAR,
    p_test_status VARCHAR,
    p_table_type VARCHAR,
    p_warehouse VARCHAR,
    p_warehouse_size VARCHAR,
    p_duration_seconds FLOAT,
    p_total_ops INTEGER,
    p_read_ops INTEGER,
    p_write_ops INTEGER,
    p_failed_ops INTEGER,
    p_qps FLOAT,
    p_p50 FLOAT,
    p_p95 FLOAT,
    p_p99 FLOAT,
    p_error_rate FLOAT,
    p_concurrent_connections INTEGER,
    p_credits_used FLOAT,
    p_test_config VARIANT
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Build prompt for CONCURRENCY mode benchmark analysis'
AS
$$
    SELECT CONCAT(
        CASE WHEN UPPER(p_table_type) = 'POSTGRES' 
             THEN 'You are analyzing a **PostgreSQL** CONCURRENCY mode benchmark test.'
             ELSE 'You are analyzing a Snowflake CONCURRENCY mode benchmark test.'
        END,
        '

**Mode: CONCURRENCY (Fixed Workers / Closed Model)**
This test ran a fixed number of concurrent workers for a set duration to measure steady-state performance under constant load.

TEST SUMMARY:
- Test Name: ', COALESCE(p_test_name, 'Unknown'), '
- Test ID: ', COALESCE(p_test_id, 'Unknown'), '
- Status: ', COALESCE(p_test_status, 'Unknown'), '
- Table Type: ', COALESCE(p_table_type, 'Unknown'), '
- ', CASE WHEN UPPER(p_table_type) = 'POSTGRES' THEN 'Instance' ELSE 'Warehouse' END, ': ', COALESCE(p_warehouse, 'Unknown'), ' (', COALESCE(p_warehouse_size, 'Unknown'), ')
- Fixed Workers: ', COALESCE(p_concurrent_connections, 0)::VARCHAR, ' workers
- Duration: ', COALESCE(p_duration_seconds, 0)::VARCHAR, 's

RESULTS:
- Total Operations: ', COALESCE(p_total_ops, 0)::VARCHAR, ' (Reads: ', COALESCE(p_read_ops, 0)::VARCHAR, ', Writes: ', COALESCE(p_write_ops, 0)::VARCHAR, ')
- Failed Operations: ', COALESCE(p_failed_ops, 0)::VARCHAR, ' (', ROUND(COALESCE(p_error_rate, 0), 2)::VARCHAR, '%)
- Throughput: ', ROUND(COALESCE(p_qps, 0), 1)::VARCHAR, ' ops/sec
- Latency: p50=', ROUND(COALESCE(p_p50, 0), 1)::VARCHAR, 'ms, p95=', ROUND(COALESCE(p_p95, 0), 1)::VARCHAR, 'ms, p99=', ROUND(COALESCE(p_p99, 0), 1)::VARCHAR, 'ms',
        CASE WHEN p_credits_used IS NOT NULL AND p_credits_used > 0 AND UPPER(p_table_type) != 'POSTGRES'
             THEN CONCAT('

COST ANALYSIS:
- Warehouse Credits Used: ', ROUND(p_credits_used, 4)::VARCHAR, '
- Cost per 1000 Operations: ', ROUND(p_credits_used / NULLIF(p_total_ops, 0) * 1000, 6)::VARCHAR, ' credits')
             ELSE ''
        END,
        '

Provide analysis structured as:

1. **Performance Summary**: How did the system perform under ', COALESCE(p_concurrent_connections, 0)::VARCHAR, ' concurrent workers?
   - Is throughput (', ROUND(COALESCE(p_qps, 0), 1)::VARCHAR, ' ops/sec) reasonable for this configuration?
   - Are latencies acceptable? (p95=', ROUND(COALESCE(p_p95, 0), 1)::VARCHAR, 'ms, p99=', ROUND(COALESCE(p_p99, 0), 1)::VARCHAR, 'ms)
   - Is error rate (', ROUND(COALESCE(p_error_rate, 0), 2)::VARCHAR, '%) acceptable?

2. **Key Findings**: What stands out in the metrics?
   - Any concerning latency outliers (compare p50 vs p95 vs p99)?
   - Read vs write performance differences?

3. **Bottleneck Analysis**: What is limiting performance?
   - Compute bound (high CPU, low queue times)?
   - Queue bound (high queue times)?
   - Data bound (high bytes scanned)?

4. **Recommendations**: Specific suggestions:
   - Should concurrency be increased or decreased?
   - Would a larger warehouse help?
   - Any query optimization opportunities?

5. **Overall Grade**: A/B/C/D/F based on:
   - **A**: Excellent - low error rate (<0.1%), stable latencies (p99/p50 < 3x), good throughput
   - **B**: Good - acceptable error rate (<1%), moderate latency variance
   - **C**: Fair - noticeable issues but functional
   - **D**: Poor - significant issues, high errors (>5%), or severe latency spikes
   - **F**: Failed - test failed, extremely high errors, or unusable performance

Keep analysis concise and actionable. Use bullet points with specific numbers.'
    )
$$;

-- =============================================================================
-- BUILD_QPS_PROMPT: Prompt builder for QPS mode
-- =============================================================================

CREATE OR REPLACE FUNCTION BUILD_QPS_PROMPT(
    p_test_id VARCHAR,
    p_test_name VARCHAR,
    p_test_status VARCHAR,
    p_table_type VARCHAR,
    p_warehouse VARCHAR,
    p_warehouse_size VARCHAR,
    p_duration_seconds FLOAT,
    p_total_ops INTEGER,
    p_read_ops INTEGER,
    p_write_ops INTEGER,
    p_failed_ops INTEGER,
    p_qps FLOAT,
    p_p50 FLOAT,
    p_p95 FLOAT,
    p_p99 FLOAT,
    p_error_rate FLOAT,
    p_concurrent_connections INTEGER,
    p_credits_used FLOAT,
    p_test_config VARIANT
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Build prompt for QPS mode benchmark analysis'
AS
$$
    SELECT CONCAT(
        CASE WHEN UPPER(p_table_type) = 'POSTGRES' 
             THEN 'You are analyzing a **PostgreSQL** QPS mode benchmark test.'
             ELSE 'You are analyzing a Snowflake QPS mode benchmark test.'
        END,
        '

**Mode: QPS (Auto-Scale to Target Throughput)**
This test dynamically scaled threads to achieve a target throughput. The controller adjusts concurrency based on achieved vs target QPS.

TEST SUMMARY:
- Test Name: ', COALESCE(p_test_name, 'Unknown'), '
- Test ID: ', COALESCE(p_test_id, 'Unknown'), '
- Status: ', COALESCE(p_test_status, 'Unknown'), '
- Table Type: ', COALESCE(p_table_type, 'Unknown'), '
- ', CASE WHEN UPPER(p_table_type) = 'POSTGRES' THEN 'Instance' ELSE 'Warehouse' END, ': ', COALESCE(p_warehouse, 'Unknown'), ' (', COALESCE(p_warehouse_size, 'Unknown'), ')
- Target QPS: ', COALESCE(p_test_config:template_config:target_qps::VARCHAR, p_test_config:target_qps::VARCHAR, 'Unknown'), ' ops/sec
- Achieved QPS: ', ROUND(COALESCE(p_qps, 0), 1)::VARCHAR, ' ops/sec
- Duration: ', COALESCE(p_duration_seconds, 0)::VARCHAR, 's

RESULTS:
- Total Operations: ', COALESCE(p_total_ops, 0)::VARCHAR, ' (Reads: ', COALESCE(p_read_ops, 0)::VARCHAR, ', Writes: ', COALESCE(p_write_ops, 0)::VARCHAR, ')
- Failed Operations: ', COALESCE(p_failed_ops, 0)::VARCHAR, ' (', ROUND(COALESCE(p_error_rate, 0), 2)::VARCHAR, '%)
- Latency: p50=', ROUND(COALESCE(p_p50, 0), 1)::VARCHAR, 'ms, p95=', ROUND(COALESCE(p_p95, 0), 1)::VARCHAR, 'ms, p99=', ROUND(COALESCE(p_p99, 0), 1)::VARCHAR, 'ms',
        CASE WHEN p_credits_used IS NOT NULL AND p_credits_used > 0 AND UPPER(p_table_type) != 'POSTGRES'
             THEN CONCAT('

COST ANALYSIS:
- Warehouse Credits Used: ', ROUND(p_credits_used, 4)::VARCHAR, '
- Cost per 1000 Operations: ', ROUND(p_credits_used / NULLIF(p_total_ops, 0) * 1000, 6)::VARCHAR, ' credits')
             ELSE ''
        END,
        '

Provide analysis structured as:

1. **Target Achievement**: Did the test hit the target?
   - Achieved: ', ROUND(COALESCE(p_qps, 0), 1)::VARCHAR, ' ops/sec
   - If target not achieved, why? (hit max workers? high latency? errors?)
   - Was the target realistic for this configuration?

2. **Auto-Scaler Performance**: How well did the controller perform?
   - Did it effectively scale to meet demand?
   - Were there oscillations or instability?

3. **Latency Analysis**: How did latency behave under auto-scaling?
   - Is variance coming from server execution or app/network layer?
   - Were there periods of high latency during scaling?

4. **Bottleneck Analysis**: What limited target achievement?
   - Max workers reached?
   - Warehouse saturation (queue times)?
   - High error rate?

5. **Recommendations**:
   - Adjust target QPS (higher or lower)?
   - Increase max workers?
   - Larger warehouse needed?

6. **Overall Grade**: A/B/C/D/F based on:
   - **A**: Excellent - achieved >95% of target QPS, stable scaling
   - **B**: Good - achieved >80% of target QPS, minor oscillations
   - **C**: Fair - achieved >60% of target QPS, scaling issues
   - **D**: Poor - achieved <60% of target QPS
   - **F**: Failed - unable to scale, unusable performance

Keep analysis concise and actionable. Use bullet points with specific numbers.'
    )
$$;

-- =============================================================================
-- BUILD_FIND_MAX_PROMPT: Prompt builder for FIND_MAX_CONCURRENCY mode
-- =============================================================================

CREATE OR REPLACE FUNCTION BUILD_FIND_MAX_PROMPT(
    p_test_id VARCHAR,
    p_test_name VARCHAR,
    p_test_status VARCHAR,
    p_table_type VARCHAR,
    p_warehouse VARCHAR,
    p_warehouse_size VARCHAR,
    p_duration_seconds FLOAT,
    p_total_ops INTEGER,
    p_read_ops INTEGER,
    p_write_ops INTEGER,
    p_failed_ops INTEGER,
    p_qps FLOAT,
    p_p50 FLOAT,
    p_p95 FLOAT,
    p_p99 FLOAT,
    p_error_rate FLOAT,
    p_concurrent_connections INTEGER,
    p_credits_used FLOAT,
    p_test_config VARIANT,
    p_find_max_result VARIANT
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Build prompt for FIND_MAX_CONCURRENCY mode benchmark analysis'
AS
$$
    SELECT CONCAT(
        CASE WHEN UPPER(p_table_type) = 'POSTGRES' 
             THEN 'You are analyzing a **PostgreSQL** FIND_MAX_CONCURRENCY benchmark test.'
             ELSE 'You are analyzing a Snowflake FIND_MAX_CONCURRENCY benchmark test.'
        END,
        '

**Mode: FIND_MAX_CONCURRENCY (Step-Load Test)**
This test incrementally increased concurrent workers to find the maximum sustainable concurrency before performance degraded.

**How it works:**
1. Runs a set duration at each concurrency level
2. Measures QPS, latency, and error rate for each step
3. Stops when: latency degrades significantly from baseline, error rate exceeds threshold, or max workers reached
4. Reports the highest "stable" concurrency level

TEST SUMMARY:
- Test Name: ', COALESCE(p_test_name, 'Unknown'), '
- Test ID: ', COALESCE(p_test_id, 'Unknown'), '
- Status: ', COALESCE(p_test_status, 'Unknown'), '
- Table Type: ', COALESCE(p_table_type, 'Unknown'), '
- ', CASE WHEN UPPER(p_table_type) = 'POSTGRES' THEN 'Instance' ELSE 'Warehouse' END, ': ', COALESCE(p_warehouse, 'Unknown'), ' (', COALESCE(p_warehouse_size, 'Unknown'), ')
- Step Configuration: Start=', COALESCE(p_test_config:template_config:start_concurrency::VARCHAR, p_test_config:start_concurrency::VARCHAR, '1'), ', Increment=', COALESCE(p_test_config:template_config:increment::VARCHAR, p_test_config:increment::VARCHAR, '1'), ', Max=', COALESCE(p_test_config:template_config:max_concurrency::VARCHAR, p_test_config:max_concurrency::VARCHAR, 'Unknown'), '

FIND MAX RESULTS:
- **Best Sustainable Concurrency: ', COALESCE(p_find_max_result:final_best_concurrency::VARCHAR, 'Unknown'), ' workers**
- **Best Stable QPS: ', COALESCE(p_find_max_result:final_best_qps::VARCHAR, 'Unknown'), '**
- Baseline p95 Latency: ', COALESCE(p_find_max_result:baseline_p95_latency_ms::VARCHAR, 'Unknown'), 'ms
- Stop Reason: ', COALESCE(p_find_max_result:final_reason::VARCHAR, 'Unknown'), '
- Total Operations: ', COALESCE(p_total_ops, 0)::VARCHAR, ' (Reads: ', COALESCE(p_read_ops, 0)::VARCHAR, ', Writes: ', COALESCE(p_write_ops, 0)::VARCHAR, ')
- Failed Operations: ', COALESCE(p_failed_ops, 0)::VARCHAR, ' (', ROUND(COALESCE(p_error_rate, 0), 2)::VARCHAR, '%)
- Overall Throughput: ', ROUND(COALESCE(p_qps, 0), 1)::VARCHAR, ' ops/sec
- Overall Latency: p50=', ROUND(COALESCE(p_p50, 0), 1)::VARCHAR, 'ms, p95=', ROUND(COALESCE(p_p95, 0), 1)::VARCHAR, 'ms, p99=', ROUND(COALESCE(p_p99, 0), 1)::VARCHAR, 'ms',
        CASE WHEN p_credits_used IS NOT NULL AND p_credits_used > 0 AND UPPER(p_table_type) != 'POSTGRES'
             THEN CONCAT('

COST ANALYSIS:
- Warehouse Credits Used: ', ROUND(p_credits_used, 4)::VARCHAR, '
- Cost per 1000 Operations: ', ROUND(p_credits_used / NULLIF(p_total_ops, 0) * 1000, 6)::VARCHAR, ' credits')
             ELSE ''
        END,
        '

Provide analysis structured as:

1. **Maximum Concurrency Found**: What was the result?
   - Best sustainable concurrency: ', COALESCE(p_find_max_result:final_best_concurrency::VARCHAR, 'Unknown'), ' workers
   - QPS at best concurrency: ', COALESCE(p_find_max_result:final_best_qps::VARCHAR, 'Unknown'), '
   - Why did the test stop? (', COALESCE(p_find_max_result:final_reason::VARCHAR, 'Unknown'), ')

2. **Scaling Curve Analysis**: How did performance scale?
   - Was scaling linear until saturation?
   - At what point did degradation begin?
   - Was the degradation gradual or abrupt?

3. **Latency Analysis**: How did latency respond to load?
   - How much did p95/p99 increase from baseline?
   - Is variance from server or app/network layer?

4. **Throughput Analysis**: How did QPS scale?
   - Did QPS continue to increase with more workers?
   - At what point did QPS plateau or decline?

5. **Bottleneck Identification**: What resource was exhausted?
   - Warehouse compute capacity (queue saturation)?
   - Multi-cluster scaling limits reached?
   - Data access contention?

6. **Recommendations**:
   - What is the sustainable operating point (usually 70-80% of max)?
   - Would a larger warehouse increase max concurrency?
   - Any optimization opportunities?

7. **Overall Grade**: A/B/C/D/F based on:
   - **A**: Excellent - found high max concurrency for ', COALESCE(p_warehouse_size, 'this size'), ', graceful degradation, clear optimal point
   - **B**: Good - reasonable max concurrency, mostly graceful degradation, useful results
   - **C**: Fair - lower than expected max, some abrupt degradation, but usable data
   - **D**: Poor - very low max concurrency, sudden failures, limited insights
   - **F**: Failed - test failed early, no useful max concurrency found

Keep analysis concise and actionable. Use bullet points with specific numbers.'
    )
$$;

-- =============================================================================
-- GET_QUICK_SUMMARY: Quick test summary without AI
-- =============================================================================
-- Returns a structured summary of a test for quick reference.
-- =============================================================================

CREATE OR REPLACE PROCEDURE GET_QUICK_SUMMARY(
    p_test_id VARCHAR
)
RETURNS VARIANT
LANGUAGE SQL
COMMENT = 'Get a quick structured summary of a test without AI analysis.'
AS
$$
DECLARE
    result VARIANT;
BEGIN
    SELECT OBJECT_CONSTRUCT(
        'test_id', test_id,
        'test_name', test_name,
        'status', status,
        'table_type', table_type,
        'warehouse', warehouse,
        'warehouse_size', warehouse_size,
        'load_mode', COALESCE(
            test_config:template_config:load_mode::VARCHAR,
            test_config:load_mode::VARCHAR,
            'CONCURRENCY'
        ),
        'duration_seconds', duration_seconds,
        'performance', OBJECT_CONSTRUCT(
            'qps', ROUND(qps, 1),
            'p50_ms', ROUND(p50_latency_ms, 1),
            'p95_ms', ROUND(p95_latency_ms, 1),
            'p99_ms', ROUND(p99_latency_ms, 1),
            'error_rate_pct', ROUND(error_rate, 2)
        ),
        'operations', OBJECT_CONSTRUCT(
            'total', total_operations,
            'reads', read_operations,
            'writes', write_operations,
            'failed', failed_operations
        ),
        'cost', CASE 
            WHEN warehouse_credits_used IS NOT NULL AND warehouse_credits_used > 0 
            THEN OBJECT_CONSTRUCT(
                'credits_used', ROUND(warehouse_credits_used, 4),
                'cost_per_1000_ops', ROUND(warehouse_credits_used / NULLIF(total_operations, 0) * 1000, 6)
            )
            ELSE NULL
        END,
        'find_max_result', CASE 
            WHEN find_max_result IS NOT NULL 
            THEN OBJECT_CONSTRUCT(
                'best_concurrency', find_max_result:final_best_concurrency,
                'best_qps', find_max_result:final_best_qps,
                'stop_reason', find_max_result:final_reason
            )
            ELSE NULL
        END,
        'timestamps', OBJECT_CONSTRUCT(
            'start_time', start_time,
            'end_time', end_time,
            'created_at', created_at
        )
    ) INTO result
    FROM TEST_RESULTS
    WHERE test_id = p_test_id
    LIMIT 1;
    
    IF (result IS NULL) THEN
        RETURN OBJECT_CONSTRUCT(
            'error', 'Test not found',
            'test_id', p_test_id
        );
    END IF;
    
    RETURN result;
END;
$$;

-- =============================================================================
-- Validation: Show created objects
-- =============================================================================
SHOW PROCEDURES LIKE 'ANALYZE_BENCHMARK' IN SCHEMA UNISTORE_BENCHMARK.TEST_RESULTS;
SHOW PROCEDURES LIKE 'GET_QUICK_SUMMARY' IN SCHEMA UNISTORE_BENCHMARK.TEST_RESULTS;
SHOW USER FUNCTIONS LIKE 'BUILD_%_PROMPT' IN SCHEMA UNISTORE_BENCHMARK.TEST_RESULTS;
