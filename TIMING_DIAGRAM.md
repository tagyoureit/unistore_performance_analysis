# Visual Timing Diagram: Worker Startup to First Query

```text
TIME →

════════════════════════════════════════════════════════════════════════════════
BEFORE _wait_for_start() - Lines 620-649
════════════════════════════════════════════════════════════════════════════════

[HEARTBEAT: STARTING] (100-200ms, SF Query)
    ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│ ⚡ POOL INITIALIZATION (Lines 620-638) - 15-20 seconds                      │
│ ✅ ALREADY OPTIMIZED - Moved before _wait_for_start()                       │
│                                                                               │
│ • Create 100 connections in batches of 8                                    │
│ • Each: TCP + TLS + Snowflake auth                                          │
│ • Progress logged at 10%, 20%, 30%...                                       │
│                                                                               │
│ Backend location: backend/connectors/snowflake_pool.py:151-268              │
│   async def initialize(self):                                               │
│       # Bounded parallelism (max_parallel_creates=8)                        │
│       # Each connection: 150-200ms                                          │
│       # Total for 100 connections: 15-20s                                   │
└─────────────────────────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│ EXECUTOR SETUP (Lines 641-648) - 100-500ms                                  │
│                                                                               │
│ • Create table managers                           <1ms  (CPU)               │
│ • Override pools if needed                        <1ms  (CPU)               │
│ • Detect warehouse name                           <1ms  (CPU)               │
│ • Setup tables (manager.setup())                  ~10ms (mostly CPU)        │
│ • Validate views vs workload                      <1ms  (CPU)               │
│ • └─→ _profile_tables()                           100-500ms                 │
│       └─→ _load_value_pools()                     50-100ms (SF Query)       │
│           • SELECT from TEMPLATE_VALUE_POOLS                                │
│       └─→ MAX(id) per table (if pools exist)      100-200ms/table (SF)     │
│           • SELECT MAX(id_column) FROM table_name                           │
│       └─→ Full profiling (if NO pools)            300-500ms/table (SF)     │
│           • DESCRIBE TABLE                                                  │
│           • SELECT MIN(id), MAX(id), MIN(time), MAX(time)                  │
│                                                                               │
│ Backend location: backend/core/test_executor.py:485-747                     │
└─────────────────────────────────────────────────────────────────────────────┘
    ↓
[HEARTBEAT: READY] (100-200ms, SF Query)
    ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│ _wait_for_start() LOOP (Lines 693-753) - Variable, up to 120s timeout      │
│                                                                               │
│ • Poll RUN_STATUS every 1s                         100-200ms/poll (SF)      │
│ • Poll RUN_CONTROL_EVENTS every 1s                 100-200ms/poll (SF)      │
│ • Send HEARTBEAT every 1s                          100-200ms (SF)           │
│ • Sleep 0.2s between iterations                    200ms (async sleep)     │
│                                                                               │
│ Exit condition: RUN_STATUS.status = 'RUNNING'                               │
│ ✅ Returns True when orchestrator signals START                             │
└─────────────────────────────────────────────────────────────────────────────┘
    ↓
════════════════════════════════════════════════════════════════════════════════
LINE 770: _wait_for_start() RETURNS TRUE ← START MEASURING FROM HERE
════════════════════════════════════════════════════════════════════════════════
    ↓
[Log: "_wait_for_start returned True..."] <1ms
    ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│ Line 772: HEARTBEAT RUNNING - 100-200ms                                     │
│ ⚠️  SLOW: Snowflake INSERT query                                            │
│                                                                               │
│ await _safe_heartbeat("RUNNING")                                            │
│   ↓                                                                          │
│   INSERT INTO WORKER_HEARTBEATS (                                           │
│       run_id, worker_id, status='RUNNING', phase=...,                       │
│       active_connections=0, target_connections=..., ...                     │
│   )                                                                          │
│                                                                               │
│ ❌ Cannot eliminate: Orchestrator needs this signal                         │
│ ✅ Could parallelize with worker spawn                                      │
└─────────────────────────────────────────────────────────────────────────────┘
    ↓
[Lines 774-777: Extract table config] <1ms (CPU)
    ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│ Lines 779-800: INSERT TEST_RESULTS - 100-200ms                              │
│ ⚠️  SLOW: Snowflake INSERT query                                            │
│                                                                               │
│ await results_store.insert_test_start(                                      │
│     test_id=..., run_id=..., scenario=..., table_name=...,                 │
│     warehouse=..., template_id=..., query_tag=...                           │
│ )                                                                            │
│   ↓                                                                          │
│   INSERT INTO TEST_RESULTS (                                                │
│       test_id, run_id, test_name, scenario_json, table_name,               │
│       table_type, warehouse, template_id, query_tag, ...                    │
│   ) VALUES (...)                                                             │
│                                                                               │
│ ❌ Cannot eliminate: Core result tracking                                   │
│ ✅ OPTIMIZATION: Could defer/batch with first metrics snapshot              │
│    • Start workers FIRST (line 877)                                         │
│    • Insert TEST_RESULTS in parallel (background task)                      │
│    • Save ~100-200ms by parallelizing with worker startup                   │
└─────────────────────────────────────────────────────────────────────────────┘
    ↓
[Lines 802-813: Update executor state, initialize variables] <2ms (CPU)
    ↓
[Lines 815-874: Define helper functions] <1ms (CPU)
    • _prune_workers()
    • _spawn_one()
    • _scale_to()
    • _stop_all()
    • _transition_to_measurement()
    ↓
[Line 876: Log "Scaling to initial target..."] <1ms (CPU)
    ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│ ⚡ Line 877: await _scale_to(current_target, warmup=True) - ~10ms           │
│ ✅ FAST: Pure CPU, no I/O                                                   │
│                                                                               │
│ For 100 workers:                                                            │
│   async with scale_lock:                           <0.1ms                   │
│   _prune_workers()                                 <0.1ms (no-op first time)│
│   Validate target                                  <0.1ms                   │
│   Update current_target                            <0.1ms                   │
│   for _ in range(100):                                                      │
│       await _spawn_one(warmup=True)                ~0.1ms each              │
│           worker_id = next_worker_id++                                      │
│           stop_signal = asyncio.Event()                                     │
│           task = asyncio.create_task(                                       │
│               executor._controlled_worker(...)                              │
│           )                                                                  │
│           worker_tasks[wid] = (task, stop_signal)                           │
│                                                                               │
│ Total: 100 workers × 0.1ms = ~10ms                                          │
│                                                                               │
│ ⚠️  IMPORTANT: Tasks are CREATED but not yet EXECUTING                      │
│    Actual query execution happens when event loop schedules them            │
└─────────────────────────────────────────────────────────────────────────────┘
    ↓
[Line 878: Log "Initial scale complete..."] <1ms (CPU)
    ↓
[Lines 880-911: More initialization] <2ms (CPU)
    ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│ Line 914+: Enter main event loop (BACKGROUND)                               │
│                                                                               │
│ while True:                                                                 │
│     • Check health every iteration                 <0.1ms                   │
│     • Poll RUN_CONTROL_EVENTS every 1s             100-200ms (SF)           │
│     • Reconcile RUN_STATUS every 5s                100-200ms (SF)           │
│     • Send heartbeat every 1s                      100-200ms (SF)           │
│     • Sleep 0.05s                                  50ms                     │
│                                                                               │
│ Runs in parallel with worker tasks                                          │
└─────────────────────────────────────────────────────────────────────────────┘

        ║
        ║  ← Event loop now schedules worker tasks to run
        ║
        ▼

════════════════════════════════════════════════════════════════════════════════
WORKER TASK EXECUTION (PARALLEL) - First Iteration
════════════════════════════════════════════════════════════════════════════════

┌─────────────────────────────────────────────────────────────────────────────┐
│ executor._controlled_worker(worker_id=0, warmup=True, stop_signal)          │
│ Location: backend/core/test_executor.py:2727-2774                           │
│                                                                               │
│ [Lines 2740-2742: Initialize worker state] <0.1ms (CPU)                     │
│     operations_executed = 0                                                  │
│     target_ops = scenario.operations_per_connection                         │
│     effective_warmup = True                                                  │
│                                                                               │
│ [Lines 2745-2755: Enter worker loop]                                        │
│     while not self._stop_event.is_set():                                    │
│         if stop_signal.is_set(): break                                      │
│         if target_ops and operations_executed >= target_ops: break          │
│                                                                               │
│         effective_warmup = warmup and not self._measurement_active          │
│                                                                               │
│         ┌───────────────────────────────────────────────────────────────┐   │
│         │ Line 2756: await self._execute_operation(worker_id, True)    │   │
│         └───────────────────────────────────────────────────────────────┘   │
│                 ↓                                                            │
└─────────────────┼────────────────────────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│ _execute_operation(worker_id=0, warmup=True)                                │
│ Location: backend/core/test_executor.py:2813-2854                           │
│                                                                               │
│ [Lines 2813-2820: Dispatch based on workload] <0.1ms (CPU)                  │
│     workload = scenario.workload_type                                        │
│     if workload == WorkloadType.READ_ONLY:                                  │
│         await self._execute_read(worker_id, warmup)  ← CALL THIS            │
│     elif workload == WorkloadType.WRITE_ONLY:                               │
│         await self._execute_write(...)                                      │
│     # ... other workload types ...                                          │
│                                                                               │
└─────────────────┼────────────────────────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│ _execute_read(worker_id=0, warmup=True)                                     │
│ Location: backend/core/test_executor.py:2855-3124                           │
│                                                                               │
│ ┌─────────────────────────────────────────────────────────────────────────┐ │
│ │ PHASE 1: Pre-Query Setup (CPU only) - <1ms                              │ │
│ ├─────────────────────────────────────────────────────────────────────────┤ │
│ │ [Lines 2857-2866: Initialize timing + variables] <0.1ms                 │ │
│ │     start_wall = datetime.now(UTC)                                      │ │
│ │     start_perf = time.perf_counter()                                    │ │
│ │     epoch_at_start = int(self._metrics_epoch)                           │ │
│ │     query = ""                                                           │ │
│ │     params = None                                                        │ │
│ │     query_kind = "RANGE_SCAN"                                           │ │
│ │     full_name = None                                                     │ │
│ │     pool = None                                                          │ │
│ └─────────────────────────────────────────────────────────────────────────┘ │
│         ↓                                                                    │
│ ┌─────────────────────────────────────────────────────────────────────────┐ │
│ │ PHASE 2: Query Generation (CPU + memory) - <1ms                         │ │
│ ├─────────────────────────────────────────────────────────────────────────┤ │
│ │ [Lines 2868-2873: Select table + pool] <0.1ms                           │ │
│ │     manager = random.choice(self.table_managers)                        │ │
│ │     full_name = manager.get_full_table_name()                           │ │
│ │     pool = getattr(manager, "pool", None)                               │ │
│ │                                                                           │ │
│ │ [Lines 2875-2888: Get profile + decide query type] <0.1ms               │ │
│ │     batch_size = scenario.read_batch_size                               │ │
│ │     state = self._table_state[full_name]                                │ │
│ │     profile = state.profile                                              │ │
│ │     point_lookup_ratio = scenario.point_lookup_ratio                    │ │
│ │     do_point_lookup = random.random() < point_lookup_ratio              │ │
│ │                                                                           │ │
│ │ IF point lookup possible:                                                │ │
│ │   [Lines 2891-2916: Build point lookup query] <0.5ms                    │ │
│ │       pooled = self._next_from_pool(worker_id, "KEY", id_column)        │ │
│ │       if pooled is None:                                                 │ │
│ │           row_id = random.randint(id_min, id_max)                       │ │
│ │       else:                                                              │ │
│ │           row_id = pooled  ← From pre-computed value pool               │ │
│ │       select_list = self._select_list_sql()                             │ │
│ │       id_col_quoted = self._quote_column(id_column, pool)               │ │
│ │       query = "SELECT {select_list} FROM {table} WHERE {id_col} = ?"    │ │
│ │       params = [row_id]                                                  │ │
│ │       query_kind = "POINT_LOOKUP"                                       │ │
│ │   OR:                                                                    │ │
│ │   [Line 2918: Build range scan query] <0.5ms                            │ │
│ │       query, params = self._build_range_scan(...)                       │ │
│ └─────────────────────────────────────────────────────────────────────────┘ │
│         ↓                                                                    │
│ ┌─────────────────────────────────────────────────────────────────────────┐ │
│ │ ⚡⚡⚡ PHASE 3: ACTUAL SNOWFLAKE QUERY - 1-100ms+ ⚡⚡⚡                   │ │
│ ├─────────────────────────────────────────────────────────────────────────┤ │
│ │ [Lines 2924-2933: Execute query via pool]                               │ │
│ │                                                                           │ │
│ │     if hasattr(pool, "execute_query_with_info"):                        │ │
│ │         # Annotate query with kind marker for QUERY_HISTORY             │ │
│ │         query = self._annotate_query_for_sf_kind(query, query_kind)    │ │
│ │         # Example: "/*UB_KIND=POINT_LOOKUP*/ SELECT ..."                │ │
│ │                                                                           │ │
│ │         ┌───────────────────────────────────────────────────────────┐   │ │
│ │         │ ⚡⚡⚡ THIS IS THE FIRST BENCHMARK QUERY ⚡⚡⚡             │   │ │
│ │         ├───────────────────────────────────────────────────────────┤   │ │
│ │         │ result, info = await pool.execute_query_with_info(        │   │ │
│ │         │     query, params=params, fetch=True                      │   │ │
│ │         │ )                                                          │   │ │
│ │         │                                                            │   │ │
│ │         │ What happens inside:                                      │   │ │
│ │         │   1. Get connection from pool          ~0.1ms (CPU)       │   │ │
│ │         │   2. Execute SQL over network          1-100ms+ (I/O)     │   │ │
│ │         │      • Network RTT                                         │   │ │
│ │         │      • Snowflake parsing                                  │   │ │
│ │         │      • Query execution                                    │   │ │
│ │         │      • Result serialization                               │   │ │
│ │         │   3. Fetch results                     (included in #2)   │   │ │
│ │         │   4. Return connection to pool         ~0.1ms (CPU)       │   │ │
│ │         │                                                            │   │ │
│ │         │ Timing breakdown (example):                               │   │ │
│ │         │   • Cache hit:        1-5ms                               │   │ │
│ │         │   • Warm cache:       10-50ms                             │   │ │
│ │         │   • Cold cache:       50-200ms                            │   │ │
│ │         │   • Complex query:    100ms+                              │   │ │
│ │         └───────────────────────────────────────────────────────────┘   │ │
│ │                                                                           │ │
│ │         sf_query_id = str(info.get("query_id") or "")                   │ │
│ │         sf_rowcount = info.get("rowcount")                               │ │
│ │         sf_execution_ms = info.get("total_elapsed_time_ms")             │ │
│ └─────────────────────────────────────────────────────────────────────────┘ │
│         ↓                                                                    │
│ ┌─────────────────────────────────────────────────────────────────────────┐ │
│ │ PHASE 4: Post-Query Metrics Update (CPU + lock) - <1ms                  │ │
│ ├─────────────────────────────────────────────────────────────────────────┤ │
│ │ [Lines 2940-2987: Update metrics]                                       │ │
│ │     end_wall = datetime.now(UTC)                                        │ │
│ │     app_elapsed_ms = (time.perf_counter() - start_perf) * 1000.0       │ │
│ │                                                                           │ │
│ │     async with self._metrics_lock:                                      │ │
│ │         if epoch == epoch_at_start:                                     │ │
│ │             self.metrics.total_operations += 1                          │ │
│ │             self.metrics.successful_operations += 1                     │ │
│ │             self.metrics.read_metrics.count += 1                        │ │
│ │             self.metrics.rows_read += len(result)                       │ │
│ │             self._latencies_ms.append(app_elapsed_ms)                   │ │
│ │             # ... more metric updates ...                               │ │
│ │                                                                           │ │
│ │ [Lines 2979-2986: Summary breakdowns (non-warmup only)]                 │ │
│ │     if not warmup:                                                       │ │
│ │         self._point_lookup_count += 1  # or range_scan_count           │ │
│ │         self._lat_read_ms.append(duration_ms)                           │ │
│ │         self._lat_by_kind_ms[query_kind].append(duration_ms)           │ │
│ │                                                                           │ │
│ │ [Lines 2988-3027: Capture query execution record]                       │ │
│ │     if warmup or scenario.collect_query_history:                        │ │
│ │         self._query_execution_records.append(                           │ │
│ │             _QueryExecutionRecord(                                      │ │
│ │                 execution_id=uuid4(),                                   │ │
│ │                 query_id=sf_query_id,                                   │ │
│ │                 query_text=query,                                       │ │
│ │                 duration_ms=app_elapsed_ms,                             │ │
│ │                 sf_execution_ms=sf_execution_ms,                        │ │
│ │                 # ... more fields ...                                   │ │
│ │             )                                                            │ │
│ │         )                                                                │ │
│ └─────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
        ↓
    [Return to worker loop]
        ↓
    [operations_executed += 1]
        ↓
    [Think time sleep if configured]
        ↓
    [Next iteration...]

════════════════════════════════════════════════════════════════════════════════
END OF FIRST QUERY EXECUTION
════════════════════════════════════════════════════════════════════════════════
```text

---

## SUMMARY: Latency Breakdown (After _wait_for_start() returns)

```text
┌────────────────────────────────────────────────────┬──────────┬──────────┐
│ Operation                                          │ Type     │ Timing   │
├────────────────────────────────────────────────────┼──────────┼──────────┤
│ Logging, variables, function defs                  │ CPU      │ ~3ms     │
│ Heartbeat RUNNING (INSERT WORKER_HEARTBEATS)       │ SF Query │ 100-200ms│
│ Insert TEST_RESULTS (INSERT TEST_RESULTS)          │ SF Query │ 100-200ms│
│ Worker task spawning (100 workers @ 0.1ms each)    │ CPU      │ ~10ms    │
│ First query pre-processing (random, dict lookups)  │ CPU      │ <1ms     │
│ **FIRST SNOWFLAKE QUERY**                          │ SF Query │ 1-100ms+ │
├────────────────────────────────────────────────────┼──────────┼──────────┤
│ TOTAL (before first query completes)               │          │ 213-313ms│
└────────────────────────────────────────────────────┴──────────┴──────────┘
```

## Breakdown

- **Fast operations (CPU):** ~14ms
- **Slow operations (required SF queries):** ~200-400ms
  - Heartbeat RUNNING: 100-200ms
  - Insert TEST_RESULTS: 100-200ms
- **First benchmark query:** Variable (1-100ms+)

### **WHERE ARE THE 7-8 SECONDS?**

They are **NOT** in the path after `_wait_for_start()` returns!

The overhead after `_wait_for_start()` is only **~213-313ms** before the first query.

The 7-8 seconds must be in:
1. **Pool initialization** (15-20s) - but this was moved BEFORE
   `_wait_for_start()` on line 620
2. **The `_wait_for_start()` loop itself** - waiting for orchestrator to set RUNNING
3. **Before line 620** - if there's slow setup code earlier in the function

**Recommendation:** Add timing instrumentation to measure:
- Time from function entry to line 620 (pool init start)
- Pool init duration (lines 620-638)
- Executor setup duration (lines 641-648)
- `_wait_for_start()` duration (lines 693-769)
- Time from line 770 to first query (lines 770-2933)
