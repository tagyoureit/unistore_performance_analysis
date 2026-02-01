# Complete Execution Timeline: _wait_for_start() → First Benchmark Query

**Objective:** Trace EVERY operation from when `_wait_for_start()` returns True (line 770) to the first actual benchmark query hitting Snowflake.

---

## PART 1: Pre-_wait_for_start() Setup (Context)

### Lines 620-649: Pool Initialization (BEFORE _wait_for_start)
**CRITICAL OPTIMIZATION ALREADY IN PLACE:**
```python
# Lines 620-638: Initialize benchmark pool BEFORE waiting for START
if not is_postgres:
    pool_override = getattr(executor, "_snowflake_pool_override", None)
    if pool_override is not None:
        await _safe_heartbeat("INITIALIZING")
        logger.info("Initializing benchmark connection pool...")
        await pool_override.initialize()  # ⚠️ SLOW: 15-20s for 100 connections
        logger.info("Benchmark connection pool initialized")
        health.record_success()
```

**Operation Type:** Snowflake connections (VERY SLOW)
- **Timing:** 15-20 seconds for 100 connections
- **Location:** `backend/connectors/snowflake_pool.py:151-268` (`initialize()`)
- **Details:**
  - Creates `pool_size` connections in parallel (max_parallel_creates=8)
  - Each connection: TCP handshake + TLS + Snowflake auth
  - Batched in groups of 8 to avoid overwhelming Snowflake
  - Progress logged at 10/20/30...% intervals
  - **NOTE:** This is already optimized! Moved before _wait_for_start() to avoid blocking warmup

### Lines 641-648: Executor Setup (BEFORE _wait_for_start)
```python
# Lines 641-648: Setup executor (table profiling, value pools, etc.)
logger.info("Setting up executor...")
ok = await executor.setup()  # ⚠️ POTENTIALLY SLOW
if not ok:
    last_error = str(getattr(executor, "_setup_error", "Setup failed"))
    logger.error("Executor setup failed: %s", last_error)
    await _safe_heartbeat("DEAD")
    return 1
```

**Operation Type:** Mixed (Snowflake queries + CPU)
- **Timing:** 100-500ms (depends on whether value pools exist)
- **Location:** `backend/core/test_executor.py:485-580` (`setup()`)
- **Key sub-operations:**
  1. Create table managers (CPU, ~1ms)
  2. Override pools if needed (CPU, ~1ms)
  3. Detect benchmark warehouse name (CPU, ~1ms)
  4. Setup tables in parallel (`manager.setup()` - mostly CPU unless creating tables)
  5. Validate views vs workload (CPU, ~1ms)
  6. **Profile tables** (lines 574-575) → calls `_profile_tables()`

### Lines 582-701: Table Profiling Details
**Location:** `backend/core/test_executor.py:582-747` (`_profile_tables()`)

**Fast Path (AI value pools exist):**
```python
# Lines 593-701: If value pools loaded, skip profiling
await self._load_value_pools()  # Query TEMPLATE_VALUE_POOLS
has_value_pools = bool(getattr(self, "_value_pools", None))
if has_value_pools:
    logger.info("⏩ Skipping table profiling - using pre-computed AI value pools")
    # Still needs MAX(id) query to avoid collision
    # Lines 671-693: One MAX(id_col) query per table
    max_query = f"SELECT MAX({id_col_expr}) FROM {full_name}"  # ⚠️ SLOW: 100-200ms per table
```
- **Timing (with value pools):** 100-200ms per table (just MAX query)
- **Queries:** 1 MAX query per table

**Slow Path (no value pools):**
```python
# Lines 703-747: Full profiling without value pools
for manager in self.table_managers:
    if is_postgres:
        profile = await profile_postgres_table(pool, full_name)  # Multiple queries
    else:
        profile = await profile_snowflake_table(pool, full_name)  # DESCRIBE + MIN/MAX
```
- **Timing (without value pools):** 300-500ms per table
- **Queries:** DESCRIBE TABLE + SELECT MIN(id), MAX(id), MIN(time), MAX(time)

---

## PART 2: The Critical Path After _wait_for_start() Returns True

### Line 770: _wait_for_start() Returns True
```python
# Line 770: _wait_for_start returns True
# Status transitions: READY → orchestrator sets RUN_STATUS to RUNNING → returns True
```
**Operation Type:** Status check (already complete)
- **Timing:** 0ms (just a return statement)
- **Line:** 770

### Line 771: Log Message
```python
logger.info("_wait_for_start returned True - starting benchmark execution!")
```
**Operation Type:** CPU (logging)
- **Timing:** <1ms
- **Line:** 771

### Line 772: Heartbeat to RUNNING
```python
await _safe_heartbeat("RUNNING")
```
**Operation Type:** Snowflake INSERT query
- **Timing:** ~100-200ms
- **Location:** lines 585-617 (`_safe_heartbeat()`)
- **Query:** `INSERT INTO WORKER_HEARTBEATS ...` via `results_store.upsert_worker_heartbeat()`
- **Line:** 772

### Lines 774-777: Table Config Extraction
```python
# Line 774: Extract table config (CPU only)
table_cfg = scenario.table_configs[0] if scenario.table_configs else None
# Lines 775-777: Comment about warehouse_config_snapshot (no code)
```
**Operation Type:** CPU (dict access)
- **Timing:** <1ms
- **Lines:** 774-777

### Lines 779-800: Insert TEST_RESULTS Row
```python
# Lines 779-800: Insert test start record
try:
    await results_store.insert_test_start(
        test_id=str(executor.test_id),
        run_id=cfg.run_id,
        # ... many fields ...
        warehouse_config_snapshot=None,  # Already captured by orchestrator
        query_tag=benchmark_query_tag_base,
    )
    health.record_success()
except Exception as exc:
    logger.error("Failed to insert TEST_RESULTS row: %s", exc)
    last_error = str(exc)
    await _safe_heartbeat("DEAD")
    return 1
```
**Operation Type:** Snowflake INSERT query
- **Timing:** ~100-200ms
- **Location:** `backend/core/results_store.py` (`insert_test_start()`)
- **Query:** `INSERT INTO TEST_RESULTS (test_id, run_id, ...) VALUES (...)`
- **Lines:** 779-800

### Lines 802-804: Update Executor State
```python
executor.status = TestStatus.RUNNING
executor.start_time = datetime.now(UTC)
executor.metrics.timestamp = executor.start_time
```
**Operation Type:** CPU (object updates)
- **Timing:** <1ms
- **Lines:** 802-804

### Line 806: Check Phase for Measurement
```python
start_in_measurement = run_status_phase == "MEASUREMENT"
```
**Operation Type:** CPU (string comparison)
- **Timing:** <1ms
- **Line:** 806

### Lines 808-809: Comment About Pool Init
```python
# Lines 808-809: Comment noting pool init already happened
# NOTE: Pool initialization and executor.setup() now happen BEFORE _wait_for_start()
```
**Operation Type:** None (just a comment)
- **Timing:** 0ms
- **Lines:** 808-809

### Lines 811-813: Worker Data Structures
```python
worker_tasks: dict[int, tuple[asyncio.Task, asyncio.Event]] = {}
next_worker_id = 0
scale_lock = asyncio.Lock()
```
**Operation Type:** CPU (object creation)
- **Timing:** <1ms
- **Lines:** 811-813

### Lines 815-818: Define _prune_workers()
```python
def _prune_workers() -> None:
    for wid, (task, _) in list(worker_tasks.items()):
        if task.done():
            worker_tasks.pop(wid, None)
```
**Operation Type:** CPU (function definition only, not called yet)
- **Timing:** <1ms
- **Lines:** 815-818

### Lines 820-830: Define _spawn_one()
```python
# Lines 820-830: Define _spawn_one function
async def _spawn_one(*, warmup: bool) -> None:
    nonlocal next_worker_id
    wid = int(next_worker_id)
    next_worker_id += 1
    stop_signal = asyncio.Event()
    task = asyncio.create_task(
        executor._controlled_worker(
            worker_id=wid, warmup=warmup, stop_signal=stop_signal
        )
    )
    worker_tasks[wid] = (task, stop_signal)
```
**Operation Type:** CPU (function definition only, not called yet)
- **Timing:** <1ms (definition only)
- **Details:** When actually called, creates asyncio Task (CPU, ~0.1ms per task)
- **Lines:** 820-830

### Lines 832-857: Define _scale_to()
```python
# Lines 832-857: Define _scale_to function
async def _scale_to(target: int, *, warmup: bool) -> None:
    nonlocal current_target
    async with scale_lock:
        _prune_workers()
        target = max(0, int(target))
        if target > per_worker_cap:
            logger.warning("Target %d exceeds per-worker cap %d; clamping", target, per_worker_cap)
            target = int(per_worker_cap)
        current_target = target
        executor._target_workers = int(target)
        running = len(worker_tasks)
        if running < target:
            spawn_n = target - running
            for _ in range(spawn_n):
                await _spawn_one(warmup=warmup)  # ⚠️ This spawns worker tasks
        elif running > target:
            stop_n = running - target
            stop_ids = sorted(worker_tasks.keys(), reverse=True)[:stop_n]
            for wid in stop_ids:
                _, stop_signal = worker_tasks.get(wid, (None, None))
                if isinstance(stop_signal, asyncio.Event):
                    stop_signal.set()
```
**Operation Type:** CPU (function definition only, not called yet)
- **Timing:** <1ms (definition only)
- **When called:** ~0.1ms per worker task created (CPU only - task creation)
- **Lines:** 832-857

### Lines 859-874: Define _stop_all()
```python
# Lines 859-874: Define _stop_all function
async def _stop_all(*, timeout_seconds: float) -> None:
    async with scale_lock:
        for _, stop_signal in worker_tasks.values():
            stop_signal.set()
    tasks = [t for t, _ in worker_tasks.values()]
    if not tasks:
        return
    try:
        await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True),
            timeout=timeout_seconds,
        )
    except asyncio.TimeoutError:
        logger.warning("Timed out draining workers after %.1fs", timeout_seconds)
        for t in tasks:
            t.cancel()
```
**Operation Type:** CPU (function definition only, not called yet)
- **Timing:** <1ms
- **Lines:** 859-874

### Line 876: Log Initial Scaling
```python
logger.info("Scaling to initial target=%d workers (phase=%s)", current_target, current_phase)
```
**Operation Type:** CPU (logging)
- **Timing:** <1ms
- **Line:** 876

### ⚠️ Line 877: **FIRST CALL TO _scale_to()** - WHERE WORKERS ARE SPAWNED
```python
await _scale_to(current_target, warmup=(current_phase == "WARMUP"))
```
**Operation Type:** CPU (worker task creation)
- **Timing:** ~0.1ms per worker task (e.g., 10ms for 100 workers)
- **What it does:**
  1. Acquires `scale_lock` (CPU, <0.1ms)
  2. Calls `_prune_workers()` (CPU, <0.1ms - no-op on first call)
  3. Validates target (CPU, <0.1ms)
  4. Updates `current_target` and `executor._target_workers` (CPU, <0.1ms)
  5. **Spawns N worker tasks via `_spawn_one()`:** (CPU, ~0.1ms each)
     - Each spawn creates:
       - Worker ID increment (CPU)
       - `asyncio.Event()` for stop signal (CPU)
       - `asyncio.create_task()` for `executor._controlled_worker()` (CPU)
       - Adds to `worker_tasks` dict (CPU)
  6. Releases lock (CPU, <0.1ms)
- **NO I/O YET** - tasks are created but not yet running
- **Line:** 877

### Line 878: Log Scale Complete
```python
logger.info("Initial scale complete, entering main event loop")
```
**Operation Type:** CPU (logging)
- **Timing:** <1ms
- **Line:** 878

### Lines 880-885: Initialize Main Loop Variables
```python
last_heartbeat = time.monotonic()
last_reconcile = time.monotonic()
last_event_poll = 0.0
stop_requested = False
drain_timeout = 120.0
exit_status = TestStatus.COMPLETED
```
**Operation Type:** CPU (variable assignments)
- **Timing:** <1ms
- **Lines:** 880-885

### Lines 887-911: Define _transition_to_measurement()
```python
# Lines 887-911: Define _transition_to_measurement function
async def _transition_to_measurement() -> None:
    await executor._transition_to_measurement_phase()
    async with executor._metrics_lock:
        executor._metrics_epoch += 1
        executor._measurement_active = True
        executor.metrics = Metrics()
        executor.metrics.timestamp = datetime.now(UTC)
        executor._measurement_start_time = datetime.now(UTC)
        # ... reset various metrics counters ...
```
**Operation Type:** CPU (function definition only)
- **Timing:** <1ms
- **Lines:** 887-911

### Lines 909-911: Conditional Measurement Transition
```python
if start_in_measurement:
    await _transition_to_measurement()
    current_phase = "MEASUREMENT"
```
**Operation Type:** CPU (metrics reset) - only if starting in MEASUREMENT phase
- **Timing:** <1ms (unlikely to execute - usually starts in WARMUP)
- **Lines:** 909-911

### Line 914+: Enter Main Event Loop
```python
try:
    while True:  # Main control loop
        # Lines 915-918: Health check
        # Lines 920-940: Event polling (every 1 second)
        # Lines 993-1018: Reconciliation (every 5 seconds)
        # Lines 1033-1035: Heartbeat (every 1 second)
        # Line 1040: Sleep 0.05s
```
**Operation Type:** Async event loop
- **Timing:** Loop continues until stop_requested
- **Lines:** 914+

---

## PART 3: Worker Task Execution (Parallel with Main Loop)

### Worker Task Start: executor._controlled_worker()
**Location:** `backend/core/test_executor.py:2727-2774` (`_controlled_worker()`)

```python
# Line 2727-2733: Worker function signature
async def _controlled_worker(
    self,
    *,
    worker_id: int,
    warmup: bool,
    stop_signal: asyncio.Event,
) -> None:
```

**Worker Initialization (CPU only):**
```python
# Lines 2740-2742: Initialize worker state
operations_executed = 0
target_ops = self.scenario.operations_per_connection
effective_warmup = bool(warmup)
```
- **Timing:** <0.1ms per worker
- **Operation Type:** CPU (variable initialization)

### Worker Main Loop: First Iteration
```python
# Lines 2745-2766: Worker main loop
try:
    while not self._stop_event.is_set():
        if stop_signal.is_set():
            break
        
        # Check operation limit
        if target_ops and operations_executed >= target_ops:
            break
        
        # Update effective_warmup based on measurement_active flag
        effective_warmup = bool(warmup) and not bool(self._measurement_active)
        
        # ⚠️ FIRST ACTUAL QUERY HAPPENS HERE
        await self._execute_operation(worker_id, effective_warmup)
        
        operations_executed += 1
        
        # Think time (if configured)
        if self.scenario.think_time_ms > 0:
            await asyncio.sleep(self.scenario.think_time_ms / 1000.0)
```

### ⚠️ FIRST BENCHMARK QUERY: _execute_operation()
**Location:** `backend/core/test_executor.py:2813-2854` (`_execute_operation()`)

```python
# Line 2813: _execute_operation called
async def _execute_operation(self, worker_id: int, warmup: bool = False):
    workload = self.scenario.workload_type
    
    # Determine operation type based on workload
    if workload == WorkloadType.READ_ONLY:
        await self._execute_read(worker_id, warmup)  # ⚠️ FIRST QUERY HERE
    elif workload == WorkloadType.WRITE_ONLY:
        await self._execute_write(worker_id, warmup)
    # ... other workload types ...
```

### ⚠️ FIRST SNOWFLAKE QUERY: _execute_read()
**Location:** `backend/core/test_executor.py:2855-3124` (`_execute_read()`)

**Pre-query Setup (CPU):**
```python
# Lines 2857-2866: Initialize timing + variables
start_wall = datetime.now(UTC)
start_perf = time.perf_counter()
epoch_at_start = int(self._metrics_epoch)
query: str = ""
params: Optional[list] = None
query_kind = "RANGE_SCAN"
full_name: str | None = None
pool = None
```
- **Timing:** <0.1ms (CPU only)

**Query Generation (CPU + memory lookups):**
```python
# Lines 2868-2920: Build query
try:
    import random
    manager = random.choice(self.table_managers)  # CPU
    full_name = manager.get_full_table_name()  # CPU
    pool = getattr(manager, "pool", None)  # CPU
    
    batch_size = self.scenario.read_batch_size
    state = self._table_state[full_name]  # CPU
    profile = state.profile  # CPU
    
    # Decide point lookup vs range scan
    point_lookup_ratio = self.scenario.point_lookup_ratio
    do_point_lookup = random.random() < point_lookup_ratio
    
    if do_point_lookup and profile and profile.id_column:
        # Lines 2904-2916: Build point lookup query
        pooled = self._next_from_pool(worker_id, "KEY", profile.id_column)
        if pooled is None:
            row_id = random.randint(profile.id_min, profile.id_max)
        else:
            row_id = pooled  # From pre-computed value pool
        
        select_list = self._select_list_sql()
        id_col_quoted = self._quote_column(profile.id_column, pool)
        query = f"SELECT {select_list} FROM {full_name} WHERE {id_col_quoted} = ?"
        params = [row_id]
        query_kind = "POINT_LOOKUP"
    else:
        # Line 2918: Build range scan query
        query, params = self._build_range_scan(
            full_name, profile, batch_size, worker_id=worker_id, pool=pool
        )
```
- **Timing:** <1ms (CPU + dict lookups + value pool access)

**⚠️ ACTUAL SNOWFLAKE QUERY EXECUTION:**
```python
# Lines 2924-2939: Execute query
sf_execution_ms: Optional[float] = None
if pool is not None:
    if hasattr(pool, "execute_query_with_info"):
        # ⚠️⚠️⚠️ THIS IS THE FIRST ACTUAL SNOWFLAKE QUERY ⚠️⚠️⚠️
        query = self._annotate_query_for_sf_kind(query, query_kind)
        result, info = await pool.execute_query_with_info(
            query, params=params, fetch=True
        )
        sf_query_id = str(info.get("query_id") or "")
        sf_rowcount = info.get("rowcount")
        sf_execution_ms = info.get("total_elapsed_time_ms")
    else:
        # PostgreSQL path
        result = await pool.fetch_all(query)
```
- **Timing:** 1-100ms (depends on query complexity, data size, cache hits)
- **Operation Type:** Snowflake query execution
  - Connection checkout from pool (if needed) - CPU ~0.1ms
  - SQL execution over network - I/O 1-100ms+
  - Result fetch - I/O
  - Connection return to pool - CPU ~0.1ms

**Post-query Metrics Update:**
```python
# Lines 2940-2987: Update metrics
end_wall = datetime.now(UTC)
app_elapsed_ms = (time.perf_counter() - start_perf) * 1000.0

async with self._metrics_lock:
    if int(self._metrics_epoch) == int(epoch_at_start):
        self.metrics.total_operations += 1
        self.metrics.successful_operations += 1
        self.metrics.read_metrics.count += 1
        # ... more metric updates ...
        self._latencies_ms.append(duration_ms)
```
- **Timing:** <1ms (CPU + lock acquisition)

---

## SUMMARY: Complete Timeline from _wait_for_start() → First Query

| Step | Line(s) | Operation | Type | Timing | Notes |
|------|---------|-----------|------|--------|-------|
| **BEFORE _wait_for_start()** | | | | | |
| 1 | 620-638 | Initialize benchmark pool | SF Connections | 15-20s | ✅ Already optimized! |
| 2 | 641-648 | Executor setup | Mixed | 100-500ms | Includes table profiling |
| 3 | 749-836 | Load value pools | SF Query | 50-100ms | SELECT from TEMPLATE_VALUE_POOLS |
| 4 | 671-693 | MAX(id) per table | SF Query | 100-200ms/table | Only if value pools exist |
| **AFTER _wait_for_start() returns** | | | | | |
| 5 | 771 | Log message | CPU | <1ms | |
| 6 | 772 | Heartbeat RUNNING | SF Query | 100-200ms | INSERT into WORKER_HEARTBEATS |
| 7 | 774-777 | Extract table config | CPU | <1ms | |
| 8 | 779-800 | Insert TEST_RESULTS | SF Query | 100-200ms | INSERT into TEST_RESULTS |
| 9 | 802-804 | Update executor state | CPU | <1ms | |
| 10 | 806-813 | Initialize variables | CPU | <1ms | |
| 11 | 815-874 | Define helper functions | CPU | <1ms | |
| 12 | 876 | Log scaling | CPU | <1ms | |
| 13 | **877** | **_scale_to() - spawn workers** | **CPU** | **~0.1ms/worker** | **Creates asyncio tasks** |
| 14 | 878 | Log scale complete | CPU | <1ms | |
| 15 | 880-911 | More initialization | CPU | <1ms | |
| 16 | 914+ | Enter main event loop | Async | N/A | Background loop |
| **Worker Execution (Parallel)** | | | | | |
| 17 | 2740-2755 | Worker initialization | CPU | <0.1ms | Per worker |
| 18 | 2756 | Call _execute_operation() | CPU | <0.1ms | |
| 19 | 2813-2820 | Dispatch to _execute_read() | CPU | <0.1ms | |
| 20 | 2857-2920 | Build query | CPU | <1ms | Generate SQL + params |
| 21 | **2928-2933** | **FIRST SNOWFLAKE QUERY** | **SF Query** | **1-100ms+** | **THE ACTUAL BENCHMARK QUERY** |

---

## TOTAL LATENCY BREAKDOWN (After _wait_for_start() returns)

### Fast Operations (CPU only, <5ms total)
- Logging, variable assignments, function definitions: **~3ms**
- Worker task creation (100 workers @ 0.1ms each): **~10ms**
- Total CPU time: **~13ms**

### Slow Operations (I/O, 200-400ms total)
1. **Heartbeat RUNNING** (line 772): **100-200ms**
   - Query: `INSERT INTO WORKER_HEARTBEATS ...`
   - Can we eliminate? **NO** - orchestrator needs to know workers started

2. **Insert TEST_RESULTS** (lines 779-800): **100-200ms**
   - Query: `INSERT INTO TEST_RESULTS (test_id, run_id, ...) VALUES (...)`
   - Can we eliminate? **NO** - required for result tracking
   - Can we defer? **MAYBE** - could batch with first metrics snapshot

3. **First benchmark query** (line 2928): **1-100ms+** (varies by query)

### Answer to "Where Are the 7-8 Seconds?"

The 7-8 seconds are **NOT** in the path after `_wait_for_start()` returns!

They are in the **BEFORE** section:
1. **Pool initialization** (15-20s) - ✅ **Already fixed** (moved before _wait_for_start)
2. **Table profiling** (100-500ms) - includes value pool load + MAX queries
3. **_wait_for_start() loop** (variable, up to 120s timeout waiting for orchestrator)

After `_wait_for_start()` returns, the **actual overhead is only ~200-400ms**:
- 100-200ms: Heartbeat RUNNING (required)
- 100-200ms: Insert TEST_RESULTS (required, possibly batchable)
- ~13ms: CPU operations

---

## POTENTIAL OPTIMIZATIONS (Post-_wait_for_start)

### ❌ **Cannot Eliminate**
1. **Heartbeat RUNNING** - Orchestrator relies on this to know workers started
2. **Insert TEST_RESULTS** - Core result tracking requirement
3. **Worker task spawning** - Necessary to start actual work

### ✅ **Possible Optimization**
**Batch TEST_RESULTS insert with first metrics snapshot** (save ~100-200ms)
- Currently: Sequential operations
  - Heartbeat RUNNING (line 772)
  - Insert TEST_RESULTS (lines 779-800)
  - Start workers (line 877)
- Alternative:
  - Heartbeat RUNNING
  - Start workers (line 877) - DO NOT WAIT
  - Insert TEST_RESULTS in background - workers already running
- **Savings:** ~100-200ms (parallelizes query with worker startup)
- **Risk:** If TEST_RESULTS insert fails, workers already running
- **Mitigation:** TEST_RESULTS failures are already handled (line 797-800)

### 🔍 **Investigation Needed**
**Where exactly are the 7-8 seconds?**

If pool init is already moved before _wait_for_start(), the overhead should be:
- Before _wait_for_start(): Pool init (already done) + setup (~500ms)
- _wait_for_start() loop: Variable (depends on orchestrator timing)
- After _wait_for_start(): ~200-400ms

**Hypothesis:** The 7-8 seconds are likely in:
1. **Orchestrator delay** - Time between worker signals READY and orchestrator sets RUNNING
2. **Pool initialization** - If it's NOT actually happening before _wait_for_start() in some code paths
3. **Table profiling** - If full profiling runs (without value pools), could be 300-500ms per table × N tables

**Next steps:**
1. Add detailed timing logs around:
   - Pool initialization start/end
   - Executor setup start/end  
   - _wait_for_start() enter/exit
   - _scale_to() start/end
   - First query execution
2. Check if pool initialization is actually happening before _wait_for_start() in all code paths
3. Profile _wait_for_start() loop to see how long it actually waits

---

## KEY FINDINGS

### ✅ **Already Optimized**
- Pool initialization moved before _wait_for_start() (lines 620-638)
- This **saves 15-20 seconds** of warmup time

### ⚠️ **Still Potentially Slow**
- Table profiling (100-500ms) - but mostly optimized with value pools
- Heartbeat + TEST_RESULTS inserts (~200-400ms) - required, but could be parallelized

### 🎯 **The Real Question**
**If pool init is before _wait_for_start() and post-wait overhead is only ~400ms, where are the 7-8 seconds?**

Possible answers:
1. **In _wait_for_start() itself** - orchestrator coordination delay
2. **Before pool init** - if there's setup code before line 620 that's slow
3. **In a different code path** - if this analysis is for worker code but the delay is in orchestrator
4. **Already fixed** - if the optimization on line 620 is recent and not yet deployed

**Recommendation:** Add comprehensive timing instrumentation to measure each section and identify the actual bottleneck.
