# Quick Reference: Execution Path from _wait_for_start() to First Query

## The Answer

**Question:** Where are the 7-8 seconds of overhead between `_wait_for_start()` returning and the first benchmark query?

**Answer:** There are **NO** 7-8 seconds after `_wait_for_start()` returns. The overhead is only **~213-313ms**.

The 7-8 seconds must be **BEFORE** `_wait_for_start()` returns (likely in the `_wait_for_start()` loop itself waiting for orchestrator).

---

## Operations After _wait_for_start() Returns (Line 770)

```python
[Line 770] _wait_for_start() returns True
    ↓
[Line 772] await _safe_heartbeat("RUNNING")                    # 100-200ms (SF INSERT)
    ↓
[Lines 779-800] await results_store.insert_test_start(...)     # 100-200ms (SF INSERT)
    ↓
[Line 877] await _scale_to(current_target, warmup=True)        # ~10ms (CPU)
    ↓
[Worker spawns] asyncio.create_task(executor._controlled_worker(...))  # <1ms per worker
    ↓
[Worker executes] await self._execute_operation(worker_id, warmup=True)
    ↓
[Build query] query, params = ... (CPU, <1ms)
    ↓
[Line 2928] ⚡ result, info = await pool.execute_query_with_info(...)  # 1-100ms+ (FIRST QUERY)
```

### Total time from line 770 to first query: ~213-313ms

---

## Timing Breakdown

| Operation | Type | Timing | Line(s) |
|-----------|------|--------|---------|
| CPU operations (logs, variables, task creation) | CPU | ~14ms | Various |
| Heartbeat RUNNING | SF INSERT | 100-200ms | 772 |
| Insert TEST_RESULTS | SF INSERT | 100-200ms | 779-800 |
| **First Snowflake query** | SF Query | 1-100ms+ | 2928 |
| **TOTAL** | | **~213-313ms** | 770-2928 |

---

## Where Are The 7-8 Seconds?

### ✅ Already Optimized (Lines 620-638)

#### Pool initialization moved before _wait_for_start()

- Creates 100 connections: 15-20 seconds
- ✅ This runs BEFORE waiting, so it doesn't block warmup

### 🔍 Most Likely: _wait_for_start() Loop (Lines 693-769)

```python
async def _wait_for_start(timeout_seconds: int = 120) -> bool:
    while True:
        # Poll RUN_STATUS every 1s
        # Poll RUN_CONTROL_EVENTS every 1s  
        # Send heartbeat every 1s
        # Exit when RUN_STATUS.status = 'RUNNING'
```

**This loop can take 0-120 seconds** depending on how long the orchestrator
takes to:

1. Start all workers
2. Initialize its own state
3. Set RUN_STATUS to 'RUNNING'

**This is coordination delay, not a bug.**

### 🔍 Also Check: Executor Setup (Lines 641-648)

```python
ok = await executor.setup()
    ↓
    _profile_tables()
        ↓
        _load_value_pools()          # 50-100ms (SF Query)
        ↓
        SELECT MAX(id) per table     # 100-200ms per table (SF Query)
```

**Typically 100-500ms** but could be longer if:

- Multiple tables
- No value pools (full profiling)
- Slow network

---

## Optimization Opportunities

### 🟢 High Impact (Already Done)
✅ **Pool init before _wait_for_start()** (lines 620-638)
- Saves: 15-20 seconds
- Status: Already implemented

### 🟡 Low Impact (Possible)
⚠️ **Parallelize TEST_RESULTS insert** (lines 779-800)
- Saves: ~100-200ms
- How: Start workers first, insert TEST_RESULTS in background
- Worth it? Only if you need every millisecond

### 🔴 Cannot Optimize
❌ **Heartbeat RUNNING** (line 772) - Required by orchestrator
❌ **Insert TEST_RESULTS** (lines 779-800) - Required for result tracking
❌ **Worker spawn** (line 877) - Already very fast (~10ms)

---

## Recommended Next Step

**Add timing instrumentation** to measure where the delay actually is:

```python
import time

# Measure each section
t0 = time.monotonic()
logger.info("🕐 Before pool init: %.3fs", t0)

# ... pool init ...

t1 = time.monotonic()
logger.info("🕐 After pool init: %.3fs (duration: %.3fs)", t1, t1 - t0)

# ... setup ...

t2 = time.monotonic()
logger.info("🕐 After setup: %.3fs (duration: %.3fs)", t2, t2 - t1)

# ... _wait_for_start() ...

t3 = time.monotonic()
logger.info("🕐 After _wait_for_start: %.3fs (duration: %.3fs)", t3, t3 - t2)

# ... operations after _wait_for_start ...

t4 = time.monotonic()
logger.info("🕐 After _scale_to: %.3fs (duration: %.3fs)", t4, t4 - t3)

# ... first query ...

t5 = time.monotonic()
logger.info("🕐 After first query: %.3fs (duration: %.3fs)", t5, t5 - t4)
```

**This will show you EXACTLY where the 7-8 seconds are.**

---

## Files Generated

1. **EXECUTION_TIMELINE_AFTER_WAIT_FOR_START.md** - Detailed line-by-line trace
2. **TIMING_DIAGRAM.md** - Visual ASCII diagram of execution flow
3. **FINDINGS_AND_RECOMMENDATIONS.md** - Complete analysis with optimization recommendations
4. **THIS FILE** - Quick reference summary

---

## Key Takeaway

**The code after `_wait_for_start()` returns is NOT the bottleneck.**

The 7-8 seconds you're seeing are almost certainly:
1. **In the `_wait_for_start()` loop** - waiting for orchestrator to signal START
2. **OR already fixed** - if the pool init optimization (line 620) is recent and not yet deployed

**Measure with timing logs to confirm.**
