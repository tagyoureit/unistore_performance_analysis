/**
 * Dashboard Display Module
 * Methods for displaying test configuration and status information.
 */
window.DashboardMixins = window.DashboardMixins || {};

window.DashboardMixins.display = {
  tableTypeLabel() {
    const tableType = (this.templateInfo?.table_type || "").toUpperCase();
    if (tableType === "POSTGRES") return "POSTGRES";
    if (tableType === "HYBRID") return "HYBRID";
    if (tableType === "STANDARD") return "STANDARD";
    if (tableType === "INTERACTIVE") return "INTERACTIVE";
    return tableType || "";
  },

  tableTypeIconSrc() {
    const tableType = (this.templateInfo?.table_type || "").toUpperCase();
    if (tableType === "POSTGRES") {
      return "/static/img/postgres_elephant.svg";
    }
    if (tableType === "HYBRID") {
      return "/static/img/table_hybrid.svg";
    }
    if (tableType === "STANDARD") {
      return "/static/img/table_standard.svg";
    }
    if (tableType === "INTERACTIVE") {
      return "/static/img/table_interactive.svg";
    }
    return "";
  },

  threadsRangeDisplay() {
    const info = this.templateInfo;
    if (!info) return "";
    const min = info.min_connections ?? 1;
    const max = info.concurrent_connections ?? "";
    if (min && max && min !== max) {
      return `${min}-${max}`;
    }
    return String(max || min || "");
  },

  formatWarehouseOption(wh) {
    if (!wh) return "";
    const gen = wh.resource_constraint === "STANDARD_GEN_2" ? "Gen2" : "Gen1";
    const parts = [gen, wh.size];

    const maxClusters = Number(wh.max_cluster_count || 0);
    if (Number.isFinite(maxClusters) && maxClusters > 1) {
      parts.push(`MCW ${wh.min_cluster_count}-${wh.max_cluster_count}`);
    }

    if (wh.enable_query_acceleration) {
      parts.push("QAS");
    }

    return `${wh.name} (${parts.filter(Boolean).join(", ")})`;
  },

  warehouseDisplay() {
    const info = this.templateInfo;
    if (!info) return "";
    if (this.isPostgresTable()) return "";

    const details = this.warehouseDetails;
    if (details && details.name) {
      return this.formatWarehouseOption(details);
    }

    const name = info.warehouse || "default";
    const size = info.warehouse_size;
    if (size) return `${name} (${size})`;
    return String(name);
  },

  isPostgresTable() {
    const tableType = (this.templateInfo?.table_type || "").toLowerCase();
    return ["postgres", "snowflake_postgres"].includes(tableType);
  },

  postgresPoolDisplay() {
    const stats = this.templateInfo?.postgres_stats;
    if (!stats || !stats.pool) return null;
    const pool = stats.pool;
    if (!pool.initialized) return null;
    const free = Number(pool.free || 0);
    const used = Number(pool.used || 0);
    const total = Number(pool.total || pool.size || 0);
    const pending = Number(pool.pending || 0);
    const min = Number(pool.minsize || pool.min_size || 0);
    const max = Number(pool.maxsize || pool.max_size || 0);
    return { free, used, total, pending, min, max };
  },

  postgresConnectionsDisplay() {
    const stats = this.templateInfo?.postgres_stats;
    if (!stats || !stats.pool) return "N/A";
    const pool = stats.pool;
    if (!pool.initialized) return "N/A";

    const used = Number(pool.used || 0);
    const total = Number(pool.total || pool.size || 0);
    const min = Number(pool.minsize || pool.min_size || 0);
    const max = Number(pool.maxsize || pool.max_size || 0);

    // Show "<used> / <total>" or "<used> / <max>" for conciseness.
    const denom = max > total ? max : total;
    let display = `${used} / ${denom}`;

    // Optionally append min..max range if different from total.
    if (min !== denom || max !== denom) {
      display += ` (pool ${min}-${max})`;
    }

    const pending = Number(pool.pending || 0);
    if (pending > 0) {
      display += `, ${pending} pending`;
    }

    return display;
  },

  workloadDisplay() {
    const info = this.templateInfo;
    if (!info) return "";

    // Always use actual custom percentages from template config
    const pl = Number(info.custom_point_lookup_pct || 0);
    const rs = Number(info.custom_range_scan_pct || 0);
    const ins = Number(info.custom_insert_pct || 0);
    const upd = Number(info.custom_update_pct || 0);
    const total = pl + rs + ins + upd;

    // If no percentages defined, return empty (don't show workload line)
    if (total === 0) return "";

    // Build compact display: "PL 25% • RS 25% • INS 35% • UPD 15%"
    const parts = [];
    if (pl > 0) parts.push(`PL ${pl}%`);
    if (rs > 0) parts.push(`RS ${rs}%`);
    if (ins > 0) parts.push(`INS ${ins}%`);
    if (upd > 0) parts.push(`UPD ${upd}%`);

    return parts.join(" • ");
  },

  loadModeDisplay() {
    const info = this.templateInfo;
    if (!info) return "";

    const loadMode = String(info.load_mode || "CONCURRENCY").toUpperCase();

    if (loadMode === "QPS") {
      const targetQps = info.target_qps ?? "";
      // Support both new and old field names
      const startingThreads = info.starting_threads ?? info.starting_qps ?? 0;
      const maxThreadIncrease = info.max_thread_increase ?? info.max_qps_increase ?? 15;
      return `QPS Mode: Target ${targetQps} QPS (start: ${startingThreads}, ±${maxThreadIncrease}/~10s)`;
    }

    // CONCURRENCY mode - just show threads
    const threads = info.concurrent_connections ?? "";
    return `Concurrency Mode: ${threads} threads`;
  },

  scalingConfig() {
    const info = this.templateInfo;
    if (!info || !info.scaling) return null;
    return info.scaling;
  },

  scalingMode() {
    const sc = this.scalingConfig();
    if (!sc) return "FIXED";
    return String(sc.mode || "AUTO").toUpperCase();
  },

  scalingSummary() {
    const mode = this.scalingMode();
    const sc = this.scalingConfig();
    if (!sc) return mode;

    // Default applied by backend for AUTO and BOUNDED modes (no max_workers default)
    const DEFAULT_MAX_CONN = 200;

    // Get connections per worker info
    const minConn = Number(sc.min_connections ?? sc.minConnections ?? 1);
    const rawMaxConn = sc.max_connections ?? sc.maxConnections;
    // For AUTO/BOUNDED, show the default that will be applied
    const effectiveMaxConn = rawMaxConn != null ? Number(rawMaxConn) : 
      (mode === "AUTO" || mode === "BOUNDED" ? DEFAULT_MAX_CONN : null);
    const connPart = effectiveMaxConn ? `${minConn}-${effectiveMaxConn} conn/worker` : `${minConn}+ conn/worker`;

    if (mode === "AUTO") {
      // AUTO now shows the effective conn bounds that will be applied
      return `Auto (${connPart})`;
    }
    if (mode === "BOUNDED") {
      const min = Number(sc.min_workers ?? sc.minWorkers ?? 1);
      const rawMax = sc.max_workers ?? sc.maxWorkers;
      const workerPart = rawMax != null ? `${min}-${Number(rawMax)} workers` : `${min}+ workers`;
      return `Bounded (${workerPart}, ${connPart})`;
    }
    const fixed = Number(sc.fixed_workers ?? sc.fixedWorkers ?? 1);
    return `Fixed (${fixed} workers, ${connPart})`;
  },

  warmupDisplay() {
    const info = this.templateInfo;
    if (!info) return "";
    const warmup = Number(info.warmup_seconds || 0);
    if (warmup <= 0) return "";
    return `${warmup}s`;
  },

  durationDisplay() {
    const info = this.templateInfo;
    if (!info) return "";
    // duration_seconds is the RUN time (measurement period), not total
    const runDuration = Number(info.duration_seconds || 0);
    const warmup = Number(info.warmup_seconds || 0);
    if (runDuration <= 0) return "";
    
    // Calculate total = warmup + run
    const total = warmup + runDuration;
    
    // If warmup exists, show breakdown
    if (warmup > 0) {
      return `${total}s (${warmup}s warmup + ${runDuration}s measurement)`;
    }
    return `${runDuration}s`;
  },

  resourceConstraintDisplay() {
    const details = this.warehouseDetails;
    if (!details) return "";
    const constraint = details.resource_constraint || "";
    if (constraint === "STANDARD_GEN_2") return "Gen2";
    if (constraint === "STANDARD_GEN_1" || constraint === "STANDARD") return "Gen1";
    return constraint ? constraint : "";
  },

  boundsStatusText() {
    if (!this.isFindMaxMode()) return null;
    const s = this.findMaxState();
    if (!s) return null;
    const mode = this.scalingMode();
    if (mode !== "BOUNDED") return null;

    const statusUpper = (this.status || "").toString().toUpperCase();
    const phaseUpper = (this.phase || "").toString().toUpperCase();
    const terminal =
      phaseUpper === "COMPLETED" || ["STOPPED", "FAILED", "CANCELLED"].includes(statusUpper);

    const sc = this.scalingConfig();
    const maxBound = Number(sc?.max_workers ?? sc?.maxWorkers ?? 100);

    // Completed runs: show "Completed: REASON"
    if (terminal) {
      const reason = this.boundsCompletionReason();
      if (reason) return `Completed: ${reason}`;
      return "Completed";
    }

    // Running: show progress text (e.g., "Searching 4-16, currently at 8")
    const current = Number(s.current_concurrency ?? 0);
    const low = Number(s.low ?? 1);
    const high = Math.min(Number(s.high ?? maxBound), maxBound);
    if (low && high && current) {
      return `Searching ${low}-${high}, currently at ${current}`;
    }
    if (current) {
      return `Currently at ${current} workers`;
    }
    return null;
  },

  boundsCompletionReason() {
    const s = this.findMaxState();
    if (!s) return null;
    const reason = s.final_reason ?? s.stop_reason ?? null;
    if (reason) return String(reason);
    return null;
  },

  qpsTarget() {
    const ti = this.templateInfo;
    if (!ti) return null;
    const loadMode = String(ti.load_mode || "CONCURRENCY").toUpperCase();
    if (loadMode !== "QPS") return null;
    return ti.target_qps || null;
  },

  qpsTargetPct() {
    const target = this.qpsTarget();
    if (!target || target <= 0) return null;
    const current = this.mode === 'history' 
      ? (this.templateInfo?.qps || 0)
      : (this.metrics.qps_avg_30s || 0);
    return (current / target) * 100;
  },

  // === LIVE COST CALCULATION ===

  /**
   * Calculate the live cost based on elapsed time and warehouse size.
   */
  liveCost() {
    if (!window.CostUtils) return 0;
    const warehouseSize = this.templateInfo?.warehouse_size || 'MEDIUM';
    const elapsedSeconds = this.elapsed || 0;
    if (elapsedSeconds <= 0) return 0;
    
    const result = window.CostUtils.calculateEstimatedCost(elapsedSeconds, warehouseSize);
    return result.estimated_cost_usd || 0;
  },

  /**
   * Calculate the live credits used based on elapsed time.
   */
  liveCredits() {
    if (!window.CostUtils) return 0;
    const warehouseSize = this.templateInfo?.warehouse_size || 'MEDIUM';
    const elapsedSeconds = this.elapsed || 0;
    if (elapsedSeconds <= 0) return 0;
    
    const result = window.CostUtils.calculateEstimatedCost(elapsedSeconds, warehouseSize);
    return result.credits_used || 0;
  },

  /**
   * Calculate the projected total cost based on full duration.
   */
  projectedCost() {
    if (!window.CostUtils) return 0;
    const warehouseSize = this.templateInfo?.warehouse_size || 'MEDIUM';
    const totalDuration = this.templateInfo?.duration_seconds || 0;
    if (totalDuration <= 0) return 0;
    
    const result = window.CostUtils.calculateEstimatedCost(totalDuration, warehouseSize);
    return result.estimated_cost_usd || 0;
  },

  /**
   * Format the live cost for display.
   */
  formatLiveCost() {
    if (!window.CostUtils) return '$0.0000';
    return window.CostUtils.formatCost(this.liveCost());
  },

  /**
   * Format the live credits for display.
   */
  formatLiveCredits() {
    if (!window.CostUtils) return '0.0000';
    return window.CostUtils.formatCredits(this.liveCredits());
  },

  /**
   * Format the projected cost for display.
   */
  formatProjectedCost() {
    if (!window.CostUtils) return '$0.0000';
    return window.CostUtils.formatCost(this.projectedCost());
  },

  /**
   * Generate a detailed cost breakdown tooltip for live cost display.
   */
  formatLiveCostTooltip() {
    if (!window.CostUtils) return '';
    const warehouseSize = this.templateInfo?.warehouse_size || 'MEDIUM';
    const tableType = this.templateInfo?.table_type || 'HYBRID';
    const elapsedSeconds = this.elapsed || 0;
    
    return window.CostUtils.generateCostTooltip({
      warehouseSize: warehouseSize,
      tableType: tableType,
      durationSeconds: elapsedSeconds,
      dollarsPerCredit: window.CostUtils.getCostSettings().dollarsPerCredit
    });
  },
};
