function dashboard(opts) {
  const options = opts && typeof opts === "object" ? opts : {};
  const modeRaw = options.mode != null ? String(options.mode) : "live";
  const mode = modeRaw === "history" ? "history" : "live";

  return {
    mode,
    debug: false,
    testRunning: false,
    testId: null,
    progress: 0,
    elapsed: 0,
    duration: 0,
    phase: null,
    warmupSeconds: 0,
    runSeconds: 0,
    status: null,
    templateInfo: null,
    qpsHistory: [],
    metrics: {
      ops_per_sec: 0,
      qps_avg_30s: 0,
      in_flight: 0,
      target_workers: 0,
      sf_running: 0,
      sf_queued: 0,
      sf_bench_available: false,
      sf_running_bench: 0,
      sf_queued_bench: 0,
      sf_blocked_bench: 0,
      sf_running_tagged_bench: 0,
      sf_running_other_bench: 0,
      sf_running_read_bench: 0,
      sf_running_write_bench: 0,
      sf_running_point_lookup_bench: 0,
      sf_running_range_scan_bench: 0,
      sf_running_insert_bench: 0,
      sf_running_update_bench: 0,
      p50_latency: 0,
      p95_latency: 0,
      p99_latency: 0,
      error_rate: 0,
      total_errors: 0,
      started_clusters: 0,
      latency_breakdown_available: false,
      sf_execution_avg_ms: 0,
      network_overhead_avg_ms: 0,
      latency_sample_count: 0,
      // App-level ops breakdown (always accurate, directly from app counters)
      app_ops_available: false,
      app_point_lookup_count: 0,
      app_range_scan_count: 0,
      app_insert_count: 0,
      app_update_count: 0,
      app_read_count: 0,
      app_write_count: 0,
      app_total_count: 0,
      app_point_lookup_ops_sec: 0,
      app_range_scan_ops_sec: 0,
      app_insert_ops_sec: 0,
      app_update_ops_sec: 0,
      app_read_ops_sec: 0,
      app_write_ops_sec: 0,
    },
    sfRunningBreakdown: "read_write", // 'read_write' | 'by_kind'
    opsSecBreakdown: "read_write", // 'read_write' | 'by_kind'
    latencyView: "end_to_end", // 'end_to_end' | 'sf_execution'
    latencyViewUserSelected: false,
    didRefreshAfterComplete: false,
    // NOTE: Do not store Chart.js instances on Alpine reactive state.
    // Chart objects have circular refs/getters; Alpine Proxy wrapping can cause
    // recursion and Chart.js internal corruption.
    websocket: null,
    logs: [],
    _logSeen: {},
    logMaxLines: 1000,
    warehouseTs: [],
    warehouseTsLoading: false,
    warehouseTsError: null,
    warehouseTsAvailable: false,
    warehouseTsLoaded: false,
    warehouseTsRetryCount: 0,
    warehouseTsRetryMax: 24,
    warehouseTsRetryIntervalMs: 10000,
    warehouseTsRetryTimerId: null,
    warehouseQueueMode: "avg", // 'avg' | 'total'
    warehouseDetails: null,
    warehouseDetailsLoading: false,
    warehouseDetailsError: null,
    errorSummaryRows: [],
    errorSummaryLoading: false,
    errorSummaryLoaded: false,
    errorSummaryError: null,
    _elapsedIntervalId: null,
    _elapsedStartTime: null,
    _elapsedBaseValue: 0,
    aiAnalysisModal: false,
    aiAnalysisLoading: false,
    aiAnalysisError: null,
    aiAnalysis: null,
    chatMessage: "",
    chatHistory: [],
    chatLoading: false,
    clusterBreakdownExpanded: false,
    stepHistoryExpanded: false,
    findMaxController: null,
    findMaxCountdownSeconds: null,
    _findMaxCountdownIntervalId: null,
    _findMaxCountdownTargetEpochMs: null,
    enrichmentRetrying: false,

    init() {
      // Enable debug logging when ?debug=1 is present.
      try {
        const params = new URLSearchParams(window.location.search || "");
        const v = String(params.get("debug") || "").toLowerCase();
        this.debug = v === "1" || v === "true" || v === "yes";
      } catch (_) {
        this.debug = false;
      }

      this.initCharts();

      const el = this.$el;
      const initialTestId = el && el.dataset ? el.dataset.testId : "";
      if (this.debug) {
        console.log("[dashboard][debug] init", {
          mode: this.mode,
          initialTestId,
          chartjs: !!window.Chart,
        });
        this._debugCharts("after initCharts");
      }
      if (initialTestId) {
        this.testId = initialTestId;
        this.loadTestInfo();
        if (this.mode === "live") {
          this.loadLogs();
          this.connectWebSocket();
        }
      }
    },

    _debugCharts(label, extra) {
      if (!this.debug) return;
      try {
        const info = (id) => {
          const canvas = document.getElementById(id);
          const chart =
            canvas &&
            (canvas.__chart ||
              (window.Chart && Chart.getChart ? Chart.getChart(canvas) : null));
          const labels =
            chart && chart.data && Array.isArray(chart.data.labels)
              ? chart.data.labels.length
              : null;
          const datasets =
            chart && chart.data && Array.isArray(chart.data.datasets)
              ? chart.data.datasets.map((d) =>
                  d && Array.isArray(d.data) ? d.data.length : null,
                )
              : null;
          return { canvas: !!canvas, chart: !!chart, labels, datasets };
        };

        console.log(`[dashboard][debug] ${label}`, {
          mode: this.mode,
          testId: this.testId,
          templateLoaded: !!this.templateInfo,
          extra: extra && typeof extra === "object" ? extra : undefined,
          throughput: info("throughputChart"),
          latency: info("latencyChart"),
          warehouse: info("warehouseQueueChart"),
        });
      } catch (e) {
        console.log(`[dashboard][debug] ${label} (failed)`, e);
      }
    },

    async startTest() {
      if (!this.testId) return;
      try {
        const resp = await fetch(`/api/tests/${this.testId}/start`, {
          method: "POST",
        });
        if (!resp.ok) {
          const err = await resp.json().catch(() => ({}));
          const detail = err && err.detail ? err.detail : null;
          throw new Error(
            (detail && (detail.message || detail.detail || detail)) ||
              "Failed to start test",
          );
        }
        // Kick a refresh; status/details will update as soon as execution starts.
        await this.loadTestInfo();
      } catch (e) {
        console.error("Failed to start test:", e);
        window.toast.error(`Failed to start test: ${e.message || e}`);
      }
    },

    async stopTest() {
      if (!this.testId) return;
      try {
        const resp = await fetch(`/api/tests/${this.testId}/stop`, {
          method: "POST",
        });
        if (!resp.ok) {
          const err = await resp.json().catch(() => ({}));
          const detail = err && err.detail ? err.detail : null;
          throw new Error(
            (detail && (detail.message || detail.detail || detail)) ||
              "Failed to stop test",
          );
        }
        // Optimistically reflect cancellation immediately; backend will finalize shortly.
        try {
          const data = await resp.json().catch(() => ({}));
          if (data && data.status) {
            this.status = data.status;
          }
        } catch (_) {}
        await this.loadTestInfo();
      } catch (e) {
        console.error("Failed to stop test:", e);
        window.toast.error(`Failed to stop test: ${e.message || e}`);
      }
    },

    openRunAnalysis() {
      if (!this.testId) return;
      window.location.href = `/dashboard/history/${this.testId}`;
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
      const inUse = Number(pool.in_use);
      const maxSize = Number(pool.max_size);
      const free = Number(pool.free);
      if (!Number.isFinite(inUse) || !Number.isFinite(maxSize)) return null;
      const base = `Pool: ${inUse}/${maxSize}`;
      if (Number.isFinite(free)) {
        return `${base} (free ${free})`;
      }
      return base;
    },

    postgresConnectionsDisplay() {
      const stats = this.templateInfo?.postgres_stats;
      if (!stats) return null;
      const active = Number(stats.active_connections);
      const maxConn = Number(stats.max_connections);
      if (!Number.isFinite(active) && !Number.isFinite(maxConn)) return null;
      if (Number.isFinite(active) && Number.isFinite(maxConn)) {
        return `Connections: ${active}/${maxConn}`;
      }
      if (Number.isFinite(active)) {
        return `Connections: ${active}`;
      }
      return `Max connections: ${maxConn}`;
    },

    async loadWarehouseDetails() {
      const info = this.templateInfo;
      if (this.isPostgresTable()) {
        this.warehouseDetails = null;
        this.warehouseDetailsError = null;
        this.warehouseDetailsLoading = false;
        return;
      }
      const name = info && info.warehouse ? String(info.warehouse).trim() : "";
      if (!name) {
        this.warehouseDetails = null;
        this.warehouseDetailsError = null;
        this.warehouseDetailsLoading = false;
        return;
      }

      const existingName =
        this.warehouseDetails && this.warehouseDetails.name
          ? String(this.warehouseDetails.name).trim()
          : "";
      if (existingName && existingName.toUpperCase() === name.toUpperCase()) {
        return;
      }

      this.warehouseDetailsLoading = true;
      this.warehouseDetailsError = null;
      try {
        const resp = await fetch(`/api/warehouses/${encodeURIComponent(name)}`);
        if (!resp.ok) {
          throw new Error(`Failed to load warehouse details (${resp.status})`);
        }
        this.warehouseDetails = await resp.json();
      } catch (e) {
        console.error("Failed to load warehouse details:", e);
        this.warehouseDetailsError = e && e.message ? e.message : String(e);
        this.warehouseDetails = null;
      } finally {
        this.warehouseDetailsLoading = false;
      }
    },

    workloadDisplay() {
      const info = this.templateInfo;
      if (!info) return "";

      const pct = (key) => {
        const v = info[key];
        const n = typeof v === "number" ? v : Number(v);
        return Number.isFinite(n) ? Math.round(n) : 0;
      };

      const parts = [];
      const add = (key, label) => {
        const p = pct(key);
        if (p > 0) parts.push(`${p}% ${label}`);
      };

      add("custom_point_lookup_pct", "point lookup");
      add("custom_range_scan_pct", "range scan");
      add("custom_insert_pct", "insert");
      add("custom_update_pct", "update");

      if (parts.length > 0) return parts.join(", ");
      return info.workload_type != null ? String(info.workload_type) : "";
    },

    formatSecondsTenths(value) {
      const n = typeof value === "number" ? value : Number(value);
      if (!Number.isFinite(n)) return "0";

      // Cap at one decimal place (tenths).
      const tenthsInt = Math.trunc(n * 10);
      if (tenthsInt % 10 === 0) return String(tenthsInt / 10);
      return (tenthsInt / 10).toFixed(1);
    },

    formatCompact(value) {
      const n = typeof value === "number" ? value : Number(value);
      if (!Number.isFinite(n)) return "0.00";

      const abs = Math.abs(n);
      const format = (x, suffix) => `${x.toFixed(2)}${suffix}`;

      if (abs >= 1e12) return format(n / 1e12, "T");
      if (abs >= 1e9) return format(n / 1e9, "B");
      if (abs >= 1e6) return format(n / 1e6, "M");
      if (abs >= 1e3) return format(n / 1e3, "k");
      return n.toFixed(2);
    },

    formatMs(value) {
      if (value == null) return "N/A";
      const n = typeof value === "number" ? value : Number(value);
      if (!Number.isFinite(n)) return "N/A";
      return n.toFixed(2);
    },

    formatMsWithUnit(value) {
      if (value == null) return "N/A";
      const n = typeof value === "number" ? value : Number(value);
      if (!Number.isFinite(n)) return "N/A";
      return `${n.toFixed(2)} ms`;
    },

    formatInt(value) {
      const n = typeof value === "number" ? value : Number(value);
      if (!Number.isFinite(n)) return "0";
      return String(Math.trunc(n));
    },

    formatPct(value, digits = 2) {
      const n = typeof value === "number" ? value : Number(value);
      if (!Number.isFinite(n)) return "N/A";
      const d = typeof digits === "number" ? digits : Number(digits);
      const pct = n > 1 ? n : n * 100.0;
      return `${pct.toFixed(Number.isFinite(d) ? d : 2)}%`;
    },

    formatPctValue(value, digits = 2) {
      // Always interpret value as a percentage already in [0,100].
      if (value == null) return "N/A";
      const n = typeof value === "number" ? value : Number(value);
      if (!Number.isFinite(n)) return "N/A";
      const d = typeof digits === "number" ? digits : Number(digits);
      return `${n.toFixed(Number.isFinite(d) ? d : 2)}%`;
    },

    formatSignedPct(value, digits = 2) {
      if (value == null) return "N/A";
      const n = typeof value === "number" ? value : Number(value);
      if (!Number.isFinite(n)) return "N/A";
      const d = typeof digits === "number" ? digits : Number(digits);
      const prefix = n >= 0 ? "+" : "";
      return `${prefix}${n.toFixed(Number.isFinite(d) ? d : 2)}%`;
    },

    formatTargetMs(value) {
      if (value == null) return "N/A";
      const n = typeof value === "number" ? value : Number(value);
      if (!Number.isFinite(n)) return "N/A";
      if (n < 0) return "Disabled";
      // 0 is considered an invalid latency target; render explicitly.
      if (n === 0) return "Unset";
      return this.formatMs(n);
    },

    formatTargetPct(value, digits = 2) {
      if (value == null) return "N/A";
      const n = typeof value === "number" ? value : Number(value);
      if (!Number.isFinite(n)) return "N/A";
      if (n < 0) return "Disabled";
      const d = typeof digits === "number" ? digits : Number(digits);
      return `${n.toFixed(Number.isFinite(d) ? d : 2)}%`;
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

    isFindMaxMode() {
      const loadMode = String(this.templateInfo?.load_mode || "").toUpperCase();
      if (loadMode === "FIND_MAX_CONCURRENCY") return true;
      const s = this.findMaxState();
      return !!(s && String(s.mode || "").toUpperCase() === "FIND_MAX_CONCURRENCY");
    },

    findMaxState() {
      if (this.findMaxController && typeof this.findMaxController === "object") {
        return this.findMaxController;
      }
      const raw = this.templateInfo?.find_max_result;
      if (raw && typeof raw === "object") return raw;
      return null;
    },

    findMaxBaselineP95Ms() {
      const s = this.findMaxState();
      if (!s) return null;
      const v = Number(s.baseline_p95_latency_ms);
      if (!Number.isFinite(v) || v <= 0) return null;
      return v;
    },

    findMaxBaselineP99Ms() {
      const s = this.findMaxState();
      if (!s) return null;
      const v = Number(s.baseline_p99_latency_ms);
      if (!Number.isFinite(v) || v <= 0) return null;
      return v;
    },

    _findMaxIsTerminal(s) {
      const statusUpper = (this.status || "").toString().toUpperCase();
      const phaseUpper = (this.phase || "").toString().toUpperCase();
      const controllerStatus = s && s.status != null ? String(s.status).toUpperCase() : "";
      return (
        phaseUpper === "COMPLETED" ||
        ["STOPPED", "FAILED", "CANCELLED"].includes(statusUpper) ||
        controllerStatus === "COMPLETED" ||
        !!(s && s.completed)
      );
    },

    _findMaxStepHistory() {
      const s = this.findMaxState();
      if (!s) return [];
      const hist = s.step_history;
      return Array.isArray(hist) ? hist : [];
    },

    _findMaxLastUnstableStep() {
      const hist = this._findMaxStepHistory();
      for (let i = hist.length - 1; i >= 0; i--) {
        const row = hist[i];
        if (!row || row.stable !== false) continue;
        if (row.is_backoff) continue;
        return row;
      }
      return null;
    },

    findMaxCurrentP95Ms() {
      const s = this.findMaxState();
      const terminal = this._findMaxIsTerminal(s);

      // Terminal: prefer the *unstable* step p95 (what caused the stop) when present.
      if (terminal) {
        const unstable = this._findMaxLastUnstableStep();
        if (unstable && unstable.p95_latency_ms != null) {
          const v = Number(unstable.p95_latency_ms);
          if (Number.isFinite(v) && v > 0) return v;
        }
        if (s && s.current_p95_latency_ms != null) {
          const v = Number(s.current_p95_latency_ms);
          if (Number.isFinite(v) && v > 0) return v;
        }
        const hist = this._findMaxStepHistory();
        const last = hist.length ? hist[hist.length - 1] : null;
        if (last && last.p95_latency_ms != null) {
          const v = Number(last.p95_latency_ms);
          if (Number.isFinite(v) && v > 0) return v;
        }
      }

      // Live: use rolling end-to-end p95 (the executor clears the latency buffer per step,
      // so this approximates the current step while it runs).
      if (this.mode === "live") {
        const v = Number(this.metrics?.p95_latency);
        if (!Number.isFinite(v) || v <= 0) return null;
        return v;
      }

      // History: prefer the persisted last-step p95 from find_max_result, else overall.
      if (s && s.current_p95_latency_ms != null) {
        const v = Number(s.current_p95_latency_ms);
        if (Number.isFinite(v) && v > 0) return v;
      }
      const v = Number(this.templateInfo?.p95_latency_ms);
      if (!Number.isFinite(v) || v <= 0) return null;
      return v;
    },

    findMaxCurrentP99Ms() {
      const s = this.findMaxState();
      const terminal = this._findMaxIsTerminal(s);

      if (terminal) {
        const unstable = this._findMaxLastUnstableStep();
        if (unstable && unstable.p99_latency_ms != null) {
          const v = Number(unstable.p99_latency_ms);
          if (Number.isFinite(v) && v > 0) return v;
        }
        if (s && s.current_p99_latency_ms != null) {
          const v = Number(s.current_p99_latency_ms);
          if (Number.isFinite(v) && v > 0) return v;
        }
        const hist = this._findMaxStepHistory();
        const last = hist.length ? hist[hist.length - 1] : null;
        if (last && last.p99_latency_ms != null) {
          const v = Number(last.p99_latency_ms);
          if (Number.isFinite(v) && v > 0) return v;
        }
      }

      if (this.mode === "live") {
        const v = Number(this.metrics?.p99_latency);
        if (!Number.isFinite(v) || v <= 0) return null;
        return v;
      }

      if (s && s.current_p99_latency_ms != null) {
        const v = Number(s.current_p99_latency_ms);
        if (Number.isFinite(v) && v > 0) return v;
      }
      const v = Number(this.templateInfo?.p99_latency_ms);
      if (!Number.isFinite(v) || v <= 0) return null;
      return v;
    },

    findMaxP95DiffPct() {
      const baseline = this.findMaxBaselineP95Ms();
      const current = this.findMaxCurrentP95Ms();
      if (!baseline || baseline <= 0 || current == null) return null;
      return ((current - baseline) / baseline) * 100.0;
    },

    findMaxP99DiffPct() {
      const baseline = this.findMaxBaselineP99Ms();
      const current = this.findMaxCurrentP99Ms();
      if (!baseline || baseline <= 0 || current == null) return null;
      return ((current - baseline) / baseline) * 100.0;
    },

    findMaxP95MaxThresholdMs() {
      const baseline = this.findMaxBaselineP95Ms();
      if (!baseline || baseline <= 0) return null;
      const s = this.findMaxState() || {};
      const pct = Number(s.latency_stability_pct);
      if (!Number.isFinite(pct) || pct <= 0) return null;
      // Baseline drift guardrail uses 2x the per-step latency threshold (matches controller logic).
      return baseline * (1.0 + (pct * 2.0) / 100.0);
    },

    findMaxP99MaxThresholdMs() {
      const baseline = this.findMaxBaselineP99Ms();
      if (!baseline || baseline <= 0) return null;
      const s = this.findMaxState() || {};
      const pct = Number(s.latency_stability_pct);
      if (!Number.isFinite(pct) || pct <= 0) return null;
      return baseline * (1.0 + (pct * 2.0) / 100.0);
    },

    findMaxCurrentErrorPct() {
      const s = this.findMaxState();
      const terminal = this._findMaxIsTerminal(s);

      if (terminal) {
        const unstable = this._findMaxLastUnstableStep();
        if (unstable && unstable.error_rate_pct != null) {
          const v = Number(unstable.error_rate_pct);
          if (Number.isFinite(v)) return v;
        }
        if (s && s.current_error_rate_pct != null) {
          const v = Number(s.current_error_rate_pct);
          if (Number.isFinite(v)) return v;
        }
        const hist = this._findMaxStepHistory();
        const last = hist.length ? hist[hist.length - 1] : null;
        if (last && last.error_rate_pct != null) {
          const v = Number(last.error_rate_pct);
          if (Number.isFinite(v)) return v;
        }
      }

      if (this.mode === "live") {
        const v = Number(this.metrics?.error_rate);
        if (!Number.isFinite(v)) return null;
        return v;
      }

      if (s && s.current_error_rate_pct != null) {
        const v = Number(s.current_error_rate_pct);
        if (Number.isFinite(v)) return v;
      }
      return null;
    },

    findMaxMaxErrorPct() {
      const s = this.findMaxState();
      if (!s) return null;
      const v = Number(s.max_error_rate_pct);
      return Number.isFinite(v) ? v : null;
    },

    findMaxCurrentWorkers() {
      const s = this.findMaxState();
      if (!s) return null;
      if (this._findMaxIsTerminal(s)) {
        const finalBest = Number(
          s.final_best_concurrency != null ? s.final_best_concurrency : s.best_concurrency,
        );
        return Number.isFinite(finalBest) ? Math.trunc(finalBest) : null;
      }
      const v = Number(s.current_concurrency);
      return Number.isFinite(v) ? Math.trunc(v) : null;
    },

    findMaxActiveWorkers() {
      const s = this.findMaxState();
      if (!s) return null;
      const v = Number(s.active_worker_count);
      return Number.isFinite(v) ? Math.trunc(v) : null;
    },

    findMaxNextWorkers() {
      const s = this.findMaxState();
      if (!s) return null;
      // When terminal, return null - no "next" workers to show
      if (this._findMaxIsTerminal(s)) {
        return null;
      }
      const v = Number(s.next_planned_concurrency);
      return Number.isFinite(v) ? Math.trunc(v) : null;
    },

    findMaxAtMax() {
      const s = this.findMaxState();
      if (!s) return false;
      const current = Number(s.current_concurrency);
      const max = Number(s.max_concurrency);
      return Number.isFinite(current) && Number.isFinite(max) && current >= max;
    },

    findMaxConclusionReason() {
      const s = this.findMaxState();
      if (!s) return null;
      const statusUpper = (this.status || "").toString().toUpperCase();
      const phaseUpper = (this.phase || "").toString().toUpperCase();
      const terminal =
        phaseUpper === "COMPLETED" ||
        ["STOPPED", "FAILED", "CANCELLED"].includes(statusUpper) ||
        !!s.completed;
      if (!terminal) return null;

      const finalReason = s.final_reason != null ? String(s.final_reason) : "";
      const trimmedFinal = finalReason.trim();
      if (trimmedFinal) {
        // Older runs may have a misleading final_reason due to controller state ordering.
        // If we have an explicit unstable step with a stop reason, prefer that.
        if (trimmedFinal.toLowerCase() === "reached max workers") {
          const unstable = this._findMaxLastUnstableStep();
          const stop = unstable && unstable.stop_reason != null ? String(unstable.stop_reason).trim() : "";
          if (stop) return stop;
        }
        return trimmedFinal;
      }
      const stopReason = s.stop_reason != null ? String(s.stop_reason) : "";
      if (stopReason.trim()) return stopReason.trim();
      const unstable = this._findMaxLastUnstableStep();
      const unstableStop = unstable && unstable.stop_reason != null ? String(unstable.stop_reason).trim() : "";
      if (unstableStop) return unstableStop;
      if (statusUpper === "CANCELLED") return "Cancelled by user";
      return null;
    },

    _clearFindMaxCountdown() {
      if (this._findMaxCountdownIntervalId) {
        clearInterval(this._findMaxCountdownIntervalId);
        this._findMaxCountdownIntervalId = null;
      }
      this._findMaxCountdownTargetEpochMs = null;
      this.findMaxCountdownSeconds = null;
    },

    syncFindMaxCountdown() {
      if (this.mode !== "live") {
        this._clearFindMaxCountdown();
        return;
      }
      if (!this.isFindMaxMode()) {
        this._clearFindMaxCountdown();
        return;
      }
      const s = this.findMaxState();
      if (!s) {
        this._clearFindMaxCountdown();
        return;
      }

      const statusUpper = (this.status || "").toString().toUpperCase();
      const phaseUpper = (this.phase || "").toString().toUpperCase();
      const terminal =
        phaseUpper === "COMPLETED" || ["STOPPED", "FAILED", "CANCELLED"].includes(statusUpper);
      if (terminal) {
        this._clearFindMaxCountdown();
        return;
      }

      const endEpoch = Number(s.step_end_at_epoch_ms);
      if (!Number.isFinite(endEpoch) || endEpoch <= 0) {
        this._clearFindMaxCountdown();
        return;
      }

      const target = Math.trunc(endEpoch);
      if (this._findMaxCountdownTargetEpochMs === target && this._findMaxCountdownIntervalId) {
        return;
      }

      this._clearFindMaxCountdown();
      this._findMaxCountdownTargetEpochMs = target;

      const tick = () => {
        const remaining = Math.max(0, Math.ceil((target - Date.now()) / 1000));
        this.findMaxCountdownSeconds = remaining;
        if (remaining <= 0 && this._findMaxCountdownIntervalId) {
          clearInterval(this._findMaxCountdownIntervalId);
          this._findMaxCountdownIntervalId = null;
        }
      };

      tick();
      this._findMaxCountdownIntervalId = setInterval(tick, 250);
    },

    _kindKey(kind) {
      const k = kind != null ? String(kind).toUpperCase() : "";
      if (k === "POINT_LOOKUP") return "point_lookup";
      if (k === "RANGE_SCAN") return "range_scan";
      if (k === "INSERT") return "insert";
      if (k === "UPDATE") return "update";
      return "";
    },

    workloadPct(kind) {
      const info = this.templateInfo;
      if (!info) return 0;
      const k = this._kindKey(kind);
      if (!k) return 0;
      const field =
        k === "point_lookup"
          ? "custom_point_lookup_pct"
          : k === "range_scan"
            ? "custom_range_scan_pct"
            : k === "insert"
              ? "custom_insert_pct"
              : "custom_update_pct";
      const n = Number(info[field] || 0);
      return Number.isFinite(n) ? n : 0;
    },

    sloTargetP95Ms(kind) {
      const info = this.templateInfo;
      if (!info) return null;
      const k = this._kindKey(kind);
      if (!k) return null;
      const field = `target_${k}_p95_latency_ms`;
      const n = Number(info[field] ?? -1);
      return Number.isFinite(n) ? n : null;
    },

    sloTargetP99Ms(kind) {
      const info = this.templateInfo;
      if (!info) return null;
      const k = this._kindKey(kind);
      if (!k) return null;
      const field = `target_${k}_p99_latency_ms`;
      const n = Number(info[field] ?? -1);
      return Number.isFinite(n) ? n : null;
    },

    sloTargetErrorPct(kind) {
      const info = this.templateInfo;
      if (!info) return null;
      const k = this._kindKey(kind);
      if (!k) return null;
      const field = `target_${k}_error_rate_pct`;
      const n = Number(info[field] ?? -1);
      return Number.isFinite(n) ? n : null;
    },

    sloObservedP95Ms(kind) {
      // Always use end-to-end (app) latencies for SLO evaluation.
      //
      // In FIND_MAX_CONCURRENCY, prefer per-step, per-kind latencies so the SLO
      // table reflects the step currently being evaluated (or the unstable step
      // when the run terminates).
      if (this.isFindMaxMode()) {
        const s = this.findMaxState();
        const terminal = this._findMaxIsTerminal(s);
        const step = terminal
          ? (this._findMaxLastUnstableStep() || null)
          : null;
        const hist = this._findMaxStepHistory();
        const effectiveStep = step || (hist.length ? hist[hist.length - 1] : null);
        return this.stepSloObservedP95Ms(kind, effectiveStep);
      }

      const info = this.templateInfo;
      if (!info) return null;
      const k = this._kindKey(kind);
      if (!k) return null;
      const field = `${k}_p95_latency_ms`;
      const n = Number(info[field] || 0);
      return Number.isFinite(n) && n > 0 ? n : null;
    },

    sloObservedP99Ms(kind) {
      // Always use end-to-end (app) latencies for SLO evaluation.
      if (this.isFindMaxMode()) {
        const s = this.findMaxState();
        const terminal = this._findMaxIsTerminal(s);
        const step = terminal
          ? (this._findMaxLastUnstableStep() || null)
          : null;
        const hist = this._findMaxStepHistory();
        const effectiveStep = step || (hist.length ? hist[hist.length - 1] : null);
        return this.stepSloObservedP99Ms(kind, effectiveStep);
      }

      const info = this.templateInfo;
      if (!info) return null;
      const k = this._kindKey(kind);
      if (!k) return null;
      const field = `${k}_p99_latency_ms`;
      const n = Number(info[field] || 0);
      return Number.isFinite(n) && n > 0 ? n : null;
    },

    sloObservedErrorPct(kind) {
      if (this.isFindMaxMode()) {
        const s = this.findMaxState();
        const terminal = this._findMaxIsTerminal(s);
        const step = terminal
          ? (this._findMaxLastUnstableStep() || null)
          : null;
        const hist = this._findMaxStepHistory();
        const effectiveStep = step || (hist.length ? hist[hist.length - 1] : null);
        return this.stepSloObservedErrorPct(kind, effectiveStep);
      }

      const info = this.templateInfo;
      if (!info) return null;
      const k = this._kindKey(kind);
      if (!k) return null;
      const field = `${k}_error_rate_pct`;
      const v = info[field];
      if (v == null) return null;
      const n = Number(v);
      return Number.isFinite(n) ? n : null;
    },

    hasSloTargets() {
      const kinds = ["POINT_LOOKUP", "RANGE_SCAN", "INSERT", "UPDATE"];
      for (const k of kinds) {
        if (this.workloadPct(k) <= 0) continue;
        const t95 = this.sloTargetP95Ms(k);
        const t99 = this.sloTargetP99Ms(k);
        const terr = this.sloTargetErrorPct(k);
        if (
          (t95 != null && t95 >= 0) ||
          (t99 != null && t99 >= 0) ||
          (terr != null && terr >= 0)
        ) {
          return true;
        }
      }
      return false;
    },

    _stepKindMetric(step, kind, field) {
      if (!step || typeof step !== "object") return null;
      const metrics = step.kind_metrics;
      if (!metrics || typeof metrics !== "object") return null;
      const row = metrics[kind];
      if (!row || typeof row !== "object") return null;
      const v = row[field];
      if (v == null) return null;
      const n = Number(v);
      return Number.isFinite(n) ? n : null;
    },

    stepSloObservedP95Ms(kind, step) {
      const n = this._stepKindMetric(step, kind, "p95_latency_ms");
      return Number.isFinite(n) && n > 0 ? n : null;
    },

    stepSloObservedP99Ms(kind, step) {
      const n = this._stepKindMetric(step, kind, "p99_latency_ms");
      return Number.isFinite(n) && n > 0 ? n : null;
    },

    stepSloObservedErrorPct(kind, step) {
      return this._stepKindMetric(step, kind, "error_rate_pct");
    },

    stepSloRowStatus(step, kind) {
      const weight = this.workloadPct(kind);
      if (weight <= 0) return "N/A";

      const t95 = this.sloTargetP95Ms(kind);
      const t99 = this.sloTargetP99Ms(kind);
      const terr = this.sloTargetErrorPct(kind);
      const p95Enabled = t95 != null && t95 >= 0;
      const p99Enabled = t99 != null && t99 >= 0;
      const errEnabled = terr != null && terr >= 0;

      if (!p95Enabled && !p99Enabled && !errEnabled) return "DISABLED";
      if (p95Enabled && !(t95 > 0)) return "UNCONFIGURED";
      if (p99Enabled && !(t99 > 0)) return "UNCONFIGURED";
      if (errEnabled && (terr < 0 || terr > 100)) return "UNCONFIGURED";

      const o95 = p95Enabled ? this.stepSloObservedP95Ms(kind, step) : null;
      const o99 = p99Enabled ? this.stepSloObservedP99Ms(kind, step) : null;
      const oerr = errEnabled ? this.stepSloObservedErrorPct(kind, step) : null;
      if (
        (p95Enabled && o95 == null) ||
        (p99Enabled && o99 == null) ||
        (errEnabled && oerr == null)
      ) {
        return "PENDING";
      }

      const ok =
        (!p95Enabled || (o95 != null && o95 <= t95)) &&
        (!p99Enabled || (o99 != null && o99 <= t99)) &&
        (!errEnabled || (oerr != null && oerr <= terr));
      return ok ? "PASS" : "FAIL";
    },

    stepSloStatus(step) {
      const kinds = ["POINT_LOOKUP", "RANGE_SCAN", "INSERT", "UPDATE"];
      let anyTargetsEnabled = false;
      let sawPending = false;
      let sawUnconfigured = false;

      for (const k of kinds) {
        if (this.workloadPct(k) <= 0) continue;
        const s = this.stepSloRowStatus(step, k);
        if (s === "FAIL") return "FAIL";
        if (s === "PENDING") sawPending = true;
        if (s === "UNCONFIGURED") sawUnconfigured = true;
        if (s !== "DISABLED") anyTargetsEnabled = true;
      }

      if (!anyTargetsEnabled) return "DISABLED";
      if (sawPending) return "PENDING";
      if (sawUnconfigured) return "UNCONFIGURED";
      return "PASS";
    },

    stepSloBadgeClass(step) {
      const s = String(this.stepSloStatus(step) || "").toUpperCase();
      if (s === "PASS") return "status-running";
      if (s === "FAIL") return "status-failed";
      if (s === "PENDING") return "status-processing";
      if (s === "UNCONFIGURED") return "status-preparing";
      return "status-prepared";
    },

    thresholdClass(observed, target, allowZeroTarget = false) {
      const o = Number(observed);
      const t = Number(target);
      if (!Number.isFinite(o) || !Number.isFinite(t)) return "";
      if (t < 0) return "";
      if (t === 0) {
        return allowZeroTarget && o > 0 ? "metric-over" : "";
      }
      if (o > t) return "metric-over";
      if (o >= t * 0.9) return "metric-near";
      return "";
    },

    sloRowStatus(kind) {
      if (this.isFindMaxMode()) {
        const s = this.findMaxState();
        const terminal = this._findMaxIsTerminal(s);
        const step = terminal
          ? (this._findMaxLastUnstableStep() || null)
          : null;
        const hist = this._findMaxStepHistory();
        const effectiveStep = step || (hist.length ? hist[hist.length - 1] : null);
        return this.stepSloRowStatus(effectiveStep, kind);
      }

      const weight = this.workloadPct(kind);
      if (weight <= 0) return "N/A";

      const t95 = this.sloTargetP95Ms(kind);
      const t99 = this.sloTargetP99Ms(kind);
      const terr = this.sloTargetErrorPct(kind);
      const p95Enabled = t95 != null && t95 >= 0;
      const p99Enabled = t99 != null && t99 >= 0;
      const errEnabled = terr != null && terr >= 0;

      if (!p95Enabled && !p99Enabled && !errEnabled) return "DISABLED";
      if (p95Enabled && !(t95 > 0)) return "UNCONFIGURED";
      if (p99Enabled && !(t99 > 0)) return "UNCONFIGURED";
      if (errEnabled && (terr < 0 || terr > 100)) return "UNCONFIGURED";

      const o95 = p95Enabled ? this.sloObservedP95Ms(kind) : null;
      const o99 = p99Enabled ? this.sloObservedP99Ms(kind) : null;
      const oerr = errEnabled ? this.sloObservedErrorPct(kind) : null;
      if (
        (p95Enabled && o95 == null) ||
        (p99Enabled && o99 == null) ||
        (errEnabled && oerr == null)
      ) {
        return "PENDING";
      }

      const ok =
        (!p95Enabled || (o95 != null && o95 <= t95)) &&
        (!p99Enabled || (o99 != null && o99 <= t99)) &&
        (!errEnabled || (oerr != null && oerr <= terr));
      return ok ? "PASS" : "FAIL";
    },

    sloBadgeClass(kind) {
      const s = String(this.sloRowStatus(kind) || "").toUpperCase();
      if (s === "PASS") return "status-running";
      if (s === "FAIL") return "status-failed";
      if (s === "PENDING") return "status-processing";
      if (s === "UNCONFIGURED") return "status-preparing";
      return "status-prepared";
    },

    sloOverallStatus() {
      if (this.isFindMaxMode()) {
        const s = this.findMaxState();
        const terminal = this._findMaxIsTerminal(s);
        const step = terminal
          ? (this._findMaxLastUnstableStep() || null)
          : null;
        const hist = this._findMaxStepHistory();
        const effectiveStep = step || (hist.length ? hist[hist.length - 1] : null);
        return this.stepSloStatus(effectiveStep);
      }

      const kinds = ["POINT_LOOKUP", "RANGE_SCAN", "INSERT", "UPDATE"];
      let anyTargetsEnabled = false;
      let sawPending = false;
      let sawUnconfigured = false;

      for (const k of kinds) {
        const weight = this.workloadPct(k);
        if (weight <= 0) continue;
        const s = this.sloRowStatus(k);
        if (s === "FAIL") return "FAIL";
        if (s === "PENDING") sawPending = true;
        if (s === "UNCONFIGURED") sawUnconfigured = true;
        if (s !== "DISABLED") anyTargetsEnabled = true;
      }

      if (!anyTargetsEnabled) return "DISABLED";
      if (sawPending) return "PENDING";
      if (sawUnconfigured) return "UNCONFIGURED";
      return "PASS";
    },

    sloOverallBadgeClass() {
      const s = String(this.sloOverallStatus() || "").toUpperCase();
      if (s === "PASS") return "status-running";
      if (s === "FAIL") return "status-failed";
      if (s === "PENDING") return "status-processing";
      if (s === "UNCONFIGURED") return "status-preparing";
      return "status-prepared";
    },

    sfLatencyAvailable() {
      return !!(this.templateInfo && this.templateInfo.sf_latency_available);
    },

    isFinalMetricsReady() {
      const status = (this.status || "").toString().toUpperCase();
      const phase = (this.phase || "").toString().toUpperCase();

      // For historical rows (no in-memory phase), status alone is the best signal.
      if (!phase) return status === "COMPLETED";

      // When phase is available, require post-processing completion as well.
      return phase === "COMPLETED" && status === "COMPLETED";
    },

    sfLatencyDisabledReason() {
      if (!this.testId) return "Select a test to view SQL execution timings.";

      const phase = (this.phase || "").toString().toUpperCase();
      if (phase && phase !== "COMPLETED") {
        return "SQL execution timings are available after processing completes.";
      }

      const status = (this.status || "").toString().toUpperCase();
      if (status && status !== "COMPLETED") {
        return "SQL execution timings are available after processing completes.";
      }

      return "SQL execution timings are not available for this run.";
    },

    setLatencyView(view) {
      const v = view != null ? String(view) : "";
      if (v === "sf_execution") {
        if (!this.sfLatencyAvailable()) return;
        this.latencyView = "sf_execution";
        this.latencyViewUserSelected = true;
        return;
      }
      this.latencyView = "end_to_end";
      this.latencyViewUserSelected = true;
    },

    latencyViewLabel() {
      if (this.latencyView === "sf_execution") return "SQL execution (Snowflake)";
      return "End-to-end (app)";
    },

    currentLatencyMs(pct) {
      const p = Number(pct);
      if (this.latencyView === "sf_execution") {
        if (!this.templateInfo) return null;
        if (!this.sfLatencyAvailable()) return null;
        if (p === 50) return this.templateInfo.sf_p50_latency_ms;
        if (p === 95) return this.templateInfo.sf_p95_latency_ms;
        if (p === 99) return this.templateInfo.sf_p99_latency_ms;
        return null;
      }
      if (p === 50) return this.metrics.p50_latency;
      if (p === 95) return this.metrics.p95_latency;
      if (p === 99) return this.metrics.p99_latency;
      return null;
    },

    detailLatency(fieldName) {
      if (!this.templateInfo) return null;
      const base = fieldName != null ? String(fieldName) : "";
      if (!base) return null;

      if (this.latencyView === "sf_execution") {
        if (!this.sfLatencyAvailable()) return null;
        const sfKey = `sf_${base}`;
        return this.templateInfo[sfKey];
      }
      return this.templateInfo[base];
    },

    async loadTestInfo() {
      if (!this.testId) return;
      try {
        const resp = await fetch(`/api/tests/${this.testId}`);
        if (!resp.ok) return;
        const data = await resp.json();
        this.templateInfo = data;
        this.duration = data.duration_seconds || 0;
        this.status = data.status || null;
        if (data.phase != null) {
          this.phase = data.phase;
        } else if (!this.phase && data.status) {
          this.phase = data.status;
        }

        // Ensure the "Concurrent Queries" chart uses Postgres-appropriate legends
        // once we know table_type (initCharts runs before templateInfo is loaded).
        try {
          const canvas = document.getElementById("concurrencyChart");
          const chart =
            canvas &&
            (canvas.__chart ||
              (window.Chart && Chart.getChart ? Chart.getChart(canvas) : null));
          const tableType = (this.templateInfo?.table_type || "").toLowerCase();
          const isPostgres = ["postgres", "snowflake_postgres"].includes(tableType);
          const ds =
            chart && chart.data && Array.isArray(chart.data.datasets)
              ? chart.data.datasets
              : [];
          const hasLabel = (idx, label) => !!(ds[idx] && ds[idx].label === label);
          const needsRebuild =
            !!canvas &&
            (!chart ||
              (isPostgres && ds.length !== 2) ||
              (!isPostgres && ds.length !== 3) ||
              (isPostgres &&
                (!hasLabel(0, "Target workers") ||
                  !hasLabel(1, "In-flight queries"))) ||
            (!isPostgres &&
                (!hasLabel(0, "Target workers") ||
                  !hasLabel(1, "In-flight (client)") ||
                  !hasLabel(2, "Snowflake queued (queries)"))));
          if (needsRebuild) {
            this.initCharts({ onlyConcurrency: true });
          }
        } catch (_) {}

        // Load warehouse details to display MCW configuration
        this.loadWarehouseDetails();
        // History view: load aggregated error summary (Snowflake query history + local errors).
        if (this.mode === "history") {
          this.loadErrorSummary();
        }

        const sfAvail = !!data.sf_latency_available;
        if (!this.latencyViewUserSelected) {
          if (this.mode === "history" && sfAvail) {
            this.latencyView = "sf_execution";
          } else {
            this.latencyView = "end_to_end";
          }
        } else if (this.latencyView === "sf_execution" && !sfAvail) {
          this.latencyView = "end_to_end";
        }
        
        // If test is terminal, populate metrics from API response.
        // For COMPLETED runs, wait until post-processing phase is also complete when available.
        const statusUpper = (data.status || "").toString().toUpperCase();
        const isTerminal = ["COMPLETED", "STOPPED", "FAILED", "CANCELLED"].includes(
          statusUpper,
        );
        if (isTerminal && (statusUpper !== "COMPLETED" || this.isFinalMetricsReady())) {
          // Stop any running timer for terminal tests
          this.stopElapsedTimer();
          // Set final elapsed to duration for completed tests
          if (this.duration > 0) {
            this.elapsed = this.duration;
            this.progress = 100;
          }
          this.metrics.ops_per_sec = data.qps || 0;
          this.metrics.in_flight = 0;
          this.metrics.p50_latency = data.p50_latency_ms || 0;
          this.metrics.p95_latency = data.p95_latency_ms || 0;
          this.metrics.p99_latency = data.p99_latency_ms || 0;
          this.metrics.error_rate = data.total_operations > 0 
            ? ((data.failed_operations || 0) / data.total_operations) * 100 
            : 0;
          this.metrics.total_errors = data.failed_operations || 0;
          
          // Load historical metrics for completed tests to populate charts
          await this.loadHistoricalMetrics();

        // On history view, load per-second warehouse series once post-processing is complete.
        if (this.mode === "history" && this.isFinalMetricsReady()) {
          this.resetWarehouseTimeseriesRetry();
          await this.loadWarehouseTimeseries();
        }
        }
      } catch (e) {
        console.error("Failed to load test info:", e);
      }
    },

    async loadErrorSummary() {
      if (!this.testId) return;
      if (this.mode !== "history") return;

      this.errorSummaryLoading = true;
      this.errorSummaryError = null;
      this.errorSummaryRows = [];
      this.errorSummaryLoaded = false;

      try {
        const resp = await fetch(`/api/tests/${this.testId}/error-summary`);
        if (!resp.ok) {
          const payload = await resp.json().catch(() => ({}));
          const detail = payload && payload.detail ? payload.detail : null;
          throw new Error(
            (detail && (detail.message || detail.detail || detail)) ||
              `Failed to load error summary (HTTP ${resp.status})`,
          );
        }

        const data = await resp.json().catch(() => ({}));
        if (data && data.error) {
          throw new Error(String(data.error));
        }

        this.errorSummaryRows = data && Array.isArray(data.rows) ? data.rows : [];
      } catch (e) {
        console.error("Failed to load error summary:", e);
        this.errorSummaryError = e && e.message ? e.message : String(e);
        this.errorSummaryRows = [];
      } finally {
        this.errorSummaryLoading = false;
        this.errorSummaryLoaded = true;
      }
    },

    setSfRunningBreakdown(mode) {
      const m = mode != null ? String(mode) : "";
      if (m === "by_kind") {
        this.sfRunningBreakdown = "by_kind";
      } else {
        this.sfRunningBreakdown = "read_write";
      }
      this.applySfRunningBreakdownToChart();
    },

    applySfRunningBreakdownToChart(opts) {
      const options = opts && typeof opts === "object" ? opts : {};
      const skipUpdate = !!options.skipUpdate;

      const canvas = document.getElementById("sfRunningChart");
      const chart =
        canvas &&
        (canvas.__chart ||
          (window.Chart && Chart.getChart ? Chart.getChart(canvas) : null));
      if (!canvas || !chart) return;

      const byKind = this.sfRunningBreakdown === "by_kind";
      const dsets =
        chart.data && Array.isArray(chart.data.datasets)
          ? chart.data.datasets
          : [];

      const isReadWrite = (label) => {
        const l = String(label || "").toLowerCase();
        return l.startsWith("reads") || l.startsWith("writes");
      };
      const isKind = (label) => {
        const l = String(label || "").toLowerCase();
        return (
          l.startsWith("point") ||
          l.startsWith("range") ||
          l.startsWith("insert") ||
          l.startsWith("update")
        );
      };

      for (const ds of dsets) {
        const label = ds && ds.label ? ds.label : "";
        if (isReadWrite(label)) ds.hidden = byKind;
        if (isKind(label)) ds.hidden = !byKind;
      }

      if (!skipUpdate) {
        chart.update();
      }
    },

    setOpsSecBreakdown(mode) {
      const m = mode != null ? String(mode) : "";
      if (m === "by_kind") {
        this.opsSecBreakdown = "by_kind";
      } else {
        this.opsSecBreakdown = "read_write";
      }
      this.applyOpsSecBreakdownToChart();
    },

    applyOpsSecBreakdownToChart(opts) {
      const options = opts && typeof opts === "object" ? opts : {};
      const skipUpdate = !!options.skipUpdate;

      const canvas = document.getElementById("opsSecChart");
      const chart =
        canvas &&
        (canvas.__chart ||
          (window.Chart && Chart.getChart ? Chart.getChart(canvas) : null));
      if (!canvas || !chart) return;

      const byKind = this.opsSecBreakdown === "by_kind";
      const dsets =
        chart.data && Array.isArray(chart.data.datasets)
          ? chart.data.datasets
          : [];

      const isReadWrite = (label) => {
        const l = String(label || "").toLowerCase();
        return l === "reads" || l === "writes";
      };
      const isKind = (label) => {
        const l = String(label || "").toLowerCase();
        return (
          l.startsWith("point") ||
          l.startsWith("range") ||
          l.startsWith("insert") ||
          l.startsWith("update")
        );
      };

      for (const ds of dsets) {
        const label = ds && ds.label ? ds.label : "";
        if (isReadWrite(label)) ds.hidden = byKind;
        if (isKind(label)) ds.hidden = !byKind;
      }

      if (!skipUpdate) {
        chart.update();
      }
    },

    setWarehouseQueueMode(mode) {
      const m = mode != null ? String(mode) : "";
      if (m === "total") {
        this.warehouseQueueMode = "total";
      } else {
        this.warehouseQueueMode = "avg";
      }
      this.renderWarehouseTimeseriesChart();
    },

    resetWarehouseTimeseriesRetry() {
      this.warehouseTsRetryCount = 0;
      if (this.warehouseTsRetryTimerId) {
        clearTimeout(this.warehouseTsRetryTimerId);
      }
      this.warehouseTsRetryTimerId = null;
    },

    async loadWarehouseTimeseries() {
      if (!this.testId) return;
      this.warehouseTsLoading = true;
      this.warehouseTsError = null;
      this.warehouseTs = [];
      this.warehouseTsAvailable = false;
      this.warehouseTsLoaded = false;

      try {
        const resp = await fetch(`/api/tests/${this.testId}/warehouse-timeseries`);
        if (!resp.ok) {
          const payload = await resp.json().catch(() => ({}));
          const detail = payload && payload.detail ? payload.detail : null;
          throw new Error(
            (detail && (detail.message || detail.detail || detail)) ||
              `Failed to load warehouse timeseries (HTTP ${resp.status})`,
          );
        }

        const data = await resp.json();
        if (data && data.error) {
          throw new Error(String(data.error));
        }
        this.warehouseTs = data && Array.isArray(data.points) ? data.points : [];
        this.warehouseTsAvailable = !!(data && data.available);

        if (this.debug) {
          this._debugCharts("loadWarehouseTimeseries: before init", {
            available: this.warehouseTsAvailable,
            points: Array.isArray(this.warehouseTs) ? this.warehouseTs.length : 0,
          });
        }

        // Ensure warehouse chart exists, then render (do NOT reset other charts).
        this.initCharts({ onlyWarehouse: true });
        this.renderWarehouseTimeseriesChart();

        if (this.debug) {
          this._debugCharts("loadWarehouseTimeseries: after render", {
            available: this.warehouseTsAvailable,
            points: Array.isArray(this.warehouseTs) ? this.warehouseTs.length : 0,
          });
        }
        if (this.warehouseTsAvailable) {
          this.resetWarehouseTimeseriesRetry();
        } else if (this.mode === "history") {
          this.warehouseTsRetryCount += 1;
          if (this.warehouseTsRetryCount <= this.warehouseTsRetryMax) {
            if (!this.warehouseTsRetryTimerId) {
              this.warehouseTsRetryTimerId = setTimeout(() => {
                this.warehouseTsRetryTimerId = null;
                this.loadWarehouseTimeseries();
              }, this.warehouseTsRetryIntervalMs);
            }
          }
        }
      } catch (e) {
        console.error("Failed to load warehouse timeseries:", e);
        this.warehouseTsError = e && e.message ? e.message : String(e);
        this.warehouseTs = [];
        this.warehouseTsAvailable = false;
        this.resetWarehouseTimeseriesRetry();
      } finally {
        this.warehouseTsLoading = false;
        this.warehouseTsLoaded = true;
      }
    },

    renderWarehouseTimeseriesChart() {
      const canvas = document.getElementById("warehouseQueueChart");
      const chart =
        canvas &&
        (canvas.__chart ||
          (window.Chart && Chart.getChart ? Chart.getChart(canvas) : null));
      if (!canvas || !chart) return;

      const points = Array.isArray(this.warehouseTs) ? this.warehouseTs : [];

      const labels = [];
      const clusters = [];
      const overload = [];
      const provisioning = [];

      for (const p of points) {
        if (!p) continue;
        // Use elapsed_seconds for labels to avoid browser-specific parsing issues
        // with microsecond timestamps (e.g., "2026-01-08T18:24:28.734695").
        const secs = Number(p.elapsed_seconds || 0);
        const ts = `${this.formatSecondsTenths(secs)}s`;
        labels.push(ts);

        clusters.push(Number(p.active_clusters || 0));

        if (this.warehouseQueueMode === "total") {
          overload.push(Number(p.total_queue_overload_ms || 0));
          provisioning.push(Number(p.total_queue_provisioning_ms || 0));
        } else {
          overload.push(Number(p.avg_queue_overload_ms || 0));
          provisioning.push(Number(p.avg_queue_provisioning_ms || 0));
        }
      }

      chart.data.labels = labels;
      chart.data.datasets[0].data = clusters;
      chart.data.datasets[1].data = overload;
      chart.data.datasets[2].data = provisioning;

      const yTitle =
        this.warehouseQueueMode === "total"
          ? "Queued time (ms, total per second)"
          : "Queued time (ms, avg per query / second)";
      chart.options.scales.yQueue.title.text = yTitle;

      chart.update();
    },

    async loadLogs() {
      if (!this.testId) return;
      try {
        const resp = await fetch(`/api/tests/${this.testId}/logs?limit=${this.logMaxLines}`);
        if (!resp.ok) return;
        const data = await resp.json().catch(() => ({}));
        const logs = data && Array.isArray(data.logs) ? data.logs : [];
        this.appendLogs(logs);
      } catch (e) {
        console.error("Failed to load logs:", e);
      }
    },

    appendLogs(logs) {
      if (!logs) return;
      const items = Array.isArray(logs) ? logs : [logs];
      for (const item of items) {
        if (!item) continue;
        const logId = item.log_id || item.logId || `${item.timestamp || ""}-${item.seq || ""}`;
        if (this._logSeen[logId]) continue;
        this._logSeen[logId] = true;
        this.logs.push({
          log_id: logId,
          seq: Number(item.seq || 0),
          timestamp: item.timestamp || null,
          level: item.level || "INFO",
          logger: item.logger || null,
          message: item.message || "",
          exception: item.exception || null,
        });
      }

      this.logs.sort((a, b) => (a.seq || 0) - (b.seq || 0));
      if (this.logs.length > this.logMaxLines) {
        const removeCount = this.logs.length - this.logMaxLines;
        const removed = this.logs.splice(0, removeCount);
        for (const r of removed) {
          if (r && r.log_id) delete this._logSeen[r.log_id];
        }
      }
    },

    logsText() {
      if (!this.logs || this.logs.length === 0) return "";
      return this.logs
        .map((l) => {
          const ts = l.timestamp ? new Date(l.timestamp).toLocaleTimeString() : "";
          const lvl = String(l.level || "").toUpperCase();
          const logger = l.logger ? ` ${l.logger}` : "";
          const msg = l.message || "";
          const exc = l.exception ? `\n${l.exception}` : "";
          return `${ts} ${lvl}${logger} - ${msg}${exc}`;
        })
        .join("\n");
    },

    async loadHistoricalMetrics() {
      if (!this.testId) return;
      try {
        const resp = await fetch(`/api/tests/${this.testId}/metrics`);
        if (!resp.ok) return;
        const data = await resp.json();
        
        if (!data.snapshots || data.snapshots.length === 0) {
          console.log("No historical metrics snapshots found for test");
          return;
        }

        if (this.debug) {
          this._debugCharts("loadHistoricalMetrics: start", {
            snapshots: data.snapshots.length,
          });
        }
        
        // Populate charts with historical data
        let throughputCanvas = document.getElementById("throughputChart");
        let concurrencyCanvas = document.getElementById("concurrencyChart");
        let latencyCanvas = document.getElementById("latencyChart");
        let sfRunningCanvas = document.getElementById("sfRunningChart");
        let opsSecCanvas = document.getElementById("opsSecChart");
        let throughputChart = throughputCanvas && (throughputCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(throughputCanvas) : null));
        let concurrencyChart = concurrencyCanvas && (concurrencyCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(concurrencyCanvas) : null));
        let latencyChart = latencyCanvas && (latencyCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(latencyCanvas) : null));
        let sfRunningChart = sfRunningCanvas && (sfRunningCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(sfRunningCanvas) : null));
        let opsSecChart = opsSecCanvas && (opsSecCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(opsSecCanvas) : null));
        
        // If charts don't exist yet, initialize them
        if (!throughputChart || !latencyChart || (concurrencyCanvas && !concurrencyChart) || (sfRunningCanvas && !sfRunningChart) || (opsSecCanvas && !opsSecChart)) {
          this.initCharts();
        }
        
        // Re-query DOM after potential init (canvas refs may have been null initially)
        throughputCanvas = document.getElementById("throughputChart");
        concurrencyCanvas = document.getElementById("concurrencyChart");
        latencyCanvas = document.getElementById("latencyChart");
        sfRunningCanvas = document.getElementById("sfRunningChart");
        opsSecCanvas = document.getElementById("opsSecChart");
        const throughputChart2 = throughputCanvas && (throughputCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(throughputCanvas) : null));
        const concurrencyChart2 = concurrencyCanvas && (concurrencyCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(concurrencyCanvas) : null));
        const latencyChart2 = latencyCanvas && (latencyCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(latencyCanvas) : null));
        const sfRunningChart2 = sfRunningCanvas && (sfRunningCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(sfRunningCanvas) : null));
        const opsSecChart2 = opsSecCanvas && (opsSecCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(opsSecCanvas) : null));
        
        // Clear existing data
        if (throughputChart2) {
          throughputChart2.data.labels = [];
          throughputChart2.data.datasets[0].data = [];
          if (throughputChart2.data.datasets[1]) {
            throughputChart2.data.datasets[1].data = [];
          }
        }
        if (concurrencyChart2) {
          concurrencyChart2.data.labels = [];
          concurrencyChart2.data.datasets.forEach((ds) => {
            ds.data = [];
          });
        }
        if (latencyChart2) {
          latencyChart2.data.labels = [];
          latencyChart2.data.datasets[0].data = [];
          latencyChart2.data.datasets[1].data = [];
          latencyChart2.data.datasets[2].data = [];
        }
        if (sfRunningChart2) {
          sfRunningChart2.data.labels = [];
          sfRunningChart2.data.datasets.forEach((ds) => {
            ds.data = [];
          });
        }
        if (opsSecChart2) {
          opsSecChart2.data.labels = [];
          opsSecChart2.data.datasets.forEach((ds) => {
            ds.data = [];
          });
        }

        const tableType = (this.templateInfo?.table_type || "").toLowerCase();
        const isPostgres = ["postgres", "snowflake_postgres"].includes(tableType);
        
        // Populate with historical data
        for (const snapshot of data.snapshots) {
          // Use elapsed_seconds for labels to avoid browser-specific parsing issues
          // with microsecond timestamps (e.g., "2026-01-08T18:24:28.734695").
          const secs = Number(snapshot.elapsed_seconds || 0);
          const ts = `${this.formatSecondsTenths(secs)}s`;
          
          if (throughputChart2) {
            throughputChart2.data.labels.push(ts);
            throughputChart2.data.datasets[0].data.push(snapshot.ops_per_sec);
            // Error rate from historical snapshot (if available)
            if (throughputChart2.data.datasets[1]) {
              const errRate = Number(snapshot.error_rate || 0) * 100.0;
              throughputChart2.data.datasets[1].data.push(errRate);
            }
          }

          if (concurrencyChart2) {
            concurrencyChart2.data.labels.push(ts);
            const target = Number(snapshot.target_workers || 0);
            const inFlight = Number(snapshot.active_connections || 0);
            const sfRunning = Number(snapshot.sf_running || 0);
            const sfQueuedBench = Number(snapshot.sf_queued_bench || 0);
            const sfQueued = sfQueuedBench > 0 ? sfQueuedBench : Number(snapshot.sf_queued || 0);

            if (isPostgres) {
              if (concurrencyChart2.data.datasets[0]) {
                concurrencyChart2.data.datasets[0].data.push(target);
              }
              if (concurrencyChart2.data.datasets[1]) {
                concurrencyChart2.data.datasets[1].data.push(inFlight);
              }
            } else {
              // Snowflake: three datasets (target_workers, in_flight, sf_queued)
              if (concurrencyChart2.data.datasets[0]) {
                concurrencyChart2.data.datasets[0].data.push(target);
              }
              if (concurrencyChart2.data.datasets[1]) {
                concurrencyChart2.data.datasets[1].data.push(inFlight);
              }
              if (concurrencyChart2.data.datasets[2]) {
                concurrencyChart2.data.datasets[2].data.push(sfQueued);
              }
            }
          }
          
          if (latencyChart2) {
            latencyChart2.data.labels.push(ts);
            latencyChart2.data.datasets[0].data.push(snapshot.p50_latency);
            latencyChart2.data.datasets[1].data.push(snapshot.p95_latency);
            latencyChart2.data.datasets[2].data.push(snapshot.p99_latency);
          }

          if (sfRunningChart2) {
            sfRunningChart2.data.labels.push(ts);
            const totalTagged = Number(snapshot.sf_running_tagged || 0);
            const totalRaw = Number(snapshot.sf_running || 0);
            const total = totalTagged > 0 ? totalTagged : totalRaw;
            sfRunningChart2.data.datasets[0].data.push(total);
            sfRunningChart2.data.datasets[1].data.push(
              Number(snapshot.sf_running_read || 0),
            );
            sfRunningChart2.data.datasets[2].data.push(
              Number(snapshot.sf_running_point_lookup || 0),
            );
            sfRunningChart2.data.datasets[3].data.push(
              Number(snapshot.sf_running_range_scan || 0),
            );
            sfRunningChart2.data.datasets[4].data.push(
              Number(snapshot.sf_running_write || 0),
            );
            sfRunningChart2.data.datasets[5].data.push(
              Number(snapshot.sf_running_insert || 0),
            );
            sfRunningChart2.data.datasets[6].data.push(
              Number(snapshot.sf_running_update || 0),
            );
            sfRunningChart2.data.datasets[7].data.push(
              Number(snapshot.sf_blocked || 0),
            );
          }

          if (opsSecChart2) {
            opsSecChart2.data.labels.push(ts);
            // Total QPS (from the snapshot, already computed)
            const totalOps = Number(snapshot.ops_per_sec || 0);
            const readOps = Number(snapshot.app_read_ops_sec || 0);
            const writeOps = Number(snapshot.app_write_ops_sec || 0);
            const plOps = Number(snapshot.app_point_lookup_ops_sec || 0);
            const rsOps = Number(snapshot.app_range_scan_ops_sec || 0);
            const insOps = Number(snapshot.app_insert_ops_sec || 0);
            const updOps = Number(snapshot.app_update_ops_sec || 0);
            opsSecChart2.data.datasets[0].data.push(totalOps);
            opsSecChart2.data.datasets[1].data.push(readOps);
            opsSecChart2.data.datasets[2].data.push(plOps);
            opsSecChart2.data.datasets[3].data.push(rsOps);
            opsSecChart2.data.datasets[4].data.push(writeOps);
            opsSecChart2.data.datasets[5].data.push(insOps);
            opsSecChart2.data.datasets[6].data.push(updOps);
          }
        }
        
        // Update charts
        if (throughputChart2) throughputChart2.update();
        if (concurrencyChart2) concurrencyChart2.update();
        if (latencyChart2) latencyChart2.update();
        if (sfRunningChart2) {
          this.applySfRunningBreakdownToChart({ skipUpdate: true });
          sfRunningChart2.update();
        }
        if (opsSecChart2) {
          this.applyOpsSecBreakdownToChart({ skipUpdate: true });
          opsSecChart2.update();
        }

        if (this.debug) {
          this._debugCharts("loadHistoricalMetrics: after render", {
            snapshots: data.snapshots.length,
          });
        }
        
        console.log(`Loaded ${data.snapshots.length} historical metrics snapshots`);
      } catch (e) {
        console.error("Failed to load historical metrics:", e);
        try {
          if (window.toast && typeof window.toast.error === "function") {
            window.toast.error(
              `Failed to render charts: ${e && e.message ? e.message : String(e)}`,
            );
          }
        } catch (_) {}
      }
    },

    initCharts(opts) {
      const initOpts = opts && typeof opts === "object" ? opts : {};
      const onlyWarehouse = !!initOpts.onlyWarehouse;
      const onlyConcurrency = !!initOpts.onlyConcurrency;

      if (this.debug) {
        console.log("[dashboard][debug] initCharts", { onlyWarehouse });
      }

      // Make chart init idempotent. HTMX/Alpine can re-initialize components and
      // Chart.js will break if we re-use the same canvas without destroying.
      const safeDestroy = (chart) => {
        if (!chart) return;
        try {
          chart.destroy();
        } catch (_) {}
      };

      const formatCompact = (v) => this.formatCompact(v);

      // Allow callers (e.g. after templateInfo loads) to rebuild ONLY the concurrency chart
      // so we can correct legends/datasets for Postgres without resetting other charts.
      if (!onlyWarehouse && onlyConcurrency) {
        const concurrencyCanvas = document.getElementById("concurrencyChart");
        if (concurrencyCanvas && window.Chart && Chart.getChart) {
          safeDestroy(Chart.getChart(concurrencyCanvas));
        }
        const concurrencyCtx =
          concurrencyCanvas && concurrencyCanvas.getContext
            ? concurrencyCanvas.getContext("2d")
            : null;
        if (concurrencyCtx) {
          const tableType = (this.templateInfo?.table_type || "").toLowerCase();
          const isPostgres = ["postgres", "snowflake_postgres"].includes(tableType);
          // Target workers dataset (common to all modes)
          const targetWorkersDataset = {
            label: "Target workers",
            data: [],
            borderColor: "rgb(34, 197, 94)",
            backgroundColor: "transparent",
            tension: 0.1,
            borderDash: [4, 4],
            borderWidth: 2,
          };

          // For Postgres: show Target + In-flight; for Snowflake: show all four series
          const datasets = isPostgres
            ? [
                targetWorkersDataset,
                {
                  label: "In-flight queries",
                  data: [],
                  borderColor: "rgb(245, 158, 11)",
                  backgroundColor: "transparent",
                  tension: 0.4,
                },
              ]
            : [
                targetWorkersDataset,
                {
                  label: "In-flight (client)",
                  data: [],
                  borderColor: "rgb(245, 158, 11)",
                  backgroundColor: "transparent",
                  tension: 0.4,
                },
                {
                  label: "Snowflake queued (queries)",
                  data: [],
                  borderColor: "rgb(239, 68, 68)",
                  backgroundColor: "transparent",
                  tension: 0.4,
                  borderDash: [6, 4],
                },
              ];

          concurrencyCanvas.__chart = new Chart(concurrencyCtx, {
            type: "line",
            data: { labels: [], datasets: datasets },
            options: {
              responsive: true,
              maintainAspectRatio: false,
              animation: { duration: 0 },
              interaction: { mode: "index", intersect: false },
              scales: {
                y: {
                  beginAtZero: true,
                  ticks: { callback: (value) => formatCompact(value) },
                },
              },
              plugins: {
                tooltip: {
                  callbacks: {
                    label: (ctx) =>
                      `${ctx.dataset.label}: ${formatCompact(ctx.parsed.y)}`,
                  },
                },
              },
            },
          });
        }
        return;
      }

      const throughputCanvas = document.getElementById("throughputChart");
      if (!onlyWarehouse) {
        // If Alpine re-initializes, our old chart refs are lost, but Chart.js still has
        // a chart bound to the canvas. Destroy by canvas first.
        if (throughputCanvas && window.Chart && Chart.getChart) {
          safeDestroy(Chart.getChart(throughputCanvas));
        }

        const throughputCtx = throughputCanvas && throughputCanvas.getContext
          ? throughputCanvas.getContext("2d")
          : null;
        if (throughputCtx) {
          // Store chart instance on the canvas (non-reactive).
          throughputCanvas.__chart = new Chart(throughputCtx, {
            type: "line",
            data: {
              labels: [],
              datasets: [
                {
                  label: "QPS",
                  data: [],
                  yAxisID: "yQPS",
                  borderColor: "rgb(59, 130, 246)",
                  backgroundColor: "rgba(59, 130, 246, 0.1)",
                  tension: 0.4,
                  fill: true,
                },
                {
                  label: "Error Rate (%)",
                  data: [],
                  yAxisID: "yError",
                  borderColor: "rgb(239, 68, 68)",
                  backgroundColor: "transparent",
                  tension: 0.4,
                  borderDash: [4, 2],
                },
              ],
            },
            options: {
              responsive: true,
              maintainAspectRatio: false,
              animation: { duration: 0 },
              interaction: { mode: "index", intersect: false },
              scales: {
                yQPS: {
                  position: "left",
                  beginAtZero: true,
                  ticks: {
                    callback: (value) => formatCompact(value),
                  },
                  title: { display: true, text: "QPS" },
                },
                yError: {
                  position: "right",
                  beginAtZero: true,
                  min: 0,
                  max: 100,
                  grid: { drawOnChartArea: false },
                  ticks: {
                    callback: (value) => `${value}%`,
                  },
                  title: { display: true, text: "Error Rate (%)"},
                },
              },
              plugins: {
                tooltip: {
                  callbacks: {
                    label: (ctx) => {
                      if (ctx.dataset.yAxisID === "yError") {
                        return `${ctx.dataset.label}: ${Number(ctx.parsed.y).toFixed(2)}%`;
                      }
                      return `${ctx.dataset.label}: ${formatCompact(ctx.parsed.y)}`;
                    },
                  },
                },
              },
            },
          });
        }
      }

      const concurrencyCanvas = document.getElementById("concurrencyChart");
      if (!onlyWarehouse) {
        if (concurrencyCanvas && window.Chart && Chart.getChart) {
          safeDestroy(Chart.getChart(concurrencyCanvas));
        }

        const concurrencyCtx =
          concurrencyCanvas && concurrencyCanvas.getContext
            ? concurrencyCanvas.getContext("2d")
            : null;
        if (concurrencyCtx) {
          // Check if this is a Postgres table type
          const tableType = (this.templateInfo?.table_type || "").toLowerCase();
          const isPostgres = ["postgres", "snowflake_postgres"].includes(tableType);

          // Target workers dataset (common to all modes)
          const targetWorkersDataset = {
            label: "Target workers",
            data: [],
            borderColor: "rgb(34, 197, 94)",
            backgroundColor: "transparent",
            tension: 0.1,
            borderDash: [4, 4],
            borderWidth: 2,
          };

          // For Postgres: show Target + In-flight; for Snowflake: show all four series
          const datasets = isPostgres
            ? [
                targetWorkersDataset,
                {
                  label: "In-flight queries",
                  data: [],
                  borderColor: "rgb(245, 158, 11)",
                  backgroundColor: "transparent",
                  tension: 0.4,
                },
              ]
            : [
                targetWorkersDataset,
                {
                  label: "In-flight (client)",
                  data: [],
                  borderColor: "rgb(245, 158, 11)",
                  backgroundColor: "transparent",
                  tension: 0.4,
                },
                {
                  label: "Snowflake queued (warehouse)",
                  data: [],
                  borderColor: "rgb(239, 68, 68)",
                  backgroundColor: "transparent",
                  tension: 0.4,
                  borderDash: [6, 4],
                },
              ];

          concurrencyCanvas.__chart = new Chart(concurrencyCtx, {
            type: "line",
            data: {
              labels: [],
              datasets: datasets,
            },
            options: {
              responsive: true,
              maintainAspectRatio: false,
              animation: { duration: 0 },
              interaction: { mode: "index", intersect: false },
              scales: {
                y: {
                  beginAtZero: true,
                  ticks: {
                    callback: (value) => formatCompact(value),
                  },
                },
              },
              plugins: {
                tooltip: {
                  callbacks: {
                    label: (ctx) =>
                      `${ctx.dataset.label}: ${formatCompact(ctx.parsed.y)}`,
                  },
                },
              },
            },
          });
        }
      }

      const sfRunningCanvas = document.getElementById("sfRunningChart");
      if (!onlyWarehouse) {
        if (sfRunningCanvas && window.Chart && Chart.getChart) {
          safeDestroy(Chart.getChart(sfRunningCanvas));
        }

        const sfRunningCtx =
          sfRunningCanvas && sfRunningCanvas.getContext
            ? sfRunningCanvas.getContext("2d")
            : null;
        if (sfRunningCtx) {
          const byKind = this.sfRunningBreakdown === "by_kind";

          sfRunningCanvas.__chart = new Chart(sfRunningCtx, {
            type: "line",
            data: {
              labels: [],
              datasets: [
                {
                  label: "Total (tagged)",
                  data: [],
                  borderColor: "rgb(59, 130, 246)",
                  backgroundColor: "transparent",
                  tension: 0.4,
                },
                {
                  label: "Reads (tagged)",
                  data: [],
                  borderColor: "rgb(34, 197, 94)",
                  backgroundColor: "transparent",
                  tension: 0.4,
                  hidden: byKind,
                },
                {
                  label: "Point Lookup",
                  data: [],
                  borderColor: "rgb(20, 184, 166)",
                  backgroundColor: "transparent",
                  tension: 0.4,
                  hidden: !byKind,
                },
                {
                  label: "Range Scan",
                  data: [],
                  borderColor: "rgb(6, 182, 212)",
                  backgroundColor: "transparent",
                  tension: 0.4,
                  hidden: !byKind,
                },
                {
                  label: "Writes (tagged)",
                  data: [],
                  borderColor: "rgb(249, 115, 22)",
                  backgroundColor: "transparent",
                  tension: 0.4,
                  hidden: byKind,
                },
                {
                  label: "Insert",
                  data: [],
                  borderColor: "rgb(245, 158, 11)",
                  backgroundColor: "transparent",
                  tension: 0.4,
                  hidden: !byKind,
                },
                {
                  label: "Update",
                  data: [],
                  borderColor: "rgb(239, 68, 68)",
                  backgroundColor: "transparent",
                  tension: 0.4,
                  hidden: !byKind,
                },
                {
                  label: "Blocked",
                  data: [],
                  borderColor: "rgb(168, 85, 247)",
                  backgroundColor: "transparent",
                  tension: 0.4,
                  borderDash: [6, 4],
                },
              ],
            },
            options: {
              responsive: true,
              maintainAspectRatio: false,
              animation: { duration: 0 },
              interaction: { mode: "index", intersect: false },
              scales: {
                y: {
                  beginAtZero: true,
                  ticks: {
                    callback: (value) => formatCompact(value),
                  },
                  title: { display: true, text: "Running Queries" },
                },
              },
              plugins: {
                tooltip: {
                  mode: 'index',
                  intersect: false,
                  callbacks: {
                    label: (ctx) =>
                      `${ctx.dataset.label}: ${formatCompact(ctx.parsed.y)}`,
                  },
                },
              },
            },
          });
        }
      }

      // OPS/SEC Breakdown Chart (history view)
      const opsSecCanvas = document.getElementById("opsSecChart");
      if (!onlyWarehouse) {
        if (opsSecCanvas && window.Chart && Chart.getChart) {
          safeDestroy(Chart.getChart(opsSecCanvas));
        }

        const opsSecCtx =
          opsSecCanvas && opsSecCanvas.getContext
            ? opsSecCanvas.getContext("2d")
            : null;
        if (opsSecCtx) {
          const byKindOps = this.opsSecBreakdown === "by_kind";

          opsSecCanvas.__chart = new Chart(opsSecCtx, {
            type: "line",
            data: {
              labels: [],
              datasets: [
                {
                  label: "Total",
                  data: [],
                  borderColor: "rgb(59, 130, 246)",
                  backgroundColor: "transparent",
                  tension: 0.4,
                },
                {
                  label: "Reads",
                  data: [],
                  borderColor: "rgb(34, 197, 94)",
                  backgroundColor: "transparent",
                  tension: 0.4,
                  hidden: byKindOps,
                },
                {
                  label: "Point Lookup",
                  data: [],
                  borderColor: "rgb(20, 184, 166)",
                  backgroundColor: "transparent",
                  tension: 0.4,
                  hidden: !byKindOps,
                },
                {
                  label: "Range Scan",
                  data: [],
                  borderColor: "rgb(6, 182, 212)",
                  backgroundColor: "transparent",
                  tension: 0.4,
                  hidden: !byKindOps,
                },
                {
                  label: "Writes",
                  data: [],
                  borderColor: "rgb(249, 115, 22)",
                  backgroundColor: "transparent",
                  tension: 0.4,
                  hidden: byKindOps,
                },
                {
                  label: "Insert",
                  data: [],
                  borderColor: "rgb(245, 158, 11)",
                  backgroundColor: "transparent",
                  tension: 0.4,
                  hidden: !byKindOps,
                },
                {
                  label: "Update",
                  data: [],
                  borderColor: "rgb(239, 68, 68)",
                  backgroundColor: "transparent",
                  tension: 0.4,
                  hidden: !byKindOps,
                },
              ],
            },
            options: {
              responsive: true,
              maintainAspectRatio: false,
              animation: { duration: 0 },
              interaction: { mode: "index", intersect: false },
              scales: {
                y: {
                  beginAtZero: true,
                  ticks: {
                    callback: (value) => formatCompact(value),
                  },
                  title: { display: true, text: "QPS" },
                },
              },
              plugins: {
                tooltip: {
                  mode: 'index',
                  intersect: false,
                  callbacks: {
                    label: (ctx) =>
                      `${ctx.dataset.label}: ${formatCompact(ctx.parsed.y)} QPS`,
                  },
                },
              },
            },
          });
        }
      }

      const latencyCanvas = document.getElementById("latencyChart");
      if (!onlyWarehouse) {
        if (latencyCanvas && window.Chart && Chart.getChart) {
          safeDestroy(Chart.getChart(latencyCanvas));
        }

        const latencyCtx = latencyCanvas && latencyCanvas.getContext
          ? latencyCanvas.getContext("2d")
          : null;
        if (latencyCtx) {
          latencyCanvas.__chart = new Chart(latencyCtx, {
            type: "line",
            data: {
              labels: [],
              datasets: [
                {
                  label: "P50",
                  data: [],
                  borderColor: "rgb(16, 185, 129)",
                  backgroundColor: "transparent",
                  tension: 0.4,
                },
                {
                  label: "P95",
                  data: [],
                  borderColor: "rgb(245, 158, 11)",
                  backgroundColor: "transparent",
                  tension: 0.4,
                },
                {
                  label: "P99",
                  data: [],
                  borderColor: "rgb(239, 68, 68)",
                  backgroundColor: "transparent",
                  tension: 0.4,
                },
              ],
            },
            options: {
              responsive: true,
              maintainAspectRatio: false,
              animation: { duration: 0 },
              scales: {
                y: {
                  beginAtZero: true,
                  ticks: {
                    callback: (value) => `${Number(value).toFixed(2)} ms`,
                  },
                },
              },
              plugins: {
                tooltip: {
                  mode: 'index',
                  intersect: false,
                  callbacks: {
                    label: (ctx) =>
                      `${ctx.dataset.label}: ${Number(ctx.parsed.y).toFixed(2)} ms`,
                  },
                },
              },
            },
          });
        }
      }

      // MCW Live Chart (real-time active clusters for live dashboard)
      const mcwLiveCanvas = document.getElementById("mcwLiveChart");
      if (!onlyWarehouse) {
        if (mcwLiveCanvas && window.Chart && Chart.getChart) {
          safeDestroy(Chart.getChart(mcwLiveCanvas));
        }

        const mcwLiveCtx = mcwLiveCanvas && mcwLiveCanvas.getContext
          ? mcwLiveCanvas.getContext("2d")
          : null;
        if (mcwLiveCtx) {
          mcwLiveCanvas.__chart = new Chart(mcwLiveCtx, {
            type: "line",
            data: {
              labels: [],
              datasets: [
                {
                  label: "Active Clusters",
                  data: [],
                  borderColor: "rgb(59, 130, 246)",
                  backgroundColor: "rgba(59, 130, 246, 0.1)",
                  tension: 0.0,
                  stepped: true,
                  fill: true,
                },
              ],
            },
            options: {
              responsive: true,
              maintainAspectRatio: false,
              animation: { duration: 0 },
              scales: {
                y: {
                  beginAtZero: true,
                  ticks: { precision: 0 },
                  title: { display: true, text: "Active Clusters (MCW)" },
                },
              },
              plugins: {
                tooltip: {
                  callbacks: {
                    label: (ctx) =>
                      `${ctx.dataset.label}: ${Math.trunc(Number(ctx.parsed.y))}`,
                  },
                },
              },
            },
          });
        }
      }

      const warehouseCanvas = document.getElementById("warehouseQueueChart");
      if (warehouseCanvas && window.Chart && Chart.getChart) {
        safeDestroy(Chart.getChart(warehouseCanvas));
      }

      const warehouseCtx =
        warehouseCanvas && warehouseCanvas.getContext
          ? warehouseCanvas.getContext("2d")
          : null;
      if (warehouseCtx) {
        warehouseCanvas.__chart = new Chart(warehouseCtx, {
          type: "line",
          data: {
            labels: [],
            datasets: [
              {
                label: "Active clusters",
                data: [],
                yAxisID: "yClusters",
                borderColor: "rgb(59, 130, 246)",
                backgroundColor: "transparent",
                tension: 0.0,
                stepped: true,
              },
              {
                label: "Queue overload (ms)",
                data: [],
                yAxisID: "yQueue",
                borderColor: "rgb(245, 158, 11)",
                backgroundColor: "transparent",
                tension: 0.2,
              },
              {
                label: "Queue provisioning (ms)",
                data: [],
                yAxisID: "yQueue",
                borderColor: "rgb(239, 68, 68)",
                backgroundColor: "transparent",
                tension: 0.2,
              },
            ],
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: { duration: 0 },
            interaction: { mode: "index", intersect: false },
            scales: {
              yClusters: {
                position: "left",
                beginAtZero: true,
                ticks: { precision: 0 },
                title: { display: true, text: "Active clusters" },
              },
              yQueue: {
                position: "right",
                beginAtZero: true,
                grid: { drawOnChartArea: false },
                title: { display: true, text: "Queued time (ms)" },
                ticks: {
                  callback: (value) => `${Number(value).toFixed(0)} ms`,
                },
              },
              x: {
                ticks: { maxTicksLimit: 12 },
              },
            },
            plugins: {
              tooltip: {
                callbacks: {
                  label: (ctx) => {
                    const y = ctx && ctx.parsed ? ctx.parsed.y : null;
                    if (y == null) return ctx.dataset.label;
                    if (ctx.dataset.yAxisID === "yClusters") {
                      return `${ctx.dataset.label}: ${Math.trunc(Number(y))}`;
                    }
                    return `${ctx.dataset.label}: ${Number(y).toFixed(2)} ms`;
                  },
                },
              },
            },
          },
        });
      }
    },

    connectWebSocket() {
      if (!this.testId) return;
      const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
      const wsUrl = `${protocol}//${window.location.host}/ws/test/${this.testId}`;
      this.websocket = new WebSocket(wsUrl);

      this.websocket.onopen = () => {
        // WebSocket connection indicates the dashboard is live, not that the test is running.
        this.testRunning = true;
      };

      this.websocket.onmessage = (event) => {
        let data = null;
        try {
          data = JSON.parse(event.data);
        } catch (e) {
          console.error("WebSocket message parse failed:", e, event.data);
          return;
        }
        // Ignore the initial connected message.
        if (data && data.status === "connected") return;
        if (data && data.kind === "log") {
          this.appendLogs(data);
          return;
        }
        if (data && data.kind === "log_batch") {
          const logs = data.logs && Array.isArray(data.logs) ? data.logs : [];
          this.appendLogs(logs);
          return;
        }
        this.applyMetricsPayload(data);
      };

      this.websocket.onerror = (error) => {
        console.error("WebSocket error:", error);
      };

      this.websocket.onclose = () => {
        this.testRunning = false;
      };
    },

    disconnectWebSocket() {
      if (this.websocket) {
        this.websocket.close();
        this.websocket = null;
      }
    },

    startElapsedTimer(baseValue) {
      this.stopElapsedTimer();
      const base = Number(baseValue) || 0;
      this._elapsedBaseValue = base;
      this._elapsedStartTime = Date.now();
      this._elapsedIntervalId = setInterval(() => {
        const secondsSinceStart = (Date.now() - this._elapsedStartTime) / 1000;
        this.elapsed = Math.floor(this._elapsedBaseValue + secondsSinceStart);
        if (this.duration > 0) {
          this.progress = Math.min(100, (this.elapsed / this.duration) * 100);
        }
      }, 250);
    },

    stopElapsedTimer() {
      if (this._elapsedIntervalId) {
        clearInterval(this._elapsedIntervalId);
        this._elapsedIntervalId = null;
      }
      this._elapsedStartTime = null;
    },

    syncElapsedTimer(serverElapsed) {
      const elapsed = Number(serverElapsed);
      if (!Number.isFinite(elapsed) || elapsed < 0) return;
      this._elapsedBaseValue = elapsed;
      this._elapsedStartTime = Date.now();
      this.elapsed = Math.floor(elapsed);
    },

    phaseLabel() {
      const phase = this.phase ? String(this.phase) : "";
      const status = (this.status || "").toString().toUpperCase();

      // Show CANCELLING immediately when user clicks stop
      if (status === "CANCELLING") {
        return "Cancelling";
      }

      // Terminal state should reflect actual status (e.g., FAILED) rather than the
      // terminal phase label ("COMPLETED" is used as the final phase even on failure).
      if (status === "FAILED" || status === "CANCELLED" || status === "STOPPED") {
        return status;
      }

      // If backend reports phase=COMPLETED but status is a failure, show the failure.
      if (phase.toUpperCase() === "COMPLETED" && status === "FAILED") {
        return "FAILED";
      }

      return phase;
    },

    phaseBadgeClass() {
      const phase = (this.phase || "").toString().toUpperCase();
      const status = (this.status || "").toString().toUpperCase();

      // Show cancelling state immediately when user clicks stop
      if (status === "CANCELLING") return "status-cancelling";

      if (phase === "PREPARING") return "status-preparing";
      if (phase === "WARMUP") return "status-warmup";
      if (phase === "RUNNING") return "status-running";
      if (phase === "PROCESSING") return "status-processing";
      if (phase === "COMPLETED") {
        if (status === "FAILED") return "status-failed";
        return "status-completed";
      }
      return "";
    },

    phaseTimingText() {
      const warmup = Number(this.warmupSeconds || 0);
      const run = Number(this.runSeconds || 0);
      const total = warmup + run;

      const phase = (this.phase || "").toString().toUpperCase();
      if (phase === "PREPARING") {
        return "Preparing: initializing connections and profiling tables...";
      }
      if (phase === "WARMUP") {
        return `Warmup: start 0s • est ${warmup}s • est complete ${warmup}s`;
      }
      if (phase === "RUNNING") {
        return `Running: start ${warmup}s • est ${run}s • est complete ${total}s`;
      }
      if (phase === "PROCESSING") {
        return `Processing: start ${total}s`;
      }
      return "";
    },

    // Phase pipeline helper methods
    phaseNames() {
      return ["PREPARING", "WARMUP", "RUNNING", "PROCESSING", "COMPLETED"];
    },

    phaseDisplayNames() {
      return {
        PREPARING: "Preparing",
        WARMUP: "Warm-up",
        RUNNING: "Running",
        PROCESSING: "Processing",
        COMPLETED: "Completed",
      };
    },

    currentPhaseIndex() {
      const phase = (this.phase || "").toString().toUpperCase();
      const status = (this.status || "").toString().toUpperCase();
      // Handle terminal failure states
      if (status === "FAILED" || status === "CANCELLED" || status === "STOPPED") {
        return this.phaseNames().indexOf("COMPLETED");
      }
      const idx = this.phaseNames().indexOf(phase);
      return idx >= 0 ? idx : -1;
    },

    phaseState(phaseName) {
      // Returns: 'inactive', 'active', 'completed'
      const currentIdx = this.currentPhaseIndex();
      const phaseIdx = this.phaseNames().indexOf(phaseName);
      if (phaseIdx < 0 || currentIdx < 0) return "inactive";
      if (phaseIdx < currentIdx) return "completed";
      if (phaseIdx === currentIdx) return "active";
      return "inactive";
    },

    phaseBadgeClassNew(phaseName) {
      const state = this.phaseState(phaseName);
      const status = (this.status || "").toString().toUpperCase();
      if (state === "inactive") return "phase-badge--inactive";
      if (state === "completed") return "phase-badge--completed";
      // active state
      const phaseKey = phaseName.toLowerCase();
      // Handle failure states
      if (phaseName === "COMPLETED" && (status === "FAILED" || status === "CANCELLED" || status === "STOPPED")) {
        return `phase-badge--active phase-failed`;
      }
      return `phase-badge--active phase-${phaseKey}`;
    },

    isTimedPhase() {
      const phase = (this.phase || "").toString().toUpperCase();
      return phase === "WARMUP" || phase === "RUNNING";
    },

    phaseElapsedSeconds() {
      // Returns elapsed seconds within the current timed phase
      const phase = (this.phase || "").toString().toUpperCase();
      const warmup = Number(this.warmupSeconds || 0);
      const totalElapsed = Number(this.elapsed || 0);

      if (phase === "WARMUP") {
        return Math.min(totalElapsed, warmup);
      }
      if (phase === "RUNNING") {
        // Running phase starts after warmup
        const runningElapsed = Math.max(0, totalElapsed - warmup);
        const run = Number(this.runSeconds || 0);
        if (run > 0) {
          return Math.min(runningElapsed, run);
        }
        return runningElapsed;
      }
      return 0;
    },

    phaseDurationSeconds() {
      // Returns total duration of the current timed phase
      const phase = (this.phase || "").toString().toUpperCase();
      if (phase === "WARMUP") {
        return Number(this.warmupSeconds || 0);
      }
      if (phase === "RUNNING") {
        return Number(this.runSeconds || 0);
      }
      return 0;
    },

    phaseProgressPercent() {
      const duration = this.phaseDurationSeconds();
      if (duration <= 0) return 0;
      const elapsed = this.phaseElapsedSeconds();
      return Math.min(100, (elapsed / duration) * 100);
    },

    phaseProgressBarClass() {
      const phase = (this.phase || "").toString().toUpperCase();
      const status = (this.status || "").toString().toUpperCase();
      if (status === "CANCELLING") return "phase-progress-bar-fill--cancelling";
      if (phase === "WARMUP") return "phase-progress-bar-fill--warmup";
      if (phase === "RUNNING") return "phase-progress-bar-fill--running";
      return "";
    },

    phaseTimingLabel() {
      const duration = this.phaseDurationSeconds();
      const elapsed = Math.floor(this.phaseElapsedSeconds());
      if (duration > 0) {
        return `${elapsed}s / ${duration}s`;
      }
      return `${elapsed}s`;
    },

    currentPhaseDisplayName() {
      const phase = (this.phase || "").toString().toUpperCase();
      const status = (this.status || "").toString().toUpperCase();
      if (status === "CANCELLING") return "Cancelling";
      return this.phaseDisplayNames()[phase] || phase;
    },

    completedPhaseDisplayName(phaseName) {
      const status = (this.status || "").toString().toUpperCase();
      const currentPhase = (this.phase || "").toString().toUpperCase();
      // When cancelling, show it on the active phase badge (WARMUP/RUNNING/etc).
      if (status === "CANCELLING" && currentPhase && phaseName === currentPhase) {
        return "Cancelling";
      }
      if (phaseName === "COMPLETED") {
        if (status === "FAILED") return "Failed";
        if (status === "CANCELLED") return "Cancelled";
        if (status === "STOPPED") return "Stopped";
      }
      return this.phaseDisplayNames()[phaseName] || phaseName;
    },

    applyMetricsPayload(payload) {
      if (!payload) return;
      // Never allow dashboard updates to crash the whole Alpine component.
      // (Any exception here prevents subsequent metrics from rendering.)
      try {
        // Handle error payloads - display toast and abort
        if (payload.error) {
          console.log("[dashboard] Received error payload:", payload.error);
        }
        const errorTypes = ["connection_error", "setup_error", "execution_error"];
        if (payload.error && errorTypes.includes(payload.error.type)) {
          const errorTypeLabels = {
            "connection_error": "Connection error",
            "setup_error": "Setup error",
            "execution_error": "Execution error"
          };
          const errorType = errorTypeLabels[payload.error.type] || "Error";
          const errorMsg = payload.error.message || `${errorType}: Test failed`;
          console.error(`[dashboard] ${errorType} - showing toast:`, errorMsg);
          if (window.toast && typeof window.toast.error === "function") {
            window.toast.error(errorMsg);
          }
          // Update status to reflect failure
          this.status = "FAILED";
          this.phase = "COMPLETED";
          this.testRunning = false;
          return;
        }

        const throughputCanvas = document.getElementById("throughputChart");
        const latencyCanvas = document.getElementById("latencyChart");
        const concurrencyCanvas = document.getElementById("concurrencyChart");
        const sfRunningCanvas = document.getElementById("sfRunningChart");
        const throughputChart =
          throughputCanvas && (throughputCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(throughputCanvas) : null));
        const latencyChart =
          latencyCanvas && (latencyCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(latencyCanvas) : null));
        const concurrencyChart =
          concurrencyCanvas && (concurrencyCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(concurrencyCanvas) : null));
        const sfRunningChart =
          sfRunningCanvas && (sfRunningCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(sfRunningCanvas) : null));

        // If charts weren't available at init time (HTMX swaps), re-init on first payload.
        if (
          !throughputChart ||
          !latencyChart ||
          (concurrencyCanvas && !concurrencyChart) ||
          (sfRunningCanvas && !sfRunningChart)
        ) {
          this.initCharts();
        }

        const timing = payload.timing || {};
        const phase = payload.phase ? String(payload.phase) : null;
        const status = payload.status ? String(payload.status) : null;
        const prevPhase = this.phase ? String(this.phase).toUpperCase() : "";
        if (phase) this.phase = phase;
        if (status) this.status = status;

        const warmupSeconds = Number(timing.warmup_seconds);
        if (Number.isFinite(warmupSeconds) && warmupSeconds >= 0) {
          this.warmupSeconds = Math.round(warmupSeconds);
        }
        const runSeconds = Number(timing.run_seconds);
        if (Number.isFinite(runSeconds) && runSeconds >= 0) {
          this.runSeconds = Math.round(runSeconds);
        }
        const totalExpectedSeconds = Number(timing.total_expected_seconds);
        if (Number.isFinite(totalExpectedSeconds) && totalExpectedSeconds >= 0) {
          this.duration = Math.round(totalExpectedSeconds);
        }

        const elapsedDisplay = Number(timing.elapsed_display_seconds);
        let serverElapsed = null;
        if (Number.isFinite(elapsedDisplay) && elapsedDisplay >= 0) {
          serverElapsed = elapsedDisplay;
        } else if (payload.elapsed != null) {
          const fallback = Number(payload.elapsed);
          if (Number.isFinite(fallback) && fallback >= 0) {
            serverElapsed = fallback;
          }
        }

        const phaseUpper = (this.phase || "").toString().toUpperCase();
        const isTerminal = phaseUpper === "COMPLETED" || 
          ["STOPPED", "FAILED", "CANCELLED"].includes((this.status || "").toString().toUpperCase());

        // Handle phase transitions for elapsed timer
        if (phase) {
          const newPhaseUpper = phase.toUpperCase();
          // Start timer when entering PREPARING
          if (newPhaseUpper === "PREPARING" && prevPhase !== "PREPARING") {
            this.startElapsedTimer(serverElapsed || 0);
          }
          // Reset and restart timer when entering WARMUP (elapsed resets to 0)
          else if (newPhaseUpper === "WARMUP" && prevPhase !== "WARMUP") {
            this.startElapsedTimer(serverElapsed || 0);
          }
        }

        // Sync with server elapsed if provided (keeps timer accurate)
        if (serverElapsed !== null) {
          if (this._elapsedIntervalId) {
            this.syncElapsedTimer(serverElapsed);
          } else if (!isTerminal) {
            // Timer not running but we're not terminal - start it
            this.startElapsedTimer(serverElapsed);
          } else {
            // Terminal state - just set elapsed directly
            this.elapsed = serverElapsed;
          }
        }

        if (this.duration > 0) {
          this.progress = Math.min(100, (this.elapsed / this.duration) * 100);
        } else {
          this.progress = 0;
        }

        // Stop timer on terminal state
        if (isTerminal) {
          this.stopElapsedTimer();
        }

        const ts = payload.timestamp
          ? new Date(payload.timestamp).toLocaleTimeString()
          : new Date().toLocaleTimeString();

        const ops = payload.ops;
        const latency = payload.latency;
        const errors = payload.errors;
        const allowCharts =
          !phaseUpper || phaseUpper === "WARMUP" || phaseUpper === "RUNNING";

        if (ops) {
          this.metrics.ops_per_sec = ops.current_per_sec || 0;
          this.qpsHistory.push(this.metrics.ops_per_sec);
          if (this.qpsHistory.length > 30) {
            this.qpsHistory.shift();
          }
          const sum = this.qpsHistory.reduce((a, b) => a + b, 0);
          this.metrics.qps_avg_30s = this.qpsHistory.length > 0 ? sum / this.qpsHistory.length : 0;
        }
        if (latency) {
          this.metrics.p50_latency = latency.p50 || 0;
          this.metrics.p95_latency = latency.p95 || 0;
          this.metrics.p99_latency = latency.p99 || 0;
        }
        if (errors) {
          this.metrics.error_rate = (errors.rate || 0) * 100.0;
          this.metrics.total_errors = errors.count || 0;
        }
        const connections = payload.connections;
        if (connections) {
          this.metrics.in_flight = connections.active || 0;
          this.metrics.target_workers = connections.target || 0;
        }

        const custom = payload.custom_metrics;
        if (custom) {
          const fmc = custom.find_max_controller;
          if (fmc && typeof fmc === "object") {
            this.findMaxController = fmc;
          }

          const wh = custom.warehouse;
          if (wh) {
            this.metrics.sf_running = wh.running || 0;
            this.metrics.sf_queued = wh.queued || 0;
            this.metrics.started_clusters = wh.started_clusters || 0;
          }
          const bench = custom.sf_bench;
          if (bench) {
            this.metrics.sf_bench_available = true;
            this.metrics.sf_running_bench = bench.running || 0;
            this.metrics.sf_queued_bench = bench.queued || 0;
            this.metrics.sf_blocked_bench = bench.blocked || 0;
            this.metrics.sf_running_tagged_bench = bench.running_tagged || 0;
            this.metrics.sf_running_other_bench = bench.running_other || 0;
            this.metrics.sf_running_read_bench = bench.running_read || 0;
            this.metrics.sf_running_write_bench = bench.running_write || 0;
            this.metrics.sf_running_point_lookup_bench = bench.running_point_lookup || 0;
            this.metrics.sf_running_range_scan_bench = bench.running_range_scan || 0;
            this.metrics.sf_running_insert_bench = bench.running_insert || 0;
            this.metrics.sf_running_update_bench = bench.running_update || 0;
          }
          const latBreakdown = custom.latency_breakdown;
          if (latBreakdown) {
            this.metrics.latency_breakdown_available = true;
            this.metrics.sf_execution_avg_ms = latBreakdown.sf_execution_avg_ms || 0;
            this.metrics.network_overhead_avg_ms = latBreakdown.network_overhead_avg_ms || 0;
            this.metrics.latency_sample_count = latBreakdown.sample_count || 0;
          }
          // App-level ops breakdown (always accurate from internal counters)
          const appOps = custom.app_ops_breakdown;
          if (appOps) {
            this.metrics.app_ops_available = true;
            this.metrics.app_point_lookup_count = appOps.point_lookup_count || 0;
            this.metrics.app_range_scan_count = appOps.range_scan_count || 0;
            this.metrics.app_insert_count = appOps.insert_count || 0;
            this.metrics.app_update_count = appOps.update_count || 0;
            this.metrics.app_read_count = appOps.read_count || 0;
            this.metrics.app_write_count = appOps.write_count || 0;
            this.metrics.app_total_count = appOps.total_count || 0;
            this.metrics.app_point_lookup_ops_sec = appOps.point_lookup_ops_sec || 0;
            this.metrics.app_range_scan_ops_sec = appOps.range_scan_ops_sec || 0;
            this.metrics.app_insert_ops_sec = appOps.insert_ops_sec || 0;
            this.metrics.app_update_ops_sec = appOps.update_ops_sec || 0;
            this.metrics.app_read_ops_sec = appOps.read_ops_sec || 0;
            this.metrics.app_write_ops_sec = appOps.write_ops_sec || 0;
          }
        }

        // Keep the FIND_MAX countdown ticking smoothly and stop it on terminal states.
        this.syncFindMaxCountdown();

        const throughputChart2 =
          throughputCanvas && (throughputCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(throughputCanvas) : null));
        if (throughputChart2) {
          if (throughputChart2.data.labels.length > 60) {
            throughputChart2.data.labels.shift();
            throughputChart2.data.datasets.forEach((ds) => ds.data.shift());
          }
          if (ops && allowCharts) {
            throughputChart2.data.labels.push(ts);
            throughputChart2.data.datasets[0].data.push(this.metrics.ops_per_sec);
            throughputChart2.data.datasets[1].data.push(this.metrics.error_rate);
            throughputChart2.update();
          }
        }

        const latencyChart2 =
          latencyCanvas && (latencyCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(latencyCanvas) : null));
        if (latencyChart2) {
          if (latencyChart2.data.labels.length > 60) {
            latencyChart2.data.labels.shift();
            latencyChart2.data.datasets.forEach((ds) => ds.data.shift());
          }
          if (latency && allowCharts) {
            latencyChart2.data.labels.push(ts);
            latencyChart2.data.datasets[0].data.push(latency.p50 || 0);
            latencyChart2.data.datasets[1].data.push(latency.p95 || 0);
            latencyChart2.data.datasets[2].data.push(latency.p99 || 0);
            latencyChart2.update();
          }
        }

        const concurrencyChart2 =
          concurrencyCanvas && (concurrencyCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(concurrencyCanvas) : null));
        if (concurrencyChart2) {
          if (concurrencyChart2.data.labels.length > 60) {
            concurrencyChart2.data.labels.shift();
            concurrencyChart2.data.datasets.forEach((ds) => ds.data.shift());
          }
          if (allowCharts) {
            // Check if this is a Postgres table type (chart has different dataset structure)
            const tableType = (this.templateInfo?.table_type || "").toLowerCase();
            const isPostgres = ["postgres", "snowflake_postgres"].includes(tableType);

            concurrencyChart2.data.labels.push(ts);

            if (isPostgres) {
              // Postgres: two datasets (target_workers, in_flight)
              concurrencyChart2.data.datasets[0].data.push(this.metrics.target_workers);
              concurrencyChart2.data.datasets[1].data.push(this.metrics.in_flight);
            } else {
              // Snowflake: three datasets (target_workers, in_flight, sf_queued)
              // Prefer per-test queued queries; fall back to warehouse queued clusters if needed.
              const sfQueued = this.metrics.sf_queued_bench > 0
                ? this.metrics.sf_queued_bench
                : this.metrics.sf_queued;

              concurrencyChart2.data.datasets[0].data.push(this.metrics.target_workers);
              concurrencyChart2.data.datasets[1].data.push(this.metrics.in_flight);
              concurrencyChart2.data.datasets[2].data.push(sfQueued);
            }
            concurrencyChart2.update();
          }
        }

        const sfRunningCanvas2 = document.getElementById("sfRunningChart");
        const sfRunningChart2 =
          sfRunningCanvas2 &&
          (sfRunningCanvas2.__chart ||
            (window.Chart && Chart.getChart ? Chart.getChart(sfRunningCanvas2) : null));
        if (sfRunningChart2) {
          if (sfRunningChart2.data.labels.length > 60) {
            sfRunningChart2.data.labels.shift();
            sfRunningChart2.data.datasets.forEach((ds) => ds.data.shift());
          }
          if (allowCharts) {
            const totalTagged = this.metrics.sf_bench_available
              ? this.metrics.sf_running_tagged_bench
              : 0;
            const totalRaw = this.metrics.sf_bench_available
              ? this.metrics.sf_running_bench
              : 0;
            // Prefer tagged total (sum of the 4 benchmark kinds) when available;
            // fall back to raw Snowflake bench RUNNING when tagging isn't present yet.
            const total = totalTagged > 0 ? totalTagged : totalRaw;

            // SF bench breakdown from QUERY_HISTORY (may be 0 if kind tagging unavailable)
            const sfReads = this.metrics.sf_bench_available
              ? this.metrics.sf_running_read_bench
              : 0;
            const sfWrites = this.metrics.sf_bench_available
              ? this.metrics.sf_running_write_bench
              : 0;
            const sfPl = this.metrics.sf_bench_available
              ? this.metrics.sf_running_point_lookup_bench
              : 0;
            const sfRs = this.metrics.sf_bench_available
              ? this.metrics.sf_running_range_scan_bench
              : 0;
            const sfIns = this.metrics.sf_bench_available
              ? this.metrics.sf_running_insert_bench
              : 0;
            const sfUpd = this.metrics.sf_bench_available
              ? this.metrics.sf_running_update_bench
              : 0;
            const sfBreakdownHasData = (sfPl + sfRs + sfIns + sfUpd) > 0;

            // Fallback to app-level QPS when SF kind tagging is unavailable.
            // These are accurate internal counters (ops completed by the app).
            // Scale by ratio if we have a total from SF but no breakdown.
            let reads = sfReads, writes = sfWrites, pl = sfPl, rs = sfRs, ins = sfIns, upd = sfUpd;
            if (!sfBreakdownHasData && this.metrics.app_ops_available && total > 0) {
              // Use app QPS proportions scaled to SF total running count.
              const appTotal = this.metrics.app_read_ops_sec + this.metrics.app_write_ops_sec;
              if (appTotal > 0) {
                const scale = total / appTotal;
                reads = Math.round(this.metrics.app_read_ops_sec * scale);
                writes = Math.round(this.metrics.app_write_ops_sec * scale);
                pl = Math.round(this.metrics.app_point_lookup_ops_sec * scale);
                rs = Math.round(this.metrics.app_range_scan_ops_sec * scale);
                ins = Math.round(this.metrics.app_insert_ops_sec * scale);
                upd = Math.round(this.metrics.app_update_ops_sec * scale);
              }
            }

            sfRunningChart2.data.labels.push(ts);
            sfRunningChart2.data.datasets[0].data.push(total);
            sfRunningChart2.data.datasets[1].data.push(reads);
            sfRunningChart2.data.datasets[2].data.push(pl);
            sfRunningChart2.data.datasets[3].data.push(rs);
            sfRunningChart2.data.datasets[4].data.push(writes);
            sfRunningChart2.data.datasets[5].data.push(ins);
            sfRunningChart2.data.datasets[6].data.push(upd);
            sfRunningChart2.data.datasets[7].data.push(
              this.metrics.sf_bench_available ? this.metrics.sf_blocked_bench : 0
            );
            sfRunningChart2.update();
          }
        }

        // MCW Live Chart (real-time active clusters)
        const mcwLiveCanvas = document.getElementById("mcwLiveChart");
        const mcwLiveChart2 =
          mcwLiveCanvas && (mcwLiveCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(mcwLiveCanvas) : null));
        if (mcwLiveChart2) {
          if (mcwLiveChart2.data.labels.length > 60) {
            mcwLiveChart2.data.labels.shift();
            mcwLiveChart2.data.datasets[0].data.shift();
          }
          if (allowCharts && this.metrics.started_clusters > 0) {
            mcwLiveChart2.data.labels.push(ts);
            mcwLiveChart2.data.datasets[0].data.push(this.metrics.started_clusters);
            mcwLiveChart2.update();
          }
        }

        // Once we hit the terminal phase, refresh from the API so we pick up:
        // - persisted final metrics
        // - QUERY_HISTORY enrichment fields (sf_* latency summaries)
        if (
          phaseUpper === "COMPLETED" &&
          this.mode === "live" &&
          !this.didRefreshAfterComplete
        ) {
          this.didRefreshAfterComplete = true;
          this.loadTestInfo();
        }
      } catch (e) {
        console.error("applyMetricsPayload error:", e, payload);
      }
    },

    async openAiAnalysis() {
      if (!this.testId) return;
      this.aiAnalysisModal = true;
      this.aiAnalysisLoading = true;
      this.aiAnalysisError = null;
      this.aiAnalysis = null;
      this.chatHistory = [];
      this.chatMessage = "";

      try {
        const resp = await fetch(`/api/tests/${this.testId}/ai-analysis`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({}),
        });
        if (!resp.ok) {
          const err = await resp.json().catch(() => ({}));
          const detail = err.detail;
          const msg = typeof detail === 'object' && detail !== null
            ? (detail.message || JSON.stringify(detail))
            : (detail || `HTTP ${resp.status}`);
          throw new Error(msg);
        }
        this.aiAnalysis = await resp.json();
      } catch (e) {
        console.error("AI analysis failed:", e);
        this.aiAnalysisError = e.message || String(e);
      } finally {
        this.aiAnalysisLoading = false;
      }
    },

    closeAiAnalysis() {
      this.aiAnalysisModal = false;
      this.aiAnalysis = null;
      this.aiAnalysisError = null;
      this.chatHistory = [];
      this.chatMessage = "";
    },

    /**
     * Check if a test status is terminal (test execution has finished).
     * Terminal statuses are: COMPLETED, FAILED, STOPPED, CANCELLED, ERROR
     */
    isTerminalStatus(status) {
      if (!status) return false;
      const s = String(status).toUpperCase();
      return ["COMPLETED", "FAILED", "STOPPED", "CANCELLED", "ERROR"].includes(s);
    },

    /**
     * Retry enrichment for a test that has failed enrichment.
     */
    async retryEnrichment() {
      if (!this.testId || this.enrichmentRetrying) return;

      this.enrichmentRetrying = true;
      try {
        const resp = await fetch(`/api/tests/${this.testId}/retry-enrichment`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
        });
        if (!resp.ok) {
          const err = await resp.json().catch(() => ({}));
          const detail = err.detail;
          const msg = typeof detail === 'object' && detail !== null
            ? (detail.message || JSON.stringify(detail))
            : (detail || `HTTP ${resp.status}`);
          throw new Error(msg);
        }
        const data = await resp.json();
        // Update templateInfo with the new enrichment status
        if (this.templateInfo) {
          this.templateInfo.enrichment_status = data.enrichment_status;
          this.templateInfo.enrichment_error = null;
          this.templateInfo.can_retry_enrichment = false;
        }
        if (window.toast && typeof window.toast.success === "function") {
          const ratio = data.stats?.enrichment_ratio || 0;
          window.toast.success(`Enrichment completed (${ratio}% queries enriched)`);
        }
        // Reload test info to refresh all metrics
        await this.loadTestInfo();
      } catch (e) {
        console.error("Retry enrichment failed:", e);
        if (window.toast && typeof window.toast.error === "function") {
          window.toast.error(`Enrichment failed: ${e.message || e}`);
        }
      } finally {
        this.enrichmentRetrying = false;
      }
    },

    async sendChatMessage() {
      const msg = this.chatMessage.trim();
      if (!msg || !this.testId || this.chatLoading) return;

      this.chatHistory.push({ role: "user", content: msg });
      this.chatMessage = "";
      this.chatLoading = true;

      this.$nextTick(() => {
        const container = this.$refs.chatContainer;
        if (container) container.scrollTop = container.scrollHeight;
      });

      try {
        const resp = await fetch(`/api/tests/${this.testId}/ai-chat`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            message: msg,
            history: this.chatHistory,
          }),
        });
        if (!resp.ok) {
          const err = await resp.json().catch(() => ({}));
          throw new Error(err.detail || `HTTP ${resp.status}`);
        }
        const data = await resp.json();
        this.chatHistory.push({ role: "assistant", content: data.response });
      } catch (e) {
        console.error("AI chat failed:", e);
        this.chatHistory.push({
          role: "assistant",
          content: `Error: ${e.message || e}`,
        });
      } finally {
        this.chatLoading = false;
        this.$nextTick(() => {
          const container = this.$refs.chatContainer;
          if (container) container.scrollTop = container.scrollHeight;
        });
      }
    },

    formatMarkdown(text) {
      if (!text) return "";
      let str = text;
      if (str.startsWith('"') && str.endsWith('"')) {
        str = str.slice(1, -1);
      }
      str = str.replace(/\\n/g, "\n");
      str = str.replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>");
      str = str.replace(/\*(.+?)\*/g, "<em>$1</em>");
      str = str.replace(/^### (.+)$/gm, "<h4 style='margin: 0.75rem 0 0.25rem; font-size: 1rem;'>$1</h4>");
      str = str.replace(/^## (.+)$/gm, "<h3 style='margin: 1rem 0 0.5rem; font-size: 1.1rem;'>$1</h3>");
      str = str.replace(/^# (.+)$/gm, "<h2 style='margin: 1rem 0 0.5rem; font-size: 1.25rem;'>$1</h2>");
      str = str.replace(/^- (.+)$/gm, "<li style='margin-left: 1rem;'>$1</li>");
      str = str.replace(/^(\d+)\. (.+)$/gm, "<li style='margin-left: 1rem;'>$2</li>");
      str = str.replace(/`([^`]+)`/g, "<code style='background: #e5e7eb; padding: 0.125rem 0.25rem; border-radius: 0.25rem; font-size: 0.875rem;'>$1</code>");
      str = str.replace(/\n/g, "<br>");
      return str;
    },

    toggleClusterBreakdown() {
      this.clusterBreakdownExpanded = !this.clusterBreakdownExpanded;
    },

    clusterBreakdownSummary() {
      const breakdown = this.templateInfo?.cluster_breakdown || [];
      if (!breakdown.length) return null;

      const count = breakdown.length;
      let totalQueries = 0;
      let sumP50 = 0;
      let sumP95 = 0;
      let sumQueueOverload = 0;
      let sumQueueProvisioning = 0;
      let totalPl = 0;
      let totalRs = 0;
      let totalIns = 0;
      let totalUpd = 0;

      for (const c of breakdown) {
        totalQueries += Number(c.query_count || 0);
        sumP50 += Number(c.p50_exec_ms || 0);
        sumP95 += Number(c.p95_exec_ms || 0);
        sumQueueOverload += Number(c.avg_queued_overload_ms || 0);
        sumQueueProvisioning += Number(c.avg_queued_provisioning_ms || 0);
        totalPl += Number(c.point_lookups || 0);
        totalRs += Number(c.range_scans || 0);
        totalIns += Number(c.inserts || 0);
        totalUpd += Number(c.updates || 0);
      }

      return {
        cluster_count: count,
        total_queries: totalQueries,
        avg_p50_exec_ms: sumP50 / count,
        avg_p95_exec_ms: sumP95 / count,
        avg_queued_overload_ms: sumQueueOverload / count,
        avg_queued_provisioning_ms: sumQueueProvisioning / count,
        total_point_lookups: totalPl,
        total_range_scans: totalRs,
        total_inserts: totalIns,
        total_updates: totalUpd,
      };
    },

    toggleStepHistory() {
      this.stepHistoryExpanded = !this.stepHistoryExpanded;
    },

    hasStepHistory() {
      const fmr = this.templateInfo?.find_max_result;
      if (!fmr || typeof fmr !== "object") return false;
      const hist = fmr.step_history;
      return Array.isArray(hist) && hist.length > 0;
    },

    stepHistory() {
      const fmr = this.templateInfo?.find_max_result;
      if (!fmr || typeof fmr !== "object") return [];
      const hist = fmr.step_history;
      return Array.isArray(hist) ? hist : [];
    },

    stepHistorySummary() {
      const fmr = this.templateInfo?.find_max_result;
      if (!fmr || typeof fmr !== "object") return null;
      return {
        best_concurrency: fmr.final_best_concurrency ?? fmr.best_concurrency ?? null,
        best_qps: fmr.final_best_qps ?? fmr.best_qps ?? null,
        baseline_p95_ms: fmr.baseline_p95_latency_ms ?? null,
        final_reason: fmr.final_reason ?? null,
        total_steps: Array.isArray(fmr.step_history) ? fmr.step_history.length : 0,
      };
    },

    stepP99DiffPct(step) {
      const baseline = this.findMaxBaselineP99Ms();
      const current = Number(step?.p99_latency_ms);
      if (!baseline || baseline <= 0 || !Number.isFinite(current) || current <= 0) {
        return null;
      }
      return ((current - baseline) / baseline) * 100.0;
    },
  };
}


