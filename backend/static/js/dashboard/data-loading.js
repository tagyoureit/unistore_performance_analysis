/**
 * Dashboard Data Loading Module
 * Methods for loading test info, metrics, and related data from the API.
 */
window.DashboardMixins = window.DashboardMixins || {};

window.DashboardMixins.dataLoading = {
  async loadTestInfo() {
    if (!this.testId) return;
    if (this._destroyed) return;
    try {
      const resp = await fetch(`/api/tests/${this.testId}`);
      if (!resp.ok) return;
      const data = await resp.json();
      // DEBUG: Only log API response on state changes
      const apiStatus = data.status;
      const apiPhase = data.phase;
      const apiElapsed = data.timing?.elapsed_display_seconds;
      const prevApiStatus = window._lastApiStatus;
      const prevApiPhase = window._lastApiPhase;
      const prevApiElapsed = window._lastApiElapsed || 0;
      
      if (apiStatus !== prevApiStatus || apiPhase !== prevApiPhase || Math.abs((apiElapsed || 0) - prevApiElapsed) > 2) {
        console.log("[STATE-DEBUG] API loadTestInfo", {
          status: { prev: prevApiStatus, new: apiStatus },
          phase: { prev: prevApiPhase, new: apiPhase },
          elapsed: apiElapsed,
        });
        window._lastApiStatus = apiStatus;
        window._lastApiPhase = apiPhase;
        window._lastApiElapsed = apiElapsed;
      }
      
      this.templateInfo = data;
      if ("latency_aggregation_method" in data) {
        this.latencyAggregationMethod = data.latency_aggregation_method;
      }
      this.duration = data.duration_seconds || 0;
      
      // Populate QPS controller state from API response (for history mode)
      // This enables the QPS Controller card to show step history on the history page
      if (data.qps_controller_state && typeof data.qps_controller_state === "object") {
        this.qpsController = data.qps_controller_state;
      }
      
      // apiElapsed already extracted above (line 18) for logging
      const hasApiElapsed = apiElapsed != null && Number.isFinite(Number(apiElapsed)) && Number(apiElapsed) >= 0;
      
      // Use setStatusIfAllowed to prevent API from overwriting optimistic status
      // (e.g., don't let stale "PREPARED" overwrite optimistic "RUNNING" after Start clicked)
      this.setStatusIfAllowed(data.status || null, data.cancellation_reason);
      if (data.phase != null) {
        this.setPhaseIfAllowed(data.phase, data.status);
      } else if (!this.phase && data.status) {
        this.setPhaseIfAllowed(data.status, data.status);
      }

      // Extract timing info for phase-specific counters
      if (data.timing) {
        const timing = data.timing;
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
        const hasElapsedDisplay =
          Number.isFinite(elapsedDisplay) && elapsedDisplay >= 0;
        // Only start/sync elapsed timer if test is actually running (not PREPARED)
        const statusUpper = (this.status || "").toString().toUpperCase();
        const phaseUpper = (this.phase || "").toString().toUpperCase();
        const isActiveStatus = ["RUNNING", "CANCELLING", "STOPPING"].includes(
          statusUpper,
        );
        // Keep timer running during PREPARING phase (even if status hasn't updated yet)
        // Also keep timer running during PROCESSING phase (enrichment in progress)
        const isActivePhase = ["PREPARING", "WARMUP", "MEASUREMENT", "PROCESSING"].includes(
          phaseUpper,
        );
        // Timer is local-only - don't sync with server elapsed time
        // Only start timer if not already running and we're in an active state
        if (this.mode === "live" && isActiveStatus && !this._elapsedIntervalId) {
          this.startElapsedTimer(0);
        } else if (!isActiveStatus && !isActivePhase) {
          // Stop timer if test is not active (e.g., terminal status with COMPLETED phase)
          // But don't stop if we're in an active phase (timer was started optimistically)
          // PROCESSING is now active - timer keeps running until PHASE=COMPLETED
          if (this._elapsedIntervalId) {
            this.stopElapsedTimer();
          }
          if (this.hasTestStarted()) {
            if (hasElapsedDisplay) {
              this.elapsed = Math.floor(elapsedDisplay);
              if (this.duration > 0) {
                this.progress = Math.min(100, (this.elapsed / this.duration) * 100);
              }
            }
          } else {
            this.elapsed = 0;
            this.progress = 0;
          }
        }
      }

      const phaseUpper = (this.phase || "").toString().toUpperCase();
      if (phaseUpper === "PROCESSING") {
        this.startProcessingLogTimer();
      } else {
        this.stopProcessingLogTimer();
      }

      if (this.mode === "live" && typeof this.updateLiveTransport === "function") {
        this.updateLiveTransport();
      }

      // Active phases where metrics polling + warehouse details are needed
      const activePhases = ["WARMUP", "MEASUREMENT"];

      if (this.mode === "live") {
        const shouldUseWebSocket =
          typeof this.shouldUseWebSocket === "function"
            ? this.shouldUseWebSocket()
            : false;
        const shouldPollMetrics =
          !shouldUseWebSocket && activePhases.includes(phaseUpper);
        const shouldPollLogs = false;
        if (shouldPollMetrics) {
          this.startMultiNodeMetricsPolling();
        } else {
          this.stopMultiNodeMetricsPolling();
        }
        if (shouldPollLogs) {
          this.startMultiNodeLogPolling();
        } else {
          this.stopMultiNodeLogPolling();
        }
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
        const isPostgres = tableType === "postgres";
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

      // Load warehouse details only during active phases to avoid 404 errors
      // before the test starts running.
      if (activePhases.includes(phaseUpper) && (this.mode === "history" || !wsOpen)) {
        this.loadWarehouseDetails();
      }
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
        // Stop metrics polling - test is done
        if (typeof this.stopMultiNodeMetricsPolling === "function") {
          this.stopMultiNodeMetricsPolling();
        }
        // Keep the current elapsed time - don't reset it to the configured duration.
        // The elapsed value already reflects actual time from the timer.
        // Only sync with server elapsed when phase is fully COMPLETED (not during PROCESSING).
        // This prevents the timer from jumping backwards when status=COMPLETED but 
        // phase=PROCESSING, since server elapsed excludes PREPARING/PROCESSING time
        // but local timer includes it.
        const phaseForSync = (data.phase || this.phase || "").toString().toUpperCase();
        if (phaseForSync === "COMPLETED" && data.timing && typeof data.timing.elapsed_display_seconds === "number") {
          this.elapsed = Math.floor(data.timing.elapsed_display_seconds);
        }
        // Keep elapsed as-is if no API value available (timer was tracking it)
        this.progress = 100;
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
        await this.loadNodeMetrics();

      // On history view, load per-second warehouse series once post-processing is complete.
      // Warehouse timeseries (MCW chart) is available during PROCESSING phase and doesn't
      // require full enrichment to complete - it uses V_WAREHOUSE_TIMESERIES which is
      // populated independently.
      if (this.mode === "history" && this.isFinalMetricsReady()) {
        this.resetWarehouseTimeseriesRetry();
        await this.loadWarehouseTimeseries();
      }

      // Load overhead timeseries independently (doesn't require full enrichment complete)
      if (this.mode === "history" && this.isFinalMetricsReady()) {
        await this.loadOverheadTimeseries();
      }
      }
    } catch (e) {
      console.error("Failed to load test info:", e);
    }
  },

  async loadNodeMetrics() {
    if (!this.testId) return;
    if (this._destroyed) return;
    if (this.mode !== "history") return;

    this.workerMetricsLoading = true;
    this.workerMetricsError = null;
    this.workerMetricsAvailable = false;
    this.workerMetricsWorkers = [];
    this.workerMetricsExpanded = {};

    try {
      const resp = await fetch(`/api/tests/${this.testId}/worker-metrics`);
      if (!resp.ok) {
        const payload = await resp.json().catch(() => ({}));
        const detail = payload && payload.detail ? payload.detail : null;
        throw new Error(
          (detail && (detail.message || detail.detail || detail)) ||
            `Failed to load worker metrics (HTTP ${resp.status})`,
        );
      }

      const data = await resp.json().catch(() => ({}));
      if (data && data.error) {
        throw new Error(String(data.error));
      }

      const workers = data && Array.isArray(data.workers) ? data.workers : [];
      workers.sort((a, b) => {
        const aId = Number(a?.worker_group_id ?? 0);
        const bId = Number(b?.worker_group_id ?? 0);
        if (aId !== bId) return aId - bId;
        const aWorker = String(a?.worker_id ?? "");
        const bWorker = String(b?.worker_id ?? "");
        return aWorker.localeCompare(bWorker);
      });
      this.workerMetricsWorkers = workers.map((worker) => {
        const snapshots = Array.isArray(worker.snapshots) ? worker.snapshots : [];
        const latest = snapshots.length ? snapshots[snapshots.length - 1] : null;
        return { ...worker, snapshots, latest };
      });
      this.workerMetricsAvailable = !!(data && data.available && workers.length);
    } catch (e) {
      console.error("Failed to load worker metrics:", e);
      this.workerMetricsError = e && e.message ? e.message : String(e);
      this.workerMetricsWorkers = [];
      this.workerMetricsAvailable = false;
    } finally {
      this.workerMetricsLoading = false;
    }
  },

  async loadLiveWorkerMetrics(force = false) {
    if (!this.testId) return;
    if (this._destroyed) return;
    if (this.mode !== "live") return;
    if (this._liveWorkerMetricsPromise) return this._liveWorkerMetricsPromise;

    const now = Date.now();
    if (
      !force &&
      this.liveWorkerMetricsLoadedAt &&
      now - this.liveWorkerMetricsLoadedAt < 3000
    ) {
      return null;
    }

    this.workerMetricsLoading = true;
    this.workerMetricsError = null;
    const promise = (async () => {
      try {
        const resp = await fetch(`/api/tests/${this.testId}/worker-metrics`);
        if (!resp.ok) {
          const payload = await resp.json().catch(() => ({}));
          const detail = payload && payload.detail ? payload.detail : null;
          throw new Error(
            (detail && (detail.message || detail.detail || detail)) ||
              `Failed to load worker metrics (HTTP ${resp.status})`,
          );
        }

        const data = await resp.json().catch(() => ({}));
        if (data && data.error) {
          throw new Error(String(data.error));
        }

        const workers = data && Array.isArray(data.workers) ? data.workers : [];
        workers.sort((a, b) => {
          const aId = Number(a?.worker_group_id ?? 0);
          const bId = Number(b?.worker_group_id ?? 0);
          if (aId !== bId) return aId - bId;
          const aWorker = String(a?.worker_id ?? "");
          const bWorker = String(b?.worker_id ?? "");
          return aWorker.localeCompare(bWorker);
        });
        this.workerMetricsWorkers = workers.map((worker) => {
          const snapshots = Array.isArray(worker.snapshots)
            ? worker.snapshots
            : [];
          const latest = snapshots.length ? snapshots[snapshots.length - 1] : null;
          const key = this.workerKey(worker);
          return { ...worker, key, snapshots, latest };
        });
        this.workerMetricsAvailable = !!(data && data.available && workers.length);
        this.liveWorkerMetricsLoadedAt = Date.now();
      } catch (e) {
        console.error("Failed to load live worker metrics:", e);
        this.workerMetricsError = e && e.message ? e.message : String(e);
        this.workerMetricsAvailable = false;
      } finally {
        this.workerMetricsLoading = false;
      }
    })();

    this._liveWorkerMetricsPromise = promise;
    await promise;
    this._liveWorkerMetricsPromise = null;
    return promise;
  },

  async toggleLiveWorker(worker) {
    const key = this.workerKey(worker);
    if (!key) return;
    if (this.workerMetricsExpanded[key]) {
      this.workerMetricsExpanded[key] = false;
      return;
    }
    await this.loadLiveWorkerMetrics();
    const detail =
      this.workerMetricsWorkers.find((item) => item.key === key) || null;
    this.workerMetricsExpanded[key] = true;
    if (detail) {
      this.$nextTick(() => {
        this.renderWorkerMetricsChart(detail);
      });
    }
  },

  async loadMultiWorkerLiveMetrics() {
    if (!this.testId) return;
    if (this._destroyed) return;
    if (this.mode !== "live") return;

    try {
      const resp = await fetch(`/api/tests/${this.testId}/metrics`);
      if (!resp.ok) return;
      const data = await resp.json().catch(() => ({}));
      const snapshots = data && Array.isArray(data.snapshots) ? data.snapshots : [];
      if (!snapshots.length) return;
      const latest = snapshots[snapshots.length - 1];
      const payload = this._multiNodePayloadFromSnapshot(latest);
      if (payload) {
        this.applyMetricsPayload(payload);
      }
    } catch (e) {
      console.error("[dashboard] multi-worker metrics polling failed:", e);
    }
  },

  async loadWarehouseDetails() {
    if (!this.testId) return;
    if (this._destroyed) return;
    if (this.isPostgresTable()) return;

    this.warehouseDetailsLoading = true;
    this.warehouseDetailsError = null;

    try {
      const resp = await fetch(`/api/tests/${this.testId}/warehouse-details`);
      if (!resp.ok) {
        const payload = await resp.json().catch(() => ({}));
        const detail = payload && payload.detail ? payload.detail : null;
        throw new Error(
          (detail && (detail.message || detail.detail || detail)) ||
            `Failed to load warehouse details (HTTP ${resp.status})`,
        );
      }

      const data = await resp.json();
      if (data && data.error) {
        throw new Error(String(data.error));
      }
      this.warehouseDetails = data || null;
    } catch (e) {
      console.warn("Failed to load warehouse details:", e);
      this.warehouseDetailsError = e && e.message ? e.message : String(e);
      this.warehouseDetails = null;
    } finally {
      this.warehouseDetailsLoading = false;
    }
  },

  applyWarehouseDetails(payload) {
    if (!payload || typeof payload !== "object") return;
    this.warehouseDetails = payload || null;
    this.warehouseDetailsLoading = false;
    this.warehouseDetailsError = null;
  },

  async loadErrorSummary() {
    if (!this.testId) return;
    if (this._destroyed) return;
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

      const data = await resp.json();
      if (data && data.error) {
        throw new Error(String(data.error));
      }
      this.errorSummaryRows = data && Array.isArray(data.rows) ? data.rows : [];
      this.errorSummaryHierarchy = data && data.hierarchy ? data.hierarchy : null;
    } catch (e) {
      console.warn("Failed to load error summary:", e);
      this.errorSummaryError = e && e.message ? e.message : String(e);
      this.errorSummaryRows = [];
    } finally {
      this.errorSummaryLoading = false;
      this.errorSummaryLoaded = true;
    }
  },
};
