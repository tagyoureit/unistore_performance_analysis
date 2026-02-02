/***
 * Dashboard Component (Refactored)
 *
 * This Alpine.js component composes multiple modules from /js/dashboard/
 * to provide a modular, maintainable dashboard for test monitoring.
 *
 * Modules:
 * - state.js: Initial reactive state
 * - formatters.js: Value formatting utilities
 * - test-actions.js: Start/stop/rerun test actions
 * - display.js: Display helpers for configuration info
 * - workers.js: Worker metrics handling
 * - find-max.js: FindMax mode methods
 * - slo.js: SLO tracking methods
 * - latency.js: Latency view methods
 * - phase.js: Phase management
 * - websocket.js: WebSocket connection
 * - timers.js: Elapsed time management
 * - logs.js: Log handling
 * - polling.js: Multi-node and enrichment polling
 * - timeseries.js: Warehouse/overhead timeseries
 * - breakdown.js: Chart breakdown toggles
 * - step-history.js: FindMax step history
 * - ai.js: AI analysis and chat
 * - ui.js: UI toggles and helpers
 * - data-loading.js: API data loading
 * - historical-metrics.js: Historical chart population
 */
function dashboard(opts) {
  // Get mixin modules (loaded via script tags before this file)
  const M = window.DashboardMixins || {};

  // Compose state from the state module
  const state = M.state ? M.state(opts) : {};

  return {
    // Spread all state properties
    ...state,

    // Spread all mixin modules
    ...(M.formatters || {}),
    ...(M.testActions || {}),
    ...(M.display || {}),
    ...(M.workers || {}),
    ...(M.findMax || {}),
    ...(M.slo || {}),
    ...(M.latency || {}),
    ...(M.phase || {}),
    ...(M.websocket || {}),
    ...(M.timers || {}),
    ...(M.logs || {}),
    ...(M.polling || {}),
    ...(M.timeseries || {}),
    ...(M.breakdown || {}),
    ...(M.stepHistory || {}),
    ...(M.ai || {}),
    ...(M.ui || {}),
    ...(M.dataLoading || {}),
    ...(M.historicalMetrics || {}),

    // === CORE METHODS (kept in main file) ===

    init() {
      // Enable debug logging when ?debug=1 is present.
      try {
        const params = new URLSearchParams(window.location.search || "");
        const v = String(params.get("debug") || "").toLowerCase();
        this.debug = v === "1" || v === "true" || v === "yes";
      } catch (_) {
        this.debug = false;
      }

      if (typeof this.initLogControls === "function") {
        this.initLogControls();
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
          this.updateLiveTransport();
        } else if (this.mode === "history") {
          this.loadLogs();
        }
      }

      // Clean up when navigating away from the page
      window.addEventListener("beforeunload", () => this.destroy());
      // Also handle HTMX navigation (if used)
      document.addEventListener("htmx:beforeSwap", () => this.destroy());
    },

    destroy() {
      // Mark as destroyed to prevent future API calls
      this._destroyed = true;

      // Clean up all polling intervals and WebSocket connections
      // This prevents API calls continuing after navigating away from dashboard
      if (this.debug) {
        console.log("[dashboard][debug] destroy - cleaning up intervals and connections");
      }

      // Stop all polling intervals
      if (typeof this.stopMultiNodeMetricsPolling === "function") {
        this.stopMultiNodeMetricsPolling();
      }
      if (typeof this.stopMultiNodeTestInfoPolling === "function") {
        this.stopMultiNodeTestInfoPolling();
      }
      if (typeof this.stopEnrichmentPolling === "function") {
        this.stopEnrichmentPolling();
      }
      if (typeof this.stopElapsedTimer === "function") {
        this.stopElapsedTimer();
      }

      // Disconnect WebSocket
      if (typeof this.disconnectWebSocket === "function") {
        this.disconnectWebSocket();
      }

      // Clear any other interval IDs that might exist
      if (this._multiWorkerMetricsIntervalId) {
        clearInterval(this._multiWorkerMetricsIntervalId);
        this._multiWorkerMetricsIntervalId = null;
      }
      if (this._multiWorkerTestInfoIntervalId) {
        clearInterval(this._multiWorkerTestInfoIntervalId);
        this._multiWorkerTestInfoIntervalId = null;
      }
      if (this._enrichmentPollIntervalId) {
        clearInterval(this._enrichmentPollIntervalId);
        this._enrichmentPollIntervalId = null;
      }
      if (this._elapsedIntervalId) {
        clearInterval(this._elapsedIntervalId);
        this._elapsedIntervalId = null;
      }
      if (this._logPollIntervalId) {
        clearInterval(this._logPollIntervalId);
        this._logPollIntervalId = null;
      }
      if (this._processingLogIntervalId) {
        clearInterval(this._processingLogIntervalId);
        this._processingLogIntervalId = null;
      }
    },

    shouldUseWebSocket() {
      if (this.mode !== "live") return false;
      const statusUpper = (this.status || "").toString().toUpperCase();
      return ["RUNNING", "CANCELLING", "STOPPING"].includes(statusUpper);
    },

    updateLiveTransport() {
      if (this.mode !== "live") return;
      if (this._destroyed) return;
      if (!this.testId) return;

      const statusUpper = (this.status || "").toString().toUpperCase();
      const phaseUpper = (this.phase || "").toString().toUpperCase();
      const isActiveTest = ["RUNNING", "CANCELLING", "STOPPING"].includes(statusUpper);

      if (!isActiveTest) {
        // If STATUS=COMPLETED but PHASE is still PROCESSING, keep WebSocket open
        // to continue receiving updates until enrichment completes
        if (statusUpper === "COMPLETED" && phaseUpper === "PROCESSING") {
          this._debugLog("TRANSPORT", "COMPLETED_BUT_PROCESSING", { status: statusUpper, phase: phaseUpper });
          // Don't disconnect WebSocket yet - keep it open for enrichment updates
          // Enrichment polling is also started in data-loading.js as backup
          return;
        }
        
        // Fully terminal - disconnect WebSocket and stop all polling
        if (typeof this.disconnectWebSocket === "function") {
          this.disconnectWebSocket();
        }
        if (typeof this.stopMultiNodeTestInfoPolling === "function") {
          this.stopMultiNodeTestInfoPolling();
        }
        return;
      }

      const useWebSocket =
        typeof this.shouldUseWebSocket === "function"
          ? this.shouldUseWebSocket()
          : false;
      if (useWebSocket) {
        if (typeof this.stopMultiNodeTestInfoPolling === "function") {
          this.stopMultiNodeTestInfoPolling();
        }
        if (!this.websocket && typeof this.connectWebSocket === "function") {
          this.connectWebSocket();
        }
        return;
      }

      if (typeof this.disconnectWebSocket === "function") {
        this.disconnectWebSocket();
      }
      if (typeof this.startMultiNodeTestInfoPolling === "function") {
        this.startMultiNodeTestInfoPolling();
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

    // === CHART INITIALIZATION ===
    // This large method is kept inline due to its complexity and tight coupling with Chart.js

    initCharts(opts) {
      const initOpts = opts && typeof opts === "object" ? opts : {};
      const onlyWarehouse = !!initOpts.onlyWarehouse;
      const onlyConcurrency = !!initOpts.onlyConcurrency;
      const onlyOverhead = !!initOpts.onlyOverhead;

      if (this.debug) {
        console.log("[dashboard][debug] initCharts", { onlyWarehouse, onlyOverhead });
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
      const formatPctValue = (v) => this.formatPctValue(v);

      // Allow callers (e.g. after templateInfo loads) to rebuild ONLY the concurrency chart
      // so we can correct legends/datasets for Postgres without resetting other charts.
      if (!onlyWarehouse && !onlyOverhead && onlyConcurrency) {
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
      if (!onlyWarehouse && !onlyOverhead) {
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
      if (!onlyWarehouse && !onlyOverhead) {
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

      const resourcesCpuCanvas = document.getElementById("resourcesCpuSparkline");
      const resourcesMemCanvas = document.getElementById("resourcesMemSparkline");
      const resourcesHistoryCanvas = document.getElementById("resourcesHistoryChart");
      if (!onlyWarehouse && !onlyOverhead) {
        if (resourcesCpuCanvas && window.Chart && Chart.getChart) {
          safeDestroy(Chart.getChart(resourcesCpuCanvas));
        }
        if (resourcesMemCanvas && window.Chart && Chart.getChart) {
          safeDestroy(Chart.getChart(resourcesMemCanvas));
        }
        if (resourcesHistoryCanvas && window.Chart && Chart.getChart) {
          safeDestroy(Chart.getChart(resourcesHistoryCanvas));
        }

        const cpuCtx =
          resourcesCpuCanvas && resourcesCpuCanvas.getContext
            ? resourcesCpuCanvas.getContext("2d")
            : null;
        if (cpuCtx) {
          resourcesCpuCanvas.__chart = new Chart(cpuCtx, {
            type: "line",
            data: {
              labels: [],
              datasets: [
                {
                  label: "CPU",
                  data: [],
                  borderColor: "rgb(59, 130, 246)",
                  backgroundColor: "rgba(59, 130, 246, 0.1)",
                  tension: 0.35,
                  fill: true,
                  pointRadius: 0,
                },
              ],
            },
            options: {
              responsive: true,
              maintainAspectRatio: false,
              animation: { duration: 0 },
              interaction: { mode: "index", intersect: false },
              scales: {
                x: { display: false },
                y: { display: false, beginAtZero: true },
              },
              plugins: {
                legend: { display: false },
                tooltip: {
                  callbacks: {
                    label: (ctx) =>
                      `${ctx.dataset.label}: ${formatPctValue(ctx.parsed.y, 1)}`,
                  },
                },
              },
            },
          });
        }

        const memCtx =
          resourcesMemCanvas && resourcesMemCanvas.getContext
            ? resourcesMemCanvas.getContext("2d")
            : null;
        if (memCtx) {
          resourcesMemCanvas.__chart = new Chart(memCtx, {
            type: "line",
            data: {
              labels: [],
              datasets: [
                {
                  label: "Memory (MB)",
                  data: [],
                  borderColor: "rgb(124, 58, 237)",
                  backgroundColor: "rgba(124, 58, 237, 0.1)",
                  tension: 0.35,
                  fill: true,
                  pointRadius: 0,
                },
              ],
            },
            options: {
              responsive: true,
              maintainAspectRatio: false,
              animation: { duration: 0 },
              interaction: { mode: "index", intersect: false },
              scales: {
                x: { display: false },
                y: { display: false, beginAtZero: true },
              },
              plugins: {
                legend: { display: false },
                tooltip: {
                  callbacks: {
                    label: (ctx) =>
                      `${ctx.dataset.label}: ${formatCompact(ctx.parsed.y)} MB`,
                  },
                },
              },
            },
          });
        }

        const resourcesHistoryCtx =
          resourcesHistoryCanvas && resourcesHistoryCanvas.getContext
            ? resourcesHistoryCanvas.getContext("2d")
            : null;
        if (resourcesHistoryCtx) {
          resourcesHistoryCanvas.__chart = new Chart(resourcesHistoryCtx, {
            type: "line",
            data: {
              labels: [],
              datasets: [
                {
                  label: "CPU (%)",
                  data: [],
                  yAxisID: "yCpu",
                  borderColor: "rgb(59, 130, 246)",
                  backgroundColor: "transparent",
                  tension: 0.35,
                },
                {
                  label: "Memory (MB)",
                  data: [],
                  yAxisID: "yMem",
                  borderColor: "rgb(124, 58, 237)",
                  backgroundColor: "transparent",
                  tension: 0.35,
                },
              ],
            },
            options: {
              responsive: true,
              maintainAspectRatio: false,
              animation: { duration: 0 },
              interaction: { mode: "index", intersect: false },
              scales: {
                yCpu: {
                  position: "left",
                  beginAtZero: true,
                  ticks: {
                    callback: (value) => formatPctValue(value, 0),
                  },
                  title: { display: true, text: "CPU (%)" },
                },
                yMem: {
                  position: "right",
                  beginAtZero: true,
                  grid: { drawOnChartArea: false },
                  ticks: {
                    callback: (value) => `${formatCompact(value)} MB`,
                  },
                  title: { display: true, text: "Memory (MB)" },
                },
              },
              plugins: {
                tooltip: {
                  callbacks: {
                    label: (ctx) => {
                      if (ctx.dataset.yAxisID === "yCpu") {
                        return `${ctx.dataset.label}: ${formatPctValue(ctx.parsed.y, 1)}`;
                      }
                      return `${ctx.dataset.label}: ${formatCompact(ctx.parsed.y)} MB`;
                    },
                  },
                },
              },
            },
          });
        }
      }

      const sfRunningCanvas = document.getElementById("sfRunningChart");
      if (!onlyWarehouse && !onlyOverhead) {
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
                { label: "Total (tagged)", data: [], borderColor: "rgb(59, 130, 246)", backgroundColor: "transparent", tension: 0.4 },
                { label: "Reads (tagged)", data: [], borderColor: "rgb(34, 197, 94)", backgroundColor: "transparent", tension: 0.4, hidden: byKind },
                { label: "Point Lookup", data: [], borderColor: "rgb(20, 184, 166)", backgroundColor: "transparent", tension: 0.4, hidden: !byKind },
                { label: "Range Scan", data: [], borderColor: "rgb(6, 182, 212)", backgroundColor: "transparent", tension: 0.4, hidden: !byKind },
                { label: "Writes (tagged)", data: [], borderColor: "rgb(249, 115, 22)", backgroundColor: "transparent", tension: 0.4, hidden: byKind },
                { label: "Insert", data: [], borderColor: "rgb(245, 158, 11)", backgroundColor: "transparent", tension: 0.4, hidden: !byKind },
                { label: "Update", data: [], borderColor: "rgb(239, 68, 68)", backgroundColor: "transparent", tension: 0.4, hidden: !byKind },
                { label: "Blocked", data: [], borderColor: "rgb(168, 85, 247)", backgroundColor: "transparent", tension: 0.4, borderDash: [6, 4] },
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
                  ticks: { callback: (value) => formatCompact(value) },
                  title: { display: true, text: "Running Queries" },
                },
              },
              plugins: {
                tooltip: {
                  mode: 'index',
                  intersect: false,
                  callbacks: {
                    label: (ctx) => `${ctx.dataset.label}: ${formatCompact(ctx.parsed.y)}`,
                  },
                },
              },
            },
          });
        }
      }

      // OPS/SEC Breakdown Chart (history view)
      const opsSecCanvas = document.getElementById("opsSecChart");
      if (!onlyWarehouse && !onlyOverhead) {
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
                { label: "Total", data: [], borderColor: "rgb(59, 130, 246)", backgroundColor: "transparent", tension: 0.4 },
                { label: "Reads", data: [], borderColor: "rgb(34, 197, 94)", backgroundColor: "transparent", tension: 0.4, hidden: byKindOps },
                { label: "Point Lookup", data: [], borderColor: "rgb(20, 184, 166)", backgroundColor: "transparent", tension: 0.4, hidden: !byKindOps },
                { label: "Range Scan", data: [], borderColor: "rgb(6, 182, 212)", backgroundColor: "transparent", tension: 0.4, hidden: !byKindOps },
                { label: "Writes", data: [], borderColor: "rgb(249, 115, 22)", backgroundColor: "transparent", tension: 0.4, hidden: byKindOps },
                { label: "Insert", data: [], borderColor: "rgb(245, 158, 11)", backgroundColor: "transparent", tension: 0.4, hidden: !byKindOps },
                { label: "Update", data: [], borderColor: "rgb(239, 68, 68)", backgroundColor: "transparent", tension: 0.4, hidden: !byKindOps },
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
                  ticks: { callback: (value) => formatCompact(value) },
                  title: { display: true, text: "QPS" },
                },
              },
              plugins: {
                tooltip: {
                  mode: 'index',
                  intersect: false,
                  callbacks: {
                    label: (ctx) => `${ctx.dataset.label}: ${formatCompact(ctx.parsed.y)} QPS`,
                  },
                },
              },
            },
          });
        }
      }

      const latencyCanvas = document.getElementById("latencyChart");
      if (!onlyWarehouse && !onlyOverhead) {
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
                { label: "P50", data: [], borderColor: "rgb(16, 185, 129)", backgroundColor: "transparent", tension: 0.4 },
                { label: "P95", data: [], borderColor: "rgb(245, 158, 11)", backgroundColor: "transparent", tension: 0.4 },
                { label: "P99", data: [], borderColor: "rgb(239, 68, 68)", backgroundColor: "transparent", tension: 0.4 },
              ],
            },
            options: {
              responsive: true,
              maintainAspectRatio: false,
              animation: { duration: 0 },
              scales: {
                y: {
                  beginAtZero: true,
                  ticks: { callback: (value) => `${Number(value).toFixed(2)} ms` },
                },
              },
              plugins: {
                tooltip: {
                  mode: 'index',
                  intersect: false,
                  callbacks: {
                    label: (ctx) => `${ctx.dataset.label}: ${Number(ctx.parsed.y).toFixed(2)} ms`,
                  },
                },
              },
            },
          });
        }
      }

      // MCW Live Chart (real-time active clusters for live dashboard)
      const mcwLiveCanvas = document.getElementById("mcwLiveChart");
      if (!onlyWarehouse && !onlyOverhead) {
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
                    label: (ctx) => `${ctx.dataset.label}: ${Math.trunc(Number(ctx.parsed.y))}`,
                  },
                },
              },
            },
          });
        }
      }

      const warehouseCanvas = document.getElementById("warehouseQueueChart");
      const existingWarehouseChart =
        warehouseCanvas &&
        (warehouseCanvas.__chart ||
          (window.Chart && Chart.getChart ? Chart.getChart(warehouseCanvas) : null));
      if (existingWarehouseChart) {
        if (onlyWarehouse) {
          return;
        }
      } else {
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
                { label: "Active clusters", data: [], yAxisID: "yClusters", borderColor: "rgb(59, 130, 246)", backgroundColor: "transparent", tension: 0.0, stepped: true },
                { label: "Queue overload (ms)", data: [], yAxisID: "yQueue", borderColor: "rgb(245, 158, 11)", backgroundColor: "transparent", tension: 0.2 },
                { label: "Queue provisioning (ms)", data: [], yAxisID: "yQueue", borderColor: "rgb(239, 68, 68)", backgroundColor: "transparent", tension: 0.2 },
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
                  ticks: { callback: (value) => `${Number(value).toFixed(0)} ms` },
                },
                x: { ticks: { maxTicksLimit: 12 } },
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
      }

      const overheadCanvas = document.getElementById("overheadChart");
      if (overheadCanvas && window.Chart && Chart.getChart) {
        safeDestroy(Chart.getChart(overheadCanvas));
      }

      const overheadCtx =
        overheadCanvas && overheadCanvas.getContext
          ? overheadCanvas.getContext("2d")
          : null;
      if (overheadCtx) {
        overheadCanvas.__chart = new Chart(overheadCtx, {
          type: "line",
          data: {
            labels: [],
            datasets: [
              { label: "Overhead (P50)", data: [], yAxisID: "yMs", borderColor: "rgb(245, 158, 11)", backgroundColor: "rgba(245, 158, 11, 0.1)", tension: 0.2, fill: true },
              { label: "End-to-end (avg)", data: [], yAxisID: "yMs", borderColor: "rgb(239, 68, 68)", backgroundColor: "transparent", tension: 0.2, borderDash: [4, 4] },
              { label: "SF total (avg)", data: [], yAxisID: "yMs", borderColor: "rgb(34, 197, 94)", backgroundColor: "transparent", tension: 0.2, borderDash: [4, 4] },
              { label: "Enriched queries", data: [], yAxisID: "yCount", borderColor: "rgb(59, 130, 246)", backgroundColor: "transparent", tension: 0.2, borderWidth: 1 },
            ],
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: { duration: 0 },
            interaction: { mode: "index", intersect: false },
            scales: {
              yMs: {
                position: "left",
                beginAtZero: true,
                title: { display: true, text: "Latency (ms)" },
                ticks: { callback: (value) => `${Number(value).toFixed(0)} ms` },
              },
              yCount: {
                position: "right",
                beginAtZero: true,
                grid: { drawOnChartArea: false },
                title: { display: true, text: "Enriched samples / sec" },
                ticks: { callback: (value) => formatCompact(value) },
              },
              x: { ticks: { maxTicksLimit: 12 } },
            },
            plugins: {
              tooltip: {
                callbacks: {
                  label: (ctx) => {
                    const y = ctx && ctx.parsed ? ctx.parsed.y : null;
                    if (y == null) return ctx.dataset.label;
                    if (ctx.dataset.yAxisID === "yCount") {
                      return `${ctx.dataset.label}: ${formatCompact(y)}`;
                    }
                    return `${ctx.dataset.label}: ${Number(y).toFixed(1)} ms`;
                  },
                },
              },
            },
          },
        });
      }
    },

    // === PHASE CHANGE HANDLING ===
    // Unified handler for all phase transitions from WebSocket

    handlePhaseChanged(phase, status, reason) {
      const normalizedPhase = this.normalizePhase ? this.normalizePhase(phase) : phase;
      const prevPhase = this.phase;
      const prevStatus = this.status;
      
      if (this.debug) {
        console.log("[PHASE] PHASE_CHANGED_EVENT", {
          phase: normalizedPhase,
          status,
          reason,
          prevPhase,
          prevStatus,
        });
      }

      // Handle cancellation with toast notification
      if (status === "CANCELLING" || status === "CANCELLED") {
        const displayReason = reason || "User requested";
        if (window.toast && typeof window.toast.warning === "function") {
          window.toast.warning(`Test cancelled: ${displayReason}`);
        }
        if (this.debug) {
          console.log("[PHASE] CANCELLATION", { status, reason: displayReason });
        }
      }

      // Update status and phase
      if (status) this.setStatusIfAllowed(status);
      if (phase) this.setPhaseIfAllowed(phase, status);

      // Start timer on first active phase if not already running
      const statusUpper = (this.status || "").toUpperCase();
      const isActive = statusUpper === "RUNNING" || statusUpper === "CANCELLING" || statusUpper === "STOPPING";
      
      if (isActive && !this.isTimerRunning()) {
        if (normalizedPhase === "WARMUP" || normalizedPhase === "RUNNING") {
          this.startElapsedTimer(0);
        }
      }

      // Stop timer on terminal states
      const isTerminal = ["COMPLETED", "FAILED", "CANCELLED", "STOPPED"].includes(statusUpper);
      if (isTerminal) {
        this.stopElapsedTimer();
        this.stopProcessingLogTimer();
      }

      // Handle processing phase
      if (normalizedPhase === "PROCESSING" && this.normalizePhase(prevPhase) !== "PROCESSING") {
        this.startProcessingLogTimer();
      }
    },

    // === METRICS PAYLOAD PROCESSING ===
    // This large method is kept inline due to its complexity and chart updates

    applyMetricsPayload(payload) {
      if (!payload) return;
      // Never allow dashboard updates to crash the whole Alpine component.
      try {
        let data = payload;
        if (data && data.event === "RUN_UPDATE" && data.data) {
          data = data.data;
        }
        if (!data) return;
        const normalized = { ...data };
        const run = normalized.run && typeof normalized.run === "object" ? normalized.run : null;
        if (run) {
          this.runInfo = run;
          if (!normalized.status && run.status) normalized.status = run.status;
          if (!normalized.phase && run.phase) normalized.phase = run.phase;
          if (!normalized.timing && run.timing) normalized.timing = run.timing;
          const aggregate = run.aggregate_metrics && typeof run.aggregate_metrics === "object" ? run.aggregate_metrics : null;
          if (aggregate) {
            if (!normalized.ops) {
              normalized.ops = { total: aggregate.total_ops || 0, current_per_sec: aggregate.qps || 0 };
            }
            if (!normalized.latency) {
              normalized.latency = { p50: aggregate.p50_latency_ms || 0, p95: aggregate.p95_latency_ms || 0, p99: aggregate.p99_latency_ms || 0, avg: aggregate.avg_latency_ms || 0 };
            }
            if (!normalized.errors) {
              normalized.errors = { count: aggregate.total_errors || 0, rate: aggregate.error_rate || 0 };
            }
            if (!normalized.connections) {
              normalized.connections = { active: aggregate.active_connections || 0, target: aggregate.target_connections || 0 };
            }
          }
        }
        if (Array.isArray(normalized.workers)) {
          this.updateLiveWorkers(normalized.workers);
        }
        if (
          normalized.enrichment_progress &&
          typeof this.applyEnrichmentProgress === "function"
        ) {
          this.applyEnrichmentProgress(normalized.enrichment_progress);
        }
        if (
          normalized.warehouse_details &&
          typeof this.applyWarehouseDetails === "function"
        ) {
          this.applyWarehouseDetails(normalized.warehouse_details);
        }
        payload = normalized;
        if ("latency_aggregation_method" in payload) {
          this.latencyAggregationMethod = payload.latency_aggregation_method;
        }

        // Handle error payloads
        if (payload.error) {
          console.log("[dashboard] Received error payload:", payload.error);
        }
        const errorTypes = ["connection_error", "setup_error", "execution_error"];
        if (payload.error && errorTypes.includes(payload.error.type)) {
          const errorTypeLabels = { "connection_error": "Connection error", "setup_error": "Setup error", "execution_error": "Execution error" };
          const errorType = errorTypeLabels[payload.error.type] || "Error";
          const errorMsg = payload.error.message || `${errorType}: Test failed`;
          console.error(`[dashboard] ${errorType} - showing toast:`, errorMsg);
          if (window.toast && typeof window.toast.error === "function") {
            window.toast.error(errorMsg);
          }
          this.status = "FAILED";
          this.phase = "COMPLETED";
          this.testRunning = false;
          return;
        }

        const throughputCanvas = document.getElementById("throughputChart");
        const latencyCanvas = document.getElementById("latencyChart");
        const concurrencyCanvas = document.getElementById("concurrencyChart");
        const sfRunningCanvas = document.getElementById("sfRunningChart");
        const throughputChart = throughputCanvas && (throughputCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(throughputCanvas) : null));
        const latencyChart = latencyCanvas && (latencyCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(latencyCanvas) : null));
        const concurrencyChart = concurrencyCanvas && (concurrencyCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(concurrencyCanvas) : null));
        const sfRunningChart = sfRunningCanvas && (sfRunningCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(sfRunningCanvas) : null));
        const resourcesCpuCanvas = document.getElementById("resourcesCpuSparkline");
        const resourcesMemCanvas = document.getElementById("resourcesMemSparkline");
        const resourcesCpuChart = resourcesCpuCanvas && (resourcesCpuCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(resourcesCpuCanvas) : null));
        const resourcesMemChart = resourcesMemCanvas && (resourcesMemCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(resourcesMemCanvas) : null));

        // If charts weren't available at init time, re-init on first payload
        if (!throughputChart || !latencyChart || (concurrencyCanvas && !concurrencyChart) || (sfRunningCanvas && !sfRunningChart) || (resourcesCpuCanvas && !resourcesCpuChart) || (resourcesMemCanvas && !resourcesMemChart)) {
          this.initCharts();
        }

        const timing = payload.timing || {};
        const hasTiming = timing && typeof timing === "object" && Object.keys(timing).length > 0;
        const runSnapshot = payload.run || null;
        const allowPayloadPhase = hasTiming || !!runSnapshot;
        const phase = payload.phase ? String(payload.phase) : null;
        const status = payload.status ? String(payload.status) : null;
        const prevPhase = this.phase ? String(this.phase).toUpperCase() : "";
        const prevStatus = this.status;
        
        // Extract cancellation reason if present
        const cancellationReason = payload.cancellation_reason || payload.reason || null;
        
        // Handle phase changes through unified handler
        const phaseChanged = phase && this.normalizePhase(phase) !== this.normalizePhase(prevPhase);
        const statusChanged = status && status !== prevStatus;
        
        if (phaseChanged || statusChanged) {
          this.handlePhaseChanged(phase, status, cancellationReason);
        } else {
          // Just update status/phase without full handler
          if (status) this.setStatusIfAllowed(status);
          if (phase && allowPayloadPhase) {
            this.setPhaseIfAllowed(phase, status);
          }
        }

        const nextPhaseUpper = (this.phase || "").toString().toUpperCase();
        if (nextPhaseUpper === "PROCESSING" && prevPhase !== "PROCESSING") {
          this.startProcessingLogTimer();
        } else if (prevPhase === "PROCESSING" && nextPhaseUpper !== "PROCESSING") {
          this.stopProcessingLogTimer();
        }

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

        // Timer is now purely local - no server sync needed
        // Timer starts in handlePhaseChanged() on first active phase
        // Timer stops in handlePhaseChanged() on terminal states
        
        const phaseUpper = typeof this.normalizePhase === "function" 
          ? this.normalizePhase(this.phase).toUpperCase() 
          : (this.phase || "").toString().toUpperCase();
        const statusUpper = (this.status || "").toString().toUpperCase();
        const isTerminal =
          phaseUpper === "COMPLETED" ||
          ["COMPLETED", "STOPPED", "FAILED", "CANCELLED"].includes(statusUpper);

        if (this.duration > 0) {
          this.progress = Math.min(100, (this.elapsed / this.duration) * 100);
        } else {
          this.progress = 0;
        }

        if (isTerminal) {
          this.stopElapsedTimer();
          // Stop HTTP polling - test is done
          if (typeof this.stopMultiNodeTestInfoPolling === "function") {
            this.stopMultiNodeTestInfoPolling();
          }
          if (typeof this.stopMultiNodeMetricsPolling === "function") {
            this.stopMultiNodeMetricsPolling();
          }
          // Reset live Snowflake metrics to zero - no queries running after test ends
          this.metrics.sf_running = 0;
          this.metrics.sf_queued = 0;
          this.metrics.sf_running_bench = 0;
          this.metrics.sf_queued_bench = 0;
          this.metrics.sf_blocked_bench = 0;
          this.metrics.sf_running_tagged_bench = 0;
          this.metrics.sf_running_other_bench = 0;
          this.metrics.sf_running_read_bench = 0;
          this.metrics.sf_running_write_bench = 0;
          this.metrics.sf_running_point_lookup_bench = 0;
          this.metrics.sf_running_range_scan_bench = 0;
          this.metrics.sf_running_insert_bench = 0;
          this.metrics.sf_running_update_bench = 0;
          this.metrics.in_flight = 0;
        }

        if (this.mode === "live" && typeof this.updateLiveTransport === "function") {
          this.updateLiveTransport();
        }

        const ts = payload.timestamp ? new Date(payload.timestamp).toLocaleTimeString() : new Date().toLocaleTimeString();
        const ops = payload.ops;
        const latency = payload.latency;
        const errors = payload.errors;
        const allowCharts = !phaseUpper || phaseUpper === "WARMUP" || phaseUpper === "RUNNING";

        const runData = payload.run || {};
        const workersActive = runData.workers_active;
        const workersExpected = runData.workers_expected;
        const hasActiveWorkers = workersActive === undefined || workersActive === null || workersActive > 0;

        if (ops) {
          this.metrics.ops_per_sec = ops.current_per_sec || 0;
          if (hasActiveWorkers) {
            this.qpsHistory.push(this.metrics.ops_per_sec);
            if (this.qpsHistory.length > 30) this.qpsHistory.shift();
            const sum = this.qpsHistory.reduce((a, b) => a + b, 0);
            this.metrics.qps_avg_30s = this.qpsHistory.length > 0 ? sum / this.qpsHistory.length : 0;
          }
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
        }
        const warehousePayload = payload.warehouse || (custom && typeof custom === "object" ? custom.warehouse : null);
        if (warehousePayload) {
          this.metrics.sf_running = warehousePayload.running || 0;
          this.metrics.sf_queued = warehousePayload.queued || 0;
          this.metrics.started_clusters = warehousePayload.started_clusters || 0;
        }
        if (custom) {
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
          const resources = custom.resources;
          if (resources && typeof resources === "object") {
            this.metrics.resources_available = true;
            this.metrics.cpu_percent = resources.process_cpu_percent ?? resources.cpu_percent ?? 0;
            this.metrics.memory_mb = resources.process_memory_mb ?? resources.memory_mb ?? 0;
            this.metrics.host_cpu_percent = resources.host_cpu_percent ?? this.metrics.host_cpu_percent;
            this.metrics.host_cpu_cores = resources.host_cpu_cores ?? this.metrics.host_cpu_cores;
            this.metrics.host_memory_mb = resources.host_memory_mb ?? this.metrics.host_memory_mb;
            this.metrics.host_memory_total_mb = resources.host_memory_total_mb ?? this.metrics.host_memory_total_mb;
            this.metrics.host_memory_available_mb = resources.host_memory_available_mb ?? this.metrics.host_memory_available_mb;
            this.metrics.host_memory_percent = resources.host_memory_percent ?? this.metrics.host_memory_percent;
            this.metrics.cgroup_cpu_percent = resources.cgroup_cpu_percent ?? this.metrics.cgroup_cpu_percent;
            this.metrics.cgroup_cpu_quota_cores = resources.cgroup_cpu_quota_cores ?? this.metrics.cgroup_cpu_quota_cores;
            this.metrics.cgroup_memory_mb = resources.cgroup_memory_mb ?? this.metrics.cgroup_memory_mb;
            this.metrics.cgroup_memory_limit_mb = resources.cgroup_memory_limit_mb ?? this.metrics.cgroup_memory_limit_mb;
            this.metrics.cgroup_memory_percent = resources.cgroup_memory_percent ?? this.metrics.cgroup_memory_percent;
          }
        }
        const findMax = payload.find_max;
        if (findMax && typeof findMax === "object") {
          this.findMaxController = findMax;
        }

        this.syncFindMaxCountdown();

        // Update charts
        const throughputChart2 = throughputCanvas && (throughputCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(throughputCanvas) : null));
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

        const latencyChart2 = latencyCanvas && (latencyCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(latencyCanvas) : null));
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

        const concurrencyChart2 = concurrencyCanvas && (concurrencyCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(concurrencyCanvas) : null));
        if (concurrencyChart2) {
          if (concurrencyChart2.data.labels.length > 60) {
            concurrencyChart2.data.labels.shift();
            concurrencyChart2.data.datasets.forEach((ds) => ds.data.shift());
          }
          if (allowCharts) {
            const tableType = (this.templateInfo?.table_type || "").toLowerCase();
            const isPostgres = ["postgres", "snowflake_postgres"].includes(tableType);

            concurrencyChart2.data.labels.push(ts);

            if (isPostgres) {
              concurrencyChart2.data.datasets[0].data.push(this.metrics.target_workers);
              concurrencyChart2.data.datasets[1].data.push(this.metrics.in_flight);
            } else {
              const sfQueued = this.metrics.sf_queued_bench > 0 ? this.metrics.sf_queued_bench : this.metrics.sf_queued;
              concurrencyChart2.data.datasets[0].data.push(this.metrics.target_workers);
              concurrencyChart2.data.datasets[1].data.push(this.metrics.in_flight);
              concurrencyChart2.data.datasets[2].data.push(sfQueued);
            }
            concurrencyChart2.update();
          }
        }

        const cpuSparkCanvas = document.getElementById("resourcesCpuSparkline");
        const memSparkCanvas = document.getElementById("resourcesMemSparkline");
        const cpuSparkChart = cpuSparkCanvas && (cpuSparkCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(cpuSparkCanvas) : null));
        const memSparkChart = memSparkCanvas && (memSparkCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(memSparkCanvas) : null));
        const cpuValue = Number.isFinite(this.metrics.host_cpu_percent) ? this.metrics.host_cpu_percent : this.metrics.cpu_percent;
        const memValue = Number.isFinite(this.metrics.host_memory_mb) ? this.metrics.host_memory_mb : this.metrics.memory_mb;
        if (cpuSparkChart) {
          if (cpuSparkChart.data.labels.length > 30) {
            cpuSparkChart.data.labels.shift();
            cpuSparkChart.data.datasets[0].data.shift();
          }
          if (allowCharts) {
            cpuSparkChart.data.labels.push(ts);
            cpuSparkChart.data.datasets[0].data.push(cpuValue);
            cpuSparkChart.update();
          }
        }
        if (memSparkChart) {
          if (memSparkChart.data.labels.length > 30) {
            memSparkChart.data.labels.shift();
            memSparkChart.data.datasets[0].data.shift();
          }
          if (allowCharts) {
            memSparkChart.data.labels.push(ts);
            memSparkChart.data.datasets[0].data.push(memValue);
            memSparkChart.update();
          }
        }

        const sfRunningCanvas2 = document.getElementById("sfRunningChart");
        const sfRunningChart2 = sfRunningCanvas2 && (sfRunningCanvas2.__chart || (window.Chart && Chart.getChart ? Chart.getChart(sfRunningCanvas2) : null));
        if (sfRunningChart2) {
          if (sfRunningChart2.data.labels.length > 60) {
            sfRunningChart2.data.labels.shift();
            sfRunningChart2.data.datasets.forEach((ds) => ds.data.shift());
          }
          if (allowCharts) {
            const totalTagged = this.metrics.sf_bench_available ? this.metrics.sf_running_tagged_bench : 0;
            const totalRaw = this.metrics.sf_bench_available ? this.metrics.sf_running_bench : 0;
            const total = totalTagged > 0 ? totalTagged : totalRaw;

            const sfReads = this.metrics.sf_bench_available ? this.metrics.sf_running_read_bench : 0;
            const sfWrites = this.metrics.sf_bench_available ? this.metrics.sf_running_write_bench : 0;
            const sfPl = this.metrics.sf_bench_available ? this.metrics.sf_running_point_lookup_bench : 0;
            const sfRs = this.metrics.sf_bench_available ? this.metrics.sf_running_range_scan_bench : 0;
            const sfIns = this.metrics.sf_bench_available ? this.metrics.sf_running_insert_bench : 0;
            const sfUpd = this.metrics.sf_bench_available ? this.metrics.sf_running_update_bench : 0;
            const sfBreakdownHasData = (sfPl + sfRs + sfIns + sfUpd) > 0;

            let reads = sfReads, writes = sfWrites, pl = sfPl, rs = sfRs, ins = sfIns, upd = sfUpd;
            if (!sfBreakdownHasData && this.metrics.app_ops_available && total > 0) {
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
            sfRunningChart2.data.datasets[7].data.push(this.metrics.sf_bench_available ? this.metrics.sf_blocked_bench : 0);
            sfRunningChart2.update();
          }
        }

        // MCW Live Chart
        const mcwLiveCanvas = document.getElementById("mcwLiveChart");
        const mcwLiveChart2 = mcwLiveCanvas && (mcwLiveCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(mcwLiveCanvas) : null));
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

        // Once we hit the terminal phase, refresh from the API
        if (!this.didRefreshAfterComplete && isTerminal) {
          this.didRefreshAfterComplete = true;
          this.loadTestInfo();
        }
      } catch (e) {
        console.error("[dashboard] applyMetricsPayload error:", e);
      }
    },
  };
}
