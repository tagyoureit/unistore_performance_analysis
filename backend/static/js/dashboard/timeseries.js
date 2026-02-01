/**
 * Dashboard Timeseries Module
 * Methods for loading and rendering warehouse/overhead timeseries charts.
 */
window.DashboardMixins = window.DashboardMixins || {};

window.DashboardMixins.timeseries = {
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
      this.warehouseTsWarmupEndElapsed = data && data.warmup_end_elapsed_seconds != null
        ? Number(data.warmup_end_elapsed_seconds)
        : null;

      if (this.debug) {
        this._debugCharts("loadWarehouseTimeseries: before init", {
          available: this.warehouseTsAvailable,
          points: Array.isArray(this.warehouseTs) ? this.warehouseTs.length : 0,
        });
      }

      // Wait for Alpine to update DOM visibility (x-show="warehouseTsAvailable")
      // before initializing Chart.js on the canvas. Without this, the canvas may
      // still be hidden (display: none) and Chart.js won't render properly.
      this.$nextTick(() => {
        // Ensure warehouse chart exists, then render (do NOT reset other charts).
        this.initCharts({ onlyWarehouse: true });
        this.renderWarehouseTimeseriesChart();

        if (this.debug) {
          this._debugCharts("loadWarehouseTimeseries: after render", {
            available: this.warehouseTsAvailable,
            points: Array.isArray(this.warehouseTs) ? this.warehouseTs.length : 0,
          });
        }
      });
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

    const allPoints = Array.isArray(this.warehouseTs) ? this.warehouseTs : [];
    const warmupEnd = this.warehouseTsWarmupEndElapsed;
    const showWarmup = this.showWarmup;

    const points = showWarmup
      ? allPoints
      : allPoints.filter((p) => !p || !p.warmup);

    const labels = [];
    const clusters = [];
    const overload = [];
    const provisioning = [];

    for (const p of points) {
      if (!p) continue;
      let secs = Number(p.elapsed_seconds || 0);
      if (!showWarmup && warmupEnd != null) {
        secs = secs - warmupEnd;
      }
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

    if (showWarmup && warmupEnd != null && warmupEnd > 0) {
      const warmupLabel = `${this.formatSecondsTenths(warmupEnd)}s`;
      const warmupIndex = labels.indexOf(warmupLabel);
      if (!chart.options.plugins.annotation) {
        chart.options.plugins.annotation = { annotations: {} };
      }
      chart.options.plugins.annotation.annotations.warmupLine = {
        type: "line",
        xMin: warmupIndex >= 0 ? warmupIndex : warmupEnd,
        xMax: warmupIndex >= 0 ? warmupIndex : warmupEnd,
        borderColor: "rgba(255, 165, 0, 0.8)",
        borderWidth: 2,
        borderDash: [6, 4],
        label: {
          display: true,
          content: "Warmup End",
          position: "start",
          backgroundColor: "rgba(255, 165, 0, 0.8)",
          color: "#fff",
          font: { size: 10 },
        },
      };
    } else if (chart.options.plugins.annotation?.annotations?.warmupLine) {
      delete chart.options.plugins.annotation.annotations.warmupLine;
    }

    chart.update();
  },

  async loadOverheadTimeseries() {
    if (!this.testId) return;
    if (this.mode !== "history") return;

    this.overheadTsLoading = true;
    this.overheadTsError = null;
    this.overheadTs = [];
    this.overheadTsAvailable = false;
    this.overheadTsLoaded = false;

    try {
      const resp = await fetch(`/api/tests/${this.testId}/overhead-timeseries`);
      if (!resp.ok) {
        const payload = await resp.json().catch(() => ({}));
        const detail = payload && payload.detail ? payload.detail : null;
        throw new Error(
          (detail && (detail.message || detail.detail || detail)) ||
            `Failed to load overhead timeseries (HTTP ${resp.status})`,
        );
      }

      const data = await resp.json();
      if (data && data.error) {
        throw new Error(String(data.error));
      }
      this.overheadTs = data && Array.isArray(data.points) ? data.points : [];
      this.overheadTsAvailable = !!(data && data.available);
      this.overheadTsWarmupEndElapsed = data && data.warmup_end_elapsed_seconds != null
        ? Number(data.warmup_end_elapsed_seconds)
        : null;

      this.initCharts({ onlyOverhead: true });
      this.renderOverheadTimeseriesChart();
    } catch (e) {
      console.error("Failed to load overhead timeseries:", e);
      this.overheadTsError = e && e.message ? e.message : String(e);
      this.overheadTs = [];
      this.overheadTsAvailable = false;
    } finally {
      this.overheadTsLoading = false;
      this.overheadTsLoaded = true;
    }
  },

  renderOverheadTimeseriesChart() {
    const canvas = document.getElementById("overheadChart");
    const chart =
      canvas &&
      (canvas.__chart ||
        (window.Chart && Chart.getChart ? Chart.getChart(canvas) : null));
    if (!canvas || !chart) return;

    const allPoints = Array.isArray(this.overheadTs) ? this.overheadTs : [];
    const warmupEnd = this.overheadTsWarmupEndElapsed;
    const showWarmup = this.showWarmup;

    const points = showWarmup
      ? allPoints
      : allPoints.filter((p) => !p || !p.warmup);

    const labels = [];
    const overheadData = [];
    const appData = [];
    const sfTotalData = [];
    const enrichedCountData = [];

    for (const p of points) {
      if (!p) continue;
      let secs = Number(p.elapsed_seconds || 0);
      if (!showWarmup && warmupEnd != null) {
        secs = secs - warmupEnd;
      }
      labels.push(`${this.formatSecondsTenths(secs)}s`);

      const overhead = p.p50_overhead_ms != null ? Number(p.p50_overhead_ms) : null;
      overheadData.push(overhead);
      appData.push(p.avg_app_ms != null ? Number(p.avg_app_ms) : null);
      sfTotalData.push(p.avg_sf_total_ms != null ? Number(p.avg_sf_total_ms) : null);
      enrichedCountData.push(Number(p.enriched_queries || 0));
    }

    chart.data.labels = labels;
    chart.data.datasets[0].data = overheadData;
    chart.data.datasets[1].data = appData;
    chart.data.datasets[2].data = sfTotalData;
    chart.data.datasets[3].data = enrichedCountData;

    if (showWarmup && warmupEnd != null && warmupEnd > 0) {
      const warmupLabel = `${this.formatSecondsTenths(warmupEnd)}s`;
      const warmupIndex = labels.indexOf(warmupLabel);
      if (!chart.options.plugins.annotation) {
        chart.options.plugins.annotation = { annotations: {} };
      }
      chart.options.plugins.annotation.annotations.warmupLine = {
        type: "line",
        xMin: warmupIndex >= 0 ? warmupIndex : warmupEnd,
        xMax: warmupIndex >= 0 ? warmupIndex : warmupEnd,
        borderColor: "rgba(255, 165, 0, 0.8)",
        borderWidth: 2,
        borderDash: [6, 4],
        label: {
          display: true,
          content: "Warmup End",
          position: "start",
          backgroundColor: "rgba(255, 165, 0, 0.8)",
          color: "#fff",
          font: { size: 10 },
        },
      };
    } else if (chart.options.plugins.annotation?.annotations?.warmupLine) {
      delete chart.options.plugins.annotation.annotations.warmupLine;
    }

    chart.update();
  },

  toggleShowWarmup() {
    this.showWarmup = !this.showWarmup;
    this.renderWarehouseTimeseriesChart();
    this.renderOverheadTimeseriesChart();
    if (typeof this.renderHistoricalCharts === "function") {
      this.renderHistoricalCharts();
    }
  },
};
