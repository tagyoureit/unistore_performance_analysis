/**
 * Dashboard UI Module
 * Methods for UI toggles and miscellaneous display helpers.
 */
window.DashboardMixins = window.DashboardMixins || {};

window.DashboardMixins.ui = {
  toggleClusterBreakdown() {
    this.clusterBreakdownExpanded = !this.clusterBreakdownExpanded;
  },

  clusterBreakdownSummary() {
    const breakdown = this.templateInfo?.cluster_breakdown || [];
    if (!breakdown.length) return null;

    let count = 0;
    let totalQueries = 0;
    let sumP50 = 0;
    let sumP95 = 0;
    let sumQueueOverload = 0;
    let sumQueueProvisioning = 0;
    let totalPl = 0;
    let totalRs = 0;
    let totalIns = 0;
    let totalUpd = 0;
    let unattributedQueries = 0;

    for (const c of breakdown) {
      const clusterNumber = Number(c.cluster_number || 0);
      if (clusterNumber > 0) {
        count += 1;
      } else {
        unattributedQueries += Number(c.query_count || 0);
      }
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
      avg_p50_exec_ms: count > 0 ? sumP50 / count : null,
      avg_p95_exec_ms: count > 0 ? sumP95 / count : null,
      avg_queued_overload_ms: count > 0 ? sumQueueOverload / count : null,
      avg_queued_provisioning_ms: count > 0 ? sumQueueProvisioning / count : null,
      total_point_lookups: totalPl,
      total_range_scans: totalRs,
      total_inserts: totalIns,
      total_updates: totalUpd,
      unattributed_queries: unattributedQueries,
    };
  },

  clusterLabel(cluster) {
    const clusterNumber = Number(cluster?.cluster_number || 0);
    return clusterNumber > 0 ? clusterNumber : "Unattributed";
  },

  toggleStepHistory() {
    this.stepHistoryExpanded = !this.stepHistoryExpanded;
  },

  toggleResourcesHistory() {
    this.resourcesHistoryExpanded = !this.resourcesHistoryExpanded;
  },

  toggleWorkerMetrics(worker) {
    if (!worker || !worker.key) return;
    const key = String(worker.key);
    this.workerMetricsExpanded[key] = !this.workerMetricsExpanded[key];
    if (this.workerMetricsExpanded[key]) {
      this.$nextTick(() => {
        this.renderWorkerMetricsChart(worker);
      });
    }
  },

  renderWorkerMetricsChart(worker) {
    if (!worker || !worker.key) return;
    const canvasId = `workerMetricsChart-${worker.key}`;
    const canvas = document.getElementById(canvasId);
    if (!canvas || !window.Chart) return;

    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    // Destroy existing chart if any
    if (canvas.__chart) {
      try {
        canvas.__chart.destroy();
      } catch (_) {}
    }

    const snapshots = Array.isArray(worker.snapshots) ? worker.snapshots : [];
    const labels = snapshots.map((s) => s.timestamp);
    const qpsData = snapshots.map((s) => Number(s.qps || 0));
    const p95Data = snapshots.map((s) => Number(s.p95_latency || 0));

    canvas.__chart = new Chart(ctx, {
      type: "line",
      data: {
        labels,
        datasets: [
          {
            label: "QPS",
            data: qpsData,
            borderColor: "rgb(59, 130, 246)",
            backgroundColor: "transparent",
            tension: 0.1,
            borderWidth: 2,
            yAxisID: "y",
          },
          {
            label: "P95 latency (ms)",
            data: p95Data,
            borderColor: "rgb(234, 88, 12)",
            backgroundColor: "transparent",
            tension: 0.1,
            borderWidth: 2,
            yAxisID: "y1",
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        scales: {
          y: {
            title: { display: true, text: "QPS" },
            ticks: { callback: (value) => this.formatCompact(value) },
          },
          y1: {
            position: "right",
            title: { display: true, text: "P95 (ms)" },
            ticks: { callback: (value) => this.formatCompact(value) },
            grid: { drawOnChartArea: false },
          },
          x: { display: false },
        },
        plugins: {
          legend: { display: true },
          tooltip: {
            callbacks: {
              label: (ctx) =>
                `${ctx.dataset.label}: ${this.formatCompact(ctx.parsed.y)}`,
            },
          },
        },
      },
    });
  },

  initFloatingToolbar() {
    if (this.mode !== "history") return;

    const SCROLL_THRESHOLD = 300;
    
    const handleScroll = () => {
      this.floatingToolbarVisible = window.scrollY > SCROLL_THRESHOLD;
    };

    window.addEventListener("scroll", handleScroll, { passive: true });
    this._floatingToolbarScrollHandler = handleScroll;
    handleScroll();

    const chartsSection = document.querySelector('[data-section="charts"]');
    const latencySection = document.querySelector('[data-section="latency"]');

    if (chartsSection || latencySection) {
      const observerCallback = (entries) => {
        for (const entry of entries) {
          const section = entry.target.dataset.section;
          if (section === "charts") {
            this.chartsInView = entry.isIntersecting;
          } else if (section === "latency") {
            this.latencyInView = entry.isIntersecting;
          }
        }
      };

      const observer = new IntersectionObserver(observerCallback, {
        root: null,
        rootMargin: "0px",
        threshold: 0,
      });

      if (chartsSection) observer.observe(chartsSection);
      if (latencySection) observer.observe(latencySection);
      this._floatingToolbarObserver = observer;
    }
  },

  destroyFloatingToolbar() {
    if (this._floatingToolbarScrollHandler) {
      window.removeEventListener("scroll", this._floatingToolbarScrollHandler);
      this._floatingToolbarScrollHandler = null;
    }
    if (this._floatingToolbarObserver) {
      this._floatingToolbarObserver.disconnect();
      this._floatingToolbarObserver = null;
    }
  },
};
