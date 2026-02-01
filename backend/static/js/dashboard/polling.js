/**
 * Dashboard Polling Module
 * Methods for multi-node metrics polling and enrichment status polling.
 */
window.DashboardMixins = window.DashboardMixins || {};

window.DashboardMixins.polling = {
  _multiNodePayloadFromSnapshot(snapshot) {
    if (!snapshot) return null;
    const phase = this.phase || this.status || null;
    const status = this.status || null;

    const resources = {
      cpu_percent: snapshot.resources_cpu_percent,
      memory_mb: snapshot.resources_memory_mb,
      process_cpu_percent: snapshot.resources_process_cpu_percent,
      process_memory_mb: snapshot.resources_process_memory_mb,
      host_cpu_percent: snapshot.resources_host_cpu_percent,
      host_cpu_cores: snapshot.resources_host_cpu_cores,
      host_memory_mb: snapshot.resources_host_memory_mb,
      host_memory_total_mb: snapshot.resources_host_memory_total_mb,
      host_memory_available_mb: snapshot.resources_host_memory_available_mb,
      host_memory_percent: snapshot.resources_host_memory_percent,
      cgroup_cpu_percent: snapshot.resources_cgroup_cpu_percent,
      cgroup_cpu_quota_cores: snapshot.resources_cgroup_cpu_quota_cores,
      cgroup_memory_mb: snapshot.resources_cgroup_memory_mb,
      cgroup_memory_limit_mb: snapshot.resources_cgroup_memory_limit_mb,
      cgroup_memory_percent: snapshot.resources_cgroup_memory_percent,
    };
    const resourcesAvailable =
      snapshot.resources_cpu_percent != null ||
      snapshot.resources_memory_mb != null ||
      snapshot.resources_host_cpu_percent != null ||
      snapshot.resources_host_memory_mb != null;

    return {
      phase,
      status,
      elapsed: snapshot.elapsed_seconds,
      ops: {
        total: snapshot.total_ops,
        current_per_sec: snapshot.ops_per_sec,
      },
      latency: {
        avg: snapshot.avg_latency,
        p50: snapshot.p50_latency,
        p95: snapshot.p95_latency,
        p99: snapshot.p99_latency,
      },
      errors: {
        count: snapshot.error_count,
        rate: snapshot.error_rate,
      },
      connections: {
        active: snapshot.active_connections,
        target: snapshot.target_workers,
      },
      snowflake: {
        running: snapshot.sf_running,
        queued: snapshot.sf_queued,
        bench_available: snapshot.sf_bench_available,
        running_bench: snapshot.sf_running_bench,
        queued_bench: snapshot.sf_queued_bench,
        blocked_bench: snapshot.sf_blocked_bench,
        running_tagged: snapshot.sf_running_tagged,
        running_other: snapshot.sf_running_other,
        running_read: snapshot.sf_running_read,
        running_write: snapshot.sf_running_write,
        running_point_lookup: snapshot.sf_running_point_lookup,
        running_range_scan: snapshot.sf_running_range_scan,
        running_insert: snapshot.sf_running_insert,
        running_update: snapshot.sf_running_update,
      },
      app_ops: snapshot.app_ops_available
        ? {
            available: true,
            point_lookup_count: snapshot.app_point_lookup_count,
            range_scan_count: snapshot.app_range_scan_count,
            insert_count: snapshot.app_insert_count,
            update_count: snapshot.app_update_count,
            read_count: snapshot.app_read_count,
            write_count: snapshot.app_write_count,
            total_count: snapshot.app_total_count,
            point_lookup_ops_sec: snapshot.app_point_lookup_ops_sec,
            range_scan_ops_sec: snapshot.app_range_scan_ops_sec,
            insert_ops_sec: snapshot.app_insert_ops_sec,
            update_ops_sec: snapshot.app_update_ops_sec,
            read_ops_sec: snapshot.app_read_ops_sec,
            write_ops_sec: snapshot.app_write_ops_sec,
          }
        : null,
      resources_available: resourcesAvailable,
      resources,
      started_clusters: snapshot.started_clusters,
    };
  },

  startMultiNodeMetricsPolling() {
    if (this._multiWorkerMetricsIntervalId) return;
    if (this._destroyed) return;
    const poll = () => {
      if (this._destroyed) {
        this.stopMultiNodeMetricsPolling();
        return;
      }
      this.loadMultiWorkerLiveMetrics();
    };
    poll();
    this._multiWorkerMetricsIntervalId = setInterval(poll, 1000);
  },

  stopMultiNodeMetricsPolling() {
    if (!this._multiWorkerMetricsIntervalId) return;
    clearInterval(this._multiWorkerMetricsIntervalId);
    this._multiWorkerMetricsIntervalId = null;
  },

  startMultiNodeTestInfoPolling() {
    if (this._multiWorkerTestInfoIntervalId) return;
    if (this._destroyed) return;
    const poll = () => {
      if (this._destroyed) {
        this.stopMultiNodeTestInfoPolling();
        return;
      }
      this.loadTestInfo();
    };
    this._multiWorkerTestInfoIntervalId = setInterval(poll, 1000);
  },

  stopMultiNodeTestInfoPolling() {
    if (!this._multiWorkerTestInfoIntervalId) return;
    clearInterval(this._multiWorkerTestInfoIntervalId);
    this._multiWorkerTestInfoIntervalId = null;
  },

  startEnrichmentPolling() {
    if (this._enrichmentPollIntervalId) return;
    if (this._destroyed) return;
    const wsOpen = this.websocket && this.websocket.readyState === 1;
    if (this.mode === "live" && wsOpen) return;
    this.pollEnrichmentStatus();
    this._enrichmentPollIntervalId = setInterval(() => {
      if (this._destroyed) {
        this.stopEnrichmentPolling();
        return;
      }
      this.pollEnrichmentStatus();
    }, 5000);
  },

  stopEnrichmentPolling() {
    if (this._enrichmentPollIntervalId) {
      clearInterval(this._enrichmentPollIntervalId);
      this._enrichmentPollIntervalId = null;
    }
  },

  applyEnrichmentProgress(data) {
    if (!data || typeof data !== "object") return;
    const prevStatus = this.enrichmentProgress?.enrichment_status;
    this.enrichmentProgress = data;
    if (!this.templateInfo || typeof this.templateInfo !== "object") {
      this.templateInfo = {};
    }
    if ("enrichment_status" in data) {
      this.templateInfo.enrichment_status = data.enrichment_status;
    }
    if ("enrichment_error" in data) {
      this.templateInfo.enrichment_error = data.enrichment_error;
    }
    if (typeof data.enrichment_ratio_pct === "number") {
      this.templateInfo.sf_enrichment_ratio_pct = data.enrichment_ratio_pct;
    }
    if (data.total_queries != null) {
      this.templateInfo.sf_enrichment_total_queries = data.total_queries;
    }
    if (data.enriched_queries != null) {
      this.templateInfo.sf_enrichment_enriched_queries = data.enriched_queries;
    }
    if (typeof data.can_retry === "boolean") {
      this.templateInfo.can_retry_enrichment = data.can_retry;
    }

    const status = String(data.enrichment_status || "").toUpperCase();
    const isComplete =
      data.is_complete === true || status === "COMPLETED" || status === "SKIPPED";
    if (status && status !== "PENDING") {
      this.stopEnrichmentPolling();
    }
    if (isComplete && status === "COMPLETED" && prevStatus !== "COMPLETED") {
      if (typeof this.loadTestInfo === "function") {
        this.loadTestInfo();
      }
      this.phase = "COMPLETED";
      if (typeof this.stopElapsedTimer === "function") {
        this.stopElapsedTimer();
      }
      if (window.toast && typeof window.toast.success === "function") {
        const ratio = Number.isFinite(Number(data.enrichment_ratio_pct))
          ? data.enrichment_ratio_pct
          : 0;
        window.toast.success(
          `Enrichment completed (${ratio}% queries enriched)`,
        );
      }
    }
  },

  async pollEnrichmentStatus() {
    if (!this.testId) return;
    if (this._destroyed) return;
    try {
      const resp = await fetch(`/api/tests/${this.testId}/enrichment-status`);
      if (!resp.ok) return;
      const data = await resp.json();
      this.enrichmentProgress = data;
      if (data.enrichment_status !== "PENDING" || data.is_complete) {
        this.stopEnrichmentPolling();
        if (this.templateInfo) {
          this.templateInfo.enrichment_status = data.enrichment_status;
        }
        if (data.enrichment_status === "COMPLETED") {
          await this.loadTestInfo();
          // Explicitly set phase to COMPLETED - loadTestInfo may not update reactive state
          this.phase = "COMPLETED";
          // Stop the elapsed timer now that we're fully complete
          if (typeof this.stopElapsedTimer === "function") {
            this.stopElapsedTimer();
          }
          if (window.toast && typeof window.toast.success === "function") {
            window.toast.success(`Enrichment completed (${data.enrichment_ratio_pct}% queries enriched)`);
          }
        }
      }
    } catch (e) {
      console.warn("Failed to poll enrichment status:", e);
    }
  },
};
