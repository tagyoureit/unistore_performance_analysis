/**
 * Dashboard Historical Metrics Module
 * Methods for loading and populating historical metrics into charts.
 */
window.DashboardMixins = window.DashboardMixins || {};

window.DashboardMixins.historicalMetrics = {
  async loadHistoricalMetrics() {
    if (!this.testId) return;
    try {
      // Fetch both main metrics and per-worker metrics in parallel
      const [resp, workerResp] = await Promise.all([
        fetch(`/api/tests/${this.testId}/metrics`),
        fetch(`/api/tests/${this.testId}/worker-metrics`),
      ]);
      if (!resp.ok) return;
      const data = await resp.json();
      
      if (!data.snapshots || data.snapshots.length === 0) {
        console.log("No historical metrics snapshots found for test");
        return;
      }

      this._metricsSnapshots = data.snapshots;
      this.metricsWarmupEndElapsed = data.warmup_end_elapsed_seconds != null
        ? Number(data.warmup_end_elapsed_seconds)
        : null;

      // Store per-worker metrics if available
      this._workerMetrics = null;
      this._workerMetricsRunStartAt = null;
      if (workerResp.ok) {
        const workerData = await workerResp.json();
        if (workerData.available && workerData.workers && workerData.workers.length > 1) {
          this._workerMetrics = workerData.workers;
          this._workerMetricsRunStartAt = workerData.run_start_at;
          console.log(`Loaded per-worker metrics for ${workerData.workers.length} workers`);
        }
      }

      if (this.debug) {
        this._debugCharts("loadHistoricalMetrics: start", {
          snapshots: data.snapshots.length,
          warmupEnd: this.metricsWarmupEndElapsed,
          workers: this._workerMetrics ? this._workerMetrics.length : 0,
        });
      }
      
      this.initCharts();
      this.renderHistoricalCharts();

      const lastSnapshot = data.snapshots && data.snapshots.length ? data.snapshots[data.snapshots.length - 1] : null;
      if (lastSnapshot) {
        this._updateResourceMetricsFromSnapshot(lastSnapshot);
      }

      if (this.debug) {
        this._debugCharts("loadHistoricalMetrics: after render", { snapshots: data.snapshots.length });
      }
      
      console.log(`Loaded ${data.snapshots.length} historical metrics snapshots`);
    } catch (e) {
      console.error("Failed to load historical metrics:", e);
      try {
        if (window.toast && typeof window.toast.error === "function") {
          window.toast.error(`Failed to render charts: ${e && e.message ? e.message : String(e)}`);
        }
      } catch (_) {}
    }
  },

  _updateResourceMetricsFromSnapshot(lastSnapshot) {
    const hostCpu = Number(lastSnapshot.resources_host_cpu_percent);
    const cpu = Number(Number.isFinite(hostCpu) ? hostCpu : lastSnapshot.resources_cpu_percent);
    const hostMem = Number(lastSnapshot.resources_host_memory_mb);
    const mem = Number(Number.isFinite(hostMem) ? hostMem : lastSnapshot.resources_memory_mb);
    const hostTotal = Number(lastSnapshot.resources_host_memory_total_mb);
    const hostAvail = Number(lastSnapshot.resources_host_memory_available_mb);
    const hostPct = Number(lastSnapshot.resources_host_memory_percent);
    const hostCores = Number(lastSnapshot.resources_host_cpu_cores);
    const cgroupCpu = Number(lastSnapshot.resources_cgroup_cpu_percent);
    const cgroupCores = Number(lastSnapshot.resources_cgroup_cpu_quota_cores);
    const cgroupMem = Number(lastSnapshot.resources_cgroup_memory_mb);
    const cgroupMemLimit = Number(lastSnapshot.resources_cgroup_memory_limit_mb);
    const cgroupMemPct = Number(lastSnapshot.resources_cgroup_memory_percent);
    this.metrics.resources_available = Number.isFinite(cpu) || Number.isFinite(mem);
    if (Number.isFinite(cpu)) this.metrics.cpu_percent = cpu;
    if (Number.isFinite(mem)) this.metrics.memory_mb = mem;
    if (Number.isFinite(hostCpu)) this.metrics.host_cpu_percent = hostCpu;
    if (Number.isFinite(hostMem)) this.metrics.host_memory_mb = hostMem;
    if (Number.isFinite(hostTotal)) this.metrics.host_memory_total_mb = hostTotal;
    if (Number.isFinite(hostAvail)) this.metrics.host_memory_available_mb = hostAvail;
    if (Number.isFinite(hostPct)) this.metrics.host_memory_percent = hostPct;
    if (Number.isFinite(hostCores)) this.metrics.host_cpu_cores = hostCores;
    if (Number.isFinite(cgroupCpu)) this.metrics.cgroup_cpu_percent = cgroupCpu;
    if (Number.isFinite(cgroupCores)) this.metrics.cgroup_cpu_quota_cores = cgroupCores;
    if (Number.isFinite(cgroupMem)) this.metrics.cgroup_memory_mb = cgroupMem;
    if (Number.isFinite(cgroupMemLimit)) this.metrics.cgroup_memory_limit_mb = cgroupMemLimit;
    if (Number.isFinite(cgroupMemPct)) this.metrics.cgroup_memory_percent = cgroupMemPct;
  },

  renderHistoricalCharts() {
    const allSnapshots = this._metricsSnapshots || [];
    const warmupEnd = this.metricsWarmupEndElapsed;
    const showWarmup = this.showWarmup;

    const snapshots = showWarmup
      ? allSnapshots
      : allSnapshots.filter((s) => !s || !s.warmup);

    const throughputCanvas = document.getElementById("throughputChart");
    const concurrencyCanvas = document.getElementById("concurrencyChart");
    const latencyCanvas = document.getElementById("latencyChart");
    const sfRunningCanvas = document.getElementById("sfRunningChart");
    const opsSecCanvas = document.getElementById("opsSecChart");
    const resourcesCpuCanvas = document.getElementById("resourcesCpuSparkline");
    const resourcesMemCanvas = document.getElementById("resourcesMemSparkline");
    const resourcesHistoryCanvas = document.getElementById("resourcesHistoryChart");

    const throughputChart = throughputCanvas && (throughputCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(throughputCanvas) : null));
    const concurrencyChart = concurrencyCanvas && (concurrencyCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(concurrencyCanvas) : null));
    const latencyChart = latencyCanvas && (latencyCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(latencyCanvas) : null));
    const sfRunningChart = sfRunningCanvas && (sfRunningCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(sfRunningCanvas) : null));
    const opsSecChart = opsSecCanvas && (opsSecCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(opsSecCanvas) : null));
    const resourcesCpuChart = resourcesCpuCanvas && (resourcesCpuCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(resourcesCpuCanvas) : null));
    const resourcesMemChart = resourcesMemCanvas && (resourcesMemCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(resourcesMemCanvas) : null));
    const resourcesHistoryChart = resourcesHistoryCanvas && (resourcesHistoryCanvas.__chart || (window.Chart && Chart.getChart ? Chart.getChart(resourcesHistoryCanvas) : null));

    const tableType = (this.templateInfo?.table_type || "").toLowerCase();
    const isPostgres = ["postgres", "snowflake_postgres"].includes(tableType);

    const labels = [];
    const throughputData = [];
    const errorRateData = [];
    const targetData = [];
    const inFlightData = [];
    const sfQueuedData = [];
    const p50Data = [];
    const p95Data = [];
    const p99Data = [];
    const sfTotalData = [];
    const sfReadData = [];
    const sfPlData = [];
    const sfRsData = [];
    const sfWriteData = [];
    const sfInsData = [];
    const sfUpdData = [];
    const sfBlockedData = [];
    const opsTotalData = [];
    const opsReadData = [];
    const opsPlData = [];
    const opsRsData = [];
    const opsWriteData = [];
    const opsInsData = [];
    const opsUpdData = [];
    const cpuData = [];
    const memData = [];

    let resourcesSeen = false;
    for (const snapshot of snapshots) {
      if (!snapshot) continue;
      let secs = Number(snapshot.elapsed_seconds || 0);
      if (!showWarmup && warmupEnd != null) {
        secs = secs - warmupEnd;
      }
      const ts = `${this.formatSecondsTenths(secs)}s`;
      labels.push(ts);

      throughputData.push(Number(snapshot.ops_per_sec || 0));
      errorRateData.push(Number(snapshot.error_rate || 0) * 100.0);

      targetData.push(Number(snapshot.target_workers || 0));
      inFlightData.push(Number(snapshot.active_connections || 0));
      const sfQueuedBench = Number(snapshot.sf_queued_bench || 0);
      sfQueuedData.push(sfQueuedBench > 0 ? sfQueuedBench : Number(snapshot.sf_queued || 0));

      p50Data.push(Number(snapshot.p50_latency || 0));
      p95Data.push(Number(snapshot.p95_latency || 0));
      p99Data.push(Number(snapshot.p99_latency || 0));

      const totalTagged = Number(snapshot.sf_running_tagged || 0);
      const totalRaw = Number(snapshot.sf_running || 0);
      sfTotalData.push(totalTagged > 0 ? totalTagged : totalRaw);
      sfReadData.push(Number(snapshot.sf_running_read || 0));
      sfPlData.push(Number(snapshot.sf_running_point_lookup || 0));
      sfRsData.push(Number(snapshot.sf_running_range_scan || 0));
      sfWriteData.push(Number(snapshot.sf_running_write || 0));
      sfInsData.push(Number(snapshot.sf_running_insert || 0));
      sfUpdData.push(Number(snapshot.sf_running_update || 0));
      sfBlockedData.push(Number(snapshot.sf_blocked || 0));

      opsTotalData.push(Number(snapshot.ops_per_sec || 0));
      opsReadData.push(Number(snapshot.app_read_ops_sec || 0));
      opsPlData.push(Number(snapshot.app_point_lookup_ops_sec || 0));
      opsRsData.push(Number(snapshot.app_range_scan_ops_sec || 0));
      opsWriteData.push(Number(snapshot.app_write_ops_sec || 0));
      opsInsData.push(Number(snapshot.app_insert_ops_sec || 0));
      opsUpdData.push(Number(snapshot.app_update_ops_sec || 0));

      const cpu = Number(snapshot.resources_host_cpu_percent ?? snapshot.resources_cpu_percent);
      const mem = Number(snapshot.resources_host_memory_mb ?? snapshot.resources_memory_mb);
      if (Number.isFinite(cpu) || Number.isFinite(mem)) {
        resourcesSeen = true;
      }
      cpuData.push(Number.isFinite(cpu) ? cpu : 0);
      memData.push(Number.isFinite(mem) ? mem : 0);
    }

    this.metrics.resources_available = resourcesSeen;

    const addWarmupAnnotation = (chart) => {
      if (!chart) return;
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
    };

    if (throughputChart) {
      throughputChart.data.labels = labels;
      throughputChart.data.datasets[0].data = throughputData;
      if (throughputChart.data.datasets[1]) {
        throughputChart.data.datasets[1].data = errorRateData;
      }
      addWarmupAnnotation(throughputChart);
      throughputChart.update();
    }

    if (concurrencyChart) {
      // Check if we have per-worker breakdown data
      const workerMetrics = this._workerMetrics;
      if (workerMetrics && workerMetrics.length > 1) {
        // Build per-worker datasets with proper time alignment
        const workerColors = [
          "rgb(59, 130, 246)",   // blue
          "rgb(245, 158, 11)",   // amber
          "rgb(168, 85, 247)",   // purple
          "rgb(236, 72, 153)",   // pink
          "rgb(34, 197, 94)",    // green
          "rgb(239, 68, 68)",    // red
          "rgb(6, 182, 212)",    // cyan
          "rgb(249, 115, 22)",   // orange
        ];

        // Build time-indexed data for each worker using ABSOLUTE timestamps
        // This is critical for multi-worker runs where workers start at different times.
        // We bucket by seconds since run_start to align with main metrics' elapsed_seconds.
        const workerDataByTime = {};  // run-relative seconds -> { workerId: active_connections }
        const workerTargetByTime = {}; // run-relative seconds -> { workerId: target_workers }
        
        // Parse the reference timestamp for converting absolute times to run-relative.
        // Use run_start_at which is the first data timestamp from worker_metrics.
        let referenceTime = null;
        if (this._workerMetricsRunStartAt) {
          referenceTime = new Date(this._workerMetricsRunStartAt).getTime();
        }
        
        for (const worker of workerMetrics) {
          const workerId = worker.worker_id || worker.key;
          for (const snap of (worker.snapshots || [])) {
            let bucket;
            if (referenceTime && snap.timestamp) {
              // Use absolute timestamp converted to run-relative seconds
              const snapTime = new Date(snap.timestamp).getTime();
              bucket = Math.round((snapTime - referenceTime) / 1000);
            } else {
              // Fallback to per-worker elapsed_seconds (may be misaligned for multi-worker)
              bucket = Math.round(Number(snap.elapsed_seconds || 0));
            }
            if (!workerDataByTime[bucket]) {
              workerDataByTime[bucket] = {};
              workerTargetByTime[bucket] = {};
            }
            workerDataByTime[bucket][workerId] = Number(snap.active_connections || 0);
            workerTargetByTime[bucket][workerId] = Number(snap.target_workers || 0);
          }
        }

        // Sort workers for consistent ordering
        const workerIds = workerMetrics.map(w => w.worker_id || w.key).sort();
        
        // Build labels and data arrays aligned with main snapshots
        const workerLabels = [];
        const totalInFlightData = [];
        const totalTargetData = [];
        const perWorkerData = {};  // workerId -> array of values
        
        for (const workerId of workerIds) {
          perWorkerData[workerId] = [];
        }

        for (const snapshot of snapshots) {
          if (!snapshot) continue;
          let secs = Number(snapshot.elapsed_seconds || 0);
          if (!showWarmup && warmupEnd != null) {
            secs = secs - warmupEnd;
          }
          const bucket = Math.round(Number(snapshot.elapsed_seconds || 0));
          const ts = `${this.formatSecondsTenths(secs)}s`;
          workerLabels.push(ts);

          // Sum active connections from all workers for this time bucket
          let totalInFlight = 0;
          let totalTarget = 0;
          const bucketData = workerDataByTime[bucket] || {};
          const bucketTargetData = workerTargetByTime[bucket] || {};
          
          for (const workerId of workerIds) {
            const val = bucketData[workerId] || 0;
            const targetVal = bucketTargetData[workerId] || 0;
            perWorkerData[workerId].push(val);
            totalInFlight += val;
            totalTarget += targetVal;
          }
          totalInFlightData.push(totalInFlight);
          totalTargetData.push(totalTarget);
        }

        // Build datasets: Target (dashed), Total In-flight, then per-worker breakdown
        const datasets = [
          {
            label: "Target workers (total)",
            data: totalTargetData,
            borderColor: "rgb(34, 197, 94)",
            backgroundColor: "transparent",
            tension: 0.1,
            borderDash: [4, 4],
            borderWidth: 2,
          },
          {
            label: "In-flight (total)",
            data: totalInFlightData,
            borderColor: "rgb(100, 116, 139)",  // slate
            backgroundColor: "transparent",
            tension: 0.4,
            borderWidth: 3,
          },
        ];

        // Add per-worker datasets
        workerIds.forEach((workerId, idx) => {
          const color = workerColors[idx % workerColors.length];
          // Create a display name: use group index if available
          const worker = workerMetrics.find(w => (w.worker_id || w.key) === workerId);
          const groupId = worker?.worker_group_id;
          const displayName = groupId != null ? `Worker ${groupId}` : workerId;
          datasets.push({
            label: displayName,
            data: perWorkerData[workerId],
            borderColor: color,
            backgroundColor: "transparent",
            tension: 0.4,
            borderWidth: 1.5,
          });
        });

        // Replace chart datasets
        concurrencyChart.data.labels = workerLabels;
        concurrencyChart.data.datasets = datasets;
      } else {
        // Single worker or no per-worker data - use aggregated data
        concurrencyChart.data.labels = labels;
        if (isPostgres) {
          if (concurrencyChart.data.datasets[0]) concurrencyChart.data.datasets[0].data = targetData;
          if (concurrencyChart.data.datasets[1]) concurrencyChart.data.datasets[1].data = inFlightData;
        } else {
          if (concurrencyChart.data.datasets[0]) concurrencyChart.data.datasets[0].data = targetData;
          if (concurrencyChart.data.datasets[1]) concurrencyChart.data.datasets[1].data = inFlightData;
          if (concurrencyChart.data.datasets[2]) concurrencyChart.data.datasets[2].data = sfQueuedData;
        }
      }
      addWarmupAnnotation(concurrencyChart);
      concurrencyChart.update();
    }

    if (latencyChart) {
      latencyChart.data.labels = labels;
      latencyChart.data.datasets[0].data = p50Data;
      latencyChart.data.datasets[1].data = p95Data;
      latencyChart.data.datasets[2].data = p99Data;
      addWarmupAnnotation(latencyChart);
      latencyChart.update();
    }

    if (sfRunningChart) {
      sfRunningChart.data.labels = labels;
      sfRunningChart.data.datasets[0].data = sfTotalData;
      sfRunningChart.data.datasets[1].data = sfReadData;
      sfRunningChart.data.datasets[2].data = sfPlData;
      sfRunningChart.data.datasets[3].data = sfRsData;
      sfRunningChart.data.datasets[4].data = sfWriteData;
      sfRunningChart.data.datasets[5].data = sfInsData;
      sfRunningChart.data.datasets[6].data = sfUpdData;
      sfRunningChart.data.datasets[7].data = sfBlockedData;
      
      // Check if breakdown data is available (any non-zero values in breakdown series)
      const hasBreakdownData = sfReadData.some(v => v > 0) || 
                               sfWriteData.some(v => v > 0) ||
                               sfPlData.some(v => v > 0) ||
                               sfRsData.some(v => v > 0) ||
                               sfInsData.some(v => v > 0) ||
                               sfUpdData.some(v => v > 0);
      this._sfRunningHasBreakdown = hasBreakdownData;
      
      this.applySfRunningBreakdownToChart({ skipUpdate: true });
      addWarmupAnnotation(sfRunningChart);
      sfRunningChart.update();
    }

    if (opsSecChart) {
      opsSecChart.data.labels = labels;
      opsSecChart.data.datasets[0].data = opsTotalData;
      opsSecChart.data.datasets[1].data = opsReadData;
      opsSecChart.data.datasets[2].data = opsPlData;
      opsSecChart.data.datasets[3].data = opsRsData;
      opsSecChart.data.datasets[4].data = opsWriteData;
      opsSecChart.data.datasets[5].data = opsInsData;
      opsSecChart.data.datasets[6].data = opsUpdData;
      
      // Check if breakdown data is available (any non-zero values in breakdown series)
      const hasOpsBreakdownData = opsReadData.some(v => v > 0) || 
                                  opsWriteData.some(v => v > 0) ||
                                  opsPlData.some(v => v > 0) ||
                                  opsRsData.some(v => v > 0) ||
                                  opsInsData.some(v => v > 0) ||
                                  opsUpdData.some(v => v > 0);
      this._opsSecHasBreakdown = hasOpsBreakdownData;
      
      this.applyOpsSecBreakdownToChart({ skipUpdate: true });
      addWarmupAnnotation(opsSecChart);
      opsSecChart.update();
    }

    if (resourcesCpuChart) {
      resourcesCpuChart.data.labels = labels;
      resourcesCpuChart.data.datasets[0].data = cpuData;
      resourcesCpuChart.update();
    }

    if (resourcesMemChart) {
      resourcesMemChart.data.labels = labels;
      resourcesMemChart.data.datasets[0].data = memData;
      resourcesMemChart.update();
    }

    if (resourcesHistoryChart) {
      resourcesHistoryChart.data.labels = labels;
      resourcesHistoryChart.data.datasets[0].data = cpuData;
      resourcesHistoryChart.data.datasets[1].data = memData;
      addWarmupAnnotation(resourcesHistoryChart);
      resourcesHistoryChart.update();
    }
  },
};
