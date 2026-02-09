/**
 * Dashboard State Module
 * Initial reactive state for the Alpine.js dashboard component.
 */
window.DashboardMixins = window.DashboardMixins || {};

window.DashboardMixins.state = function (opts) {
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
    runInfo: null,
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
      // Postgres query stats (from pg_stat_activity polling)
      pg_bench_available: false,
      pg_running_bench: 0,
      pg_waiting_bench: 0,
      pg_running_tagged_bench: 0,
      pg_running_other_bench: 0,
      pg_running_read_bench: 0,
      pg_running_write_bench: 0,
      pg_running_point_lookup_bench: 0,
      pg_running_range_scan_bench: 0,
      pg_running_insert_bench: 0,
      pg_running_update_bench: 0,
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
      resources_available: false,
      cpu_percent: 0,
      memory_mb: 0,
      host_cpu_percent: null,
      host_cpu_cores: null,
      host_memory_mb: null,
      host_memory_total_mb: null,
      host_memory_available_mb: null,
      host_memory_percent: null,
      cgroup_cpu_percent: null,
      cgroup_cpu_quota_cores: null,
      cgroup_memory_mb: null,
      cgroup_memory_limit_mb: null,
      cgroup_memory_percent: null,
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
    pgRunningBreakdown: "read_write", // 'read_write' | 'by_kind' (Postgres)
    opsSecBreakdown: "read_write", // 'read_write' | 'by_kind'
    _sfRunningHasBreakdown: true, // Set to false when breakdown data not available
    _pgRunningHasBreakdown: true, // Set to false when breakdown data not available (Postgres)
    _opsSecHasBreakdown: true, // Set to false when breakdown data not available
    latencyView: "end_to_end", // 'end_to_end' | 'sf_execution'
    latencyViewUserSelected: false,
    latencyAggregationMethod: null,
    didRefreshAfterComplete: false,
    // NOTE: Do not store Chart.js instances on Alpine reactive state.
    // Chart objects have circular refs/getters; Alpine Proxy wrapping can cause
    // recursion and Chart.js internal corruption.
    websocket: null,
    logs: [],
    _logSeen: {},
    logMaxLines: 1000,
    logLevelFilter: "INFO",
    logVerboseMode: false,
    logWorkerFilter: "",
    logTargets: [],
    logTargetsLoading: false,
    logSelectedTargetIds: [],
    logSelectedTestId: null,
    _logPollIntervalId: null,
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
    showWarmup: false, // Toggle for including warmup phase in charts
    metricsWarmupEndElapsed: null, // Global warmup end from /metrics API (for all charts)
    _metricsSnapshots: [], // Raw metrics snapshots for re-rendering with warmup toggle
    _metricsSmoothingApplied: false, // Whether multi-worker smoothing was applied
    warehouseTsWarmupEndElapsed: null, // Seconds where warmup ends (for annotation)
    overheadTs: [],
    overheadTsLoading: false,
    overheadTsError: null,
    overheadTsAvailable: false,
    overheadTsLoaded: false,
    overheadTsWarmupEndElapsed: null, // Seconds where warmup ends (for annotation)
    warehouseDetails: null,
    warehouseDetailsLoading: false,
    warehouseDetailsError: null,
    errorSummaryRows: [],
    errorSummaryLoading: false,
    errorSummaryLoaded: false,
    errorSummaryError: null,
    errorSummaryHierarchy: null,  // { by_level: {}, by_query_type: {} }
    errorSummarySelectedLevel: null,  // null = show level 1 (KPIs), 'ERROR'/'WARNING' = show level 2
    errorSummarySelectedQueryType: null,  // null = show query type KPIs, 'INSERT'/'SELECT'/etc = show detail table
    errorSummarySelectedRow: null,  // Selected row for Level 4 drill-down
    errorDetailRows: [],  // Individual error occurrences for Level 4
    errorDetailLoading: false,
    errorDetailError: null,
    workerMetricsLoading: false,
    workerMetricsError: null,
    workerMetricsAvailable: false,
    workerMetricsWorkers: [],
    workerMetricsExpanded: {},
    liveWorkers: [],
    liveWorkerMetricsLoadedAt: null,
    _liveWorkerMetricsPromise: null,
    _elapsedIntervalId: null,
    _elapsedStartTime: null,
    _elapsedBaseValue: 0,
    _warmupStartElapsed: 0,
    _runningStartElapsed: 0,
    _multiWorkerMetricsIntervalId: null,
    aiAnalysisModal: false,
    aiAnalysisLoading: false,
    aiAnalysisError: null,
    aiAnalysis: null,
    chatMessage: "",
    chatHistory: [],
    chatLoading: false,
    clusterBreakdownExpanded: false,
    stepHistoryExpanded: false,
    selectedStepHistoryWorker: "aggregate",
    resourcesHistoryExpanded: false,
    findMaxController: null,
    findMaxCountdownSeconds: null,
    _findMaxCountdownIntervalId: null,
    _findMaxCountdownTargetEpochMs: null,
    _serverClockOffsetMs: 0,
    // QPS Controller state (for QPS load mode)
    qpsController: null,
    qpsControllerCountdownSeconds: null,
    _qpsControllerCountdownIntervalId: null,
    _qpsControllerCountdownTargetEpochMs: null,
    enrichmentRetrying: false,
    enrichmentProgress: null,
    _processingLogIntervalId: null,
    _processingLogStartMs: null,
    // Button loading states
    stoppingTest: false,
    rerunningTestId: null,
    // Worker saturation detection
    _saturationSamples: [],        // Recent samples of {target, inflight} for detection
    _saturationWarningShown: false, // Only show warning once per test
    // Floating toolbar visibility state
    floatingToolbarVisible: false,
    chartsInView: false,
    latencyInView: false,
    _floatingToolbarObserver: null,
  };
};
