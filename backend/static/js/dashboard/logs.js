/**
 * Dashboard Logs Module
 * Methods for loading and managing test logs.
 */
window.DashboardMixins = window.DashboardMixins || {};

window.DashboardMixins.logs = {
  initLogControls() {
    try {
      const params = new URLSearchParams(window.location.search || "");
      const verboseRaw = String(params.get("verbose") || "").toLowerCase();
      this.logVerboseMode = ["1", "true", "yes"].includes(verboseRaw);
    } catch (_) {
      this.logVerboseMode = false;
    }
  },

  logLevelOptions() {
    return [
      { value: "ALL", label: "ALL" },
      { value: "DEBUG", label: "DEBUG+" },
      { value: "INFO", label: "INFO+" },
      { value: "WARNING", label: "WARNING+" },
      { value: "ERROR", label: "ERROR" },
    ];
  },

  _getLogTargetById(targetId) {
    if (!targetId) return null;
    const targets = Array.isArray(this.logTargets) ? this.logTargets : [];
    const wanted = String(targetId);
    return (
      targets.find(
        (target) =>
          String(target.target_id || target.test_id || "") === wanted,
      ) || null
    );
  },

  _getDefaultLogTarget() {
    const targets = Array.isArray(this.logTargets) ? this.logTargets : [];
    if (!targets.length) return null;
    const preferred = targets.find(
      (target) => String(target.worker_id || "") === "ORCHESTRATOR",
    );
    return preferred || targets[0];
  },

  _applyLogTargetSelection(target) {
    if (!target) return;
    const targetId = String(target.target_id || target.test_id || "");
    const targetKind = String(target.kind || "");
    if (targetId === "all" || targetKind === "all") {
      this.logSelectedTestId = this.testId || null;
      this.logWorkerFilter = "";
      return;
    }
    const targetTestId = target.test_id ? String(target.test_id) : "";
    this.logSelectedTestId = targetTestId || null;
    const workerId = target.worker_id != null ? String(target.worker_id) : "";
    this.logWorkerFilter = workerId ? this._normalizeWorkerId(workerId) : "";
  },

  updateLogTargetsFromWorkers(workers) {
    if (this.mode !== "live") return;
    const list = Array.isArray(workers) ? workers : [];
    const workerMap = new Map();
    for (const worker of list) {
      if (!worker || typeof worker !== "object") continue;
      const workerId = worker.worker_id ?? worker.workerId ?? worker.id ?? "";
      if (!workerId) continue;
      const groupId = Number(
        worker.worker_group_id ?? worker.worker_group ?? worker.group_id ?? 0,
      );
      if (!workerMap.has(workerId)) {
        workerMap.set(workerId, {
          worker_id: String(workerId),
          worker_group_id: groupId,
        });
      }
    }

    const targets = [];
    for (const source of ["ORCHESTRATOR", "CONTROLLER", "UNKNOWN"]) {
      targets.push({
        target_id: `parent:${source}`,
        test_id: this.testId || null,
        worker_id: source,
        worker_group_id: 0,
        worker_group_count: 1,
        label: source,
        kind: "parent",
      });
    }
    targets.push({
      target_id: "all",
      test_id: this.testId || null,
      worker_id: null,
      worker_group_id: 0,
      worker_group_count: 1,
      label: "All",
      kind: "all",
    });

    const workerItems = Array.from(workerMap.values());
    workerItems.sort((a, b) => {
      const aGroup = Number(a.worker_group_id ?? 0);
      const bGroup = Number(b.worker_group_id ?? 0);
      if (aGroup !== bGroup) return aGroup - bGroup;
      return String(a.worker_id || "").localeCompare(String(b.worker_id || ""));
    });
    for (const worker of workerItems) {
      const workerId = String(worker.worker_id || "");
      if (!workerId) continue;
      targets.push({
        target_id: `worker:${workerId}`,
        test_id: this.testId || null,
        worker_id: workerId,
        worker_group_id: Number(worker.worker_group_id ?? 0),
        worker_group_count: 1,
        label: workerId,
        kind: "worker",
      });
    }

    this.logTargets = targets;
    if (!this.logSelectedTargetId && this.logTargets.length) {
      const fallback = this._getDefaultLogTarget();
      if (fallback) {
        const targetId = fallback.target_id || fallback.test_id || "";
        if (targetId) {
          this.logSelectedTargetId = String(targetId);
          this._applyLogTargetSelection(fallback);
        }
      }
    } else if (
      this.logSelectedTargetId &&
      !this._getLogTargetById(this.logSelectedTargetId) &&
      this.logTargets.length
    ) {
      const fallback = this._getDefaultLogTarget();
      if (fallback) {
        const targetId = fallback.target_id || fallback.test_id || "";
        if (targetId) {
          this.logSelectedTargetId = String(targetId);
          this._applyLogTargetSelection(fallback);
        }
      }
    }
  },

  async loadLogs() {
    if (!this.testId) return;
    if (this._destroyed) return;
    if (this.mode === "live") return;
    try {
      const params = new URLSearchParams({ limit: String(this.logMaxLines) });
      const selectedTarget = this._getLogTargetById(this.logSelectedTargetId);
      if (selectedTarget) {
        const targetId = selectedTarget.target_id || selectedTarget.test_id || "";
        if (targetId) {
          params.set("target_id", String(targetId));
        }
      }
      const selectedTestId = selectedTarget
        ? selectedTarget.test_id
        : this.logSelectedTestId;
      if (
        selectedTestId &&
        this.testId &&
        String(selectedTestId) !== String(this.testId)
      ) {
        params.set("child_test_id", String(selectedTestId));
      }
      const resp = await fetch(
        `/api/tests/${this.testId}/logs?${params.toString()}`,
      );
      if (!resp.ok) return;
      const data = await resp.json().catch(() => ({}));
      const targets =
        data && Array.isArray(data.targets)
          ? data.targets
          : data && Array.isArray(data.workers)
            ? data.workers
            : [];
      if (Array.isArray(targets)) {
        this.logTargets = targets;
      }
      if (!this.logSelectedTargetId && this.logTargets.length) {
        const fallback = this._getDefaultLogTarget();
        if (fallback) {
          const targetId = fallback.target_id || fallback.test_id || "";
          if (targetId) {
            this.logSelectedTargetId = String(targetId);
            this._applyLogTargetSelection(fallback);
          }
        }
      } else if (
        this.logSelectedTargetId &&
        !this._getLogTargetById(this.logSelectedTargetId) &&
        this.logTargets.length
      ) {
        const fallback = this._getDefaultLogTarget();
        if (fallback) {
          const targetId = fallback.target_id || fallback.test_id || "";
          if (targetId) {
            this.logSelectedTargetId = String(targetId);
            this._applyLogTargetSelection(fallback);
          }
        }
      }
      const logs = data && Array.isArray(data.logs) ? data.logs : [];
      this.appendLogs(logs);
    } catch (e) {
      console.error("Failed to load logs:", e);
    }
  },

  onLogTargetChange() {
    const target = this._getLogTargetById(this.logSelectedTargetId);
    if (this.mode === "live") {
      if (target) {
        this._applyLogTargetSelection(target);
      } else {
        this.logSelectedTestId = this.testId;
        this.logWorkerFilter = "";
      }
      return;
    }
    this.logs = [];
    this._logSeen = {};
    if (target) {
      this._applyLogTargetSelection(target);
    } else {
      this.logSelectedTestId = this.testId;
      this.logWorkerFilter = "";
    }
    this.loadLogs();
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
        timestamp_ms: item.timestamp ? Date.parse(item.timestamp) : NaN,
        test_id: item.test_id || item.testId || null,
        level: item.level || "INFO",
        logger: item.logger || null,
        message: item.message || "",
        exception: item.exception || null,
        worker_id: item.worker_id || item.workerId || null,
      });
    }

    this.logs.sort((a, b) => {
      const aTime = Number.isFinite(a.timestamp_ms) ? a.timestamp_ms : null;
      const bTime = Number.isFinite(b.timestamp_ms) ? b.timestamp_ms : null;
      if (aTime != null && bTime != null && aTime !== bTime) {
        return aTime - bTime;
      }
      if (aTime != null && bTime == null) return -1;
      if (aTime == null && bTime != null) return 1;
      return (a.seq || 0) - (b.seq || 0);
    });
    if (this.logs.length > this.logMaxLines) {
      const removeCount = this.logs.length - this.logMaxLines;
      const removed = this.logs.splice(0, removeCount);
      for (const r of removed) {
        if (r && r.log_id) delete this._logSeen[r.log_id];
      }
    }
  },

  startMultiNodeLogPolling() {
    if (this._logPollIntervalId) return;
    if (this._destroyed) return;
    if (this.mode === "live") return;
    const poll = () => {
      if (this._destroyed) {
        this.stopMultiNodeLogPolling();
        return;
      }
      this.loadLogs();
    };
    poll();
    this._logPollIntervalId = setInterval(poll, 1000);
  },

  stopMultiNodeLogPolling() {
    if (!this._logPollIntervalId) return;
    clearInterval(this._logPollIntervalId);
    this._logPollIntervalId = null;
  },

  _normalizeLogLevel(level) {
    const raw = String(level || "").toUpperCase();
    if (raw === "WARN") return "WARNING";
    if (raw === "ERR") return "ERROR";
    return raw || "INFO";
  },

  _logLevelWeight(level) {
    const normalized = this._normalizeLogLevel(level);
    const weights = {
      DEBUG: 10,
      INFO: 20,
      WARNING: 30,
      ERROR: 40,
      CRITICAL: 50,
    };
    return weights[normalized] || 20;
  },

  _normalizeWorkerId(value) {
    if (value == null) return "UNKNOWN";
    const cleaned = String(value).trim();
    return cleaned || "UNKNOWN";
  },

  logWorkerOptions() {
    const seen = new Set();
    const options = [];
    const logs = Array.isArray(this.logs) ? this.logs : [];
    for (const log of logs) {
      const worker = this._normalizeWorkerId(log.worker_id);
      if (!seen.has(worker)) {
        seen.add(worker);
      }
    }

    const priority = ["ORCHESTRATOR", "CONTROLLER", "UNKNOWN"];
    const ordered = [];
    for (const key of priority) {
      if (seen.has(key)) ordered.push(key);
    }
    const rest = Array.from(seen)
      .filter((k) => !priority.includes(k))
      .sort((a, b) => a.localeCompare(b));
    const values = ordered.concat(rest);

    options.push({ value: "", label: "All" });
    for (const value of values) {
      options.push({ value, label: value });
    }
    return options;
  },

  hasMultipleLogWorkers() {
    const options = this.logWorkerOptions();
    return Array.isArray(options) && options.length > 2;
  },

  setLogWorkerFilter(value) {
    this.logWorkerFilter = String(value || "");
  },

  clearLogWorkerFilter() {
    this.logWorkerFilter = "";
  },

  logWorkerBadgeClass(workerId) {
    const worker = this._normalizeWorkerId(workerId);
    if (worker === "ORCHESTRATOR") return "log-worker-badge log-worker-orchestrator";
    if (worker === "CONTROLLER") return "log-worker-badge log-worker-controller";
    if (worker === "UNKNOWN") return "log-worker-badge log-worker-unknown";
    return "log-worker-badge log-worker-default";
  },

  formatLogTimestamp(timestamp) {
    if (!timestamp) return "";
    try {
      return new Date(timestamp).toLocaleTimeString();
    } catch (_) {
      return String(timestamp || "");
    }
  },

  formatLogLogger(loggerName) {
    const logger = String(loggerName || "");
    if (!logger) return "";
    if (this.logVerboseMode) return logger;
    const parts = logger.split(".");
    return parts[parts.length - 1] || logger;
  },

  logLineLevelClass(level) {
    const normalized = this._normalizeLogLevel(level);
    return `log-level-${normalized.toLowerCase()}`;
  },

  filteredLogs() {
    const logs = Array.isArray(this.logs) ? this.logs : [];
    const levelFilter = this._normalizeLogLevel(this.logLevelFilter || "INFO");
    const minWeight =
      levelFilter === "ALL" ? 0 : this._logLevelWeight(levelFilter);
    const workerFilter = String(this.logWorkerFilter || "");
    return logs.filter((log) => {
      if (levelFilter !== "ALL") {
        if (this._logLevelWeight(log.level) < minWeight) return false;
      }
      if (workerFilter) {
        const worker = this._normalizeWorkerId(log.worker_id);
        if (worker !== workerFilter) return false;
      }
      return true;
    });
  },

  logsText() {
    const logs = this.filteredLogs();
    if (!logs || logs.length === 0) return "";
    return logs
      .map((l) => {
        const ts = l.timestamp ? new Date(l.timestamp).toLocaleTimeString() : "";
        const lvl = String(l.level || "").toUpperCase();
        const logger = l.logger ? ` ${this.formatLogLogger(l.logger)}` : "";
        const worker = this._normalizeWorkerId(l.worker_id);
        const workerLabel = worker ? ` [${worker}]` : "";
        const msg = l.message || "";
        const exc = l.exception ? `\n${l.exception}` : "";
        return `${ts} ${lvl}${workerLabel}${logger} - ${msg}${exc}`;
      })
      .join("\n");
  },
};
