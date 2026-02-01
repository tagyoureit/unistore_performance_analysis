/**
 * Dashboard Workers Module
 * Methods for handling worker metrics and display.
 */
window.DashboardMixins = window.DashboardMixins || {};

window.DashboardMixins.workers = {
  workerKey(worker) {
    if (!worker || typeof worker !== "object") return null;
    const workerId =
      worker.worker_id ?? worker.workerId ?? worker.id ?? worker.worker ?? "";
    const groupId =
      worker.worker_group_id ?? worker.worker_group ?? worker.group_id ?? 0;
    return `${workerId || "worker"}:${Number(groupId || 0)}`;
  },

  workerLabel(worker) {
    if (!worker || typeof worker !== "object") return "Worker";
    const workerId = worker.worker_id ?? worker.workerId ?? worker.id ?? "";
    if (workerId) return String(workerId);
    const groupId =
      worker.worker_group_id ?? worker.worker_group ?? worker.group_id ?? "";
    if (groupId !== "") return `Worker ${groupId}`;
    return "Worker";
  },

  workerMetric(worker, key) {
    if (!worker || typeof worker !== "object") return null;
    const metrics =
      worker.metrics && typeof worker.metrics === "object"
        ? worker.metrics
        : worker;
    return metrics[key] != null ? metrics[key] : null;
  },

  workerHealthColor(worker) {
    const health = String(worker?.health || "").toUpperCase();
    if (health === "HEALTHY") return "#16a34a";
    if (health === "STALE") return "#f59e0b";
    if (health === "DEAD") return "#ef4444";
    return "#9ca3af";
  },

  formatHeartbeatAge(worker) {
    const raw = worker?.last_heartbeat_ago_s ?? worker?.lastHeartbeatAgoSeconds;
    const n = typeof raw === "number" ? raw : Number(raw);
    if (!Number.isFinite(n)) return "n/a";
    const value = Math.max(0, Math.round(n));
    return `${value}s`;
  },

  updateLiveWorkers(workers) {
    const list = Array.isArray(workers) ? workers : [];
    const normalized = list
      .map((worker) => {
        if (!worker || typeof worker !== "object") return null;
        const key = worker.key || this.workerKey(worker);
        return key ? { ...worker, key } : null;
      })
      .filter((worker) => worker);
    normalized.sort((a, b) => {
      const aGroup = Number(a?.worker_group_id ?? 0);
      const bGroup = Number(b?.worker_group_id ?? 0);
      if (aGroup !== bGroup) return aGroup - bGroup;
      return this.workerLabel(a).localeCompare(this.workerLabel(b));
    });
    this.liveWorkers = normalized;
    if (typeof this.updateLogTargetsFromWorkers === "function") {
      this.updateLogTargetsFromWorkers(normalized);
    }
  },
};
