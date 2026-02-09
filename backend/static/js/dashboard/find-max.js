/**
 * Dashboard Find-Max Module
 * Methods for the FIND_MAX_CONCURRENCY load mode.
 */
window.DashboardMixins = window.DashboardMixins || {};

window.DashboardMixins.findMax = {
  isFindMaxMode() {
    const loadMode = String(this.templateInfo?.load_mode || "").toUpperCase();
    if (loadMode === "FIND_MAX_CONCURRENCY") return true;
    const s = this.findMaxState();
    return !!(s && String(s.mode || "").toUpperCase() === "FIND_MAX_CONCURRENCY");
  },

  isQPSMode() {
    const loadMode = String(this.templateInfo?.load_mode || "").toUpperCase();
    if (loadMode === "QPS") return true;
    const s = this.qpsState();
    return !!(s && String(s.mode || "").toUpperCase() === "QPS");
  },

  hasControllerMode() {
    return this.isFindMaxMode() || this.isQPSMode();
  },

  qpsState() {
    if (this.qpsController && typeof this.qpsController === "object") {
      return this.qpsController;
    }
    return null;
  },

  findMaxState() {
    if (this.findMaxController && typeof this.findMaxController === "object") {
      return this.findMaxController;
    }
    const raw = this.templateInfo?.find_max_result;
    if (raw && typeof raw === "object") return raw;
    return null;
  },

  // QPS Controller methods
  qpsTargetQps() {
    const s = this.qpsState();
    if (!s) return null;
    const v = Number(s.target_qps);
    return Number.isFinite(v) && v > 0 ? v : null;
  },

  qpsCurrentQps() {
    const s = this.qpsState();
    if (!s) return null;
    const v = Number(s.current_qps);
    return Number.isFinite(v) ? v : null;
  },

  qpsCurrentThreads() {
    const s = this.qpsState();
    if (!s) return null;
    const v = Number(s.current_threads);
    return Number.isFinite(v) ? Math.trunc(v) : null;
  },

  qpsMinThreads() {
    const s = this.qpsState();
    if (!s) return null;
    const v = Number(s.min_threads);
    return Number.isFinite(v) ? Math.trunc(v) : null;
  },

  qpsMaxThreads() {
    const s = this.qpsState();
    if (!s) return null;
    const v = Number(s.max_threads);
    return Number.isFinite(v) ? Math.trunc(v) : null;
  },

  // Calculate QPS error percentage using the same QPS source as the KPI card
  // This ensures consistency between the KPI and controller displays
  qpsErrorPct() {
    const target = this.qpsTargetQps();
    if (!target || target <= 0) return null;
    
    // Use the same QPS source as qpsTargetPct() in display.js
    const current = this.mode === 'history'
      ? (this.templateInfo?.qps || 0)
      : (this.metrics?.qps_avg_30s || this.metrics?.ops_per_sec || 0);
    
    if (current <= 0) return null;
    
    // Formula: (target - current) / target * 100
    // Positive = below target, Negative = above target
    return ((target - current) / target) * 100;
  },

  // Same as qpsErrorPct but with sign flipped to match KPI card convention
  // Positive = above target (good), Negative = below target (bad)
  qpsVsTargetPct() {
    const target = this.qpsTargetQps();
    if (!target || target <= 0) return null;
    
    const current = this.mode === 'history'
      ? (this.templateInfo?.qps || 0)
      : (this.metrics?.qps_avg_30s || this.metrics?.ops_per_sec || 0);
    
    if (current <= 0) return null;
    
    // Formula: (current - target) / target * 100
    // Positive = above target, Negative = below target (matches KPI card)
    return ((current - target) / target) * 100;
  },

  qpsLastScalingStepNumber() {
    const s = this.qpsState();
    if (!s) return 0;
    const v = Number(s.last_scaling_step);
    return Number.isFinite(v) ? Math.trunc(v) : 0;
  },

  qpsStepHistory() {
    const s = this.qpsState();
    if (!s) return [];
    const hist = s.step_history;
    return Array.isArray(hist) ? hist : [];
  },

  // Get the most recent step object from history
  qpsLastStepObject() {
    const hist = this.qpsStepHistory();
    if (!hist || hist.length === 0) return null;
    return hist[hist.length - 1];
  },

  // Human-readable controller status
  qpsControllerStatus() {
    const s = this.qpsState();
    if (!s) return { label: "Initializing", icon: "⏳", class: "text-gray-600" };

    const state = String(s.state || "").toUpperCase();
    const errorPct = this.qpsErrorPct();
    const currentThreads = this.qpsCurrentThreads();
    const minThreads = this.qpsMinThreads();
    const maxThreads = this.qpsMaxThreads();

    // Check if at thread limits
    const atMin = currentThreads != null && minThreads != null && currentThreads <= minThreads;
    const atMax = currentThreads != null && maxThreads != null && currentThreads >= maxThreads;

    if (state === "SCALING_UP") {
      return { label: "Scaling Up", icon: "↑", class: "text-blue-600" };
    }
    if (state === "SCALING_DOWN") {
      return { label: "Scaling Down", icon: "↓", class: "text-orange-600" };
    }

    // Steady state analysis
    // errorPct = (target - current) / target * 100
    // errorPct > 0: current < target → QPS is BELOW target (underperforming)
    // errorPct < 0: current > target → QPS is ABOVE target (overperforming)
    if (errorPct != null) {
      const absError = Math.abs(errorPct);
      if (absError < 5) {
        return { label: "On Target", icon: "✓", class: "text-green-700" };
      }
      // Negative error = QPS above target, but at max threads (can't scale down further)
      if (errorPct < -5 && atMax) {
        return { label: "At Max Threads", icon: "⚠", class: "text-amber-600" };
      }
      // Positive error = QPS below target, but at min threads (can't scale up further)
      if (errorPct > 5 && atMin) {
        return { label: "At Min Threads", icon: "⚠", class: "text-amber-600" };
      }
      // Positive error = QPS below target (underperforming, need more threads)
      if (errorPct > 5) {
        return { label: "Below Target", icon: "↓", class: "text-orange-600" };
      }
      // Negative error = QPS above target (overperforming, could scale down)
      if (errorPct < -5) {
        return { label: "Above Target", icon: "↑", class: "text-blue-600" };
      }
    }

    return { label: "Monitoring", icon: "◉", class: "text-gray-600" };
  },

  // Format time ago for last adjustment
  qpsLastAdjustmentTimeAgo() {
    const step = this.qpsLastStepObject();
    if (!step || !step.timestamp) return null;

    const ts = new Date(step.timestamp).getTime();
    if (!Number.isFinite(ts)) return null;

    const diffMs = Date.now() - ts;
    const diffSec = Math.floor(diffMs / 1000);

    if (diffSec < 60) return `${diffSec}s ago`;
    if (diffSec < 3600) return `${Math.floor(diffSec / 60)}m ago`;
    if (diffSec < 86400) return `${Math.floor(diffSec / 3600)}h ago`;
    return `${Math.floor(diffSec / 86400)}d ago`;
  },

  // Convert internal reason codes to human-readable text
  formatControllerReason(reason) {
    if (!reason) return '—';
    const reasonMap = {
      'under_target': 'QPS below target',
      'over_target': 'QPS above target',
      'no_threads_running': 'No threads running',
      'at_max_threads': 'At max threads',
      'at_min_threads': 'At min threads',
      'within_deadband': 'Within tolerance',
      'cooldown': 'Cooling down',
      'warmup': 'Warming up',
      'initializing': 'Initializing',
      'sustained_under': 'Sustained underperformance',
      'sustained_over': 'Sustained overperformance',
    };
    return reasonMap[reason] || reason.replace(/_/g, ' ');
  },

  // Format a timestamp as "Xs ago", "Xm ago", etc.
  formatTimeAgo(timestamp) {
    if (!timestamp) return '—';
    const ts = new Date(timestamp).getTime();
    if (!Number.isFinite(ts)) return '—';
    
    const diffMs = Date.now() - ts;
    const diffSec = Math.floor(diffMs / 1000);
    
    if (diffSec < 0) return 'now';
    if (diffSec < 60) return `${diffSec}s ago`;
    if (diffSec < 3600) return `${Math.floor(diffSec / 60)}m ago`;
    if (diffSec < 86400) return `${Math.floor(diffSec / 3600)}h ago`;
    return `${Math.floor(diffSec / 86400)}d ago`;
  },

  // Format a timestamp as readable date/time (for history mode)
  formatTimestamp(timestamp) {
    if (!timestamp) return '—';
    const d = new Date(timestamp);
    if (isNaN(d.getTime())) return '—';
    
    // Format as "Jan 15, 10:30:45"
    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const month = months[d.getMonth()];
    const day = d.getDate();
    const hours = d.getHours().toString().padStart(2, '0');
    const mins = d.getMinutes().toString().padStart(2, '0');
    const secs = d.getSeconds().toString().padStart(2, '0');
    
    return `${month} ${day}, ${hours}:${mins}:${secs}`;
  },

  qpsEvaluationIntervalSeconds() {
    const s = this.qpsState();
    if (!s) return 10;
    const v = Number(s.evaluation_interval_seconds);
    return Number.isFinite(v) && v > 0 ? v : 10;
  },

  _serverNowMs() {
    const offsetMs = Number(this._serverClockOffsetMs || 0);
    if (!Number.isFinite(offsetMs)) return Date.now();
    return Date.now() + offsetMs;
  },

  _clearQpsCountdown() {
    if (this._qpsControllerCountdownIntervalId) {
      clearInterval(this._qpsControllerCountdownIntervalId);
      this._qpsControllerCountdownIntervalId = null;
    }
    this._qpsControllerCountdownTargetEpochMs = null;
    this.qpsControllerCountdownSeconds = null;
  },

  syncQpsCountdown() {
    if (this.mode !== "live") {
      this._clearQpsCountdown();
      return;
    }
    if (!this.isQPSMode()) {
      this._clearQpsCountdown();
      return;
    }
    const s = this.qpsState();
    if (!s) {
      this._clearQpsCountdown();
      return;
    }

    const statusUpper = (this.status || "").toString().toUpperCase();
    const phaseUpper = (this.phase || "").toString().toUpperCase();
    const terminal =
      phaseUpper === "COMPLETED" || ["STOPPED", "FAILED", "CANCELLED"].includes(statusUpper);
    if (terminal) {
      this._clearQpsCountdown();
      return;
    }

    const endEpoch = Number(s.next_evaluation_epoch_ms);
    if (!Number.isFinite(endEpoch) || endEpoch <= 0) {
      this._clearQpsCountdown();
      return;
    }

    const target = Math.trunc(endEpoch);
    if (this._qpsControllerCountdownTargetEpochMs === target && this._qpsControllerCountdownIntervalId) {
      return;
    }

    this._clearQpsCountdown();
    this._qpsControllerCountdownTargetEpochMs = target;

    const tick = () => {
      const remaining = Math.max(0, Math.ceil((target - this._serverNowMs()) / 1000));
      this.qpsControllerCountdownSeconds = remaining;
      if (remaining <= 0 && this._qpsControllerCountdownIntervalId) {
        clearInterval(this._qpsControllerCountdownIntervalId);
        this._qpsControllerCountdownIntervalId = null;
      }
    };

    tick();
    this._qpsControllerCountdownIntervalId = setInterval(tick, 250);
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
    if (Number.isFinite(v)) return Math.trunc(v);
    const runActive = Number(this.runInfo?.workers_active);
    if (Number.isFinite(runActive)) return Math.trunc(runActive);
    if (Array.isArray(this.liveWorkers) && this.liveWorkers.length > 0) {
      return Math.trunc(this.liveWorkers.length);
    }
    return null;
  },

  findMaxNextWorkers() {
    const s = this.findMaxState();
    if (!s) return null;
    // When terminal, return null - no "next" workers to show
    if (this._findMaxIsTerminal(s)) {
      return null;
    }
    const v = Number(s.next_planned_concurrency);
    if (Number.isFinite(v)) return Math.trunc(v);
    const current = Number(s.current_concurrency);
    const inc = Number(s.concurrency_increment);
    const max = Number(s.max_concurrency);
    if (!Number.isFinite(current) || !Number.isFinite(inc) || inc <= 0) return null;
    const next = Number.isFinite(max) && max > 0
      ? Math.min(max, current + inc)
      : (current + inc);
    return Math.trunc(next);
  },

  // Check if we're in a transitioning state between steps
  findMaxIsTransitioning() {
    const s = this.findMaxState();
    if (!s) return false;
    const status = String(s.status || "").toUpperCase();
    return status === "TRANSITIONING";
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
    // Debug logging for countdown calculation
    const nowMs = this._serverNowMs();
    const remainingSec = (endEpoch - nowMs) / 1000;
    if (this.debug) {
      console.log('[FM Countdown] step_end_at_epoch_ms:', endEpoch, 'now:', nowMs, 'remaining:', remainingSec.toFixed(1) + 's', 'step:', s.current_step, 'target:', s.target_workers);
    }
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
      const remaining = Math.max(0, Math.ceil((target - this._serverNowMs()) / 1000));
      this.findMaxCountdownSeconds = remaining;
      if (remaining <= 0 && this._findMaxCountdownIntervalId) {
        clearInterval(this._findMaxCountdownIntervalId);
        this._findMaxCountdownIntervalId = null;
      }
    };

    tick();
    this._findMaxCountdownIntervalId = setInterval(tick, 250);
  },
};
