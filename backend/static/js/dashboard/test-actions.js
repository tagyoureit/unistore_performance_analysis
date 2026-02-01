/**
 * Dashboard Test Actions Module
 * Methods for starting, stopping, and managing test runs.
 * 
 * Debug logging is controlled by ?debug=true URL parameter.
 * Logs are prefixed with [TEST] for easy filtering.
 */
const root = typeof window === "undefined" ? globalThis : window;
root.DashboardMixins = root.DashboardMixins || {};

root.DashboardMixins.testActions = {
  async startTest() {
    if (!this.testId) return;
    try {
      // Use autoscale endpoint for AUTO and BOUNDED modes (multi-worker capable)
      const scalingMode = String(this.templateInfo?.scaling?.mode || "AUTO").toUpperCase();
      const useAutoscaleEndpoint = scalingMode !== "FIXED";
      const endpoint = useAutoscaleEndpoint
        ? `/api/tests/${this.testId}/start-autoscale`
        : `/api/tests/${this.testId}/start`;
      
      this._debugLog("TEST", "START_REQUESTED", { testId: this.testId, scalingMode, endpoint });
      
      // Optimistic UI update BEFORE API call - gives immediate feedback
      if (this.mode === "live") {
        this.phase = "PREPARING";
        this.status = "RUNNING";
        // Reset phase tracking for new test
        this._warmupStartElapsed = 0;
        this._warmupStartTime = null;
        this._runningStartElapsed = 0;
        this._runningStartTime = null;
        this.startElapsedTimer(0);
        this._debugLog("TEST", "TIMER_STARTED_OPTIMISTICALLY", { elapsed: 0 });
      }
      
      const resp = await fetch(endpoint, { method: "POST" });
      if (!resp.ok) {
        // Revert optimistic update on error
        this._debugLog("TEST", "START_FAILED", { status: resp.status });
        if (this.mode === "live") {
          this.phase = "";
          this.status = "PREPARED";
          this.stopElapsedTimer();
          this.elapsed = 0;
        }
        const err = await resp.json().catch(() => ({}));
        const detail = err && err.detail ? err.detail : null;
        throw new Error(
          (detail && (detail.message || detail.detail || detail)) ||
            "Failed to start test",
        );
      }
      // Kick a refresh; status/details will update as soon as execution starts.
      this._debugLog("TEST", "START_SUCCESS", { testId: this.testId });
      await this.loadTestInfo();
    } catch (e) {
      console.error("Failed to start test:", e);
      window.toast.error(`Failed to start test: ${e.message || e}`);
    }
  },

  async stopTest() {
    if (!this.testId) return;
    try {
      this._debugLog("TEST", "STOP_REQUESTED", { testId: this.testId });
      const resp = await fetch(`/api/tests/${this.testId}/stop`, {
        method: "POST",
      });
      if (!resp.ok) {
        const err = await resp.json().catch(() => ({}));
        const detail = err && err.detail ? err.detail : null;
        throw new Error(
          (detail && (detail.message || detail.detail || detail)) ||
            "Failed to stop test",
        );
      }
      // Optimistically reflect cancellation immediately; backend will finalize shortly.
      try {
        const data = await resp.json().catch(() => ({}));
        if (data && data.status) {
          this.status = data.status;
        }
      } catch (_) {}
      await this.loadTestInfo();
    } catch (e) {
      console.error("Failed to stop test:", e);
      window.toast.error(`Failed to stop test: ${e.message || e}`);
    }
  },

  openRunAnalysis() {
    if (!this.testId) return;
    window.location.href = `/dashboard/history/${this.testId}`;
  },

  async rerunTest(testId) {
    const id = testId ? String(testId) : "";
    if (!id) return;
    const confirmed = await window.toast.confirm(
      "Re-run this test with the same configuration?",
      { confirmText: "Re-run", confirmVariant: "primary", timeoutMs: 10_000 }
    );
    if (!confirmed) return;

    try {
      const response = await fetch(`/api/tests/${id}/rerun`, {
        method: "POST",
      });
      const data = await response.json();
      window.location.href = `/dashboard/${data.new_test_id}`;
    } catch (error) {
      console.error("Failed to rerun test:", error);
      window.toast.error("Failed to rerun test");
    }
  },
};
