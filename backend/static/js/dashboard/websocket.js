/**
 * Dashboard WebSocket Module
 * Methods for WebSocket connection management.
 * 
 * Debug logging is controlled by ?debug=true URL parameter.
 * Logs are prefixed with [WS] for easy filtering.
 */
window.DashboardMixins = window.DashboardMixins || {};

window.DashboardMixins.websocket = {
  _shownCancellationToast: false,

  connectWebSocket() {
    if (!this.testId) return;
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const wsUrl = `${protocol}//${window.location.host}/ws/test/${this.testId}`;
    this._debugLog("WS", "CONNECTING", { url: wsUrl });
    this.websocket = new WebSocket(wsUrl);

    this.websocket.onopen = () => {
      this._debugLog("WS", "CONNECTED");
      this.testRunning = true;
      if (typeof this.stopMultiNodeLogPolling === "function") {
        this.stopMultiNodeLogPolling();
      }
    };

    this.websocket.onmessage = (event) => {
      let data = null;
      try {
        data = JSON.parse(event.data);
      } catch (e) {
        console.error("[WS] Message parse failed:", e, event.data);
        return;
      }
      
      // Ignore the initial connected message
      if (data && data.status === "connected") {
        this._debugLog("WS", "INITIAL_CONNECTED", { test_id: data.test_id });
        return;
      }
      
      // Handle log messages
      if (data && data.kind === "log") {
        this.appendLogs(data);
        return;
      }
      if (data && data.kind === "log_batch") {
        const logs = data.logs && Array.isArray(data.logs) ? data.logs : [];
        this.appendLogs(logs);
        return;
      }
      
      // Extract key fields for logging
      const wsPayload = data?.data || data;
      const wsStatus = wsPayload?.status;
      const wsPhase = wsPayload?.phase;
      const wsElapsed = wsPayload?.timing?.elapsed_display_seconds ?? wsPayload?.elapsed;
      const wsCancellationReason = wsPayload?.cancellation_reason || wsPayload?.reason;
      
      // Track previous values for change detection
      const prevWsStatus = window._lastWsStatus;
      const prevWsPhase = window._lastWsPhase;
      
      // Log on significant changes
      const statusChanged = wsStatus !== prevWsStatus;
      const phaseChanged = wsPhase !== prevWsPhase;
      
      if (statusChanged || phaseChanged) {
        this._debugLog("WS", "STATE_CHANGE", {
          status: { prev: prevWsStatus, new: wsStatus },
          phase: { prev: prevWsPhase, new: wsPhase },
          elapsed: wsElapsed,
          cancellationReason: wsCancellationReason,
        });
        window._lastWsStatus = wsStatus;
        window._lastWsPhase = wsPhase;
      }
      
      // Show toast for test cancellation/failure
      if (statusChanged && !this._shownCancellationToast) {
        const statusUpper = (wsStatus || "").toString().toUpperCase();
        if (statusUpper === "FAILED" || statusUpper === "CANCELLED") {
          this._shownCancellationToast = true;
          const reason = wsCancellationReason || (statusUpper === "FAILED" ? "Test failed" : "Test cancelled");
          if (typeof window.toast !== "undefined" && typeof window.toast.error === "function") {
            window.toast.error(`Test ${statusUpper.toLowerCase()}: ${reason}`);
          }
        }
      }
      
      // Handle logs embedded in RUN_UPDATE payload
      const wsLogs = wsPayload?.logs;
      if (wsLogs && Array.isArray(wsLogs) && wsLogs.length > 0) {
        if (typeof this.appendLogs === "function") {
          this.appendLogs(wsLogs);
        }
      }
      
      // Apply the payload
      this.applyMetricsPayload(data);
    };

    this.websocket.onerror = (error) => {
      console.error("[WS] Error:", error);
    };

    this.websocket.onclose = (event) => {
      this._debugLog("WS", "CLOSED", { code: event.code, reason: event.reason });
      this.testRunning = false;
      
      // Fallback: If we didn't receive PHASE=COMPLETED before socket closed,
      // do a final HTTP poll to ensure we have the latest state
      const currentPhase = (this.phase || "").toString().toUpperCase();
      const currentStatus = (this.status || "").toString().toUpperCase();
      
      if (currentStatus === "COMPLETED" && currentPhase !== "COMPLETED") {
        this._debugLog("WS", "FALLBACK_POLL_NEEDED", { status: currentStatus, phase: currentPhase });
        // Delay slightly to allow any in-flight updates to settle
        setTimeout(() => {
          if (typeof this.fetchTestInfo === "function") {
            this._debugLog("WS", "FALLBACK_POLL_EXECUTING");
            this.fetchTestInfo();
          }
        }, 500);
      }
    };
  },

  disconnectWebSocket() {
    if (this.websocket) {
      this._debugLog("WS", "DISCONNECTING");
      this.websocket.close();
      this.websocket = null;
    }
  },
};
