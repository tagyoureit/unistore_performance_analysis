/**
 * Test History page Alpine component.
 *
 * Manages the test results list, filtering, sorting, and pagination.
 * This function must be globally available for HTMX-swapped content to work.
 */
function testHistory() {
  const MAX_COMPARE = 2;

  const normalizeTestForCompare = (data) => {
    const t = data && typeof data === "object" ? data : {};

    const testId = t.test_id != null ? String(t.test_id) : "";
    const testName = t.test_name != null ? String(t.test_name) : "";
    const tableType = t.table_type != null ? String(t.table_type) : "";
    const warehouseSize = t.warehouse_size != null ? String(t.warehouse_size) : "";

    // Prefer list/search payload fields; fall back to /api/tests/{id} fields.
    const opsPerSec =
      t.ops_per_sec != null
        ? Number(t.ops_per_sec)
        : t.qps != null
          ? Number(t.qps)
          : 0;
    const p50Latency =
      t.p50_latency != null
        ? Number(t.p50_latency)
        : t.p50_latency_ms != null
          ? Number(t.p50_latency_ms)
          : 0;
    const p95Latency =
      t.p95_latency != null
        ? Number(t.p95_latency)
        : t.p95_latency_ms != null
          ? Number(t.p95_latency_ms)
          : 0;
    const p99Latency =
      t.p99_latency != null
        ? Number(t.p99_latency)
        : t.p99_latency_ms != null
          ? Number(t.p99_latency_ms)
          : 0;

    const status = t.status != null ? String(t.status) : "";

    // In list/search we already return percent; in /api/tests/{id} compute from totals.
    let errorRate = 0;
    if (t.error_rate != null) {
      errorRate = Number(t.error_rate);
    } else if (t.failed_operations != null || t.total_operations != null) {
      const failed = Number(t.failed_operations || 0);
      const total = Number(t.total_operations || 0);
      errorRate = total > 0 ? (failed / total) * 100.0 : 0;
    }

    const concurrentConnections =
      t.concurrent_connections != null ? Number(t.concurrent_connections) : 0;
    const duration =
      t.duration != null
        ? Number(t.duration)
        : t.duration_seconds != null
          ? Number(t.duration_seconds)
          : 0;

    // Prefer created_at (history/search); fall back to start_time (details endpoint).
    const createdAt =
      t.created_at != null
        ? String(t.created_at)
        : t.start_time != null
          ? String(t.start_time)
          : "";

    return {
      test_id: testId,
      test_name: testName,
      table_type: tableType,
      warehouse_size: warehouseSize,
      created_at: createdAt,
      status,
      ops_per_sec: Number.isFinite(opsPerSec) ? opsPerSec : 0,
      p50_latency: Number.isFinite(p50Latency) ? p50Latency : 0,
      p95_latency: Number.isFinite(p95Latency) ? p95Latency : 0,
      p99_latency: Number.isFinite(p99Latency) ? p99Latency : 0,
      error_rate: Number.isFinite(errorRate) ? errorRate : 0,
      concurrent_connections: Number.isFinite(concurrentConnections)
        ? Math.trunc(concurrentConnections)
        : 0,
      duration: Number.isFinite(duration) ? duration : 0,
    };
  };

  const safeToastError = (msg) => {
    try {
      if (window.toast && typeof window.toast.error === "function") {
        window.toast.error(msg);
      }
    } catch (_) {}
  };

  const safeToastSuccess = (msg) => {
    try {
      if (window.toast && typeof window.toast.success === "function") {
        window.toast.success(msg);
      }
    } catch (_) {}
  };

  const tableTypeKey = (test) =>
    String(test?.table_type || "").trim().toUpperCase();

  const tableTypeLabel = (test) => {
    const t = tableTypeKey(test);
    if (t === "POSTGRES" || t === "SNOWFLAKE_POSTGRES") return "POSTGRES";
    if (t === "HYBRID") return "HYBRID";
    if (t === "STANDARD") return "STANDARD";
    if (t === "INTERACTIVE") return "INTERACTIVE";
    return t || "";
  };

  const tableTypeIconSrc = (test) => {
    const t = tableTypeKey(test);
    if (t === "POSTGRES" || t === "SNOWFLAKE_POSTGRES") {
      return "/static/img/postgres_elephant.svg";
    }
    if (t === "HYBRID") {
      return "/static/img/table_hybrid.svg";
    }
    if (t === "STANDARD") {
      return "/static/img/table_standard.svg";
    }
    if (t === "INTERACTIVE") {
      return "/static/img/table_interactive.svg";
    }
    return "";
  };

  return {
    tests: [],
    error: null,
    filters: {
      table_type: "",
      warehouse_size: "",
      status: "",
      date_range: "all",
      start_date: "",
      end_date: "",
    },
    sortField: "created_at",
    sortDirection: "desc",
    currentPage: 1,
    pageSize: 20,
    totalPages: 1,

    // Compare (max 2)
    compareTests: [],
    compareError: null,
    searchQuery: "",
    searchResults: [],
    searchError: null,
    searchLoading: false,

    // AI Analysis
    aiAnalysisModal: false,
    aiAnalysisLoading: false,
    aiAnalysisError: null,
    aiAnalysis: null,
    aiTestId: null,
    chatMessage: "",
    chatHistory: [],
    chatLoading: false,

    _refreshInterval: null,
    tableTypeLabel,
    tableTypeIconSrc,

    init() {
      this.loadTests();
      // Start auto-refresh for running tests
      this._startAutoRefresh();
    },

    _startAutoRefresh() {
      // Clear existing interval if any
      if (this._refreshInterval) {
        clearInterval(this._refreshInterval);
      }
      // Poll every 3 seconds when there are running tests
      this._refreshInterval = setInterval(() => {
        const hasRunning = this.tests.some(
          (t) => t.status && t.status.toUpperCase() === "RUNNING"
        );
        if (hasRunning) {
          this.loadTests();
        }
      }, 3000);
    },

    destroy() {
      if (this._refreshInterval) {
        clearInterval(this._refreshInterval);
        this._refreshInterval = null;
      }
    },

    async loadTests() {
      try {
        this.error = null;
        const params = new URLSearchParams({
          page: this.currentPage,
          page_size: this.pageSize,
          ...this.filters,
        });

        const response = await fetch(`/api/tests?${params}`);
        if (!response.ok) {
          const payload = await response.json().catch(() => ({}));
          const detail = payload && payload.detail ? payload.detail : null;
          this.error =
            (detail && (detail.message || detail.detail || detail)) ||
            `Failed to load test results (HTTP ${response.status})`;
          this.tests = [];
          this.totalPages = 1;
          return;
        }

        const data = await response.json();
        this.tests = data.results || [];
        this.totalPages = data.total_pages || 1;
      } catch (error) {
        console.error("Failed to load tests:", error);
        this.error = error?.message || String(error);
        this.tests = [];
        this.totalPages = 1;
      }
    },

    // -----------------------------
    // Compare selection + search
    // -----------------------------
    isCompared(testId) {
      const id = testId != null ? String(testId) : "";
      return this.compareTests.some((t) => String(t.test_id) === id);
    },

    compareButtonLabel(testId) {
      return this.isCompared(testId) ? "Remove" : "Add to compare";
    },

    async searchTests() {
      const q = this.searchQuery != null ? String(this.searchQuery) : "";
      if (q.trim().length < 2) {
        this.searchResults = [];
        this.searchError = null;
        return;
      }

      this.searchLoading = true;
      this.searchError = null;
      try {
        const response = await fetch(
          `/api/tests/search?q=${encodeURIComponent(q.trim())}`,
        );
        if (!response.ok) {
          const payload = await response.json().catch(() => ({}));
          const detail = payload && payload.detail ? payload.detail : null;
          throw new Error(
            (detail && (detail.message || detail.detail || detail)) ||
              `Search failed (HTTP ${response.status})`,
          );
        }

        const data = await response.json().catch(() => ({}));
        const results =
          data && Array.isArray(data.results) ? data.results : data.results || [];
        this.searchResults = Array.isArray(results) ? results : [];
      } catch (err) {
        console.error("Search failed:", err);
        this.searchError = err && err.message ? err.message : String(err);
        this.searchResults = [];
      } finally {
        this.searchLoading = false;
      }
    },

    async fetchTestDetails(testId) {
      const id = testId != null ? String(testId) : "";
      if (!id) throw new Error("Missing test_id");

      const resp = await fetch(`/api/tests/${encodeURIComponent(id)}`);
      if (!resp.ok) {
        const payload = await resp.json().catch(() => ({}));
        const detail = payload && payload.detail ? payload.detail : null;
        throw new Error(
          (detail && (detail.message || detail.detail || detail)) ||
            `Failed to load test details (HTTP ${resp.status})`,
        );
      }
      const data = await resp.json().catch(() => ({}));
      return normalizeTestForCompare(data);
    },

    async toggleCompare(testOrId) {
      const id =
        testOrId && typeof testOrId === "object"
          ? String(testOrId.test_id || "")
          : String(testOrId || "");
      if (!id) return;

      this.compareError = null;

      // Remove if already selected.
      if (this.isCompared(id)) {
        this.compareTests = this.compareTests.filter(
          (t) => String(t.test_id) !== id,
        );
        safeToastSuccess("Removed from compare.");
        return;
      }

      if (this.compareTests.length >= MAX_COMPARE) {
        safeToastError(`Compare is limited to ${MAX_COMPARE} tests for now.`);
        return;
      }

      try {
        const full = await this.fetchTestDetails(id);
        if (!full || !full.test_id) throw new Error("Invalid test details payload");
        // Prevent duplicates (race-safe).
        if (this.isCompared(full.test_id)) return;
        if (this.compareTests.length >= MAX_COMPARE) {
          safeToastError(`Compare is limited to ${MAX_COMPARE} tests for now.`);
          return;
        }
        this.compareTests = [...this.compareTests, full];
        safeToastSuccess("Added to compare.");
      } catch (err) {
        console.error("Failed to add to compare:", err);
        this.compareError = err && err.message ? err.message : String(err);
        safeToastError(this.compareError);
      }
    },

    async clearCompare() {
      this.compareError = null;
      this.compareTests = [];
      safeToastSuccess("Compare cleared.");
    },

    deepCompareUrl() {
      const items = Array.isArray(this.compareTests) ? this.compareTests : [];
      if (items.length !== MAX_COMPARE) return "/history/compare";
      const ids = items
        .map((t) => (t && t.test_id != null ? String(t.test_id) : ""))
        .filter(Boolean);
      if (ids.length !== MAX_COMPARE) return "/history/compare";
      return `/history/compare?ids=${encodeURIComponent(ids.join(","))}`;
    },

    openDeepCompare() {
      window.location.href = this.deepCompareUrl();
    },

    get sortedTests() {
      return [...this.tests].sort((a, b) => {
        let aVal = a[this.sortField];
        let bVal = b[this.sortField];

        if (this.sortField === "created_at") {
          aVal = new Date(aVal);
          bVal = new Date(bVal);
        }

        if (this.sortDirection === "asc") {
          return aVal > bVal ? 1 : -1;
        } else {
          return aVal < bVal ? 1 : -1;
        }
      });
    },

    sortBy(field) {
      if (this.sortField === field) {
        this.sortDirection = this.sortDirection === "asc" ? "desc" : "asc";
      } else {
        this.sortField = field;
        this.sortDirection = "desc";
      }
    },

    applyFilters() {
      if (this.filters.date_range !== "custom") {
        this.filters.start_date = "";
        this.filters.end_date = "";
      }
      this.currentPage = 1;
      this.loadTests();
    },

    resetFilters() {
      this.filters = {
        table_type: "",
        warehouse_size: "",
        status: "",
        date_range: "all",
        start_date: "",
        end_date: "",
      };
      this.applyFilters();
    },

    previousPage() {
      if (this.currentPage > 1) {
        this.currentPage--;
        this.loadTests();
      }
    },

    nextPage() {
      if (this.currentPage < this.totalPages) {
        this.currentPage++;
        this.loadTests();
      }
    },

    dashboardUrl(test) {
      const id = test && test.test_id ? String(test.test_id) : "";
      const status = test && test.status ? String(test.status).toUpperCase() : "";
      if (!id) return "/dashboard";
      if (status === "COMPLETED") return `/dashboard/history/${id}`;
      return `/dashboard/${id}`;
    },

    viewTest(test) {
      window.location.href = this.dashboardUrl(test);
    },

    async rerunTest(testId) {
      const confirmed = await window.toast.confirm(
        "Re-run this test with the same configuration?",
        { confirmText: "Re-run", confirmVariant: "primary", timeoutMs: 10_000 }
      );
      if (!confirmed) return;

      try {
        const response = await fetch(`/api/tests/${testId}/rerun`, {
          method: "POST",
        });
        const data = await response.json();
        window.location.href = `/dashboard/${data.new_test_id}`;
      } catch (error) {
        console.error("Failed to rerun test:", error);
        window.toast.error("Failed to rerun test");
      }
    },

    async deleteTest(testId) {
      const confirmed = await window.toast.confirm(
        "Delete this test result? This cannot be undone.",
        { confirmText: "Delete", confirmVariant: "danger", timeoutMs: 10_000 }
      );
      if (!confirmed) return;

      try {
        await fetch(`/api/tests/${testId}`, {
          method: "DELETE",
        });
        this.loadTests();
        window.toast.success("Test deleted.");
      } catch (error) {
        console.error("Failed to delete test:", error);
        window.toast.error("Failed to delete test");
      }
    },

    async openAiAnalysis(testId) {
      this.aiTestId = testId;
      this.aiAnalysisModal = true;
      this.aiAnalysisLoading = true;
      this.aiAnalysisError = null;
      this.aiAnalysis = null;
      this.chatMessage = "";
      this.chatHistory = [];

      try {
        const response = await fetch(`/api/tests/${testId}/ai-analysis`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({}),
        });
        if (!response.ok) {
          const payload = await response.json().catch(() => ({}));
          const detail = payload.detail;
          const msg = typeof detail === 'object' && detail !== null
            ? (detail.message || JSON.stringify(detail))
            : (detail || `HTTP ${response.status}`);
          throw new Error(msg);
        }
        const data = await response.json();
        this.aiAnalysis = data;
      } catch (err) {
        console.error("AI analysis failed:", err);
        this.aiAnalysisError = err.message || String(err);
      } finally {
        this.aiAnalysisLoading = false;
      }
    },

    closeAiAnalysis() {
      this.aiAnalysisModal = false;
      this.aiTestId = null;
      this.aiAnalysis = null;
      this.aiAnalysisError = null;
      this.chatHistory = [];
      this.chatMessage = "";
    },

    async sendChatMessage() {
      const msg = (this.chatMessage || "").trim();
      if (!msg || !this.aiTestId) return;

      this.chatLoading = true;
      this.chatHistory.push({ role: "user", content: msg });
      this.chatMessage = "";

      try {
        const response = await fetch(`/api/tests/${this.aiTestId}/ai-chat`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            message: msg,
            history: this.chatHistory.slice(0, -1),
          }),
        });
        if (!response.ok) {
          const payload = await response.json().catch(() => ({}));
          throw new Error(payload.detail?.message || payload.detail || `HTTP ${response.status}`);
        }
        const data = await response.json();
        this.chatHistory.push({ role: "assistant", content: data.response });
      } catch (err) {
        console.error("AI chat failed:", err);
        this.chatHistory.push({ role: "assistant", content: `Error: ${err.message || err}` });
      } finally {
        this.chatLoading = false;
      }
    },

    formatMarkdown(text) {
      if (!text) return "";
      let str = text;
      if (str.startsWith('"') && str.endsWith('"')) {
        str = str.slice(1, -1);
      }
      str = str.replace(/\\n/g, "\n");
      let html = str
        .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>")
        .replace(/\*(.+?)\*/g, "<em>$1</em>")
        .replace(/`(.+?)`/g, "<code>$1</code>")
        .replace(/^### (.+)$/gm, "<h4>$1</h4>")
        .replace(/^## (.+)$/gm, "<h3>$1</h3>")
        .replace(/^# (.+)$/gm, "<h2>$1</h2>")
        .replace(/^- (.+)$/gm, "<li>$1</li>")
        .replace(/^(\d+)\. (.+)$/gm, "<li>$2</li>")
        .replace(/\n\n/g, "</p><p>")
        .replace(/\n/g, "<br>");
      html = html.replace(/(<li>.*<\/li>)+/g, (match) => `<ul>${match}</ul>`);
      return `<p>${html}</p>`;
    },
  };
}
