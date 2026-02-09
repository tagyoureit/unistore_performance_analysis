/**
 * Test History page Alpine component.
 *
 * Manages the test results list, filtering, sorting, and pagination.
 * This function must be globally available for HTMX-swapped content to work.
 */

// Polyfill for Object.groupBy (ES2024) - required for older browsers
if (typeof Object.groupBy !== "function") {
  Object.groupBy = function (items, callbackFn) {
    const result = Object.create(null);
    for (let i = 0; i < items.length; i++) {
      const key = callbackFn(items[i], i);
      if (!(key in result)) {
        result[key] = [];
      }
      result[key].push(items[i]);
    }
    return result;
  };
}

function testHistory() {
  const MAX_COMPARE = 2;

  const normalizeTestForCompare = (data) => {
    const t = data && typeof data === "object" ? data : {};

    const testId = t.test_id != null ? String(t.test_id) : "";
    const testName = t.test_name != null ? String(t.test_name) : "";
    const tableType = t.table_type != null ? String(t.table_type) : "";
    const warehouseSize = t.warehouse_size != null ? String(t.warehouse_size) : "";

    const opsPerSec = Number(t.ops_per_sec);
    const p50Latency = Number(t.p50_latency);
    const p95Latency = Number(t.p95_latency);
    const p99Latency = Number(t.p99_latency);

    const status = t.status != null ? String(t.status) : "";

    const errorRate = Number(t.error_rate);

    const concurrentConnections =
      t.concurrent_connections != null ? Number(t.concurrent_connections) : 0;
    const duration = Number(t.duration);

    const createdAt = t.created_at != null ? String(t.created_at) : "";

    // Cost fields
    const creditsUsed = Number(t.credits_used);
    const estimatedCostUsd = Number(t.estimated_cost_usd);
    const costPer1kOps = Number(t.cost_per_1k_ops || t.cost_per_1000_ops);
    const costCalculationMethod = t.cost_calculation_method != null ? String(t.cost_calculation_method) : "";

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
      // Cost fields
      credits_used: Number.isFinite(creditsUsed) ? creditsUsed : 0,
      estimated_cost_usd: Number.isFinite(estimatedCostUsd) ? estimatedCostUsd : 0,
      cost_per_1k_ops: Number.isFinite(costPer1kOps) ? costPer1kOps : 0,
      cost_calculation_method: costCalculationMethod,
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
    if (t === "POSTGRES") return "POSTGRES";
    if (t === "HYBRID") return "HYBRID";
    if (t === "STANDARD") return "STANDARD";
    if (t === "INTERACTIVE") return "INTERACTIVE";
    return t || "";
  };

  const tableTypeIconSrc = (test) => {
    const t = tableTypeKey(test);
    if (t === "POSTGRES") {
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

    // Loading states for individual buttons
    rerunningTestId: null,
    deletingTestIds: {},
    viewingTestId: null,
    applyingFilters: false,
    deepCompareLoading: false,
    togglingCompareIds: {},

    // Cost settings
    showCostSettings: false,
    costSettings: window.CostUtils ? window.CostUtils.getCostSettings() : { dollarsPerCredit: 4.0, showCredits: true },

    // Check if any action is in progress for a specific test
    isTestActionInProgress(testId) {
      return (
        this.rerunningTestId === testId ||
        this.isDeleting(testId) ||
        this.viewingTestId === testId
      );
    },

    isTogglingCompare(testId) {
      const id = testId != null ? String(testId) : "";
      return !!(this.togglingCompareIds && this.togglingCompareIds[id]);
    },

    isDeleting(testId) {
      const id = testId != null ? String(testId) : "";
      return !!(this.deletingTestIds && this.deletingTestIds[id]);
    },

    _refreshInterval: null,
    multiNodeExpanded: {},
    tableTypeLabel,
    tableTypeIconSrc,

    init() {
      this.loadTests();
      // Start auto-refresh for running tests
      this._startAutoRefresh();
    },

    saveCostSettings() {
      if (window.CostUtils) {
        window.CostUtils.saveCostSettings(this.costSettings);
        safeToastSuccess('Cost settings saved');
      }
      this.showCostSettings = false;
      // Force re-render of cost displays by triggering a small state change
      this.tests = [...this.tests];
    },

    resetCostSettings() {
      this.costSettings = { dollarsPerCredit: 4.0, showCredits: true };
      if (window.CostUtils) {
        window.CostUtils.saveCostSettings(this.costSettings);
      }
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
      this.togglingCompareIds[id] = true;

      // Remove if already selected.
      if (this.isCompared(id)) {
        this.compareTests = this.compareTests.filter(
          (t) => String(t.test_id) !== id,
        );
        safeToastSuccess("Removed from compare.");
        delete this.togglingCompareIds[id];
        return;
      }

      if (this.compareTests.length >= MAX_COMPARE) {
        safeToastError(`Compare is limited to ${MAX_COMPARE} tests for now.`);
        delete this.togglingCompareIds[id];
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
      } finally {
        delete this.togglingCompareIds[id];
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
      this.deepCompareLoading = true;
      window.location.href = this.deepCompareUrl();
    },

    get sortedTests() {
      const items = Array.isArray(this.tests) ? this.tests : [];
      const sorter = (a, b) => {
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
      };
      if (typeof items.toSorted === "function") {
        return items.toSorted(sorter);
      }
      return [...items].sort(sorter);
    },

    get displayRows() {
      const rows = [];
      const tests = Array.isArray(this.sortedTests) ? this.sortedTests : [];
      const grouped = Object.groupBy(tests, (test) => {
        const runId = test?.run_id != null ? String(test.run_id) : "";
        const testId = test?.test_id != null ? String(test.test_id) : "";
        if (runId) {
          return `run:${runId}`;
        }
        return `single:${testId}`;
      });
      const groups = new Map();
      for (const [key, groupTests] of Object.entries(grouped)) {
        if (!key.startsWith("run:")) continue;
        const runId = key.slice(4);
        const group = { run_id: runId, parent: null, children: [] };
        for (const test of groupTests) {
          const testId = test?.test_id != null ? String(test.test_id) : "";
          if (runId === testId) {
            group.parent = test;
          } else {
            group.children.push(test);
          }
        }
        groups.set(runId, group);
      }

      const renderedParents = new Set();
      const renderChildren = (group) => {
        const children = Array.isArray(group.children) ? group.children : [];
        const sortedChildren =
          typeof children.toSorted === "function"
            ? children.toSorted((a, b) => {
                const aTime = new Date(a?.created_at || 0);
                const bTime = new Date(b?.created_at || 0);
                return bTime - aTime;
              })
            : [...children].sort((a, b) => {
                const aTime = new Date(a?.created_at || 0);
                const bTime = new Date(b?.created_at || 0);
                return bTime - aTime;
              });
        for (const child of sortedChildren) {
          rows.push({
            type: "child",
            test: child,
            run_id: group.run_id,
          });
        }
      };

      for (const test of tests) {
        const runId = test?.run_id != null ? String(test.run_id) : "";
        const testId = test?.test_id != null ? String(test.test_id) : "";
        const group = runId ? groups.get(runId) : null;

        if (runId && runId === testId) {
          if (renderedParents.has(runId)) continue;
          rows.push({
            type: "parent",
            test,
            run_id: runId,
            child_count: group && Array.isArray(group.children) ? group.children.length : 0,
          });
          if (this.multiNodeExpanded[runId]) {
            if (group) {
              renderChildren(group);
            }
          }
          renderedParents.add(runId);
          continue;
        }

        if (runId && runId !== testId) {
          if (group && group.parent) {
            continue;
          }
          rows.push({
            type: "child",
            test,
            run_id: runId,
          });
          continue;
        }

        rows.push({ type: "single", test });
      }

      return rows;
    },

    toggleMultiNode(runId) {
      const id = runId != null ? String(runId) : "";
      if (!id) return;
      this.multiNodeExpanded[id] = !this.multiNodeExpanded[id];
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
      this.applyingFilters = true;
      this.loadTests().finally(() => {
        this.applyingFilters = false;
      });
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
      const phase = test && test.phase ? String(test.phase).toUpperCase() : "";
      if (!id) return "/dashboard";
      if (status === "COMPLETED" || phase === "PROCESSING") {
        return `/dashboard/history/${id}`;
      }
      return `/dashboard/${id}`;
    },

    viewTest(test) {
      this.viewingTestId = test.test_id;
      window.location.href = this.dashboardUrl(test);
    },

    async rerunTest(testId) {
      const confirmed = await window.toast.confirm(
        "Re-run this test with the same configuration?",
        { confirmText: "Run Again", confirmVariant: "primary", timeoutMs: 10_000 }
      );
      if (!confirmed) return;

      this.rerunningTestId = testId;
      try {
        const response = await fetch(`/api/tests/${testId}/rerun`, {
          method: "POST",
        });
        const data = await response.json();
        window.location.href = `/dashboard/${data.new_test_id}`;
      } catch (error) {
        console.error("Failed to rerun test:", error);
        window.toast.error("Failed to rerun test");
      } finally {
        this.rerunningTestId = null;
      }
    },

    async deleteTest(testId) {
      const confirmed = await window.toast.confirm(
        "Delete this test result? This cannot be undone.",
        { confirmText: "Delete", confirmVariant: "danger", timeoutMs: 10_000 }
      );
      if (!confirmed) return;

      this.deletingTestIds[testId] = true;
      try {
        await fetch(`/api/tests/${testId}`, {
          method: "DELETE",
        });
        this.loadTests();
        window.toast.success("Test deleted.");
      } catch (error) {
        console.error("Failed to delete test:", error);
        window.toast.error("Failed to delete test");
      } finally {
        delete this.deletingTestIds[testId];
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
