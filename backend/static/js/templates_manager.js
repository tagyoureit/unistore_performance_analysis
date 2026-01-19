function templatesManager() {
  return {
    templates: [],
    filteredTemplates: [],
    searchQuery: "",
    viewMode: "table", // 'table' | 'cards'
    loading: true,
    error: null,

    async init() {
      this.initViewMode();
      await this.loadTemplates();
    },

    initViewMode() {
      try {
        const raw = localStorage.getItem("templatesViewMode");
        const mode = String(raw || "").toLowerCase();
        if (mode === "cards" || mode === "table") {
          this.viewMode = mode;
        }
      } catch {
        // Ignore localStorage access issues (private mode, etc.)
      }
    },

    setViewMode(mode) {
      const v = String(mode || "").toLowerCase();
      if (v !== "cards" && v !== "table") {
        return;
      }
      this.viewMode = v;
      try {
        localStorage.setItem("templatesViewMode", v);
      } catch {
        // ignore
      }
    },

    tableTypeKey(template) {
      return String(template?.config?.table_type || "").trim().toUpperCase();
    },

    tableTypeLabel(template) {
      const t = this.tableTypeKey(template);
      if (t === "POSTGRES" || t === "SNOWFLAKE_POSTGRES") return "POSTGRES";
      if (t === "HYBRID") return "HYBRID";
      if (t === "STANDARD") return "STANDARD";
      if (t === "INTERACTIVE") return "INTERACTIVE";
      return t || "";
    },

    tableTypeIconSrc(template) {
      const t = this.tableTypeKey(template);
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
    },

    tableFqn(template) {
      const db = String(template?.config?.database || "").trim();
      const sch = String(template?.config?.schema || "").trim();
      const tbl = String(template?.config?.table_name || "").trim();
      const parts = [db, sch, tbl].filter(Boolean);
      return parts.join(".");
    },

    async loadTemplates() {
      this.loading = true;
      this.error = null;
      try {
        const response = await fetch("/api/templates/");
        if (response.ok) {
          this.templates = await response.json();
          this.filteredTemplates = this.templates;
          return;
        } else {
          const payload = await response.json().catch(() => ({}));
          const detail = payload && payload.detail ? payload.detail : null;
          this.error =
            (detail && (detail.message || detail.detail || detail)) ||
            `Failed to load templates (HTTP ${response.status})`;
          console.error("Failed to load templates:", this.error);
        }
      } catch (error) {
        this.error = error?.message || String(error);
        console.error("Error loading templates:", error);
      } finally {
        this.loading = false;
      }
    },

    filterTemplates() {
      if (!this.searchQuery) {
        this.filteredTemplates = this.templates;
        return;
      }

      const query = this.searchQuery.toLowerCase();
      this.filteredTemplates = this.templates.filter(
        (t) =>
          t.template_name.toLowerCase().includes(query) ||
          (t.description && t.description.toLowerCase().includes(query)) ||
          (this.deriveWorkloadLabel(t.config).toLowerCase().includes(query)) ||
          (this.tableFqn(t).toLowerCase().includes(query)) ||
          (this.tableTypeLabel(t).toLowerCase().includes(query)),
      );
    },

    deriveWorkloadLabel(config) {
      // Templates are persisted as CUSTOM with explicit weights; derive a friendly label.
      const pl = Number(config?.custom_point_lookup_pct || 0);
      const rs = Number(config?.custom_range_scan_pct || 0);
      const ins = Number(config?.custom_insert_pct || 0);
      const upd = Number(config?.custom_update_pct || 0);

      const readPct = pl + rs;
      const writePct = ins + upd;

      if (readPct === 0 && writePct > 0) return "WRITE_ONLY";
      if (writePct === 0 && readPct > 0) return "READ_ONLY";
      if (readPct >= 75) return "READ_HEAVY";
      if (writePct >= 75) return "WRITE_HEAVY";
      return "MIXED";
    },

    createNewTemplate() {
      window.location.href = "/configure?mode=new";
    },

    prepareTest(template) {
      const tableType = String(template?.config?.table_type || "").toUpperCase();
      const isPostgres =
        tableType === "POSTGRES" ||
        tableType === "SNOWFLAKE_POSTGRES";

      // For Snowflake-executed templates, enforce that the execution warehouse
      // isn't the same as the results warehouse (`SNOWFLAKE_WAREHOUSE`).
      const checkAndRun = async () => {
        if (!isPostgres) {
          const infoResp = await fetch("/api/info");
          const info = infoResp.ok ? await infoResp.json() : {};
          const resultsWarehouse = String(info.results_warehouse || "").toUpperCase();
          const execWarehouse = String(
            template?.config?.warehouse_name || "",
          ).toUpperCase();

          if (resultsWarehouse && execWarehouse && resultsWarehouse === execWarehouse) {
            window.toast.queueNext(
              "warning",
              `This template is configured to run on ${execWarehouse}, which is also your results warehouse (${resultsWarehouse}). Please edit the template and choose a different warehouse before running.`,
            );
            window.location.href = "/configure";
            return;
          }
        }

        const resp = await fetch(`/api/tests/from-template/${template.template_id}`, {
          method: "POST",
        });
        if (!resp.ok) {
          const err = await resp.json().catch(() => ({}));
          throw new Error(err.detail || "Failed to prepare test");
        }
        const data = await resp.json();
        window.location.href = data.dashboard_url || `/dashboard/${data.test_id}`;
      };

      checkAndRun().catch((e) => {
        console.error("Failed to prepare template:", e);
        window.toast.error(`Failed to prepare test: ${e.message || e}`);
      });
    },

    useTemplate(template) {
      localStorage.setItem("selectedTemplate", JSON.stringify(template));
    },

    editTemplate(template) {
      localStorage.setItem("selectedTemplate", JSON.stringify(template));
      const templateId = encodeURIComponent(String(template?.template_id || ""));
      window.location.href = templateId
        ? `/configure?template_id=${templateId}`
        : "/configure";
    },

    viewTemplateDetails(template) {
      // Route to the same /configure page, but force read-only mode.
      // (The configure page will also force read-only if usage_count > 0.)
      const templateId = encodeURIComponent(String(template?.template_id || ""));
      window.location.href = templateId
        ? `/configure?template_id=${templateId}&mode=view`
        : "/configure?mode=view";
    },

    async duplicateTemplate(template) {
      const newTemplate = {
        ...template,
        template_name: `${template.template_name} (Copy)`,
        template_id: undefined,
        created_at: undefined,
        updated_at: undefined,
        usage_count: 0,
      };

      try {
        const response = await fetch("/api/templates/", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(newTemplate),
        });

        if (response.ok) {
          await this.loadTemplates();
        } else {
          window.toast.error("Failed to duplicate template");
        }
      } catch (error) {
        console.error("Error duplicating template:", error);
        window.toast.error("Error duplicating template");
      }
    },

    async deleteTemplate(template) {
      const message =
        template.usage_count > 0
          ? `Delete template "${template.template_name}" and all ${template.usage_count} test results?`
          : `Delete template "${template.template_name}"?`;

      const confirmed = await window.toast.confirm(message, {
        confirmText: "Delete",
        confirmVariant: "danger",
        timeoutMs: 10_000,
      });
      if (!confirmed) {
        return;
      }

      try {
        const response = await fetch(`/api/templates/${template.template_id}`, {
          method: "DELETE",
        });

        if (response.ok) {
          await this.loadTemplates();
          window.toast.success("Template deleted.");
        } else {
          window.toast.error("Failed to delete template");
        }
      } catch (error) {
        console.error("Error deleting template:", error);
        window.toast.error("Error deleting template");
      }
    },
  };
}


