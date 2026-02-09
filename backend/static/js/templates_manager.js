function templatesManager() {
  return {
    templates: [],
    filteredTemplates: [],
    searchQuery: "",
    typeFilter: "",
    loading: true,
    error: null,
    preparingTemplateId: null,
    // Loading states for individual buttons
    editingTemplateId: null,
    viewingTemplateId: null,
    deletingTemplateId: null,
    copyingTemplateId: null,

    // Check if any action is in progress for a specific template
    isTemplateActionInProgress(templateId) {
      return (
        this.editingTemplateId === templateId ||
        this.viewingTemplateId === templateId ||
        this.deletingTemplateId === templateId ||
        this.copyingTemplateId === templateId ||
        this.preparingTemplateId === templateId
      );
    },

    async init() {
      await this.loadTemplates();
    },

    tableTypeKey(template) {
      return String(template?.config?.table_type || "").trim().toUpperCase();
    },

    isPostgresType(template) {
      const t = this.tableTypeKey(template);
      return t === "POSTGRES" || t === "SNOWFLAKE_POSTGRES";
    },

    tableTypeLabel(template) {
      const t = this.tableTypeKey(template);
      // Consolidate SNOWFLAKE_POSTGRES to POSTGRES for display
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
      let filtered = this.templates;

      // Apply type filter first
      if (this.typeFilter) {
        filtered = filtered.filter((t) => {
          const label = this.tableTypeLabel(t);
          return label === this.typeFilter;
        });
      }

      // Then apply search query
      if (this.searchQuery) {
        const query = this.searchQuery.toLowerCase();
        filtered = filtered.filter(
          (t) =>
            t.template_name.toLowerCase().includes(query) ||
            (t.description && t.description.toLowerCase().includes(query)) ||
            this.deriveWorkloadLabel(t.config).toLowerCase().includes(query) ||
            this.tableFqn(t).toLowerCase().includes(query) ||
            this.tableTypeLabel(t).toLowerCase().includes(query),
        );
      }

      this.filteredTemplates = filtered;
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

    // Show actual workload mix with line breaks: "PL 90%<br>RS 10%"
    workloadMixDisplay(config) {
      const pl = Number(config?.custom_point_lookup_pct || 0);
      const rs = Number(config?.custom_range_scan_pct || 0);
      const ins = Number(config?.custom_insert_pct || 0);
      const upd = Number(config?.custom_update_pct || 0);
      const total = pl + rs + ins + upd;
      if (total === 0) return "—";

      const parts = [];
      if (pl > 0) parts.push(`PL ${pl}%`);
      if (rs > 0) parts.push(`RS ${rs}%`);
      if (ins > 0) parts.push(`INS ${ins}%`);
      if (upd > 0) parts.push(`UPD ${upd}%`);
      return parts.join("<br>");
    },

    // Show duration with warmup breakdown on separate lines
    durationDisplay(config) {
      const loadMode = String(config?.load_mode || "CONCURRENCY").toUpperCase();
      if (loadMode === "FIND_MAX_CONCURRENCY") return "—";

      const duration = Number(config?.duration || 0);
      const warmup = Number(config?.warmup || 0);
      if (duration <= 0) return "—";

      const total = warmup + duration;
      if (warmup > 0) {
        return `${total}s<br><span style="color: #6b7280; font-size: 0.85em;">${warmup}s + ${duration}s</span>`;
      }
      return `${duration}s`;
    },

    // Show load mode with key details
    loadModeDisplay(config) {
      const loadMode = String(config?.load_mode || "CONCURRENCY").toUpperCase();
      
      if (loadMode === "QPS") {
        const targetQps = config?.target_qps || "—";
        return `QPS: ${targetQps}`;
      }
      if (loadMode === "FIND_MAX_CONCURRENCY") {
        const start = config?.start_concurrency || 5;
        const inc = config?.concurrency_increment || 10;
        return `Find Max: ${start}+${inc}`;
      }
      // CONCURRENCY mode
      const threads = config?.concurrent_connections || "—";
      return `Fixed: ${threads} threads`;
    },

    // Show scaling configuration with mode on top, config below
    scalingDisplay(config) {
      const scaling = config?.scaling;
      if (!scaling) return "—";

      const mode = String(scaling.mode || "AUTO").toUpperCase();
      const minW = Number(scaling.min_workers ?? 1);
      const maxW = scaling.max_workers != null ? Number(scaling.max_workers) : null;
      const minC = Number(scaling.min_connections ?? 1);
      const maxC = scaling.max_connections != null ? Number(scaling.max_connections) : null;

      if (mode === "FIXED") {
        // FIXED mode uses min_workers and min_connections as the fixed values
        return `FIXED<br><span style="color: #6b7280; font-size: 0.85em;">${minW}w × ${minC}c</span>`;
      }

      if (mode === "AUTO") {
        // AUTO with no meaningful bounds - just show AUTO
        if (maxW === null || minW === maxW) {
          return "AUTO";
        }
        // AUTO with bounds specified
        return `AUTO<br><span style="color: #6b7280; font-size: 0.85em;">${minW}-${maxW}w</span>`;
      }

      // BOUNDED mode - show range with possible unbounded
      const workerPart = maxW === null ? `${minW}+w` : (minW === maxW ? `${minW}w` : `${minW}-${maxW}w`);
      const connPart = maxC === null ? `${minC}+c` : (minC === maxC ? `${minC}c` : `${minC}-${maxC}c`);
      return `BOUNDED<br><span style="color: #6b7280; font-size: 0.85em;">${workerPart} × ${connPart}</span>`;
    },

    // Show warehouse size for Snowflake or instance size for Postgres
    warehouseDisplay(template) {
      const config = template?.config;
      if (!config) return "—";
      
      const tableType = String(config.table_type || "").toUpperCase();
      if (tableType === "POSTGRES" || tableType === "SNOWFLAKE_POSTGRES") {
        // Show Postgres instance size (e.g., STANDARD_M)
        const instanceSize = config.postgres_instance_size || "—";
        return instanceSize;
      }

      // For Snowflake, show warehouse size (and name if there's room)
      const name = config.warehouse_name || config.warehouse || "";
      const size = config.warehouse_size || "";
      if (size && name) {
        return `${size}<br><span style="color: #6b7280; font-size: 0.85em;">${name}</span>`;
      }
      if (size) return size;
      if (name) return name;
      return "—";
    },

    createNewTemplate() {
      window.location.href = "/configure?mode=new";
    },

    async prepareTest(template) {
      if (this.preparingTemplateId) return; // Already preparing
      
      const tableType = String(template?.config?.table_type || "").toUpperCase();
      const isPostgres = tableType === "POSTGRES" || tableType === "SNOWFLAKE_POSTGRES";

      this.preparingTemplateId = template.template_id;
      try {
        // For Snowflake-executed templates, enforce that the execution warehouse
        // isn't the same as the results warehouse (`SNOWFLAKE_WAREHOUSE`).
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

        // Use unified endpoint for all scaling modes (AUTO, BOUNDED, FIXED)
        const endpoint = `/api/tests/from-template/${template.template_id}`;
        const resp = await fetch(endpoint, { method: "POST" });
        if (!resp.ok) {
          const err = await resp.json().catch(() => ({}));
          throw new Error(err.detail || "Failed to prepare test");
        }
        const data = await resp.json();
        window.location.href = data.dashboard_url || `/dashboard/${data.test_id}`;
      } catch (e) {
        console.error("Failed to prepare template:", e);
        window.toast.error(`Failed to prepare test: ${e.message || e}`);
      } finally {
        this.preparingTemplateId = null;
      }
    },

    editTemplate(template) {
      this.editingTemplateId = template.template_id;
      const templateId = encodeURIComponent(String(template?.template_id || ""));
      window.location.href = templateId
        ? `/configure?template_id=${templateId}`
        : "/configure";
    },

    viewTemplateDetails(template) {
      // Route to the same /configure page, but force read-only mode.
      // (The configure page will also force read-only if usage_count > 0.)
      this.viewingTemplateId = template.template_id;
      const templateId = encodeURIComponent(String(template?.template_id || ""));
      window.location.href = templateId
        ? `/configure?template_id=${templateId}&mode=view`
        : "/configure?mode=view";
    },

    async duplicateTemplate(template) {
      if (this.copyingTemplateId) return;
      this.copyingTemplateId = template.template_id;

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
          window.toast.success("Template copied.");
        } else {
          window.toast.error("Failed to duplicate template");
        }
      } catch (error) {
        console.error("Error duplicating template:", error);
        window.toast.error("Error duplicating template");
      } finally {
        this.copyingTemplateId = null;
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

      this.deletingTemplateId = template.template_id;
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
      } finally {
        this.deletingTemplateId = null;
      }
    },
  }}
