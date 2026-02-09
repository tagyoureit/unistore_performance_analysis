# FlakeBench Documentation

This is the human landing page for project docs. For agent-specific guidance,
see `SKILL.md`.

## Start Here

- `architecture-overview.md` — system purpose, topology, and components
- `operations-and-runbooks.md` — how to run, validate, troubleshoot, and front-end dev
- `data-flow-and-lifecycle.md` — lifecycle, control-plane state, metrics flow
- `metrics-streaming-debug.md` — debugging guide for metrics streaming and phase transitions
- `find-max-transition-lag-postmortem.md` — deep dive on FIND_MAX countdown/transition lag and final fixes
- `log-streaming-architecture.md` — log flow from workers to UI, current limitations
- `ui-architecture.md` — UI contracts and dashboard behavior
- `scaling.md` — scaling model, guardrails, and sharding
- `specifications.md` — schemas, payloads, and implementation details

## Implementation Plan

- `project-plan.md` — plan index (start here)
- `plan/overview.md` — phases, terminology, migration strategy
- `plan/phase-2-checklist.md` — detailed implementation tasks
- `plan/decisions.md` — recorded decisions
- `plan/traceability.md` — docs-to-task mapping

## Quick Facts

- Results are stored in Snowflake (`FLAKEBENCH.TEST_RESULTS`).
- Templates are stored in `TEST_TEMPLATES` with `CONFIG` as the canonical payload.
- Runs are prepared from templates, then started from the dashboard.
