#!/usr/bin/env python3
"""Run 4 table-type variations via API and validate completion + metrics."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import subprocess
import sys
import time
from typing import Any, Dict, List, Tuple
from urllib import error, request

TERMINAL_STATUSES = {"completed", "failed", "cancelled"}


def _normalize_base_url(raw: str) -> str:
    base = raw.strip().rstrip("/")
    return base or "http://127.0.0.1:8000"


def _normalize_table_type(raw: Any) -> str:
    return str(raw or "").strip().upper()


def _normalize_ident(raw: Any, label: str) -> str:
    value = str(raw or "").strip()
    if not value:
        raise ValueError(f"{label} is required")
    return value.upper()


def _get_env_int(key: str, default: int) -> int:
    raw = os.environ.get(key)
    if raw is None or raw == "":
        return default
    return int(raw)


def _snow_cli_available() -> bool:
    return shutil.which("snow") is not None


def _run_snow_sql(sql: str, timeout: int) -> None:
    if not _snow_cli_available():
        raise RuntimeError(
            "snow CLI not found. Install SnowCLI before running smoke setup."
        )
    cmd = [
        "snow",
        "sql",
        "-q",
        sql,
        "--format",
        "json",
    ]
    proc = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if proc.returncode != 0:
        stderr = proc.stderr.strip()
        stdout = proc.stdout.strip()
        detail = stderr or stdout or "unknown error"
        raise RuntimeError(f"snow sql failed: {detail}")


def _snow_preamble(database: str, schema: str) -> str:
    return f"USE DATABASE {database};\nCREATE SCHEMA IF NOT EXISTS {schema};\nUSE SCHEMA {schema};"


def _smoke_table_names() -> Dict[str, str]:
    return {
        "STANDARD": "SMOKE_STANDARD",
        "HYBRID": "SMOKE_HYBRID",
        "INTERACTIVE": "SMOKE_INTERACTIVE",
    }


def _ensure_smoke_tables(database: str, schema: str, timeout: int) -> None:
    preamble = _snow_preamble(database, schema)
    sql = f"""
{preamble}
CREATE TABLE IF NOT EXISTS SMOKE_STANDARD (
  ID NUMBER,
  DATA VARCHAR,
  TIMESTAMP TIMESTAMP_NTZ
);
CREATE HYBRID TABLE IF NOT EXISTS SMOKE_HYBRID (
  ID NUMBER,
  DATA VARCHAR,
  TIMESTAMP TIMESTAMP_NTZ,
  PRIMARY KEY (ID)
);
CREATE TABLE IF NOT EXISTS SMOKE_INTERACTIVE (
  ID NUMBER,
  DATA VARCHAR,
  TIMESTAMP TIMESTAMP_NTZ
) CLUSTER BY (ID);
"""
    _run_snow_sql(sql, timeout=timeout)


def _seed_smoke_tables(database: str, schema: str, rows: int, timeout: int) -> None:
    preamble = _snow_preamble(database, schema)
    sql = f"""
{preamble}
TRUNCATE TABLE SMOKE_STANDARD;
TRUNCATE TABLE SMOKE_HYBRID;
TRUNCATE TABLE SMOKE_INTERACTIVE;

INSERT INTO SMOKE_STANDARD (ID, DATA, TIMESTAMP)
SELECT
  SEQ4() + 1,
  'DATA_' || SEQ4(),
  DATEADD('second', SEQ4(), CURRENT_TIMESTAMP())
FROM TABLE(GENERATOR(ROWCOUNT => {rows}));

INSERT INTO SMOKE_HYBRID (ID, DATA, TIMESTAMP)
SELECT
  SEQ4() + 1,
  'DATA_' || SEQ4(),
  DATEADD('second', SEQ4(), CURRENT_TIMESTAMP())
FROM TABLE(GENERATOR(ROWCOUNT => {rows}));

INSERT INTO SMOKE_INTERACTIVE (ID, DATA, TIMESTAMP)
SELECT
  SEQ4() + 1,
  'DATA_' || SEQ4(),
  DATEADD('second', SEQ4(), CURRENT_TIMESTAMP())
FROM TABLE(GENERATOR(ROWCOUNT => {rows}));
"""
    _run_snow_sql(sql, timeout=timeout)


def _drop_smoke_tables(database: str, schema: str, timeout: int) -> None:
    preamble = _snow_preamble(database, schema)
    sql = f"""
{preamble}
DROP TABLE IF EXISTS SMOKE_STANDARD;
DROP TABLE IF EXISTS SMOKE_HYBRID;
DROP TABLE IF EXISTS SMOKE_INTERACTIVE;
DROP SCHEMA IF EXISTS {schema};
"""
    _run_snow_sql(sql, timeout=timeout)


async def _setup_postgres_table(
    database: str,
    schema: str,
    table_name: str,
    rows: int,
    host: str,
    port: int,
    user: str,
    password: str,
) -> bool:
    try:
        import asyncpg
    except Exception:
        return False
    try:
        conn = await asyncpg.connect(
            user=user,
            password=password,
            database=database,
            host=host,
            port=port,
        )
    except Exception:
        return False

    try:
        await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
              ID INTEGER PRIMARY KEY,
              DATA TEXT,
              TIMESTAMP TIMESTAMPTZ
            )
            """
        )
        count = await conn.fetchval(f"SELECT COUNT(*) FROM {schema}.{table_name}")
        if count == 0:
            await conn.execute(
                f"""
                INSERT INTO {schema}.{table_name} (ID, DATA, TIMESTAMP)
                SELECT
                  g AS ID,
                  'DATA_' || g AS DATA,
                  NOW() - (g || ' seconds')::interval AS TIMESTAMP
                FROM generate_series(1, {rows}) AS g
                ON CONFLICT (ID) DO NOTHING
                """
            )
        return True
    finally:
        await conn.close()


def _maybe_setup_postgres(rows: int) -> bool:
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = int(os.environ.get("POSTGRES_PORT", "5432"))
    database = os.environ.get("POSTGRES_DATABASE", "postgres")
    user = os.environ.get("POSTGRES_USER", "postgres")
    password = os.environ.get("POSTGRES_PASSWORD", "")
    schema = os.environ.get("POSTGRES_SCHEMA", "public")
    table_name = os.environ.get("POSTGRES_SMOKE_TABLE", "smoke_orders")
    return asyncio.run(
        _setup_postgres_table(
            database=database,
            schema=schema,
            table_name=table_name,
            rows=rows,
            host=host,
            port=port,
            user=user,
            password=password,
        )
    )


def _build_template_config(
    *,
    table_type: str,
    database: str,
    schema: str,
    table_name: str,
    duration: int,
    warmup: int,
    concurrency: int,
    workload_type: str,
    warehouse_name: str | None,
    warehouse_size: str | None,
) -> Dict[str, Any]:
    cfg: Dict[str, Any] = {
        "table_type": table_type,
        "database": database,
        "schema": schema,
        "table_name": table_name,
        "workload_type": workload_type,
        "duration": duration,
        "warmup": warmup,
        "concurrent_connections": concurrency,
        "warehouse_size": warehouse_size or "XSMALL",
        "load_mode": "CONCURRENCY",
        "use_cached_result": False,
        "columns": {
            "ID": "NUMBER",
            "DATA": "VARCHAR",
            "TIMESTAMP": "TIMESTAMP_NTZ",
        },
    }
    if warehouse_name:
        cfg["warehouse_name"] = warehouse_name
    if table_type == "POSTGRES":
        cfg["columns"] = {
            "ID": "INTEGER",
            "DATA": "TEXT",
            "TIMESTAMP": "TIMESTAMPTZ",
        }
    return cfg


def _http_json(
    method: str,
    url: str,
    payload: Any | None = None,
    timeout: int = 30,
) -> Any:
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    req = request.Request(url, data=data, method=method)
    req.add_header("Accept", "application/json")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
    except error.HTTPError as exc:
        err_body = exc.read().decode("utf-8", "ignore") if exc.fp else ""
        raise RuntimeError(
            f"{method} {url} failed: {exc.code} {exc.reason} {err_body}"
        ) from exc
    except error.URLError as exc:
        raise RuntimeError(f"{method} {url} failed: {exc.reason}") from exc

    if not body:
        return None
    try:
        return json.loads(body.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{method} {url} returned non-JSON response") from exc


def _create_smoke_template(
    base_url: str,
    *,
    template_name: str,
    description: str,
    config: Dict[str, Any],
    tags: Dict[str, str],
    timeout: int,
) -> str:
    payload = {
        "template_name": template_name[:255],
        "description": description[:1000],
        "config": config,
        "tags": tags,
    }
    created = _http_json("POST", f"{base_url}/api/templates/", payload, timeout=timeout)
    template_id = created.get("template_id") if isinstance(created, dict) else None
    if not template_id:
        raise RuntimeError("Failed to create smoke template")
    return str(template_id)


def _find_existing_smoke_template(
    templates: List[Dict[str, Any]],
    *,
    table_type: str,
    database: str,
    schema: str,
    table_name: str,
) -> str | None:
    for template in templates:
        cfg = template.get("config") or {}
        tags = template.get("tags") or {}
        if not isinstance(cfg, dict):
            continue
        if not isinstance(tags, dict):
            continue
        if str(tags.get("smoke") or "").lower() != "true":
            continue
        if _normalize_table_type(cfg.get("table_type")) != table_type:
            continue
        if str(cfg.get("database") or "").upper() != database:
            continue
        if str(cfg.get("schema") or "").upper() != schema:
            continue
        if str(cfg.get("table_name") or "").upper() != table_name:
            continue
        return str(template.get("template_id") or "")
    return None


def _delete_smoke_templates(
    base_url: str,
    templates: List[Dict[str, Any]],
    *,
    database: str,
    schema: str,
    timeout: int,
) -> None:
    for template in templates:
        cfg = template.get("config") or {}
        tags = template.get("tags") or {}
        if not isinstance(cfg, dict) or not isinstance(tags, dict):
            continue
        if str(tags.get("smoke") or "").lower() != "true":
            continue
        if str(cfg.get("database") or "").upper() != database:
            continue
        if str(cfg.get("schema") or "").upper() != schema:
            continue
        template_id = template.get("template_id")
        if not template_id:
            continue
        _http_json(
            "DELETE",
            f"{base_url}/api/templates/{template_id}",
            timeout=timeout,
        )


async def _cleanup_postgres_table(
    database: str,
    schema: str,
    table_name: str,
    host: str,
    port: int,
    user: str,
    password: str,
) -> bool:
    try:
        import asyncpg
    except Exception:
        return False
    try:
        conn = await asyncpg.connect(
            user=user,
            password=password,
            database=database,
            host=host,
            port=port,
        )
    except Exception:
        return False

    try:
        await conn.execute(f"DROP TABLE IF EXISTS {schema}.{table_name}")
        return True
    finally:
        await conn.close()


def _maybe_cleanup_postgres() -> bool:
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = int(os.environ.get("POSTGRES_PORT", "5432"))
    database = os.environ.get("POSTGRES_DATABASE", "postgres")
    user = os.environ.get("POSTGRES_USER", "postgres")
    password = os.environ.get("POSTGRES_PASSWORD", "")
    schema = os.environ.get("POSTGRES_SCHEMA", "public")
    table_name = os.environ.get("POSTGRES_SMOKE_TABLE", "smoke_orders")
    return asyncio.run(
        _cleanup_postgres_table(
            database=database,
            schema=schema,
            table_name=table_name,
            host=host,
            port=port,
            user=user,
            password=password,
        )
    )


def _poll_test(
    base_url: str,
    test_id: str,
    poll_interval: int,
    max_wait: int,
) -> Dict[str, Any]:
    deadline = time.time() + max_wait
    while time.time() < deadline:
        payload = _http_json("GET", f"{base_url}/api/tests/{test_id}")
        if isinstance(payload, dict):
            status = str(payload.get("status") or "").strip().lower()
            if status in TERMINAL_STATUSES:
                return payload
        time.sleep(poll_interval)
    raise TimeoutError(f"Timed out waiting for test {test_id} to complete")


def _wait_for_metrics(
    base_url: str,
    test_id: str,
    poll_interval: int,
    max_wait: int,
) -> Dict[str, Any]:
    deadline = time.time() + max_wait
    while time.time() < deadline:
        payload = _http_json("GET", f"{base_url}/api/tests/{test_id}/metrics")
        if isinstance(payload, dict) and int(payload.get("count") or 0) > 0:
            return payload
        time.sleep(poll_interval)
    return {"count": 0}


def _analyze_test(
    base_url: str,
    test_id: str,
    timeout: int,
) -> Dict[str, Any]:
    payload = _http_json(
        "POST", f"{base_url}/api/tests/{test_id}/ai-analysis", timeout=timeout
    )
    return payload or {}


def _run_single(
    base_url: str,
    template_id: str,
    table_type: str,
    poll_interval: int,
    max_wait: int,
    metrics_wait: int,
    http_timeout: int,
) -> Tuple[bool, str]:
    created = _http_json(
        "POST",
        f"{base_url}/api/tests/from-template/{template_id}",
        timeout=http_timeout,
    )
    test_id = created.get("test_id") if isinstance(created, dict) else None
    if not test_id:
        return False, f"{table_type}: failed to create test from template {template_id}"

    _http_json("POST", f"{base_url}/api/tests/{test_id}/start", timeout=http_timeout)

    test_payload = _poll_test(
        base_url=base_url,
        test_id=test_id,
        poll_interval=poll_interval,
        max_wait=max_wait,
    )
    status = str(test_payload.get("status") or "").strip().lower()
    failure_reason = str(test_payload.get("failure_reason") or "").strip()

    total_ops = int(test_payload.get("total_operations") or 0)
    failed_ops = int(test_payload.get("failed_operations") or 0)
    error_rate_pct = (failed_ops / total_ops * 100.0) if total_ops else 0.0

    metrics_payload = _wait_for_metrics(
        base_url=base_url,
        test_id=test_id,
        poll_interval=poll_interval,
        max_wait=metrics_wait,
    )
    metrics_count = int(metrics_payload.get("count") or 0)

    ai_payload = _analyze_test(base_url=base_url, test_id=test_id, timeout=http_timeout)
    analysis_text = str(ai_payload.get("analysis") or "").strip()

    issues: List[str] = []
    if status != "completed":
        issues.append(f"status={status}")
    if failure_reason:
        issues.append(f"failure_reason={failure_reason}")
    if total_ops <= 0:
        issues.append("total_operations=0")
    if failed_ops > 0:
        issues.append(f"failed_operations={failed_ops} ({error_rate_pct:.2f}%)")
    if metrics_count <= 0:
        issues.append("metrics_count=0")

    print(f"\n[{table_type}] test_id={test_id} status={status}")
    if issues:
        print(f"[{table_type}] issues: {', '.join(issues)}")
    else:
        print(f"[{table_type}] checks: OK (metrics_count={metrics_count})")

    if analysis_text:
        print(f"[{table_type}] AI analysis:\n{analysis_text}\n")
    else:
        print(f"[{table_type}] AI analysis: unavailable\n")

    ok = not issues
    return ok, f"{table_type} test_id={test_id}"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run 4 variation smoke checks via API."
    )
    parser.add_argument(
        "--base-url",
        default=os.environ.get("BASE_URL", "http://127.0.0.1:8000"),
        help="Base URL for the app (default: http://127.0.0.1:8000)",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=int,
        default=int(os.environ.get("POLL_INTERVAL_SECONDS", "5")),
        help="Polling interval for test status",
    )
    parser.add_argument(
        "--max-wait-seconds",
        type=int,
        default=int(os.environ.get("MAX_WAIT_SECONDS", "1800")),
        help="Max wait per test before timing out",
    )
    parser.add_argument(
        "--metrics-wait-seconds",
        type=int,
        default=int(os.environ.get("METRICS_WAIT_SECONDS", "60")),
        help="Max wait for metrics snapshots after completion",
    )
    parser.add_argument(
        "--http-timeout-seconds",
        type=int,
        default=int(os.environ.get("HTTP_TIMEOUT_SECONDS", "30")),
        help="HTTP timeout per request",
    )
    parser.add_argument(
        "--duration-seconds",
        type=int,
        default=(
            int(os.environ.get("DURATION_SECONDS", "0"))
            if os.environ.get("DURATION_SECONDS")
            else None
        ),
        help="Override template duration (seconds) for smoke runs",
    )
    parser.add_argument(
        "--warmup-seconds",
        type=int,
        default=(
            int(os.environ.get("WARMUP_SECONDS", "0"))
            if os.environ.get("WARMUP_SECONDS")
            else None
        ),
        help="Override template warmup (seconds) for smoke runs",
    )
    parser.add_argument(
        "--keep-smoke-templates",
        action="store_true",
        help="Keep temporary smoke templates instead of deleting them",
    )
    parser.add_argument(
        "--keep-smoke-data",
        action="store_true",
        help="Keep smoke tables and templates after run",
    )
    parser.add_argument(
        "--setup-only",
        action="store_true",
        help="Only create smoke tables/templates, do not run tests",
    )
    parser.add_argument(
        "--cleanup-only",
        action="store_true",
        help="Only drop smoke tables/templates, do not run tests",
    )
    parser.add_argument(
        "--skip-postgres",
        action="store_true",
        help="Skip Postgres setup and tests",
    )
    parser.add_argument(
        "--results-database",
        default=os.environ.get("RESULTS_DATABASE", "FLAKEBENCH"),
        help="Snowflake database for results/smoke data",
    )
    parser.add_argument(
        "--smoke-schema",
        default=os.environ.get("SMOKE_SCHEMA", "SMOKE_DATA"),
        help="Snowflake schema for smoke data tables",
    )
    parser.add_argument(
        "--smoke-rows",
        type=int,
        default=_get_env_int("SMOKE_ROWS", 300),
        help="Row count for smoke data tables",
    )
    parser.add_argument(
        "--smoke-warehouse",
        default=os.environ.get(
            "SMOKE_WAREHOUSE", os.environ.get("SNOWFLAKE_WAREHOUSE", "")
        ),
        help="Snowflake warehouse for smoke tests",
    )
    parser.add_argument(
        "--smoke-warehouse-size",
        default=os.environ.get("SMOKE_WAREHOUSE_SIZE", "XSMALL"),
        help="Snowflake warehouse size label for templates",
    )
    parser.add_argument(
        "--smoke-concurrency",
        type=int,
        default=_get_env_int("SMOKE_CONCURRENCY", 5),
        help="Concurrent connections for smoke templates",
    )
    args = parser.parse_args()

    base_url = _normalize_base_url(args.base_url)
    keep_smoke_data = (
        args.keep_smoke_data
        or args.keep_smoke_templates
        or str(os.environ.get("KEEP_SMOKE_DATA", "")).lower() == "true"
    )
    skip_postgres = args.skip_postgres or (
        str(os.environ.get("SKIP_POSTGRES", "")).lower() == "true"
    )

    results_db = _normalize_ident(args.results_database, "RESULTS_DATABASE")
    smoke_schema = _normalize_ident(args.smoke_schema, "SMOKE_SCHEMA")
    smoke_rows = int(args.smoke_rows)
    smoke_warehouse = str(args.smoke_warehouse or "").strip()
    smoke_warehouse = _normalize_ident(smoke_warehouse, "SMOKE_WAREHOUSE")
    smoke_warehouse_size = _normalize_ident(
        args.smoke_warehouse_size, "SMOKE_WAREHOUSE_SIZE"
    )

    if args.cleanup_only:
        try:
            templates = _http_json(
                "GET", f"{base_url}/api/templates/", timeout=args.http_timeout_seconds
            )
            if isinstance(templates, list):
                _delete_smoke_templates(
                    base_url,
                    templates,
                    database=results_db,
                    schema=smoke_schema,
                    timeout=args.http_timeout_seconds,
                )
        except Exception as exc:
            print(f"Template cleanup skipped: {exc}", file=sys.stderr)
        _drop_smoke_tables(results_db, smoke_schema, timeout=args.http_timeout_seconds)
        if not skip_postgres:
            _maybe_cleanup_postgres()
        print("Smoke cleanup completed.")
        return 0

    _ensure_smoke_tables(results_db, smoke_schema, timeout=args.http_timeout_seconds)
    _seed_smoke_tables(
        results_db, smoke_schema, rows=smoke_rows, timeout=args.http_timeout_seconds
    )

    postgres_available = False
    if not skip_postgres:
        postgres_available = _maybe_setup_postgres(rows=smoke_rows)
        if not postgres_available:
            print("Postgres unavailable - skipping POSTGRES smoke test.")

    if args.setup_only:
        templates = _http_json(
            "GET", f"{base_url}/api/templates/", timeout=args.http_timeout_seconds
        )
        if not isinstance(templates, list):
            print(
                "Template listing failed or returned non-list response", file=sys.stderr
            )
            return 1
        for ttype, table_name in _smoke_table_names().items():
            workload = "READ_ONLY" if ttype == "INTERACTIVE" else "MIXED"
            cfg = _build_template_config(
                table_type=ttype,
                database=results_db,
                schema=smoke_schema,
                table_name=table_name,
                duration=int(args.duration_seconds or 45),
                warmup=int(args.warmup_seconds or 0),
                concurrency=int(args.smoke_concurrency),
                workload_type=workload,
                warehouse_name=smoke_warehouse,
                warehouse_size=smoke_warehouse_size,
            )
            existing = _find_existing_smoke_template(
                templates,
                table_type=ttype,
                database=results_db,
                schema=smoke_schema,
                table_name=table_name,
            )
            if existing:
                continue
            _create_smoke_template(
                base_url,
                template_name=f"SMOKE_{ttype}",
                description="Smoke template (setup-only)",
                config=cfg,
                tags={
                    "smoke": "true",
                    "table_type": ttype,
                    "database": results_db,
                    "schema": smoke_schema,
                    "table_name": table_name,
                },
                timeout=args.http_timeout_seconds,
            )
        if postgres_available:
            pg_db = _normalize_ident(
                os.environ.get("POSTGRES_DATABASE", "postgres"),
                "POSTGRES_DATABASE",
            )
            pg_schema = _normalize_ident(
                os.environ.get("POSTGRES_SCHEMA", "public"),
                "POSTGRES_SCHEMA",
            )
            pg_table = _normalize_ident(
                os.environ.get("POSTGRES_SMOKE_TABLE", "smoke_orders"),
                "POSTGRES_SMOKE_TABLE",
            )
            existing = _find_existing_smoke_template(
                templates,
                table_type="POSTGRES",
                database=pg_db,
                schema=pg_schema,
                table_name=pg_table,
            )
            if not existing:
                _create_smoke_template(
                    base_url,
                    template_name="SMOKE_POSTGRES",
                    description="Smoke template (setup-only)",
                    config=_build_template_config(
                        table_type="POSTGRES",
                        database=pg_db,
                        schema=pg_schema,
                        table_name=pg_table,
                        duration=int(args.duration_seconds or 45),
                        warmup=int(args.warmup_seconds or 0),
                        concurrency=int(args.smoke_concurrency),
                        workload_type="MIXED",
                        warehouse_name=None,
                        warehouse_size=None,
                    ),
                    tags={
                        "smoke": "true",
                        "table_type": "POSTGRES",
                        "database": pg_db,
                        "schema": pg_schema,
                        "table_name": pg_table,
                    },
                    timeout=args.http_timeout_seconds,
                )
        print("Smoke setup completed.")
        return 0

    templates = _http_json(
        "GET", f"{base_url}/api/templates/", timeout=args.http_timeout_seconds
    )
    if not isinstance(templates, list):
        print("Template listing failed or returned non-list response", file=sys.stderr)
        return 1

    duration = int(args.duration_seconds or 45)
    warmup = int(args.warmup_seconds or 0)
    temp_template_ids: List[str] = []
    failures: List[str] = []

    print(f"Running smoke checks against {base_url}")
    for ttype, table_name in _smoke_table_names().items():
        workload = "READ_ONLY" if ttype == "INTERACTIVE" else "MIXED"
        cfg = _build_template_config(
            table_type=ttype,
            database=results_db,
            schema=smoke_schema,
            table_name=table_name,
            duration=duration,
            warmup=warmup,
            concurrency=int(args.smoke_concurrency),
            workload_type=workload,
            warehouse_name=smoke_warehouse,
            warehouse_size=smoke_warehouse_size,
        )
        template_id = None
        if keep_smoke_data:
            template_id = _find_existing_smoke_template(
                templates,
                table_type=ttype,
                database=results_db,
                schema=smoke_schema,
                table_name=table_name,
            )
        if not template_id:
            template_id = _create_smoke_template(
                base_url,
                template_name=f"SMOKE_{ttype}",
                description="Temporary smoke template",
                config=cfg,
                tags={
                    "smoke": "true",
                    "table_type": ttype,
                    "database": results_db,
                    "schema": smoke_schema,
                    "table_name": table_name,
                },
                timeout=args.http_timeout_seconds,
            )
            if not keep_smoke_data:
                temp_template_ids.append(template_id)

        ok, label = _run_single(
            base_url=base_url,
            template_id=template_id,
            table_type=ttype,
            poll_interval=args.poll_interval_seconds,
            max_wait=args.max_wait_seconds,
            metrics_wait=args.metrics_wait_seconds,
            http_timeout=args.http_timeout_seconds,
        )
        if not ok:
            failures.append(label)

    if postgres_available:
        pg_db = _normalize_ident(
            os.environ.get("POSTGRES_DATABASE", "postgres"),
            "POSTGRES_DATABASE",
        )
        pg_schema = _normalize_ident(
            os.environ.get("POSTGRES_SCHEMA", "public"),
            "POSTGRES_SCHEMA",
        )
        pg_table = _normalize_ident(
            os.environ.get("POSTGRES_SMOKE_TABLE", "smoke_orders"),
            "POSTGRES_SMOKE_TABLE",
        )
        pg_cfg = _build_template_config(
            table_type="POSTGRES",
            database=pg_db,
            schema=pg_schema,
            table_name=pg_table,
            duration=duration,
            warmup=warmup,
            concurrency=int(args.smoke_concurrency),
            workload_type="MIXED",
            warehouse_name=None,
            warehouse_size=None,
        )
        pg_template_id = None
        if keep_smoke_data:
            pg_template_id = _find_existing_smoke_template(
                templates,
                table_type="POSTGRES",
                database=pg_db,
                schema=pg_schema,
                table_name=pg_table,
            )
        if not pg_template_id:
            pg_template_id = _create_smoke_template(
                base_url,
                template_name="SMOKE_POSTGRES",
                description="Temporary smoke template",
                config=pg_cfg,
                tags={"smoke": "true", "table_type": "POSTGRES"},
                timeout=args.http_timeout_seconds,
            )
            if not keep_smoke_data:
                temp_template_ids.append(pg_template_id)

        ok, label = _run_single(
            base_url=base_url,
            template_id=pg_template_id,
            table_type="POSTGRES",
            poll_interval=args.poll_interval_seconds,
            max_wait=args.max_wait_seconds,
            metrics_wait=args.metrics_wait_seconds,
            http_timeout=args.http_timeout_seconds,
        )
        if not ok:
            failures.append(label)

    if not keep_smoke_data:
        for template_id in temp_template_ids:
            try:
                _http_json(
                    "DELETE",
                    f"{base_url}/api/templates/{template_id}",
                    timeout=args.http_timeout_seconds,
                )
            except Exception as exc:
                print(
                    f"Failed to delete smoke template {template_id}: {exc}",
                    file=sys.stderr,
                )
        _drop_smoke_tables(results_db, smoke_schema, timeout=args.http_timeout_seconds)
        if postgres_available:
            _maybe_cleanup_postgres()

    if failures:
        print("\nSmoke check failed:")
        for item in failures:
            print(f"- {item}")
        return 1

    print("\nSmoke check passed for all variations.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
