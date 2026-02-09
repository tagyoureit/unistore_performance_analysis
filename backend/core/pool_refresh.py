"""
Value pool refresh after write-capable test runs.

Refreshes KEY pools by re-sampling from the table after writes have occurred.
Called during PROCESSING phase to avoid adding visible latency to the user.
"""

import logging
from typing import Any
from uuid import uuid4

from backend.config import settings

logger = logging.getLogger(__name__)


def _quote_ident(name: str) -> str:
    """Quote identifier for Snowflake SQL."""
    return f'"{name.upper()}"'


async def refresh_key_pool_after_writes(
    template_id: str,
    scenario_config: dict[str, Any],
    pool: Any,
) -> dict[str, Any]:
    """
    Refresh KEY value pools after a test that performed writes.

    This re-samples key values from the table and updates TEMPLATE_VALUE_POOLS
    with fresh data, ensuring subsequent runs can target newly inserted rows.

    Args:
        template_id: The template ID to refresh pools for
        scenario_config: The scenario configuration containing table and workload info
        pool: Snowflake connection pool for executing queries

    Returns:
        Dict with refresh status and counts
    """
    result = {
        "refreshed": False,
        "keys_sampled": 0,
        "error": None,
    }

    try:
        # Extract table info from scenario config
        target_cfg = scenario_config.get("target", {})
        full_table_name = target_cfg.get("full_table_name")
        table_type = str(target_cfg.get("table_type", "")).upper()

        if not full_table_name:
            result["error"] = "No table name in scenario config"
            return result

        # Get AI workload config for key column
        ai_workload = scenario_config.get("ai_workload", {})
        key_col = ai_workload.get("key_column")
        old_pool_id = ai_workload.get("pool_id")

        if not key_col:
            result["error"] = "No key column configured"
            return result

        if not old_pool_id:
            result["error"] = "No existing pool_id to refresh"
            return result

        # Generate new pool ID for the refreshed data
        new_pool_id = str(uuid4())
        key_expr = _quote_ident(key_col)
        prefix = f"{settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}"

        # Determine concurrency for pool sizing
        workload_cfg = scenario_config.get("workload", {})
        concurrency = workload_cfg.get("concurrency", 100)
        target_n = max(5000, concurrency * 50)
        target_n = min(1_000_000, target_n)
        sample_n = min(1_000_000, max(target_n * 2, target_n))

        # Use appropriate sampling strategy based on table type
        is_hybrid = table_type in ("HYBRID", "UNISTORE")
        is_postgres = table_type == "POSTGRES"

        if is_postgres:
            # For Postgres tables, we need to use the Postgres pool, not Snowflake
            # Skip refresh for now - Postgres pools need different handling
            logger.info(
                "Skipping KEY pool refresh for Postgres table %s (not yet implemented)",
                full_table_name,
            )
            result["error"] = "Postgres pool refresh not yet implemented"
            return result

        if is_hybrid:
            # Hybrid tables don't support TABLESAMPLE, use LIMIT
            insert_sql = f"""
            INSERT INTO {prefix}.TEMPLATE_VALUE_POOLS (
                POOL_ID, TEMPLATE_ID, POOL_KIND, COLUMN_NAME, SEQ, VALUE
            )
            SELECT
                ?, ?, 'KEY', ?, SEQ4(), TO_VARIANT(KEY_VAL)
            FROM (
                SELECT DISTINCT {key_expr} AS KEY_VAL
                FROM {full_table_name}
                WHERE {key_expr} IS NOT NULL
                LIMIT {sample_n}
            )
            LIMIT {target_n}
            """
        else:
            # Standard tables: use TABLESAMPLE for efficient random sampling
            insert_sql = f"""
            INSERT INTO {prefix}.TEMPLATE_VALUE_POOLS (
                POOL_ID, TEMPLATE_ID, POOL_KIND, COLUMN_NAME, SEQ, VALUE
            )
            SELECT
                ?, ?, 'KEY', ?, SEQ4(), TO_VARIANT(KEY_VAL)
            FROM (
                SELECT DISTINCT {key_expr} AS KEY_VAL
                FROM {full_table_name} TABLESAMPLE SYSTEM (10)
                WHERE {key_expr} IS NOT NULL
                LIMIT {sample_n}
            )
            LIMIT {target_n}
            """

        logger.info(
            "Refreshing KEY pool for template %s (table=%s, target=%d keys)",
            template_id,
            full_table_name,
            target_n,
        )

        # Insert new pool values
        await pool.execute_query(
            insert_sql,
            params=[new_pool_id, template_id, key_col.upper()],
        )

        # Count how many keys were sampled
        count_rows = await pool.execute_query(
            f"""
            SELECT COUNT(*) AS CNT
            FROM {prefix}.TEMPLATE_VALUE_POOLS
            WHERE TEMPLATE_ID = ? AND POOL_ID = ? AND POOL_KIND = 'KEY'
            """,
            params=[template_id, new_pool_id],
        )
        keys_sampled = count_rows[0][0] if count_rows else 0

        # Update template config to point to new pool_id
        # We update the ai_workload.pool_id in TEST_TEMPLATES.CONFIG
        await pool.execute_query(
            f"""
            UPDATE {prefix}.TEST_TEMPLATES
            SET CONFIG = OBJECT_INSERT(
                CONFIG,
                'ai_workload',
                OBJECT_INSERT(
                    CONFIG:ai_workload,
                    'pool_id',
                    ?
                ),
                TRUE
            ),
            UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE TEMPLATE_ID = ?
            """,
            params=[new_pool_id, template_id],
        )

        logger.info(
            "KEY pool refreshed: %d keys sampled, new pool_id=%s",
            keys_sampled,
            new_pool_id,
        )

        result["refreshed"] = True
        result["keys_sampled"] = keys_sampled
        result["new_pool_id"] = new_pool_id

    except Exception as e:
        logger.warning("Failed to refresh KEY pool for template %s: %s", template_id, e)
        result["error"] = str(e)

    return result


def test_had_writes(scenario_config: dict[str, Any]) -> bool:
    """
    Determine if a test scenario includes write operations.

    Args:
        scenario_config: The scenario configuration

    Returns:
        True if the test includes INSERT, UPDATE, or DELETE operations
    """
    workload_cfg_raw = scenario_config.get("workload", {})
    workload_cfg = workload_cfg_raw if isinstance(workload_cfg_raw, dict) else {}

    # Runtime is CUSTOM-only, but tolerate legacy key names in persisted rows.
    custom_queries = workload_cfg.get("custom_queries", [])
    if isinstance(custom_queries, list):
        for q in custom_queries:
            if isinstance(q, dict):
                kind = str(q.get("query_kind") or q.get("kind") or "").upper()
                raw_weight = q.get("weight_pct", q.get("weight", 0))
                try:
                    weight = float(raw_weight)
                except (TypeError, ValueError):
                    weight = 0.0
                normalized_weight = weight / 100.0 if weight > 1.0 else weight
                if kind in ("INSERT", "UPDATE", "DELETE") and normalized_weight > 0:
                    return True

    # Check for insert/update ratio in AI workload
    ai_workload = scenario_config.get("ai_workload", {})
    if isinstance(ai_workload, dict):
        try:
            if float(ai_workload.get("insert_pct", ai_workload.get("custom_insert_pct", 0)) or 0) > 0:
                return True
            if float(ai_workload.get("update_pct", ai_workload.get("custom_update_pct", 0)) or 0) > 0:
                return True
        except (TypeError, ValueError):
            pass

    return False
