"""
Cost Calculator Module

Provides utilities for calculating Snowflake credit consumption and estimated costs
based on warehouse size and test duration.

IMPORTANT: All Snowflake compute uses credits, but with different rates:
- STANDARD / HYBRID / INTERACTIVE: Use standard warehouse credit rates
- POSTGRES: Uses Postgres Compute credit rates (much lower per hour)

From Snowflake Service Consumption Table (February 2026):
- Table 1(a): Standard Warehouse credits per hour
- Table 1(d): Interactive Warehouse credits per hour  
- Table 1(i): Snowflake Postgres Compute credits per hour
"""

import logging
from typing import Optional, Any
from enum import Enum

logger = logging.getLogger(__name__)

# Cache for Postgres instance info (host -> compute_family mapping)
_postgres_instance_cache: dict[str, str] = {}
_postgres_instance_cache_loaded: bool = False


class WarehouseSize(str, Enum):
    """Snowflake warehouse sizes with their credit consumption rates."""

    XSMALL = "XSMALL"
    SMALL = "SMALL"
    MEDIUM = "MEDIUM"
    LARGE = "LARGE"
    XLARGE = "XLARGE"
    XXLARGE = "2XLARGE"
    XXXLARGE = "3XLARGE"
    XXXXLARGE = "4XLARGE"
    XXXXXLARGE = "5XLARGE"
    XXXXXXLARGE = "6XLARGE"


# Table types that use standard warehouse credit pricing
WAREHOUSE_CREDIT_TABLE_TYPES = {"STANDARD", "HYBRID"}

# Table types that use interactive warehouse credit pricing
INTERACTIVE_TABLE_TYPES = {"INTERACTIVE"}

# Table types that use Postgres compute credit pricing
POSTGRES_TABLE_TYPES = {"POSTGRES"}


# Table 1(a): Standard Warehouse - Credits consumed per hour
WAREHOUSE_CREDITS_PER_HOUR: dict[str, float] = {
    "XSMALL": 1,
    "X-SMALL": 1,
    "SMALL": 2,
    "MEDIUM": 4,
    "LARGE": 8,
    "XLARGE": 16,
    "X-LARGE": 16,
    "2XLARGE": 32,
    "2X-LARGE": 32,
    "XXLARGE": 32,
    "3XLARGE": 64,
    "3X-LARGE": 64,
    "XXXLARGE": 64,
    "4XLARGE": 128,
    "4X-LARGE": 128,
    "XXXXLARGE": 128,
    "5XLARGE": 256,
    "5X-LARGE": 256,
    "XXXXXLARGE": 256,
    "6XLARGE": 512,
    "6X-LARGE": 512,
    "XXXXXXLARGE": 512,
}


# Table 1(d): Interactive Warehouse - Credits consumed per hour
INTERACTIVE_CREDITS_PER_HOUR: dict[str, float] = {
    "XSMALL": 0.6,
    "X-SMALL": 0.6,
    "SMALL": 1.2,
    "MEDIUM": 2.4,
    "LARGE": 4.8,
    "XLARGE": 9.6,
    "X-LARGE": 9.6,
    "2XLARGE": 19.2,
    "2X-LARGE": 19.2,
    "3XLARGE": 38.4,
    "3X-LARGE": 38.4,
    "4XLARGE": 76.8,
    "4X-LARGE": 76.8,
}


# Table 1(i): Snowflake Postgres Compute - Credits per hour (AWS rates)
# Instance families: STANDARD, HIGHMEM, BURST
# Note: Azure rates are slightly higher (see full table for Azure-specific rates)
POSTGRES_CREDITS_PER_HOUR: dict[str, float] = {
    # Standard instance family
    "STANDARD_M": 0.0356,
    "STANDARD_L": 0.0712,
    "STANDARD_XL": 0.1424,
    "STANDARD_2X": 0.2848,
    "STANDARD_4XL": 0.5696,
    "STANDARD_8XL": 1.1392,
    "STANDARD_12XL": 1.7088,
    "STANDARD_24XL": 3.4176,
    # High Memory instance family
    "HIGHMEM_L": 0.1024,
    "HIGHMEM_XL": 0.2048,
    "HIGHMEM_2XL": 0.4096,
    "HIGHMEM_4XL": 0.8192,
    "HIGHMEM_8XL": 1.6384,
    "HIGHMEM_12XL": 2.4576,
    "HIGHMEM_16XL": 3.2768,
    "HIGHMEM_24XL": 4.9152,
    "HIGHMEM_32XL": 6.5536,
    "HIGHMEM_48XL": 9.8304,
    # Burst instance family
    "BURST_XS": 0.0068,
    "BURST_S": 0.0136,
    "BURST_M": 0.0272,
    # Aliases using traditional warehouse size names (map to STANDARD family)
    # These allow using familiar size names for Postgres
    "XSMALL": 0.0068,    # Maps to BURST_XS
    "X-SMALL": 0.0068,
    "SMALL": 0.0136,     # Maps to BURST_S
    "MEDIUM": 0.0356,    # Maps to STANDARD_M
    "LARGE": 0.0712,     # Maps to STANDARD_L
    "XLARGE": 0.1424,    # Maps to STANDARD_XL
    "X-LARGE": 0.1424,
    "2XLARGE": 0.2848,   # Maps to STANDARD_2X
    "2X-LARGE": 0.2848,
}


def get_table_type_category(table_type: Optional[str]) -> str:
    """
    Determine the pricing category for a table type.

    Args:
        table_type: Table type string (e.g., "HYBRID", "POSTGRES", "INTERACTIVE")

    Returns:
        Category string: "warehouse", "interactive", or "postgres"
    """
    if not table_type:
        return "warehouse"  # Default
    normalized = table_type.upper().strip()
    if normalized in POSTGRES_TABLE_TYPES:
        return "postgres"
    if normalized in INTERACTIVE_TABLE_TYPES:
        return "interactive"
    return "warehouse"


async def load_postgres_instances() -> dict[str, str]:
    """
    Load Postgres instance information from Snowflake.
    
    Returns a mapping of host -> compute_family (instance size).
    Results are cached for the lifetime of the application.
    """
    global _postgres_instance_cache, _postgres_instance_cache_loaded
    
    if _postgres_instance_cache_loaded:
        return _postgres_instance_cache
    
    try:
        from backend.connectors.snowflake_pool import get_default_pool
        
        pool = get_default_pool()
        result = await pool.execute_query("SHOW POSTGRES INSTANCES")
        
        # Get column names from the cursor description
        # The result is a list of tuples, so we need the column mapping
        # SHOW commands return: name,owner,owner_role_type,created_on,updated_on,type,origin,host,
        #                       privatelink_service_identifier,compute_family,authentication_authority,
        #                       storage_size,postgres_version,postgres_settings,is_ha,retention_time,state,comment
        # Indices: name=0, host=7, compute_family=9
        for row in result:
            if len(row) > 9:
                instance_name = row[0] or ""
                host = row[7] or ""
                compute_family = row[9] or ""
                
                if host and compute_family:
                    # Store by full host
                    _postgres_instance_cache[host.lower()] = compute_family.upper()
                    # Also store by instance name for convenience
                    if instance_name:
                        _postgres_instance_cache[f"instance:{instance_name.lower()}"] = compute_family.upper()
                    
        _postgres_instance_cache_loaded = True
        logger.info(f"Loaded {len(result)} Postgres instances")
        
    except Exception as e:
        logger.warning(f"Failed to load Postgres instances: {e}")
        # Don't mark as loaded so we can retry
        
    return _postgres_instance_cache


def get_postgres_instance_size_by_host(host: Optional[str]) -> Optional[str]:
    """
    Get the Postgres instance size (compute_family) for a given host.
    
    Args:
        host: The Postgres host URL
        
    Returns:
        Instance size (e.g., "STANDARD_M") or None if not found
    """
    if not host:
        return None
    
    # Normalize host for lookup
    host_lower = host.lower().strip()
    
    # Try exact match first
    if host_lower in _postgres_instance_cache:
        return _postgres_instance_cache[host_lower]
    
    # Try partial match (in case we have just the hostname without full URL)
    for cached_host, size in _postgres_instance_cache.items():
        if not cached_host.startswith("instance:"):
            if host_lower in cached_host or cached_host in host_lower:
                return size
                
    return None


def get_postgres_instance_size_by_name(instance_name: Optional[str]) -> Optional[str]:
    """
    Get the Postgres instance size (compute_family) for a given instance name.
    
    Args:
        instance_name: The Postgres instance name (e.g., "Postgres18_Std_1Core")
        
    Returns:
        Instance size (e.g., "STANDARD_M") or None if not found
    """
    if not instance_name:
        return None
    
    key = f"instance:{instance_name.lower().strip()}"
    return _postgres_instance_cache.get(key)


def get_postgres_credits_per_hour(instance_size: Optional[str]) -> float:
    """
    Get credits per hour for a Postgres instance size.

    Args:
        instance_size: Instance size (e.g., "STANDARD_M", "MEDIUM", "BURST_S")

    Returns:
        Credits per hour, or 0 if unknown
    """
    if not instance_size:
        return 0.0
    normalized = instance_size.upper().strip()
    return POSTGRES_CREDITS_PER_HOUR.get(normalized, 0.0)


def get_credits_per_hour(warehouse_size: Optional[str]) -> float:
    """
    Get the credits consumed per hour for a given warehouse size.

    Args:
        warehouse_size: Warehouse size string (e.g., "XSMALL", "MEDIUM", "2XLARGE")

    Returns:
        Credits per hour, or 0 if warehouse size is unknown/None
    """
    if not warehouse_size:
        return 0.0

    normalized = warehouse_size.upper().strip()
    return WAREHOUSE_CREDITS_PER_HOUR.get(normalized, 0.0)


def calculate_credits_used(
    duration_seconds: Optional[float],
    warehouse_size: Optional[str],
) -> float:
    """
    Calculate estimated credits consumed for a test run.

    Args:
        duration_seconds: Duration of the test in seconds
        warehouse_size: Warehouse size string

    Returns:
        Estimated credits consumed
    """
    if not duration_seconds or duration_seconds <= 0:
        return 0.0

    credits_per_hour = get_credits_per_hour(warehouse_size)
    if credits_per_hour <= 0:
        return 0.0

    # Convert duration to hours and multiply by credits/hour
    duration_hours = duration_seconds / 3600.0
    return duration_hours * credits_per_hour


def calculate_estimated_cost(
    duration_seconds: Optional[float],
    warehouse_size: Optional[str],
    dollars_per_credit: float = 4.00,
    actual_credits_used: Optional[float] = None,
    table_type: Optional[str] = None,
    postgres_instance_size: Optional[str] = None,
) -> dict:
    """
    Calculate the estimated cost for a test run.

    ALL Snowflake compute uses credits, but with different rates:
    - Standard/Hybrid warehouses: Table 1(a) rates
    - Interactive warehouses: Table 1(d) rates (lower)
    - Postgres Compute: Table 1(i) rates (much lower)

    Args:
        duration_seconds: Duration of the test in seconds
        warehouse_size: Warehouse size string (for warehouse-based pricing)
        dollars_per_credit: Cost per Snowflake credit in dollars
        actual_credits_used: If available, use actual credits from query history
        table_type: Table type (e.g., "HYBRID", "POSTGRES", "INTERACTIVE")
        postgres_instance_size: For Postgres, the instance size (e.g., "STANDARD_M")

    Returns:
        Dictionary with cost breakdown:
        {
            "credits_used": float,
            "estimated_cost_usd": float,
            "cost_per_hour": float,
            "credits_per_hour": float,
            "warehouse_size": str,  # or instance_size for Postgres
            "calculation_method": "actual" | "estimated" | "unavailable"
        }
    """
    result = {
        "credits_used": 0.0,
        "estimated_cost_usd": 0.0,
        "cost_per_hour": 0.0,
        "credits_per_hour": 0.0,
        "warehouse_size": warehouse_size or "UNKNOWN",
        "calculation_method": "unavailable",
    }

    # Determine which pricing table to use
    category = get_table_type_category(table_type)

    if category == "postgres":
        # Postgres uses different instance family names (STANDARD_M, HIGHMEM_L, etc.)
        instance_size = postgres_instance_size or warehouse_size
        credits_per_hour = get_postgres_credits_per_hour(instance_size)
        result["warehouse_size"] = instance_size or "UNKNOWN"
        result["credits_per_hour"] = credits_per_hour

        if duration_seconds and duration_seconds > 0 and credits_per_hour > 0:
            duration_hours = duration_seconds / 3600.0
            credits_used = duration_hours * credits_per_hour
            result["credits_used"] = credits_used
            result["estimated_cost_usd"] = credits_used * dollars_per_credit
            result["cost_per_hour"] = credits_per_hour * dollars_per_credit
            result["calculation_method"] = "estimated"
        return result

    elif category == "interactive":
        # Interactive warehouses use lower credit rates
        size = warehouse_size or "MEDIUM"
        normalized = size.upper().strip()
        credits_per_hour = INTERACTIVE_CREDITS_PER_HOUR.get(normalized, 0.0)
        result["credits_per_hour"] = credits_per_hour

        # Use actual credits if available
        if actual_credits_used is not None and actual_credits_used > 0:
            result["credits_used"] = actual_credits_used
            result["estimated_cost_usd"] = actual_credits_used * dollars_per_credit
            result["cost_per_hour"] = credits_per_hour * dollars_per_credit
            result["calculation_method"] = "actual"
            return result

        # Estimate from duration
        if duration_seconds and duration_seconds > 0 and credits_per_hour > 0:
            duration_hours = duration_seconds / 3600.0
            credits_used = duration_hours * credits_per_hour
            result["credits_used"] = credits_used
            result["estimated_cost_usd"] = credits_used * dollars_per_credit
            result["cost_per_hour"] = credits_per_hour * dollars_per_credit
            result["calculation_method"] = "estimated"
        return result

    else:
        # Standard/Hybrid warehouses use standard warehouse credit rates
        credits_per_hour = get_credits_per_hour(warehouse_size)
        result["credits_per_hour"] = credits_per_hour

        # Use actual credits if available
        if actual_credits_used is not None and actual_credits_used > 0:
            result["credits_used"] = actual_credits_used
            result["estimated_cost_usd"] = actual_credits_used * dollars_per_credit
            result["cost_per_hour"] = credits_per_hour * dollars_per_credit
            result["calculation_method"] = "actual"
            return result

        # Otherwise estimate from duration and warehouse size
        credits_used = calculate_credits_used(duration_seconds, warehouse_size)
        if credits_used > 0:
            result["credits_used"] = credits_used
            result["estimated_cost_usd"] = credits_used * dollars_per_credit
            result["cost_per_hour"] = credits_per_hour * dollars_per_credit
            result["calculation_method"] = "estimated"

        return result


def calculate_cost_efficiency(
    total_cost: float,
    total_operations: int,
    qps: float,
    duration_seconds: float,
) -> dict:
    """
    Calculate cost efficiency metrics for comparing test runs.

    Args:
        total_cost: Total estimated cost in dollars
        total_operations: Total number of operations executed
        qps: Queries/operations per second
        duration_seconds: Duration of the test in seconds

    Returns:
        Dictionary with efficiency metrics:
        {
            "cost_per_operation": float,
            "cost_per_1000_ops": float,
            "cost_per_1000_qps": float,
        }
    """
    result = {
        "cost_per_operation": 0.0,
        "cost_per_1000_ops": 0.0,
        "cost_per_1000_qps": 0.0,
    }

    if total_operations > 0 and total_cost > 0:
        result["cost_per_operation"] = total_cost / total_operations
        result["cost_per_1000_ops"] = (total_cost / total_operations) * 1000

    if qps > 0 and duration_seconds > 0 and total_cost > 0:
        # Cost per 1000 QPS sustained for the full duration
        duration_hours = duration_seconds / 3600.0
        if duration_hours > 0:
            result["cost_per_1000_qps"] = (total_cost / qps) * 1000

    return result


def format_cost(amount: float, currency: str = "USD", decimals: int = 2) -> str:
    """
    Format a cost amount as a currency string.

    Args:
        amount: Cost amount
        currency: Currency code (currently only USD supported)
        decimals: Number of decimal places

    Returns:
        Formatted cost string (e.g., "$1.23")
    """
    if currency.upper() == "USD":
        return f"${amount:.{decimals}f}"
    return f"{amount:.{decimals}f} {currency}"


def format_credits(credits: float, decimals: int = 4) -> str:
    """
    Format credits as a readable string.

    Args:
        credits: Number of credits
        decimals: Number of decimal places

    Returns:
        Formatted credits string (e.g., "0.0125 credits")
    """
    if credits == 1.0:
        return f"{credits:.{decimals}f} credit"
    return f"{credits:.{decimals}f} credits"
