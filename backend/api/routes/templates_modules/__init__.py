"""
Template management API routes package.

This package provides:
- Constants: SQL templates and regex patterns
- Utils: Identifier validation, quoting, type coercion
- Models: Pydantic request/response models
- Config normalizer: Template configuration validation

All symbols are re-exported for backward compatibility.
"""

# Constants
from .constants import (
    _IDENT_RE,
    _CUSTOM_QUERY_FIELDS,
    _CUSTOM_PCT_FIELDS,
    _DEFAULT_CUSTOM_QUERIES_SNOWFLAKE,
    _DEFAULT_CUSTOM_QUERIES_POSTGRES,
)

# Utility functions
from .utils import (
    _upper_str,
    _validate_ident,
    _quote_ident,
    _is_postgres_family_table_type,
    _pg_quote_ident,
    _pg_qualified_name,
    _pg_placeholders,
    _full_table_name,
    _sample_clause,
    _results_prefix,
    _coerce_int,
    _enrich_postgres_instance_size,
    _row_to_dict,
)

# Config normalization
from .config_normalizer import _normalize_template_config

# Pydantic models
from .models import (
    TemplateConfig,
    TemplateCreate,
    TemplateUpdate,
    TemplateResponse,
    AiPrepareResponse,
    AiAdjustSqlRequest,
    AiAdjustSqlResponse,
)

__all__ = [
    # Constants
    "_IDENT_RE",
    "_CUSTOM_QUERY_FIELDS",
    "_CUSTOM_PCT_FIELDS",
    "_DEFAULT_CUSTOM_QUERIES_SNOWFLAKE",
    "_DEFAULT_CUSTOM_QUERIES_POSTGRES",
    # Utils
    "_upper_str",
    "_validate_ident",
    "_quote_ident",
    "_is_postgres_family_table_type",
    "_pg_quote_ident",
    "_pg_qualified_name",
    "_pg_placeholders",
    "_full_table_name",
    "_sample_clause",
    "_results_prefix",
    "_coerce_int",
    "_enrich_postgres_instance_size",
    "_row_to_dict",
    # Config normalizer
    "_normalize_template_config",
    # Models
    "TemplateConfig",
    "TemplateCreate",
    "TemplateUpdate",
    "TemplateResponse",
    "AiPrepareResponse",
    "AiAdjustSqlRequest",
    "AiAdjustSqlResponse",
]
