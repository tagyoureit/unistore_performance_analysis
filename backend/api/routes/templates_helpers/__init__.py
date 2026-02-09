"""
Templates Helpers Package

Helper functions and constants for template routes that can be imported
to reduce the size of the main templates.py file.

Usage:
    from backend.api.routes.templates_helpers import (
        validate_ident,
        normalize_template_config,
        full_table_name,
    )
"""

from .helpers import (
    _CUSTOM_PCT_FIELDS,
    _CUSTOM_QUERY_FIELDS,
    _DEFAULT_CUSTOM_QUERIES_POSTGRES,
    _DEFAULT_CUSTOM_QUERIES_SNOWFLAKE,
    coerce_int,
    coerce_num,
    full_table_name,
    is_postgres_family_table_type,
    normalize_template_config,
    pg_placeholders,
    pg_qualified_name,
    pg_quote_ident,
    quote_ident,
    results_prefix,
    row_to_dict,
    sample_clause,
    upper_str,
    validate_ident,
)

__all__ = [
    "_CUSTOM_QUERY_FIELDS",
    "_CUSTOM_PCT_FIELDS",
    "_DEFAULT_CUSTOM_QUERIES_SNOWFLAKE",
    "_DEFAULT_CUSTOM_QUERIES_POSTGRES",
    "upper_str",
    "validate_ident",
    "quote_ident",
    "is_postgres_family_table_type",
    "pg_quote_ident",
    "pg_qualified_name",
    "pg_placeholders",
    "full_table_name",
    "sample_clause",
    "results_prefix",
    "coerce_int",
    "coerce_num",
    "normalize_template_config",
    "row_to_dict",
]
