"""
API routes for connection management.

Provides CRUD operations for database connection definitions.

SECURITY NOTE:
- Connection credentials are stored securely in Snowflake's CONNECTIONS table
- Passwords are never returned in API responses (masked)
- Access to the CONNECTIONS table should be restricted via Snowflake RBAC
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query, status

from backend.api.error_handling import http_exception
from backend.core import connection_manager
from backend.models.connection import (
    ConnectionCreate,
    ConnectionListResponse,
    ConnectionResponse,
    ConnectionTestRequest,
    ConnectionTestResponse,
    ConnectionType,
    ConnectionUpdate,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/", response_model=ConnectionListResponse)
async def list_connections(
    include_inactive: bool = Query(False, description="Include inactive connections"),
    connection_type: ConnectionType | None = Query(None, description="Filter by connection type"),
) -> ConnectionListResponse:
    """
    List all saved connections.

    Returns connection metadata and credential status (has_password, has_private_key).
    Actual credentials are never returned.
    """
    try:
        connections = await connection_manager.list_connections(
            include_inactive=include_inactive,
            connection_type=connection_type,
        )
        return ConnectionListResponse(
            connections=connections,
            total=len(connections),
        )
    except Exception as e:
        raise http_exception("list connections", e)


@router.post("/", response_model=ConnectionResponse, status_code=status.HTTP_201_CREATED)
async def create_connection(data: ConnectionCreate) -> ConnectionResponse:
    """
    Create a new connection definition.

    Stores connection metadata and credentials securely in Snowflake.
    Credentials are encrypted at rest via Snowflake's built-in encryption.
    """
    try:
        # Validate type-specific fields
        _validate_connection_fields(data)

        connection = await connection_manager.create_connection(data)
        logger.info(f"Created connection: {connection.connection_name} ({connection.connection_type})")
        return connection
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        # Check for unique constraint violation
        error_msg = str(e).lower()
        if "unique" in error_msg or "duplicate" in error_msg:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Connection with name '{data.connection_name}' already exists",
            )
        raise http_exception("create connection", e)


@router.get("/{connection_id}", response_model=ConnectionResponse)
async def get_connection(connection_id: str) -> ConnectionResponse:
    """
    Get a single connection by ID.

    Returns connection metadata and credential status.
    Actual credentials (passwords) are never returned.
    """
    try:
        connection = await connection_manager.get_connection(connection_id)
        if not connection:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Connection not found: {connection_id}",
            )
        return connection
    except HTTPException:
        raise
    except Exception as e:
        raise http_exception("get connection", e)


@router.put("/{connection_id}", response_model=ConnectionResponse)
async def update_connection(connection_id: str, data: ConnectionUpdate) -> ConnectionResponse:
    """
    Update an existing connection.

    Only updates provided fields. Credentials can be updated by providing
    username/password/private_key fields - they will be merged with existing credentials.
    """
    try:
        connection = await connection_manager.update_connection(connection_id, data)
        if not connection:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Connection not found: {connection_id}",
            )
        logger.info(f"Updated connection: {connection.connection_name}")
        return connection
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        # Check for unique constraint violation
        error_msg = str(e).lower()
        if "unique" in error_msg or "duplicate" in error_msg:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Connection name already exists",
            )
        raise http_exception("update connection", e)


@router.delete("/{connection_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_connection(
    connection_id: str,
    hard_delete: bool = Query(False, description="Permanently delete instead of soft delete"),
) -> None:
    """
    Delete a connection.

    By default performs a soft delete (sets is_active=False).
    Use hard_delete=true to permanently remove the record.
    """
    try:
        # Check if connection exists first
        connection = await connection_manager.get_connection(connection_id)
        if not connection:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Connection not found: {connection_id}",
            )

        await connection_manager.delete_connection(connection_id, hard_delete=hard_delete)
        logger.info(f"Deleted connection: {connection.connection_name} (hard={hard_delete})")
    except HTTPException:
        raise
    except Exception as e:
        raise http_exception("delete connection", e)


@router.post("/{connection_id}/test", response_model=ConnectionTestResponse)
async def test_connection(
    connection_id: str,
    credentials: Optional[ConnectionTestRequest] = None,
) -> ConnectionTestResponse:
    """
    Test a connection using stored or provided credentials.

    By default uses the credentials stored with the connection.
    Optionally provide credentials to override stored values for testing.

    Args:
        connection_id: The connection to test
        credentials: Optional credential overrides (uses stored if not provided)

    Returns:
        Test result with success status, message, and latency
    """
    try:
        # Verify connection exists
        connection = await connection_manager.get_connection(connection_id)
        if not connection:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Connection not found: {connection_id}",
            )

        result = await connection_manager.test_connection(connection_id, credentials)
        if result.success:
            logger.info(f"Connection test successful: {connection.connection_name}")
        else:
            logger.warning(f"Connection test failed: {connection.connection_name} - {result.message}")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise http_exception("test connection", e)


@router.post("/{connection_id}/set-default", response_model=ConnectionResponse)
async def set_default_connection(connection_id: str) -> ConnectionResponse:
    """
    Set a connection as the default for its type.

    This clears the default flag from other connections of the same type.
    """
    try:
        connection = await connection_manager.set_default_connection(connection_id)
        if not connection:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Connection not found: {connection_id}",
            )
        logger.info(f"Set default connection: {connection.connection_name} ({connection.connection_type})")
        return connection
    except HTTPException:
        raise
    except Exception as e:
        raise http_exception("set default connection", e)


def _validate_connection_fields(data: ConnectionCreate) -> None:
    """
    Validate that required fields are present based on connection type.

    Raises:
        ValueError: If required fields are missing
    """
    if data.connection_type == ConnectionType.SNOWFLAKE:
        if not data.account:
            raise ValueError("Snowflake connections require 'account' field")
    elif data.connection_type == ConnectionType.POSTGRES:
        if not data.host:
            raise ValueError("Postgres connections require 'host' field")
