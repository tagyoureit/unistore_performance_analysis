"""
Pydantic models for database connection definitions.

These models define the API contract for connection CRUD operations.

SECURITY NOTE:
- Credentials are stored securely in Snowflake's CONNECTIONS table
- Passwords are never returned in API responses (masked as "********")
- Access to the CONNECTIONS table should be restricted via Snowflake RBAC
"""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class ConnectionType(str, Enum):
    """Supported connection types."""

    SNOWFLAKE = "SNOWFLAKE"
    POSTGRES = "POSTGRES"


class CredentialsBase(BaseModel):
    """Credentials for database authentication."""

    username: Optional[str] = Field(None, min_length=1, description="Username for authentication")
    password: Optional[str] = Field(None, min_length=1, description="Password for authentication")
    # Optional: for Snowflake key-pair auth
    private_key: Optional[str] = Field(None, description="Private key content (PEM format)")
    private_key_passphrase: Optional[str] = Field(None, description="Passphrase for private key")


class ConnectionBase(BaseModel):
    """Base model for connection data.
    
    NOTE: database_name, schema_name, warehouse, pool_size, max_overflow, pool_timeout
    have been removed from connections - they belong in template configuration instead.
    The CONNECTIONS table still has these columns for backward compatibility (they'll be NULL).
    """

    connection_name: str = Field(..., min_length=1, max_length=100, description="Unique name for this connection")
    connection_type: ConnectionType = Field(..., description="Type of database connection")

    # Common fields
    host: Optional[str] = Field(None, max_length=255, description="Database host (for Postgres)")
    port: Optional[int] = Field(None, ge=1, le=65535, description="Database port")
    pgbouncer_port: Optional[int] = Field(None, ge=1, le=65535, description="PgBouncer port (for Postgres)")

    # Snowflake-specific fields
    account: Optional[str] = Field(None, max_length=100, description="Snowflake account identifier")
    role: Optional[str] = Field(None, max_length=100, description="Snowflake role")

    @field_validator("connection_name")
    @classmethod
    def validate_connection_name(cls, v: str) -> str:
        """Ensure connection name is trimmed and not empty."""
        v = v.strip()
        if not v:
            raise ValueError("Connection name cannot be empty")
        return v


class ConnectionCreate(ConnectionBase):
    """Model for creating a new connection."""

    # Credentials - required for create
    username: Optional[str] = Field(None, min_length=1, description="Username for authentication")
    password: Optional[str] = Field(None, min_length=1, description="Password for authentication")
    private_key: Optional[str] = Field(None, description="Private key content (PEM format)")
    private_key_passphrase: Optional[str] = Field(None, description="Passphrase for private key")


class ConnectionUpdate(BaseModel):
    """Model for updating an existing connection (all fields optional).
    
    NOTE: database_name, schema_name, warehouse, pool_size, max_overflow, pool_timeout
    have been removed - they belong in template configuration instead.
    """

    connection_name: Optional[str] = Field(None, min_length=1, max_length=100)
    connection_type: Optional[ConnectionType] = None
    host: Optional[str] = Field(None, max_length=255)
    port: Optional[int] = Field(None, ge=1, le=65535)
    pgbouncer_port: Optional[int] = Field(None, ge=1, le=65535)
    account: Optional[str] = Field(None, max_length=100)
    role: Optional[str] = Field(None, max_length=100)
    is_active: Optional[bool] = None
    # Credentials - optional for update (only update if provided)
    username: Optional[str] = Field(None, min_length=1, description="Username for authentication")
    password: Optional[str] = Field(None, min_length=1, description="Password for authentication")
    private_key: Optional[str] = Field(None, description="Private key content (PEM format)")
    private_key_passphrase: Optional[str] = Field(None, description="Passphrase for private key")


class ConnectionResponse(ConnectionBase):
    """Model for connection response (includes server-generated fields)."""

    connection_id: str = Field(..., description="Unique connection identifier")
    is_default: bool = Field(False, description="Whether this is the default connection for its type")
    is_active: bool = Field(True, description="Whether the connection is active")
    created_at: datetime = Field(..., description="When the connection was created")
    updated_at: datetime = Field(..., description="When the connection was last updated")
    created_by: Optional[str] = Field(None, description="Who created the connection")
    # Credentials - masked in response
    username: Optional[str] = Field(None, description="Username (if set)")
    has_password: bool = Field(False, description="Whether a password is configured")
    has_private_key: bool = Field(False, description="Whether a private key is configured")

    class Config:
        from_attributes = True


class ConnectionTestRequest(BaseModel):
    """Model for testing a connection with credentials.

    If credentials are not provided, uses the stored credentials from the connection.
    """

    username: Optional[str] = Field(None, description="Username override for test")
    password: Optional[str] = Field(None, description="Password override for test")
    private_key: Optional[str] = Field(None, description="Private key override for test")
    private_key_passphrase: Optional[str] = Field(None, description="Passphrase override for test")


class ConnectionTestResponse(BaseModel):
    """Response from connection test."""

    success: bool = Field(..., description="Whether the connection test succeeded")
    message: str = Field(..., description="Result message")
    latency_ms: Optional[float] = Field(None, description="Connection latency in milliseconds")
    server_version: Optional[str] = Field(None, description="Database server version")


class ConnectionListResponse(BaseModel):
    """Response for listing connections."""

    connections: list[ConnectionResponse]
    total: int
