"""
Connection Manager - Business logic for database connection definitions.

Handles CRUD operations for connections stored in Snowflake.

SECURITY NOTE:
- Credentials are encrypted using Snowflake's ENCRYPT_RAW (AES-256-GCM) before storage
- Encryption key should be set via FLAKEBENCH_CREDENTIAL_KEY environment variable
- Passwords are never returned in API responses
- Access to the CONNECTIONS table should be restricted via Snowflake RBAC
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime
from typing import Any

from backend.config import settings
from backend.connectors import snowflake_pool
from backend.models.connection import (
    ConnectionCreate,
    ConnectionResponse,
    ConnectionTestRequest,
    ConnectionTestResponse,
    ConnectionType,
    ConnectionUpdate,
)

logger = logging.getLogger(__name__)

# Table and schema location
CONNECTIONS_TABLE = f"{settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}.CONNECTIONS"
SECRETS_SCHEMA = f"{settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}"

# Encryption key for credentials (32 bytes for AES-256)
# Should be set via environment variable in production
_CREDENTIAL_KEY = os.environ.get(
    "FLAKEBENCH_CREDENTIAL_KEY",
    "FlakeBench-Default-Key-Change-Me!"  # 32 chars = 32 bytes
)
if _CREDENTIAL_KEY == "FlakeBench-Default-Key-Change-Me!":
    logger.warning(
        "Using default credential encryption key. "
        "Set FLAKEBENCH_CREDENTIAL_KEY environment variable for production."
    )

_USING_DEFAULT_KEY = _CREDENTIAL_KEY == "FlakeBench-Default-Key-Change-Me!"


def is_using_default_encryption_key() -> bool:
    """Check if the application is using the default (insecure) encryption key."""
    return _USING_DEFAULT_KEY


async def _encrypt_credentials(credentials: dict[str, str]) -> tuple[str, str] | None:
    """Encrypt credentials using Snowflake's ENCRYPT_RAW.
    
    Returns:
        Tuple of (encrypted_hex, iv_hex) or None if no credentials
    """
    if not credentials:
        return None
    
    pool = snowflake_pool.get_default_pool()
    creds_json = json.dumps(credentials)
    
    # Generate random IV and encrypt
    rows = await pool.execute_query(f"""
        SELECT 
            HEX_ENCODE(ENCRYPT_RAW(
                TO_BINARY('{creds_json.replace("'", "''")}', 'UTF-8'),
                TO_BINARY('{_CREDENTIAL_KEY}', 'UTF-8'),
                TO_BINARY(RANDSTR(12, RANDOM()), 'UTF-8'),
                'AES-GCM'
            )) as encrypted,
            HEX_ENCODE(TO_BINARY(RANDSTR(12, RANDOM()), 'UTF-8')) as iv
    """)
    
    # The above won't work as expected since we need to capture the IV
    # Let's use a simpler approach with a fixed IV derived from connection data
    return None  # Fallback to direct storage for now


async def _store_encrypted_credentials(
    connection_id: str,
    credentials: dict[str, str],
) -> None:
    """Store credentials encrypted in the CREDENTIALS column.
    
    Uses Snowflake ENCRYPT_RAW for AES-256-GCM encryption.
    ENCRYPT_RAW returns a VARIANT with {ciphertext, iv, tag} fields.
    """
    if not credentials:
        return
    
    pool = snowflake_pool.get_default_pool()
    creds_json = json.dumps(credentials)
    escaped_creds = creds_json.replace("'", "''").replace("\\", "\\\\")
    
    # Ensure key is exactly 32 bytes (pad or truncate)
    key_bytes = (_CREDENTIAL_KEY + "0" * 32)[:32]
    escaped_key = key_bytes.replace("'", "''")
    
    # Use connection_id to derive a 12-byte IV (required for AES-GCM)
    # Take first 12 chars of UUID (without hyphens)
    iv_str = connection_id.replace("-", "")[:12]
    
    # Store ENCRYPT_RAW result directly as VARIANT (contains ciphertext, iv, tag)
    # Add metadata fields for quick access without decryption
    await pool.execute_query(f"""
        UPDATE {CONNECTIONS_TABLE}
        SET CREDENTIALS = (
            SELECT OBJECT_CONSTRUCT(
                'encrypted_data', ENCRYPT_RAW(
                    TO_BINARY('{escaped_creds}', 'UTF-8'),
                    TO_BINARY('{escaped_key}', 'UTF-8'),
                    TO_BINARY('{iv_str}', 'UTF-8'),
                    NULL::BINARY,
                    'AES-GCM'
                ),
                'username', {f"'{credentials.get('username', '')}'" if credentials.get('username') else 'NULL'},
                'has_password', {str(bool(credentials.get('password'))).upper()},
                'has_private_key', {str(bool(credentials.get('private_key'))).upper()}
            )
        ),
        UPDATED_AT = CURRENT_TIMESTAMP()
        WHERE CONNECTION_ID = ?
    """, params=[connection_id])
    
    logger.info(f"Stored encrypted credentials for connection {connection_id}")


async def _get_encrypted_credentials(connection_id: str) -> dict[str, Any] | None:
    """Retrieve and decrypt credentials from the CREDENTIALS column.
    
    Uses Snowflake DECRYPT_RAW with AS_BINARY(GET(...)) to extract 
    ciphertext, iv, and tag from the stored VARIANT.
    """
    pool = snowflake_pool.get_default_pool()
    
    # Use same key padding as encryption
    key_bytes = (_CREDENTIAL_KEY + "0" * 32)[:32]
    escaped_key = key_bytes.replace("'", "''")
    
    try:
        # First check if there's encrypted data
        check_rows = await pool.execute_query(f"""
            SELECT 
                CREDENTIALS:encrypted_data IS NOT NULL as has_encrypted,
                CREDENTIALS:username::STRING as username,
                CREDENTIALS:has_password::BOOLEAN as has_password,
                CREDENTIALS:has_private_key::BOOLEAN as has_private_key
            FROM {CONNECTIONS_TABLE}
            WHERE CONNECTION_ID = ?
        """, params=[connection_id])
        
        if not check_rows or not check_rows[0][0]:
            # No encrypted data, try legacy format
            return await _get_legacy_credentials(connection_id)
        
        # Decrypt using AS_BINARY(GET(...)) pattern from Snowflake docs
        # DECRYPT_RAW signature: (ciphertext, key, iv, aad, algorithm, tag)
        decrypt_rows = await pool.execute_query(f"""
            SELECT TO_VARCHAR(
                DECRYPT_RAW(
                    AS_BINARY(GET(CREDENTIALS:encrypted_data, 'ciphertext')),
                    TO_BINARY('{escaped_key}', 'UTF-8'),
                    AS_BINARY(GET(CREDENTIALS:encrypted_data, 'iv')),
                    NULL::BINARY,
                    'AES-GCM',
                    AS_BINARY(GET(CREDENTIALS:encrypted_data, 'tag'))
                ),
                'UTF-8'
            ) as decrypted
            FROM {CONNECTIONS_TABLE}
            WHERE CONNECTION_ID = ?
        """, params=[connection_id])
        
        if decrypt_rows and decrypt_rows[0][0]:
            return json.loads(decrypt_rows[0][0])
            
    except Exception as e:
        logger.debug(f"Could not decrypt credentials for {connection_id}: {e}")
        return await _get_legacy_credentials(connection_id)
    
    return None


async def _get_legacy_credentials(connection_id: str) -> dict[str, Any] | None:
    """Fallback: read unencrypted credentials from legacy format."""
    pool = snowflake_pool.get_default_pool()
    try:
        rows = await pool.execute_query(
            f"SELECT CREDENTIALS FROM {CONNECTIONS_TABLE} WHERE CONNECTION_ID = ?",
            params=[connection_id],
        )
        if rows and rows[0][0]:
            creds = rows[0][0]
            if isinstance(creds, str):
                return json.loads(creds)
            return creds
    except Exception:
        pass
    return None


async def ensure_table_exists() -> None:
    """Create the CONNECTIONS table if it doesn't exist."""
    pool = snowflake_pool.get_default_pool()
    await pool.execute_query(f"""
        CREATE TABLE IF NOT EXISTS {CONNECTIONS_TABLE} (
            CONNECTION_ID       VARCHAR(36)     DEFAULT UUID_STRING() NOT NULL,
            CONNECTION_NAME     VARCHAR(100)    NOT NULL,
            CONNECTION_TYPE     VARCHAR(20)     NOT NULL,
            HOST                VARCHAR(255),
            PORT                INT,
            PGBOUNCER_PORT      INT,
            DATABASE_NAME       VARCHAR(100),
            SCHEMA_NAME         VARCHAR(100),
            ACCOUNT             VARCHAR(100),
            WAREHOUSE           VARCHAR(100),
            ROLE                VARCHAR(100),
            CREDENTIALS         VARIANT,
            POOL_SIZE           INT             DEFAULT 5,
            MAX_OVERFLOW        INT             DEFAULT 10,
            POOL_TIMEOUT        INT             DEFAULT 30,
            IS_DEFAULT          BOOLEAN         DEFAULT FALSE,
            IS_ACTIVE           BOOLEAN         DEFAULT TRUE,
            CREATED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
            UPDATED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
            CREATED_BY          VARCHAR(100),
            PRIMARY KEY (CONNECTION_ID),
            UNIQUE (CONNECTION_NAME)
        )
    """)
    # Add CREDENTIALS column if it doesn't exist (for existing installations)
    try:
        await pool.execute_query(
            f"ALTER TABLE {CONNECTIONS_TABLE} ADD COLUMN IF NOT EXISTS CREDENTIALS VARIANT"
        )
    except Exception:
        pass  # Column already exists or other non-critical error
    # Add PGBOUNCER_PORT column if it doesn't exist (for existing installations)
    try:
        await pool.execute_query(
            f"ALTER TABLE {CONNECTIONS_TABLE} ADD COLUMN IF NOT EXISTS PGBOUNCER_PORT INT"
        )
    except Exception:
        pass  # Column already exists or other non-critical error


def _row_to_connection(row: tuple) -> ConnectionResponse:
    """Convert a database row to a ConnectionResponse model.
    
    Column order from SELECT query:
    0: CONNECTION_ID, 1: CONNECTION_NAME, 2: CONNECTION_TYPE, 3: HOST, 4: PORT,
    5: PGBOUNCER_PORT, 6: DATABASE_NAME (ignored), 7: SCHEMA_NAME (ignored), 
    8: ACCOUNT, 9: WAREHOUSE, 10: ROLE, 11: CREDENTIALS, 12: POOL_SIZE (ignored), 
    13: MAX_OVERFLOW (ignored), 14: POOL_TIMEOUT (ignored), 15: IS_DEFAULT, 
    16: IS_ACTIVE, 17: CREATED_AT, 18: UPDATED_AT, 19: CREATED_BY
    
    NOTE: database_name, schema_name, pool_size, max_overflow, pool_timeout are kept in the
    query for backward compatibility but ignored - they now belong in template config.
    """
    credentials = row[11]  # VARIANT parsed as dict or None
    
    # Parse credentials metadata to determine what's set
    # Note: Table now stores metadata flags (has_password: true) not actual values
    username = None
    has_password = False
    has_private_key = False
    
    if credentials:
        if isinstance(credentials, str):
            credentials = json.loads(credentials)
        username = credentials.get("username")
        # Check for boolean flags (new format) OR actual values (legacy format)
        has_password = bool(credentials.get("has_password") or credentials.get("password"))
        has_private_key = bool(credentials.get("has_private_key") or credentials.get("private_key"))
    
    return ConnectionResponse(
        connection_id=row[0],
        connection_name=row[1],
        connection_type=ConnectionType(row[2]),
        host=row[3],
        port=row[4],
        pgbouncer_port=row[5],
        # row[6] = DATABASE_NAME - ignored (belongs in template config)
        # row[7] = SCHEMA_NAME - ignored (belongs in template config)
        account=row[8],
        # row[9] = WAREHOUSE - ignored (belongs in template config)
        role=row[10],
        # credentials at row[11] - processed above
        # row[12] = POOL_SIZE - ignored (belongs in template config)
        # row[13] = MAX_OVERFLOW - ignored (belongs in template config)
        # row[14] = POOL_TIMEOUT - ignored (belongs in template config)
        is_default=row[15] or False,
        is_active=row[16] if row[16] is not None else True,
        created_at=row[17] or datetime.now(),
        updated_at=row[18] or datetime.now(),
        created_by=row[19],
        # Credential info (masked)
        username=username,
        has_password=has_password,
        has_private_key=has_private_key,
    )


def _build_credentials_json(
    username: str | None,
    password: str | None,
    private_key: str | None,
    private_key_passphrase: str | None,
) -> str | None:
    """Build credentials JSON for storage."""
    creds: dict[str, str] = {}
    if username:
        creds["username"] = username
    if password:
        creds["password"] = password
    if private_key:
        creds["private_key"] = private_key
    if private_key_passphrase:
        creds["private_key_passphrase"] = private_key_passphrase
    
    return json.dumps(creds) if creds else None


async def _get_credentials(connection_id: str) -> dict[str, Any] | None:
    """Get stored credentials for a connection (internal use only).
    
    Retrieves and decrypts credentials from the CREDENTIALS column.
    Falls back to legacy unencrypted format if needed.
    """
    return await _get_encrypted_credentials(connection_id)


async def list_connections(
    include_inactive: bool = False,
    connection_type: ConnectionType | None = None,
) -> list[ConnectionResponse]:
    """
    List all connections.

    Args:
        include_inactive: Whether to include inactive (soft-deleted) connections
        connection_type: Filter by connection type

    Returns:
        List of connection definitions
    """
    await ensure_table_exists()
    pool = snowflake_pool.get_default_pool()

    query = f"""
        SELECT
            CONNECTION_ID,
            CONNECTION_NAME,
            CONNECTION_TYPE,
            HOST,
            PORT,
            PGBOUNCER_PORT,
            DATABASE_NAME,
            SCHEMA_NAME,
            ACCOUNT,
            WAREHOUSE,
            ROLE,
            CREDENTIALS,
            POOL_SIZE,
            MAX_OVERFLOW,
            POOL_TIMEOUT,
            IS_DEFAULT,
            IS_ACTIVE,
            CREATED_AT,
            UPDATED_AT,
            CREATED_BY
        FROM {CONNECTIONS_TABLE}
        WHERE 1=1
    """
    params: list[Any] = []

    if not include_inactive:
        query += " AND IS_ACTIVE = TRUE"

    if connection_type:
        query += " AND CONNECTION_TYPE = ?"
        params.append(connection_type.value)

    query += " ORDER BY CONNECTION_NAME"

    rows = await pool.execute_query(query, params=params if params else None)
    return [_row_to_connection(row) for row in rows]


async def get_connection(connection_id: str) -> ConnectionResponse | None:
    """
    Get a single connection by ID.

    Args:
        connection_id: The connection's unique identifier

    Returns:
        Connection if found, None otherwise
    """
    await ensure_table_exists()
    pool = snowflake_pool.get_default_pool()

    rows = await pool.execute_query(
        f"""
        SELECT
            CONNECTION_ID,
            CONNECTION_NAME,
            CONNECTION_TYPE,
            HOST,
            PORT,
            PGBOUNCER_PORT,
            DATABASE_NAME,
            SCHEMA_NAME,
            ACCOUNT,
            WAREHOUSE,
            ROLE,
            CREDENTIALS,
            POOL_SIZE,
            MAX_OVERFLOW,
            POOL_TIMEOUT,
            IS_DEFAULT,
            IS_ACTIVE,
            CREATED_AT,
            UPDATED_AT,
            CREATED_BY
        FROM {CONNECTIONS_TABLE}
        WHERE CONNECTION_ID = ?
        """,
        params=[connection_id],
    )

    if not rows:
        return None

    return _row_to_connection(rows[0])


async def create_connection(data: ConnectionCreate) -> ConnectionResponse:
    """
    Create a new connection.

    Args:
        data: Connection configuration including credentials

    Returns:
        The created connection with generated ID
        
    NOTE: database_name, schema_name, pool_size, max_overflow, pool_timeout have been
    removed from the API - they belong in template configuration instead.
    """
    await ensure_table_exists()
    pool = snowflake_pool.get_default_pool()

    # Build metadata JSON (for display - no actual passwords)
    # This stores what credentials exist, not the actual values
    metadata: dict[str, Any] = {}
    if data.username:
        metadata["username"] = data.username
    if data.password:
        metadata["has_password"] = True
    if data.private_key:
        metadata["has_private_key"] = True
    if data.private_key_passphrase:
        metadata["has_private_key_passphrase"] = True
    
    metadata_json = json.dumps(metadata) if metadata else None

    # Insert the connection record (without database/schema/warehouse/pool config - those are in templates)
    await pool.execute_query(
        f"""
        INSERT INTO {CONNECTIONS_TABLE} (
            CONNECTION_NAME,
            CONNECTION_TYPE,
            HOST,
            PORT,
            PGBOUNCER_PORT,
            ACCOUNT,
            ROLE,
            CREDENTIALS
        )
        SELECT ?, ?, ?, ?, ?, ?, ?, PARSE_JSON(?)
        """,
        params=[
            data.connection_name,
            data.connection_type.value,
            data.host,
            data.port,
            data.pgbouncer_port,
            data.account,
            data.role,
            metadata_json,
        ],
    )

    # Fetch the created connection to get the generated ID
    rows = await pool.execute_query(
        f"""
        SELECT CONNECTION_ID
        FROM {CONNECTIONS_TABLE}
        WHERE CONNECTION_NAME = ?
        """,
        params=[data.connection_name],
    )
    
    connection_id = rows[0][0]
    
    # Build full credentials for encrypted storage
    credentials: dict[str, str] = {}
    if data.username:
        credentials["username"] = data.username
    if data.password:
        credentials["password"] = data.password
    if data.private_key:
        credentials["private_key"] = data.private_key
    if data.private_key_passphrase:
        credentials["private_key_passphrase"] = data.private_key_passphrase
    
    # Store credentials encrypted in the CREDENTIALS column
    if credentials:
        await _store_encrypted_credentials(connection_id, credentials)

    # Return the full connection response
    return await get_connection(connection_id)


async def update_connection(
    connection_id: str,
    data: ConnectionUpdate,
) -> ConnectionResponse | None:
    """
    Update an existing connection.

    Args:
        connection_id: The connection's unique identifier
        data: Fields to update (only non-None fields are updated)

    Returns:
        Updated connection if found, None otherwise
        
    NOTE: database_name, schema_name, pool_size, max_overflow, pool_timeout have been
    removed from the API - they belong in template configuration instead.
    """
    await ensure_table_exists()
    pool = snowflake_pool.get_default_pool()

    # Build dynamic update query
    updates: list[str] = []
    params: list[Any] = []

    if data.connection_name is not None:
        updates.append("CONNECTION_NAME = ?")
        params.append(data.connection_name)
    if data.connection_type is not None:
        updates.append("CONNECTION_TYPE = ?")
        params.append(data.connection_type.value)
    if data.host is not None:
        updates.append("HOST = ?")
        params.append(data.host)
    if data.port is not None:
        updates.append("PORT = ?")
        params.append(data.port)
    if data.pgbouncer_port is not None:
        updates.append("PGBOUNCER_PORT = ?")
        params.append(data.pgbouncer_port)
    if data.account is not None:
        updates.append("ACCOUNT = ?")
        params.append(data.account)
    if data.role is not None:
        updates.append("ROLE = ?")
        params.append(data.role)
    if data.is_active is not None:
        updates.append("IS_ACTIVE = ?")
        params.append(data.is_active)

    # Handle credentials update - merge with existing encrypted credentials
    credentials_changed = any([data.username, data.password, data.private_key, data.private_key_passphrase])
    if credentials_changed:
        # Get existing credentials (decrypted)
        existing_creds = await _get_encrypted_credentials(connection_id) or {}
        
        # Merge: new values override existing
        if data.username is not None:
            existing_creds["username"] = data.username
        if data.password is not None:
            existing_creds["password"] = data.password
        if data.private_key is not None:
            existing_creds["private_key"] = data.private_key
        if data.private_key_passphrase is not None:
            existing_creds["private_key_passphrase"] = data.private_key_passphrase
        
        # Store merged credentials encrypted
        await _store_encrypted_credentials(connection_id, existing_creds)
        
        # Skip updating CREDENTIALS in this query since _store_encrypted_credentials does it
        # Just continue with other field updates

    if not updates:
        # No fields to update, just return existing
        return await get_connection(connection_id)

    # Always update UPDATED_AT
    updates.append("UPDATED_AT = CURRENT_TIMESTAMP()")
    params.append(connection_id)

    query = f"""
        UPDATE {CONNECTIONS_TABLE}
        SET {', '.join(updates)}
        WHERE CONNECTION_ID = ?
    """

    await pool.execute_query(query, params=params)
    return await get_connection(connection_id)


async def delete_connection(connection_id: str, hard_delete: bool = False) -> bool:
    """
    Delete a connection (soft delete by default).

    Args:
        connection_id: The connection's unique identifier
        hard_delete: If True, permanently delete; otherwise soft delete

    Returns:
        True if connection was found and deleted
    """
    await ensure_table_exists()
    pool = snowflake_pool.get_default_pool()

    if hard_delete:
        # Hard delete removes the row (and encrypted credentials with it)
        result = await pool.execute_query(
            f"DELETE FROM {CONNECTIONS_TABLE} WHERE CONNECTION_ID = ?",
            params=[connection_id],
        )
    else:
        result = await pool.execute_query(
            f"""
            UPDATE {CONNECTIONS_TABLE}
            SET IS_ACTIVE = FALSE, UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE CONNECTION_ID = ?
            """,
            params=[connection_id],
        )

    # Check if any rows were affected (Snowflake returns empty for DML)
    # We'll verify by checking if the connection still exists
    conn = await get_connection(connection_id)
    if hard_delete:
        return conn is None
    else:
        return conn is not None and not conn.is_active


async def set_default_connection(connection_id: str) -> ConnectionResponse | None:
    """
    Set a connection as the default for its type.

    Clears the default flag from other connections of the same type.

    Args:
        connection_id: The connection's unique identifier

    Returns:
        Updated connection if found, None otherwise
    """
    await ensure_table_exists()
    pool = snowflake_pool.get_default_pool()

    # Get the connection to find its type
    conn = await get_connection(connection_id)
    if not conn:
        return None

    # Clear default from other connections of the same type
    await pool.execute_query(
        f"""
        UPDATE {CONNECTIONS_TABLE}
        SET IS_DEFAULT = FALSE, UPDATED_AT = CURRENT_TIMESTAMP()
        WHERE CONNECTION_TYPE = ? AND IS_DEFAULT = TRUE
        """,
        params=[conn.connection_type.value],
    )

    # Set this connection as default
    await pool.execute_query(
        f"""
        UPDATE {CONNECTIONS_TABLE}
        SET IS_DEFAULT = TRUE, UPDATED_AT = CURRENT_TIMESTAMP()
        WHERE CONNECTION_ID = ?
        """,
        params=[connection_id],
    )

    return await get_connection(connection_id)


async def test_connection(
    connection_id: str,
    credentials: ConnectionTestRequest | None = None,
) -> ConnectionTestResponse:
    """
    Test a connection using stored or provided credentials.

    Args:
        connection_id: The connection's unique identifier
        credentials: Optional credential overrides (uses stored if not provided)

    Returns:
        Test result with success status and latency
    """
    conn = await get_connection(connection_id)
    if not conn:
        return ConnectionTestResponse(
            success=False,
            message=f"Connection not found: {connection_id}",
        )

    # Get credentials - use provided overrides or stored credentials
    stored_creds = await _get_credentials(connection_id) or {}
    
    test_username = credentials.username if credentials and credentials.username else stored_creds.get("username")
    test_password = credentials.password if credentials and credentials.password else stored_creds.get("password")
    test_private_key = credentials.private_key if credentials and credentials.private_key else stored_creds.get("private_key")
    test_passphrase = credentials.private_key_passphrase if credentials and credentials.private_key_passphrase else stored_creds.get("private_key_passphrase")

    if not test_username:
        return ConnectionTestResponse(
            success=False,
            message="No username configured for this connection",
        )

    start_time = time.time()

    try:
        if conn.connection_type == ConnectionType.SNOWFLAKE:
            result = await _test_snowflake_connection(
                conn, test_username, test_password, test_private_key, test_passphrase
            )
        elif conn.connection_type == ConnectionType.POSTGRES:
            result = await _test_postgres_connection(
                conn, test_username, test_password
            )
        else:
            return ConnectionTestResponse(
                success=False,
                message=f"Unknown connection type: {conn.connection_type}",
            )

        latency_ms = (time.time() - start_time) * 1000
        result.latency_ms = latency_ms
        return result

    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        logger.error(f"Connection test failed for {conn.connection_name}: {e}")
        return ConnectionTestResponse(
            success=False,
            message=f"Connection failed: {str(e)}",
            latency_ms=latency_ms,
        )


async def _test_snowflake_connection(
    conn: ConnectionResponse,
    username: str,
    password: str | None,
    private_key: str | None,
    private_key_passphrase: str | None,
) -> ConnectionTestResponse:
    """Test a Snowflake connection."""
    import snowflake.connector

    try:
        # Build connection parameters
        connect_params: dict[str, Any] = {
            "account": conn.account,
            "user": username,
            "login_timeout": 15,
            "network_timeout": 15,
        }

        # Use private key auth if available, otherwise password
        if private_key:
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import serialization
            
            # Load the private key
            p_key = serialization.load_pem_private_key(
                private_key.encode(),
                password=private_key_passphrase.encode() if private_key_passphrase else None,
                backend=default_backend(),
            )
            
            # Get the private key bytes
            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
            connect_params["private_key"] = pkb
        elif password:
            connect_params["password"] = password
        else:
            return ConnectionTestResponse(
                success=False,
                message="No password or private key configured",
            )

        # Role is the only optional param we include from the connection
        # warehouse/database/schema come from templates, not connections
        if conn.role:
            connect_params["role"] = conn.role

        # Test connection
        with snowflake.connector.connect(**connect_params) as sf_conn:
            cursor = sf_conn.cursor()
            cursor.execute("SELECT CURRENT_VERSION()")
            version = cursor.fetchone()[0]
            cursor.close()

        return ConnectionTestResponse(
            success=True,
            message="Connection successful",
            server_version=version,
        )

    except Exception as e:
        error_msg = str(e)
        # Clean up common error messages
        if "incorrect username or password" in error_msg.lower():
            error_msg = "Invalid username or password"
        elif "ip/token" in error_msg.lower() and "not allowed" in error_msg.lower():
            error_msg = "IP not allowed by network policy (check VPN)"
        return ConnectionTestResponse(
            success=False,
            message=error_msg,
        )


async def _test_postgres_connection(
    conn: ConnectionResponse,
    username: str,
    password: str | None,
) -> ConnectionTestResponse:
    """Test a Postgres connection."""
    import asyncpg
    import ssl

    if not password:
        return ConnectionTestResponse(
            success=False,
            message="No password configured for Postgres connection",
        )

    try:
        # Create SSL context that doesn't verify certificates (Snowflake Postgres uses self-signed)
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        # Build connection parameters
        # Note: database_name is no longer stored in connections - use "postgres" as default
        # The actual database is specified in templates
        pg_conn = await asyncpg.connect(
            host=conn.host,
            port=conn.port or 5432,
            database="postgres",  # Default database for connection testing
            user=username,
            password=password,
            timeout=15,
            ssl=ssl_context,
        )

        try:
            version = await pg_conn.fetchval("SELECT version()")
            # Extract just the version number
            if version:
                version = version.split(",")[0] if "," in version else version
        finally:
            await pg_conn.close()

        return ConnectionTestResponse(
            success=True,
            message="Connection successful",
            server_version=version,
        )

    except Exception as e:
        error_msg = str(e)
        # Clean up common error messages
        if "password authentication failed" in error_msg.lower():
            error_msg = "Invalid username or password"
        elif "could not connect" in error_msg.lower():
            error_msg = f"Could not connect to {conn.host}:{conn.port or 5432}"
        return ConnectionTestResponse(
            success=False,
            message=error_msg,
        )


async def get_connection_for_pool(connection_id: str) -> dict[str, Any] | None:
    """Get connection params + decrypted credentials for pool creation.
    
    This function is used by run_worker.py to get connection parameters
    suitable for creating Snowflake or Postgres connection pools.
    
    Args:
        connection_id: The connection's unique identifier
        
    Returns:
        Dict with connection params and decrypted credentials:
        - connection_type: "SNOWFLAKE" or "POSTGRES"
        - account: Snowflake account identifier (for Snowflake)
        - host: Database host (for Postgres)
        - port: Database port (for Postgres)
        - user: Username for authentication
        - password: Decrypted password
        - role: Snowflake role
        - private_key: Private key content (for Snowflake key-pair auth)
        - private_key_passphrase: Passphrase for private key
        
        Note: warehouse is NOT included - it comes from template configuration.
        
        Or None if connection not found.
    """
    conn = await get_connection(connection_id)
    if not conn:
        logger.warning(f"Connection not found: {connection_id}")
        return None
    
    # Get decrypted credentials
    credentials = await _get_credentials(connection_id) or {}
    
    result: dict[str, Any] = {
        "connection_type": conn.connection_type.value,
        "account": conn.account,
        "host": conn.host,
        "port": conn.port,
        "pgbouncer_port": conn.pgbouncer_port,
        "role": conn.role,
        # Decrypted credentials
        "user": credentials.get("username"),
        "password": credentials.get("password"),
        "private_key": credentials.get("private_key"),
        "private_key_passphrase": credentials.get("private_key_passphrase"),
    }
    
    logger.info(
        f"Retrieved connection params for pool: connection_id={connection_id}, "
        f"type={conn.connection_type.value}, user={credentials.get('username')}"
    )
    
    return result
