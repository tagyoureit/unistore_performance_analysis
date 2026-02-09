-- ============================================================================
-- Settings Tables Schema
-- ============================================================================
-- Stores application configuration and connection definitions.
--
-- SECURITY NOTE:
-- - Credentials are stored encrypted in the CREDENTIALS column
-- - Use Snowflake's built-in encryption for data at rest
-- - Access to this table should be restricted via RBAC
-- ============================================================================

-- ============================================================================
-- CONNECTIONS Table
-- ============================================================================
-- Stores connection definitions for Snowflake and Postgres databases.
-- All connection details including credentials are stored here.
-- ============================================================================

CREATE TABLE IF NOT EXISTS CONNECTIONS (
    -- Primary key
    CONNECTION_ID       VARCHAR(36)     DEFAULT UUID_STRING() NOT NULL,
    
    -- Connection identity
    CONNECTION_NAME     VARCHAR(100)    NOT NULL,
    CONNECTION_TYPE     VARCHAR(20)     NOT NULL,  -- 'SNOWFLAKE' or 'POSTGRES'
    
    -- Common connection fields
    HOST                VARCHAR(255),
    PORT                INT,
    PGBOUNCER_PORT      INT,              -- PgBouncer port for Postgres connections (e.g., 5431)
    DATABASE_NAME       VARCHAR(100),
    SCHEMA_NAME         VARCHAR(100),
    
    -- Snowflake-specific fields
    ACCOUNT             VARCHAR(100),   -- Snowflake account identifier (e.g., 'org-account')
    WAREHOUSE           VARCHAR(100),
    ROLE                VARCHAR(100),
    
    -- Credentials (stored as encrypted VARIANT)
    -- For Snowflake: { "username": "...", "password": "...", "private_key": "..." }
    -- For Postgres: { "username": "...", "password": "..." }
    CREDENTIALS         VARIANT,
    
    -- Pool configuration
    POOL_SIZE           INT             DEFAULT 5,
    MAX_OVERFLOW        INT             DEFAULT 10,
    POOL_TIMEOUT        INT             DEFAULT 30,
    
    -- Status flags
    IS_DEFAULT          BOOLEAN         DEFAULT FALSE,  -- Default connection for this type
    IS_ACTIVE           BOOLEAN         DEFAULT TRUE,   -- Soft delete
    
    -- Audit fields
    CREATED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY          VARCHAR(100),
    
    -- Constraints
    PRIMARY KEY (CONNECTION_ID),
    UNIQUE (CONNECTION_NAME)
);

-- Add comment to table
COMMENT ON TABLE CONNECTIONS IS 'Connection definitions for Snowflake and Postgres databases with secure credential storage.';
COMMENT ON COLUMN CONNECTIONS.CREDENTIALS IS 'Encrypted credentials stored as VARIANT JSON. Access restricted via RBAC.';

-- ============================================================================
-- Schema Migrations (Idempotent Alterations)
-- ============================================================================
-- Add CREDENTIALS column if it doesn't exist (for existing installations)
ALTER TABLE CONNECTIONS ADD COLUMN IF NOT EXISTS CREDENTIALS VARIANT;

-- Add PGBOUNCER_PORT column if it doesn't exist (for PgBouncer support)
ALTER TABLE CONNECTIONS ADD COLUMN IF NOT EXISTS PGBOUNCER_PORT INT;

-- ============================================================================
-- Validation Queries
-- ============================================================================
-- Run after applying schema to verify tables exist

-- DESCRIBE TABLE CONNECTIONS;
-- SELECT COUNT(*) FROM CONNECTIONS;
