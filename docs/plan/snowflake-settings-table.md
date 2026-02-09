# Snowflake Settings Table Architecture

Moving application configuration from `.env` file to a Snowflake-based settings table with a UI-driven settings menu.

**Status**: ⬜ Not Started  
**Priority**: Medium  
**Complexity**: High  

## Problem Statement

Currently, all application configuration lives in the `.env` file:
- Credentials (Snowflake, Postgres, Snowflake Postgres)
- Pool sizing and timeouts
- Feature flags
- Cost estimation defaults

This approach has limitations:
- No UI for configuration changes (requires file edits and restart)
- Single connection per database type (no multi-account support)
- Cost preferences stored only in browser localStorage (per-device)
- No team sharing of connection configurations

## Current Architecture

| Setting Category | Current Storage | Sensitivity |
|-----------------|-----------------|-------------|
| App/WebSocket config | `.env` → `config.py` | Non-sensitive |
| Snowflake credentials | `.env` → `config.py` | **Sensitive** |
| Postgres credentials | `.env` → `config.py` | **Sensitive** |
| Snowflake Postgres credentials | `.env` → `config.py` | **Sensitive** |
| Pool sizing/timeouts | `.env` → `config.py` | Non-sensitive |
| Cost settings (dollars/credit) | Browser localStorage | User preference |
| Feature flags | `.env` → `config.py` | Non-sensitive |

### Key Files

- `backend/config.py`: Main configuration with all env var definitions (199 lines)
- `backend/main.py`: Startup/shutdown lifecycle, pool initialization
- `backend/static/js/cost-utils.js`: Client-side cost settings via localStorage
- `backend/connectors/snowflake_pool.py`: Snowflake connection pooling
- `backend/connectors/postgres_pool.py`: Postgres connection pooling (both types)

## Impact Analysis

### 1. Installation/Setup Impact

**Current Flow:**
1. User copies `env.example` → `.env`
2. User fills in credentials
3. App starts and loads settings from `.env`

**Proposed Flow:**

| Scenario | Complexity | Issue |
|----------|------------|-------|
| **Fresh install** | **HIGH** | Bootstrap problem - need credentials to connect to Snowflake to get credentials |
| **Existing users** | Medium | One-time migration, then settings in UI |
| **CI/CD deployments** | HIGH | Environment variables are standard; now need setup scripts |
| **Air-gapped environments** | HIGH | Can't fetch settings without connectivity |

**Bootstrap Problem (Critical):**

```
To read settings from Snowflake → You need Snowflake credentials
To get Snowflake credentials → You need to read settings from Snowflake
```

**Solution Options:**

| Approach | Trade-off |
|----------|-----------|
| **Minimal bootstrap `.env`** | Keep only `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD` (or key-pair) in `.env`. All other settings in Snowflake. |
| **First-run wizard** | UI prompts for initial credentials, stores in Snowflake, subsequent launches read from table |
| **Hybrid fallback** | Try Snowflake table first, fall back to `.env` if unavailable |

**Recommendation:** Keep **connection credentials** in `.env` (or environment variables), move **everything else** to Snowflake.

### 2. Credential Security

**The Problem:** Storing credentials for external databases (Postgres, other Snowflake connections) in Snowflake.

**Options:**

| Method | Security Level | Implementation |
|--------|---------------|----------------|
| **Plain text in table** | VERY LOW | Never do this |
| **Snowflake Secrets** | **HIGH** | `CREATE SECRET` - encrypted at rest, access-controlled |
| **External Secret Manager** | HIGH | HashiCorp Vault, AWS Secrets Manager + external access integration |
| **Application-level encryption** | Medium | Encrypt before storing, app holds key (key management problem) |
| **Dynamic Data Masking** | Medium | Mask credentials in queries, but still stored |

**Snowflake Secrets Limitation:**

```sql
-- Create a secret for Postgres credentials
CREATE OR REPLACE SECRET SETTINGS.SECRETS.POSTGRES_MAIN
  TYPE = PASSWORD
  USERNAME = 'postgres_user'
  PASSWORD = 'p@ssw0rd';

-- Secrets can only be used in certain contexts (external functions, 
-- Snowpark Container Services) - NOT directly queryable!
```

Snowflake Secrets are designed for UDFs/external functions, **not** for application retrieval. You cannot `SELECT` the password value.

**Practical Credential Storage Options:**

| Option | How It Works | Pros | Cons |
|--------|--------------|------|------|
| **Browser-side only** | Credentials never leave browser, stored in localStorage/IndexedDB | Most secure for credentials | Each user must configure, can't share across team |
| **Encrypted in Snowflake** | App encrypts with user-derived key, stores ciphertext in table | Shareable, at-rest encryption | Key management, decrypt on fetch |
| **External Secret Manager** | Store reference in Snowflake, fetch from Vault/AWS SM at runtime | Enterprise-grade | Complex setup, external dependency |

**Recommendation:** 
- **Non-sensitive settings** → Snowflake table (pools, timeouts, feature flags, cost settings)
- **Credentials** → Browser localStorage (per-user) OR environment variables (server deployment)

### 3. Cost Information Storage

**Current Implementation:**

Browser `localStorage` with key `unistore_cost_settings`:

```javascript
{
  dollarsPerCredit: 4.0,
  showCredits: true,
  currency: "USD"
}
```

Located in `backend/static/js/cost-utils.js` (lines 181-228).

**Moving to Snowflake:**

| Aspect | Benefit | Drawback |
|--------|---------|----------|
| **Persistence** | Survives browser clear, accessible from any device | Requires Snowflake connection |
| **Sharing** | Team can share cost config | Per-user preferences harder |
| **Default values** | Central default for org | Individual overrides need user ID |

**Hybrid Approach (Recommended):**
1. Store **organization defaults** in Snowflake table
2. Load defaults on app init
3. Allow **user overrides** in localStorage (persists locally)
4. Frontend priority: `localStorage || snowflakeDefaults`

### 4. Multi-Connection Architecture

**Current State:**
- 1 Snowflake connection (from `.env`)
- 1 Postgres connection (from `.env`)  
- 1 Snowflake Postgres connection (from `.env`)

**Proposed: Connection Registry**

Benefits:
- Multiple Snowflake accounts (dev/prod/customer)
- Multiple Postgres instances
- Template-to-connection binding
- Team collaboration on connection configs

**Credential Handling for Multi-Connection:**

```javascript
// Browser localStorage structure
const connectionCredentials = {
  "conn-uuid-1": { username: "...", password: "..." },
  "conn-uuid-2": { username: "...", password: "..." },
};
localStorage.setItem("unistore_connection_credentials", 
  JSON.stringify(connectionCredentials));
```

**Backend API:**
```python
# POST /api/connections/{connection_id}/test
# Body: { "username": "...", "password": "..." }
# Response: { "status": "success", "message": "Connected successfully" }

# Credentials are NEVER persisted server-side, only used for connection test
```

## Proposed Schema

### Settings Table

```sql
CREATE TABLE SETTINGS.CONFIG.APP_SETTINGS (
  setting_key   VARCHAR(100) PRIMARY KEY,
  setting_value VARIANT,
  category      VARCHAR(50),
  description   VARCHAR(500),
  updated_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  updated_by    VARCHAR(100)
);

-- Example data
INSERT INTO SETTINGS.CONFIG.APP_SETTINGS VALUES
  ('pool.snowflake.size', '5', 'pool', 'Snowflake connection pool size', CURRENT_TIMESTAMP(), 'system'),
  ('pool.snowflake.max_overflow', '10', 'pool', 'Max overflow connections', CURRENT_TIMESTAMP(), 'system'),
  ('pool.snowflake.timeout', '30', 'pool', 'Pool checkout timeout (seconds)', CURRENT_TIMESTAMP(), 'system'),
  ('cost.dollars_per_credit', '4.00', 'cost', 'Default cost per Snowflake credit', CURRENT_TIMESTAMP(), 'system'),
  ('cost.show_credits', 'true', 'cost', 'Show credits alongside dollar amounts', CURRENT_TIMESTAMP(), 'system'),
  ('cost.display_currency', '"USD"', 'cost', 'Display currency for costs', CURRENT_TIMESTAMP(), 'system'),
  ('feature.enable_postgres', 'true', 'feature', 'Enable Postgres benchmarks', CURRENT_TIMESTAMP(), 'system'),
  ('feature.enable_cost_estimation', 'true', 'feature', 'Enable cost estimation', CURRENT_TIMESTAMP(), 'system');
```

### Connections Table

```sql
CREATE TABLE SETTINGS.CONFIG.CONNECTIONS (
  connection_id    VARCHAR(36) DEFAULT UUID_STRING(),
  connection_name  VARCHAR(100) NOT NULL UNIQUE,
  connection_type  VARCHAR(20) NOT NULL,  -- 'SNOWFLAKE' or 'POSTGRES'
  
  -- Connection details (non-sensitive)
  host             VARCHAR(255),
  port             INT,
  account          VARCHAR(100),  -- Snowflake account identifier
  database_name    VARCHAR(100),
  schema_name      VARCHAR(100),
  warehouse        VARCHAR(100),
  role             VARCHAR(100),
  
  -- Pool configuration
  pool_size        INT DEFAULT 5,
  max_overflow     INT DEFAULT 10,
  pool_timeout     INT DEFAULT 30,
  
  -- Metadata
  is_default       BOOLEAN DEFAULT FALSE,
  is_active        BOOLEAN DEFAULT TRUE,
  created_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  updated_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  created_by       VARCHAR(100),
  
  PRIMARY KEY (connection_id)
);

-- Template-to-connection mapping
ALTER TABLE TEST_TEMPLATES ADD COLUMN connection_id VARCHAR(36);
```

### Cost Defaults Table (Optional)

```sql
CREATE TABLE SETTINGS.CONFIG.COST_DEFAULTS (
  setting_key      VARCHAR(50) PRIMARY KEY,
  setting_value    VARIANT,
  updated_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO SETTINGS.CONFIG.COST_DEFAULTS VALUES
  ('dollars_per_credit', '4.00', CURRENT_TIMESTAMP()),
  ('show_credits', 'true', CURRENT_TIMESTAMP()),
  ('display_currency', '"USD"', CURRENT_TIMESTAMP());
```

## Recommended Architecture

```
+-------------------------------------------------------------+
|                         .env (minimal)                       |
|  SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD       |
|  (or SNOWFLAKE_PRIVATE_KEY_PATH)                             |
+----------------------------+--------------------------------+
                             | Bootstrap connection
                             v
+-------------------------------------------------------------+
|                    Snowflake Settings Tables                 |
|  +-----------------+  +-----------------+  +---------------+ |
|  |  APP_SETTINGS   |  |   CONNECTIONS   |  | COST_DEFAULTS | |
|  |  (feature flags,|  |  (host, port,   |  | ($/credit,    | |
|  |   pool sizes,   |  |   database,     |  |  show_credits)| |
|  |   timeouts)     |  |   warehouse)    |  |               | |
|  +-----------------+  +-----------------+  +---------------+ |
+----------------------------+--------------------------------+
                             |
                             v
+-------------------------------------------------------------+
|                     Browser localStorage                     |
|  +--------------------------+  +---------------------------+ |
|  | connection_credentials   |  | user_cost_overrides       | |
|  | {conn_id: {user, pass}}  |  | {dollarsPerCredit: 4.0}   | |
|  +--------------------------+  +---------------------------+ |
+-------------------------------------------------------------+
```

## Potential Issues & Mitigations

| Issue | Severity | Mitigation |
|-------|----------|------------|
| **Bootstrap problem** | HIGH | Keep bootstrap credentials in `.env` |
| **Credential security** | HIGH | Browser-only storage for sensitive data |
| **Offline/disconnected** | MEDIUM | Cache last-known settings in localStorage |
| **Migration complexity** | MEDIUM | Provide migration script, backward-compatible fallback |
| **Performance** | LOW | Cache settings at app startup, refresh on demand |
| **Multi-user conflicts** | MEDIUM | Per-user preferences vs. global defaults |
| **Table doesn't exist** | HIGH | Auto-create on first run with safe defaults |

## Implementation Phases

### Phase 1: Schema & Backend Infrastructure ⬜

- [ ] Create `SETTINGS` database and `CONFIG` schema
- [ ] Create `APP_SETTINGS` table with initial data
- [ ] Create `CONNECTIONS` table
- [ ] Create `backend/core/settings_manager.py`
  - [ ] `load_settings_from_snowflake()`
  - [ ] `get_setting(key, default)`
  - [ ] `update_setting(key, value)`
  - [ ] `list_settings(category=None)`
- [ ] Create `backend/core/connection_manager.py`
  - [ ] `list_connections()`
  - [ ] `get_connection(connection_id)`
  - [ ] `create_connection()`
  - [ ] `update_connection()`
  - [ ] `delete_connection()`
  - [ ] `test_connection()` (accepts runtime credentials)
- [ ] Implement fallback chain: Snowflake table → `.env` → defaults
- [ ] Auto-create tables on first run if missing

### Phase 2: API Layer ⬜

- [ ] Create `/api/settings` routes
  - [ ] `GET /api/settings` - List all settings
  - [ ] `GET /api/settings/{key}` - Get single setting
  - [ ] `PUT /api/settings/{key}` - Update setting
- [ ] Create `/api/connections` routes
  - [ ] `GET /api/connections` - List connections
  - [ ] `POST /api/connections` - Create connection
  - [ ] `GET /api/connections/{id}` - Get connection details
  - [ ] `PUT /api/connections/{id}` - Update connection
  - [ ] `DELETE /api/connections/{id}` - Delete connection
  - [ ] `POST /api/connections/{id}/test` - Test connection (credentials in body)
- [ ] Add connection_id parameter to template endpoints

### Phase 3: Frontend - Settings UI ⬜

- [ ] Create settings page (`/settings`)
  - [ ] General settings tab (pool sizes, timeouts, feature flags)
  - [ ] Cost settings tab (dollars/credit, show credits, currency)
  - [ ] Connections tab (list, add, edit, delete, test)
- [ ] Connection management UI
  - [ ] Connection list with status indicators
  - [ ] Add/Edit connection modal
  - [ ] Secure credential input (never sent to Snowflake)
  - [ ] Connection test button with feedback
- [ ] Update template editor
  - [ ] Connection dropdown selector
  - [ ] Show connection details on hover

### Phase 4: Migration & Cleanup ⬜

- [ ] Create migration script for existing users
  - [ ] Read current `.env` values
  - [ ] Populate `APP_SETTINGS` table
  - [ ] Create default connections from existing `.env` credentials
- [ ] Update documentation
  - [ ] Installation guide updates
  - [ ] Settings management guide
  - [ ] Connection management guide
- [ ] Update `env.example` to minimal bootstrap only
- [ ] Add deprecation warnings for settings that moved to UI

## Open Questions

1. **Multi-user access**: Should settings be per-user or global? (Recommend: global with user overrides for cost)
2. **Audit trail**: Should we track who changed which setting and when? (Recommend: yes, add `updated_by` column)
3. **Connection sharing**: Can users share connections without sharing credentials? (Recommend: yes, credentials in browser only)
4. **Default connection**: How to handle the bootstrap connection vs. user-defined connections? (Recommend: bootstrap connection is implicit "system" connection)

## Acceptance Criteria

- [ ] Fresh install works with minimal `.env` (account, user, password only)
- [ ] All non-credential settings manageable via UI
- [ ] Multiple connections can be defined and assigned to templates
- [ ] Credentials never stored in Snowflake (browser-only)
- [ ] Cost defaults persist in Snowflake, user overrides in localStorage
- [ ] Backward compatible: existing `.env` configs continue to work
- [ ] Connection test validates connectivity without persisting credentials
