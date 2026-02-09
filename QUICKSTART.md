# Quick Start Guide

## 🚀 Running the Application

### 1. First Time Setup

```bash
# Install dependencies
uv sync

# Create your environment file
cp env.example .env

# Edit .env with your Snowflake credentials
nano .env  # or use your favorite editor
```

### 2. Start the Server

```bash
# Development mode (with auto-reload)
uv run uvicorn backend.main:app --reload --host 127.0.0.1 --port 8000 --log-config logging_config.yaml
```

The server will start at: <http://localhost:8000>

### 3. Test the Server

```bash
# Run setup tests
uv run python tests/test_app_setup.py

# Or check health endpoint
curl http://localhost:8000/health
```

## 📡 Available Endpoints

- **Home:** <http://localhost:8000>
- **Health Check:** <http://localhost:8000/health>
- **API Info:** <http://localhost:8000/api/info>
- **API Docs:** <http://localhost:8000/api/docs> (interactive Swagger UI)
- **ReDoc:** <http://localhost:8000/api/redoc> (alternative API docs)
- **WebSocket Test:** <ws://localhost:8000/ws/test/{test_id}>

## 🔧 Configuration

Edit `.env` file to configure:

### Required Settings

```bash
SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=FLAKEBENCH
```

### Security Settings

```bash
# Encryption key for stored credentials (32 characters)
# If not set, a default key is used (not secure for production!)
FLAKEBENCH_CREDENTIAL_KEY=YourSecure32CharacterKeyHere!!!
```

**Important:** Connection credentials (passwords, private keys) are encrypted using AES-256-GCM before storage in Snowflake. 

| Scenario | Behavior |
|----------|----------|
| No key set | Uses default key, shows warning in UI |
| Key set | Uses your key, no warning |
| Key changed after credentials stored | **Existing credentials become unreadable** - you'll need to re-enter them |

For production deployments, always set `FLAKEBENCH_CREDENTIAL_KEY` to a unique 32-character value.

### Optional Settings

- Test defaults (duration, concurrency)
- Logging configuration
- Feature flags

## 🔐 Connection Management

FlakeBench supports two ways to configure database connections:

### 1. Environment Variables (Default/Fallback)

Configure credentials in `.env` for the control plane and backward-compatible templates:

```bash
# Snowflake connection (for results storage and legacy benchmarks)
SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH

# Optional: Postgres connection
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
```

### 2. Stored Connections (Recommended for Benchmarks)

Stored connections allow you to:
- Manage multiple database connections in the UI
- Select which connection to use per benchmark template
- Keep credentials encrypted in Snowflake (not in local files)

**To add a connection:**
1. Go to **Settings** → **Connections**
2. Click **Add Connection**
3. Enter connection details:
   - **Name**: A friendly identifier (e.g., "Production Snowflake")
   - **Type**: SNOWFLAKE or POSTGRES
   - **Account/Host**: Snowflake account identifier or Postgres hostname
   - **Role**: Snowflake role (optional)
   - **Credentials**: Username and password (encrypted at rest)

**Using connections in benchmarks:**
1. When creating or editing a template on the **Configure** page
2. Select a connection from the **Connection** dropdown
3. The benchmark will use credentials from the stored connection
4. If no connection is selected, credentials from `.env` are used (backward compatible)

### Architecture Notes

- **Control plane** (results storage, orchestrator) always uses `.env` credentials
- **Benchmark execution** can use either stored connections or `.env` credentials
- **Database/schema/warehouse** are configured per-template, not per-connection
- Connections only store authentication credentials (account, user, password, role)

## 🧪 Testing

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=backend

# Run specific test file
uv run pytest tests/test_app_setup.py
```

### Smoke Check (4 Variations)

Requires the server running locally (default base URL is `http://127.0.0.1:8000`).
Smoke setup uses SnowCLI (`snow sql`) and assumes results tables were created via
`uv run python -m backend.setup_schema`.

```bash
task test:variations:smoke
```

Setup only (no tests):

```bash
task test:variations:setup
```

Cleanup only:

```bash
task test:variations:cleanup
```

Overrides:

```bash
BASE_URL="http://127.0.0.1:8000" \
MAX_WAIT_SECONDS=300 \
POLL_INTERVAL_SECONDS=5 \
DURATION_SECONDS=45 \
WARMUP_SECONDS=0 \
METRICS_WAIT_SECONDS=30 \
SMOKE_ROWS=300 \
SMOKE_SCHEMA=SMOKE_DATA \
SMOKE_WAREHOUSE=SMOKE_WH \
SMOKE_CONCURRENCY=5 \
KEEP_SMOKE_DATA=true \
SKIP_POSTGRES=true \
task test:variations:smoke
```

Long smoke test:

```bash
task test:variations:smoke:long
```

## 🐛 Troubleshooting

### Port Already in Use

```bash
# Kill process on port 8000
lsof -ti:8000 | xargs kill -9

# Or use a different port
uv run uvicorn backend.main:app --port 8001 --log-config logging_config.yaml
```

### Import Errors

```bash
# Make sure you're in the project root
cd /Users/rgoldin/Programming/unistore_performance_analysis

# Reinstall dependencies
uv sync
```

### Configuration Not Loading

```bash
# Check that .env file exists
ls -la .env

# Verify environment variables
uv run python -c "from backend.config import settings; print(settings.APP_HOST)"
```

## 📝 Development Workflow

1. **Make changes** to backend code
2. **Server auto-reloads** (if using --reload flag)
3. **Test changes** at <http://localhost:8000>
4. **Check logs** in terminal output
5. **Run tests** with pytest

## 🎯 Next Steps

### Phase 1.3: Database Connectors

- Create Snowflake connection pool
- Create Postgres connection pool
- Test database connectivity

### Phase 1.4: Data Models

- Define Pydantic models for configurations
- Define models for test results
- Define models for metrics

See `.plan/project-plan.md` for full roadmap.

## 📚 Resources

- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [uv Documentation](https://github.com/astral-sh/uv)
- [Project Plan](.plan/project-plan.md)
- [README](README.md)
