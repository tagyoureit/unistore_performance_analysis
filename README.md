# Unistore Benchmark

> **Performance benchmarking tool for Snowflake and Postgres databases**
> "3DMark for databases" - Test Standard Tables, Hybrid Tables, Interactive
> Tables, and Postgres

![Version](https://img.shields.io/badge/version-0.1.0-blue)
![Python](https://img.shields.io/badge/python-3.11+-green)
![License](https://img.shields.io/badge/license-MIT-lightgrey)

## 🎯 Overview

Unistore Benchmark is a comprehensive performance testing tool designed to
benchmark and compare different Snowflake table types (Standard, Hybrid,
Interactive) and Postgres databases. It provides real-time metrics visualization,
configurable test scenarios, and side-by-side comparison of up to 5 test results.

### Key Features

- ⚡ **Real-time Dashboard** - Live metrics updates every 1 second via WebSocket
- 🔧 **Configurable Tests** - Select existing tables/views, pick warehouses, and
  tune workload parameters
- 📊 **Performance Metrics** - Operations/sec, latency percentiles (p50, p95,
  p99), throughput
- 🔄 **Comparison View** - Side-by-side comparison of up to 5 test configurations
- 📚 **Test Templates** - Pre-built scenarios including R180 POC template
- 💾 **Results Storage** - All test results stored in Snowflake for historical analysis
- 🖥️ **Mac Desktop App** - Standalone application, no Python installation required

## 🏗️ Architecture

**Tech Stack:**
- **Backend:** FastAPI (async Python web framework)
- **Frontend:** HTMX + Alpine.js (server-driven UI with client-side reactivity)
- **Styling:** Tailwind CSS
- **Charts:** Chart.js
- **Database:** Snowflake (primary), Postgres (optional)

**Why this stack?**
- No build step required (no npm, no webpack)
- Lightweight (~30KB total JS)
- Perfect for real-time WebSocket updates
- Easy to package as Mac desktop app

## 📋 Prerequisites

- Python 3.11 or higher
- [uv](https://github.com/astral-sh/uv) - Fast Python package manager
- Snowflake account with appropriate permissions
- (Optional) Postgres database for cross-database comparison

## 🚀 Quick Start

### 1. Clone and Setup

```bash
# Clone the repository
git clone <repository-url>
cd unistore_performance_analysis

# Install dependencies with uv
uv sync

# Create environment file from template
cp env.example .env
```

### 2. Configure Environment

Edit `.env` and add your Snowflake credentials:

```bash
SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=UNISTORE_BENCHMARK
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

### 3. Initialize Database Schema

```bash
# Run schema setup script
uv run python -m backend.setup_schema
```

### 4. Start the Application

```bash
# Development mode (with auto-reload)
uv run uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000

# Production mode
uv run uvicorn backend.main:app --host 0.0.0.0 --port 8000
```

### 5. Open Your Browser

Navigate to: <http://localhost:8000>

## 📖 Usage Guide

### Creating a Test

1. Click **"New Test"** in the navigation
2. Select table type (Standard/Hybrid/Interactive/Postgres)
3. Configure settings:
   - **Table:** Choose an existing database/schema/table (or view) from dropdowns
   - **Warehouse:** Size, multi-cluster, scaling policy
   - **Test Parameters:** Duration + load mode (fixed workers or auto-scale target)
   - **Queries, Mix, and Targets:** Templates store all SQL (4 canonical queries)
     and the per-query mix % + SLO targets (P95/P99 latency + error%).
     - **Mix preset:** Quickly adjusts weights (does not change SQL)
     - **Generate SQL for This Table Type:** Auto-fills the 4 canonical queries
       (point lookup / range scan / insert / update) to match the selected table
       and backend (Snowflake vs Postgres-family).
     - Preview-only: **no DB writes happen until you save the template**
     - If a usable key/time column can’t be detected, the affected SQL will be
       blank and its % set to 0 (toast will be yellow with details)
4. Click **"Start Test"**

**Note:** Views are supported for benchmarking, but they are read-only. Use
`READ_ONLY` workloads when selecting a view.

After saving a template, you can optionally run **"Prepare AI Workload (Pools +
Metadata)"** (or use **"Save & Prepare"**) to persist large value pools for
high-concurrency runs (stored in `TEMPLATE_VALUE_POOLS`) and avoid generating
values at runtime.

### Viewing Real-Time Results

The dashboard updates every 1 second with:
- Operations per second (read/write/query)
- Latency percentiles (p50, p95, p99, max)
- Throughput (rows/sec, MB/sec)
- Error rates
- Live charts

### Comparing Tests

1. Navigate to **"Compare"**
2. Search and select up to 5 completed tests
3. View side-by-side metrics comparison
4. Export comparison as PDF/CSV

### Using Templates

Pre-built templates available:
- **R180 POC** - Event processing with hybrid staging and standard archive
- **OLTP Simple** - Basic transactional workload
- **OLAP Analytics** - Complex analytical queries
- **Mixed Workload** - Concurrent reads and writes
- **High Concurrency** - Stress test for throughput

### Smoke Check (4 Variations)

Run a quick, on-demand smoke check across the four table-type variations
(STANDARD, HYBRID, INTERACTIVE, POSTGRES). This validates that each variation
completes and produces metrics, and prints an AI analysis summary per run.

The smoke runner is self-contained: it creates small smoke tables in
`RESULTS_DATABASE.SMOKE_DATA`, builds temporary templates, runs the tests, and
cleans up unless you opt to keep the data. Postgres smoke setup is attempted
only if a Postgres connection is available (otherwise it is skipped).

Requirements:
- App server running at `http://127.0.0.1:8000` (or set `BASE_URL`)
- SnowCLI installed and configured (the smoke setup uses `snow sql`)
- Results schema created via `uv run python -m backend.setup_schema`

```bash
task test:variations:smoke
```

Setup only (no tests):

```bash
task test:variations:setup
```

Cleanup only (drops smoke tables/templates):

```bash
task test:variations:cleanup
```

Optional overrides:

```bash
BASE_URL="http://127.0.0.1:8000" \
MAX_WAIT_SECONDS=300 \
POLL_INTERVAL_SECONDS=5 \
METRICS_WAIT_SECONDS=30 \
DURATION_SECONDS=45 \
WARMUP_SECONDS=0 \
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

## 🎨 Project Structure

```text
unistore_performance_analysis/
├── backend/
│   ├── api/routes/          # REST and WebSocket routes
│   ├── core/                # Test execution engine
│   │   ├── table_managers/  # Table type implementations
│   │   └── workload_generators/  # Workload generation
│   ├── connectors/          # Database connection pools
│   ├── models/              # Pydantic data models
│   ├── templates/           # Jinja2 HTML templates
│   └── static/              # CSS, JS, images
├── sql/
│   ├── schema/              # Results storage schema
│   └── templates/           # Test scenario templates
├── config/                  # Configuration files
├── tests/                   # Test suite
└── .plan/                   # Project planning documents
```

## 🔧 Configuration

### Table Type Configurations

**Standard & Hybrid (Snowflake):**
- Select an existing table (or view)
- The app introspects the object schema at runtime
- No table/index/clustering DDL is created by the app

**Interactive Tables:**
- CLUSTER BY requirements
- Interactive warehouse configuration
- Cache warming strategies
- 5-second query timeout handling

**Postgres (including Snowflake via Postgres protocol):**
- Select an existing schema + table/view
- Database selection is fixed by the configured Postgres connection

### Postgres Startup Behavior

Postgres is optional. By default, the app does **not** try to connect to Postgres
at startup.

- **POSTGRES_CONNECT_ON_STARTUP**: Set to `true` to initialize the Postgres pool
  during FastAPI startup (default: `false`).

### Warehouse Configurations

- Sizes: XSMALL, SMALL, MEDIUM, LARGE, XLARGE, 2XLARGE, 3XLARGE
- Multi-cluster: Min/max cluster count
- Scaling policy: Standard vs Economy
- Auto-suspend settings

### Test Scenarios

Create custom scenarios in `config/test_scenarios/*.yaml`:

```yaml
name: "My Custom Test"
description: "Test description"

tables:
  - name: my_table
    type: hybrid
    indexes:
      - columns: [id]
        primary: true
      - columns: [timestamp, user_id]

workload:
  duration: 300
  concurrency: 50
  load_pattern: steady
  
  operations:
    - type: read
      queries: ["SELECT * FROM my_table WHERE id = ?"]
      rate: 1000
    - type: write
      batch_size: 100
      rate: 500
```

## 📊 Metrics Collected

### Performance Metrics

- **Operations/Second:** Read, write, query throughput
- **Latency:** p50, p95, p99, max response times
- **Throughput:** Rows/sec, MB/sec
- **Errors:** Error count, error rate, error types

### Resource Metrics

- **Warehouse Utilization:** CPU, memory, concurrency
- **Connection Pool:** Active connections, pool saturation
- **Cache Statistics:** Hit rates, evictions (when available)

### Cost Metrics (when enabled)

- **Credit Consumption:** Warehouse compute credits
- **Storage Costs:** Data storage estimates
- **Total Cost:** Estimated cost per test

## 🧪 Testing

```bash
# Run all tests
uv run pytest

# Run specific test file
uv run pytest tests/test_connectors.py

# Run with coverage
uv run pytest --cov=backend --cov-report=html
```

## 📦 Building Mac Desktop App

```bash
# Install PyInstaller
uv pip install pyinstaller

# Build standalone app
pyinstaller --onefile --windowed \
  --name "Unistore Benchmark" \
  --icon assets/icon.icns \
  backend/main.py

# App will be in dist/ folder
open dist/Unistore\ Benchmark.app
```

## 🐛 Troubleshooting

### Connection Issues

**Snowflake connection fails:**
- Check credentials in `.env`
- Verify network connectivity
- Ensure warehouse is running
- Check role has necessary privileges

**WebSocket disconnects:**
- Check firewall settings
- Increase `WS_PING_INTERVAL` in `.env`
- Verify stable network connection

### Performance Issues

**Dashboard slow to update:**
- Reduce `METRICS_INTERVAL_SECONDS` (but increases DB load)
- Check browser console for errors
- Verify WebSocket connection is stable

**High concurrency stalls at start (connection spin-up):**
- The benchmark creates a **dedicated per-test Snowflake pool** sized to the
  requested concurrency.
- If startup is slow, reduce `SNOWFLAKE_POOL_MAX_PARALLEL_CREATES` to avoid
  overwhelming the client with too many concurrent `connect()` calls.
- Ensure results persistence has its own threads via
  `SNOWFLAKE_RESULTS_EXECUTOR_MAX_WORKERS`.

**Tests timeout:**
- Increase warehouse size
- Reduce concurrency level
- Check for long-running queries
- Verify adequate connection pool size / executor capacity:
  - `SNOWFLAKE_BENCHMARK_EXECUTOR_MAX_WORKERS` must be >= requested concurrency
    per node (to avoid app-side queueing)

**Need to simulate thousands of users:**
- See `docs/scaling.md` for the current concurrency model and the recommended
  multi-process/multi-node approach.

## 📚 Additional Resources

- [Project Plan](.plan/project-plan.md) - Detailed development roadmap
- [API Documentation](docs/api.md) - REST and WebSocket API reference
- [Test Scenarios Guide](docs/scenarios.md) - Creating custom test scenarios
- [Performance Tuning](docs/performance.md) - Optimization tips
- [Scaling & Concurrency Model](docs/scaling.md) - How to run high concurrency
  and when to scale out

### External Documentation

- [Snowflake Hybrid Tables Best Practices](https://docs.snowflake.com/en/user-guide/tables-hybrid-best-practices)
- [Snowflake Interactive Tables](https://docs.snowflake.com/en/user-guide/interactive)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [HTMX Documentation](https://htmx.org/docs/)
- [Alpine.js Documentation](https://alpinejs.dev/)

## 🤝 Contributing

Contributions welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for
guidelines.

## 📄 License

MIT License - see [LICENSE](LICENSE) for details

## 🙏 Acknowledgments

- Built with [FastAPI](https://fastapi.tiangolo.com/)
- UI powered by [HTMX](https://htmx.org/) and [Alpine.js](https://alpinejs.dev/)
- Charts by [Chart.js](https://www.chartjs.org/)
- Package management by [uv](https://github.com/astral-sh/uv)

## 📞 Support

For issues, questions, or feature requests:
- Open an issue on GitHub
- Contact the development team

---

**Status:** 🚧 Active Development  
**Version:** 0.1.0  
**Last Updated:** 2025-12-17
