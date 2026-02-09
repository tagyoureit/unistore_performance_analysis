"""
Application Configuration using Pydantic Settings

Loads configuration from environment variables with sensible defaults.
"""

from typing import List
from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.
    """

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="ignore"
    )

    # ========================================================================
    # Snowflake Connection Settings
    # ========================================================================
    SNOWFLAKE_ACCOUNT: str = "your_account.region"
    SNOWFLAKE_USER: str = "your_username"
    SNOWFLAKE_PASSWORD: str = ""
    SNOWFLAKE_WAREHOUSE: str = "COMPUTE_WH"
    SNOWFLAKE_DATABASE: str = "FLAKEBENCH"
    SNOWFLAKE_SCHEMA: str = "PUBLIC"
    SNOWFLAKE_ROLE: str = "ACCOUNTADMIN"

    # Connector-level timeouts (seconds).
    #
    # These limit how long the Snowflake Python connector will block on login/network/socket
    # operations. Keeping these bounded helps ensure `uvicorn --reload` can shut down cleanly
    # even if Snowflake connectivity is degraded.
    SNOWFLAKE_CONNECT_LOGIN_TIMEOUT: int = 15
    SNOWFLAKE_CONNECT_NETWORK_TIMEOUT: int = 60
    SNOWFLAKE_CONNECT_SOCKET_TIMEOUT: int = 60

    # Benchmark/workload connector timeouts (seconds).
    #
    # These apply to the per-test pool used by workload workers. They should be
    # high enough that we observe warehouse-side queueing/slowdowns instead of
    # client-side timeouts.
    SNOWFLAKE_BENCHMARK_CONNECT_LOGIN_TIMEOUT: int = 15
    SNOWFLAKE_BENCHMARK_CONNECT_NETWORK_TIMEOUT: int = 300
    SNOWFLAKE_BENCHMARK_CONNECT_SOCKET_TIMEOUT: int = 300

    # Optional key-pair authentication
    SNOWFLAKE_PRIVATE_KEY_PATH: str = ""
    SNOWFLAKE_PRIVATE_KEY_PASSPHRASE: str = ""

    # ========================================================================
    # Postgres Connection Settings
    # ========================================================================
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    # Fallback database if template config doesn't specify one.
    # Templates now use the database selected in the UI dropdown.
    POSTGRES_DATABASE: str = "postgres"
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = ""

    # If True, attempt to connect/init the Postgres pool during FastAPI startup.
    # Default is False so local development doesn't error/hang when Postgres isn't running.
    POSTGRES_CONNECT_ON_STARTUP: bool = False

    # ========================================================================
    # Application Settings
    # ========================================================================
    APP_HOST: str = "127.0.0.1"
    APP_PORT: int = 8000
    APP_DEBUG: bool = True
    APP_RELOAD: bool = True

    # ========================================================================
    # WebSocket Settings
    # ========================================================================
    WS_PING_INTERVAL: int = 30
    WS_PING_TIMEOUT: int = 10

    # ========================================================================
    # Live Metrics Settings
    # ========================================================================
    LIVE_METRICS_CACHE_TTL_SECONDS: float = 5.0
    LIVE_METRICS_POST_URL: str = ""
    LIVE_METRICS_POST_TIMEOUT_SECONDS: float = 0.5

    # ========================================================================
    # Connection Pool Settings
    # ========================================================================
    SNOWFLAKE_POOL_SIZE: int = 5
    SNOWFLAKE_MAX_OVERFLOW: int = 10
    SNOWFLAKE_POOL_TIMEOUT: int = 30
    SNOWFLAKE_POOL_RECYCLE: int = 3600
    # Limit how many blocking Snowflake connect() calls we run concurrently.
    SNOWFLAKE_POOL_MAX_PARALLEL_CREATES: int = 32

    # Thread executors:
    # - Results pool: persistence + UI reads from results tables
    # - Benchmark pool: workload execution against the selected warehouse
    #
    # NOTE: The Snowflake Python connector is synchronous; we run it in a thread pool.
    # To avoid client-side queueing (which hides DB-side queueing), the benchmark executor
    # must have enough threads to match requested concurrency.
    SNOWFLAKE_RESULTS_EXECUTOR_MAX_WORKERS: int = 16
    SNOWFLAKE_BENCHMARK_EXECUTOR_MAX_WORKERS: int = 256

    POSTGRES_POOL_MIN_SIZE: int = 5
    POSTGRES_POOL_MAX_SIZE: int = 20

    # ========================================================================
    # Test Execution Settings
    # ========================================================================
    DEFAULT_TEST_DURATION: int = 300
    DEFAULT_CONCURRENCY: int = 10
    DEFAULT_WAREHOUSE_SIZE: str = "XSMALL"

    METRICS_INTERVAL_SECONDS: int = 1
    METRICS_BUFFER_SIZE: int = 1000

    # ========================================================================
    # Storage Settings
    # ========================================================================
    RESULTS_DATABASE: str = "FLAKEBENCH"
    RESULTS_SCHEMA: str = "TEST_RESULTS"

    TEMP_DIR: str = "/tmp/flakebench"

    # ========================================================================
    # Security Settings
    # ========================================================================
    CORS_ORIGINS: List[str] = []
    SECRET_KEY: str = "your_secret_key_here_change_this_in_production"

    @field_validator("CORS_ORIGINS", mode="before")
    @classmethod
    def _build_cors_origins(cls, v, info):
        if v:
            if isinstance(v, str):
                import json

                return json.loads(v)
            return v
        host = info.data.get("APP_HOST", "127.0.0.1")
        port = info.data.get("APP_PORT", 8000)
        origins = [f"http://{host}:{port}"]
        if host == "127.0.0.1":
            origins.append(f"http://localhost:{port}")
        elif host == "localhost":
            origins.append(f"http://127.0.0.1:{port}")
        return origins

    # ========================================================================
    # Logging Settings
    # ========================================================================
    LOG_LEVEL: str = "INFO"
    LOG_FILE: str = "flakebench.log"
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # ========================================================================
    # Feature Flags
    # ========================================================================
    ENABLE_INTERACTIVE_TABLES: bool = False
    ENABLE_POSTGRES: bool = True
    ENABLE_COST_ESTIMATION: bool = True
    ENABLE_QUERY_PROFILING: bool = True

    # ========================================================================
    # Cost Estimation Settings
    # ========================================================================
    # Default cost per Snowflake credit (Enterprise tier typical pricing)
    COST_DOLLARS_PER_CREDIT: float = 4.00
    # Display currency (USD only for now)
    COST_DISPLAY_CURRENCY: str = "USD"
    # Whether to show raw credits alongside dollar amounts
    COST_SHOW_CREDITS: bool = True

    # Snowflake Postgres Compute defaults
    # Instance sizes use Postgres-specific naming from Table 1(i):
    # STANDARD_M, STANDARD_L, STANDARD_XL, STANDARD_2X, STANDARD_4XL, etc.
    # HIGHMEM_L, HIGHMEM_XL, HIGHMEM_2XL, etc.
    # BURST_XS, BURST_S, BURST_M
    # You can also use traditional warehouse-size aliases: SMALL, MEDIUM, LARGE, etc.
    POSTGRES_INSTANCE_SIZE: str = "STANDARD_M"  # Default instance size for cost estimation


# Create global settings instance
settings = Settings()
