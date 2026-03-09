import tomllib
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

pyproject = Path(__file__).resolve().parent.parent.parent / "pyproject.toml"


class Settings(BaseSettings):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__VERSION__: str = self.get_version_number

    # K8S Settings
    NODE_ID: int = Field(
        default=1, description="Unique node ID (ordinal from pod name)"
    )
    NODE_PORT: int = Field(default=5003, description="Port for RPC server")
    NODE_HOST: str = Field(default="0.0.0.0", description="Host to bind to")
    API_PORT: int = Field(default=8080, description="Port for FastAPI server")

    CLUSTER_SIZE: int = Field(default=3, description="Number of nodes in cluster")
    POD_NAME: str = Field(
        default="", description="Kubernetes pod name (from downward API)"
    )
    SERVICE_NAME: str = Field(
        default="kvstore", description="Kubernetes service name for peer discovery"
    )
    NAMESPACE: str = Field(default="default", description="Kubernetes namespace")

    DATA_DIR: str = Field(
        default="data", description="Directory for persistent data (WAL)"
    )

    PEER_DISCOVERY_TIMEOUT: int = Field(
        default=30, description="Seconds to wait for peer discovery"
    )

    # Logging
    LOGS_DIR: str = Field(default="logs", description="Directory that logs are stored")
    LOG_TO_FILE: bool = Field(
        default=True, description="Flag to enable logging to file"
    )
    LOG_TO_CONSOLE: bool = Field(
        default=True, description="Flag to enable logging to console"
    )
    LOG_LEVEL: str = Field(
        default="INFO", description="Level at which logs are captured"
    )
    RPC_DEBUG: bool = Field(
        default=False, description="Enable verbose RPC debug logging"
    )

    # API
    ENABLE_DOCS: bool = Field(
        default=False, description="Enable documentation endpoint"
    )

    # Peer RPC reliability
    RPC_HTTP_CONNECT_TIMEOUT_SEC: float = Field(
        default=1.0, description="Peer RPC connect timeout in seconds"
    )
    RPC_HTTP_READ_TIMEOUT_SEC: float = Field(
        default=2.0, description="Peer RPC read timeout in seconds"
    )
    RPC_HTTP_TOTAL_TIMEOUT_SEC: float = Field(
        default=3.0, description="Peer RPC total request timeout in seconds"
    )
    RPC_HTTP_MAX_RETRIES: int = Field(
        default=2, description="Max retries for idempotent peer RPC calls"
    )
    RPC_HTTP_RETRY_BACKOFF_BASE_SEC: float = Field(
        default=0.05, description="Base backoff delay in seconds for retries"
    )
    RPC_HTTP_RETRY_BACKOFF_MAX_SEC: float = Field(
        default=0.5, description="Max backoff delay in seconds for retries"
    )

    # Snapshotting / compaction
    SNAPSHOT_ENABLED: bool = Field(
        default=True, description="Enable snapshot creation and WAL compaction"
    )
    SNAPSHOT_THRESHOLD: int = Field(
        default=500, description="Committed WAL entries threshold to trigger snapshot"
    )

    # CORS settings
    CORS_ALLOW_CREDENTIALS: bool = Field(
        default=True, description="Allow credentials in CORS"
    )
    CORS_ORIGINS: list[str] = Field(default=["*"], description="Allowed CORS origins")
    CORS_ALLOW_METHODS: list[str] = Field(
        default=["*"], description="Allowed CORS methods"
    )
    CORS_ALLOW_HEADERS: list[str] = Field(
        default=["*"], description="Allowed CORS headers"
    )

    model_config = SettingsConfigDict(case_sensitive=False)

    @property
    def get_version_number(self):
        with open(pyproject, "rb") as f:
            data = tomllib.load(f)

        return data["project"]["version"]


settings = Settings()
