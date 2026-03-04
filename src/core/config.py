from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    LOGS_DIR: str = Field(default="logs", description="Directory that logs are stored")
    LOG_TO_FILE: bool = Field(
        default=True, description="Flag to enable logging to file"
    )
    LOG_TO_CONSOLE: bool = Field(
        default=True, description="Flag to enable logging to console"
    )
    LOG_LEVEL: str = Field(
        default="DEBUG", description="Level at which logs are captured"
    )
    RPC_DEBUG: bool = Field(
        default=True, description="Enable verbose RPC debug logging"
    )

    model_config = SettingsConfigDict(case_sensitive=False)


settings = Settings()
