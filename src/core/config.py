from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    LOGS_DIR: str = Field(default="logs", description="Directory that logs are stored")
    LOG_TO_FILE: bool = Field(
        default=True, description="Flag to enable logging to file"
    )
    LOG_TO_CONSOLE: bool = Field(
        default=True, description="Flag to enable logging to console"
    )

    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)


settings = Settings()
