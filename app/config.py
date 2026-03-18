from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "junex"
    log_level: str = "INFO"
    database_url: str = "postgresql+psycopg://postgres:postgres@localhost:5432/junex"
    api_base_url: str = "https://example.com"
    api_timeout_seconds: float = Field(default=10.0, gt=0)
    dataset_a_path: str = "/dataset-a"
    dataset_b_path: str = "/dataset-b"
    jquants_api_base_url: str = "https://api.jquants.com"
    jquants_api_key: str = ""
    jquants_timeout_seconds: float = Field(default=10.0, gt=0)
