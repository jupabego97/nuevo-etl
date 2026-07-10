"""Configuración centralizada con validación estricta."""

from __future__ import annotations

import base64
from functools import lru_cache

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = Field(..., alias="DATABASE_URL")
    db_schema: str = Field(default="alegra_etl", alias="DB_SCHEMA")
    company_id: int = Field(default=1, alias="COMPANY_ID")

    alegra_api_key: SecretStr | None = Field(default=None, alias="ALEGRA_API_KEY")
    alegra_email: str | None = Field(default=None, alias="ALEGRA_EMAIL")
    alegra_token: SecretStr | None = Field(default=None, alias="ALEGRA_TOKEN")
    alegra_base_url: str = Field(
        default="https://api.alegra.com/api/v1",
        alias="ALEGRA_BASE_URL",
    )

    sync_overlap_days: int = Field(default=7, alias="SYNC_OVERLAP_DAYS", ge=1, le=90)
    sync_page_size: int = Field(default=30, alias="SYNC_PAGE_SIZE", ge=1, le=30)
    sync_max_concurrent: int = Field(default=5, alias="SYNC_MAX_CONCURRENT", ge=1, le=20)
    sync_request_timeout_seconds: int = Field(
        default=30,
        alias="SYNC_REQUEST_TIMEOUT_SECONDS",
        ge=5,
        le=120,
    )

    webhook_secret: SecretStr = Field(..., alias="WEBHOOK_SECRET")
    webhook_host: str = Field(default="0.0.0.0", alias="WEBHOOK_HOST")
    webhook_port: int = Field(default=8000, alias="WEBHOOK_PORT")

    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_json: bool = Field(default=True, alias="LOG_JSON")
    alert_webhook_url: str | None = Field(default=None, alias="ALERT_WEBHOOK_URL")

    enable_accounting: bool = Field(default=True, alias="ENABLE_ACCOUNTING")
    enable_banks: bool = Field(default=True, alias="ENABLE_BANKS")
    enable_global_invoices: bool = Field(default=False, alias="ENABLE_GLOBAL_INVOICES")
    enable_transportation_receipts: bool = Field(
        default=False,
        alias="ENABLE_TRANSPORTATION_RECEIPTS",
    )

    @field_validator("database_url")
    @classmethod
    def normalize_database_url(cls, value: str) -> str:
        if value.startswith("postgres://"):
            return value.replace("postgres://", "postgresql+psycopg://", 1)
        if value.startswith("postgresql://") and "+psycopg" not in value:
            return value.replace("postgresql://", "postgresql+psycopg://", 1)
        return value

    @model_validator(mode="after")
    def validate_alegra_credentials(self) -> Settings:
        has_key = self.alegra_api_key is not None and self.alegra_api_key.get_secret_value()
        has_pair = self.alegra_email and self.alegra_token
        if not has_key and not has_pair:
            raise ValueError(
                "Configura ALEGRA_API_KEY o el par ALEGRA_EMAIL + ALEGRA_TOKEN"
            )
        return self

    def alegra_authorization_header(self) -> str:
        if self.alegra_api_key and self.alegra_api_key.get_secret_value():
            return f"Basic {self.alegra_api_key.get_secret_value()}"
        assert self.alegra_email and self.alegra_token
        raw = f"{self.alegra_email}:{self.alegra_token.get_secret_value()}"
        encoded = base64.b64encode(raw.encode()).decode()
        return f"Basic {encoded}"


@lru_cache
def get_settings() -> Settings:
    return Settings()
