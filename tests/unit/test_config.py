import pytest
from pydantic import ValidationError

from alegra_etl.config import Settings, get_settings


def test_db_schema_rejects_concatenated_company_id(monkeypatch):
    get_settings.cache_clear()
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://u:p@localhost/db")
    monkeypatch.setenv("ALEGRA_EMAIL", "a@b.com")
    monkeypatch.setenv("ALEGRA_TOKEN", "token")
    monkeypatch.setenv("WEBHOOK_SECRET", "secret")
    monkeypatch.setenv("DB_SCHEMA", "alegra_etlCOMPANY_ID=1")
    with pytest.raises(ValidationError):
        Settings()
    get_settings.cache_clear()


def test_db_schema_accepts_valid_name(monkeypatch):
    get_settings.cache_clear()
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://u:p@localhost/db")
    monkeypatch.setenv("ALEGRA_EMAIL", "a@b.com")
    monkeypatch.setenv("ALEGRA_TOKEN", "token")
    monkeypatch.setenv("WEBHOOK_SECRET", "secret")
    monkeypatch.setenv("DB_SCHEMA", "alegra_etl")
    monkeypatch.setenv("COMPANY_ID", "1")
    settings = Settings()
    assert settings.db_schema == "alegra_etl"
    assert settings.company_id == 1
    get_settings.cache_clear()
