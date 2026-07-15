"""Pruebas de autorización de webhooks (compatible con Alegra sin headers)."""

from unittest.mock import MagicMock

from alegra_etl.config import get_settings
from alegra_etl.web.app import extract_presented_secret, is_webhook_authorized


def _settings(monkeypatch):
    get_settings.cache_clear()
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://postgres:postgres@localhost:5432/test")
    monkeypatch.setenv("ALEGRA_EMAIL", "test@example.com")
    monkeypatch.setenv("ALEGRA_TOKEN", "token")
    monkeypatch.setenv("WEBHOOK_SECRET", "super-secret-token")
    return get_settings()


def test_authorize_with_query_token(monkeypatch):
    settings = _settings(monkeypatch)
    request = MagicMock()
    request.query_params = {"token": "super-secret-token"}
    presented = extract_presented_secret(request)
    assert is_webhook_authorized(presented, settings) is True
    get_settings.cache_clear()


def test_authorize_with_path_token(monkeypatch):
    settings = _settings(monkeypatch)
    request = MagicMock()
    request.query_params = {}
    presented = extract_presented_secret(request, path_token="super-secret-token")
    assert is_webhook_authorized(presented, settings) is True
    get_settings.cache_clear()


def test_authorize_with_header(monkeypatch):
    settings = _settings(monkeypatch)
    request = MagicMock()
    request.query_params = {}
    presented = extract_presented_secret(
        request,
        x_webhook_secret="super-secret-token",
    )
    assert is_webhook_authorized(presented, settings) is True
    get_settings.cache_clear()


def test_reject_wrong_token(monkeypatch):
    settings = _settings(monkeypatch)
    request = MagicMock()
    request.query_params = {"token": "wrong"}
    presented = extract_presented_secret(request)
    assert is_webhook_authorized(presented, settings) is False
    get_settings.cache_clear()


def test_reject_missing_token(monkeypatch):
    settings = _settings(monkeypatch)
    request = MagicMock()
    request.query_params = {}
    presented = extract_presented_secret(request)
    assert is_webhook_authorized(presented, settings) is False
    get_settings.cache_clear()
