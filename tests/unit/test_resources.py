
from alegra_etl.alegra.client import hash_payload, hash_request
from alegra_etl.alegra.resources import get_enabled_resources, resource_for_webhook_event


def test_hash_payload_is_stable():
    payload = {"a": 1, "b": [1, 2]}
    assert hash_payload(payload) == hash_payload({"b": [1, 2], "a": 1})


def test_hash_request_uses_params():
    h1 = hash_request({"start": 0, "limit": 30})
    h2 = hash_request({"limit": 30, "start": 0})
    assert h1 == h2
    assert len(h1) == 64


def test_resource_registry_maps_webhook_events():
    resource = resource_for_webhook_event("edit-invoice")
    assert resource is not None
    assert resource.name == "invoices"


def test_feature_flags_disable_optional_resources(monkeypatch):
    from alegra_etl.config import get_settings

    get_settings.cache_clear()
    monkeypatch.setenv("ENABLE_GLOBAL_INVOICES", "false")
    settings = get_settings()
    names = [r.name for r in get_enabled_resources(settings)]
    assert "global-invoices" not in names
    get_settings.cache_clear()
