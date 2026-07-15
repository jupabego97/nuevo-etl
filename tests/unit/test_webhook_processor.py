from alegra_etl.pipeline.payload_diff import diff_payloads
from alegra_etl.pipeline.webhook_processor import (
    build_dedupe_key,
    extract_resource_id,
    parse_alegra_webhook_body,
    resolve_event_type,
)


def test_build_dedupe_key_is_deterministic():
    payload = {"id": "10", "foo": "bar"}
    assert build_dedupe_key("edit-item", payload) == build_dedupe_key("edit-item", payload)


def test_extract_resource_id_from_payload():
    assert extract_resource_id("edit-invoice", {"id": "55"}) == "55"
    assert extract_resource_id("edit-bill", {"billId": "77"}) == "77"


def test_parse_alegra_webhook_body_subject_and_message_item():
    body = {
        "subject": "edit-item",
        "message": {"item": {"id": "1899", "name": "Producto"}},
    }
    event_type, payload = parse_alegra_webhook_body(body)
    assert event_type == "edit-item"
    assert payload["id"] == "1899"
    assert payload["name"] == "Producto"


def test_parse_alegra_webhook_body_invoice():
    body = {
        "subject": "new-invoice",
        "message": {"invoice": {"id": "101", "total": 100}},
    }
    event_type, payload = parse_alegra_webhook_body(body)
    assert event_type == "new-invoice"
    assert payload["id"] == "101"


def test_resolve_event_type_from_stored_unknown_payload():
    event = type(
        "E",
        (),
        {
            "event_type": "unknown",
            "payload": {"subject": "edit-item", "message": {"item": {"id": "1899"}}},
        },
    )()
    assert resolve_event_type(event) == "edit-item"


def test_extract_resource_id_from_nested_message():
    payload = {"subject": "edit-item", "message": {"item": {"id": "1899"}}}
    assert extract_resource_id("unknown", payload) == "1899"


def test_diff_payloads_detects_field_changes():
    before = {"id": "1", "name": "A", "price": 10, "updatedAt": "old"}
    after = {"id": "1", "name": "B", "price": 10, "updatedAt": "new"}
    result = diff_payloads(before, after)
    assert result["kind"] == "updated"
    assert result["changed_fields"] == ["name"]
    assert result["before"]["name"] == "A"
    assert result["after"]["name"] == "B"
    assert "updatedAt" not in result["changed_fields"]


def test_diff_payloads_created_and_deleted():
    created = diff_payloads(None, {"id": "1", "name": "X"})
    assert created["kind"] == "created"
    assert "name" in created["changed_fields"]

    deleted = diff_payloads({"id": "1", "name": "X"}, None)
    assert deleted["kind"] == "deleted"
    assert deleted["before"]["name"] == "X"


def test_diff_payloads_nested_and_list():
    before = {"id": "1", "inventory": {"availableQuantity": 2}, "items": [1, 2]}
    after = {"id": "1", "inventory": {"availableQuantity": 5}, "items": [1, 3]}
    result = diff_payloads(before, after)
    assert "inventory.availableQuantity" in result["changed_fields"]
    assert "items" in result["changed_fields"]
    assert result["before"]["inventory.availableQuantity"] == 2
    assert result["after"]["inventory.availableQuantity"] == 5
