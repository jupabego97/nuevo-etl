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
