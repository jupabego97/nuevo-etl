from alegra_etl.pipeline.webhook_processor import build_dedupe_key, extract_resource_id


def test_build_dedupe_key_is_deterministic():
    payload = {"id": "10", "foo": "bar"}
    assert build_dedupe_key("edit-item", payload) == build_dedupe_key("edit-item", payload)


def test_extract_resource_id_from_payload():
    assert extract_resource_id("edit-invoice", {"id": "55"}) == "55"
    assert extract_resource_id("edit-bill", {"billId": "77"}) == "77"
