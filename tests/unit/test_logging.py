from alegra_etl.logging import redact_message


def test_redacts_basic_auth():
    message = "Authorization: Basic c2VjcmV0OnRva2Vu"
    redacted = redact_message(message)
    assert "c2VjcmV0" not in redacted
    assert "***" in redacted


def test_redacts_database_password():
    message = "postgresql+psycopg://user:supersecret@localhost/db"
    redacted = redact_message(message)
    assert "supersecret" not in redacted
