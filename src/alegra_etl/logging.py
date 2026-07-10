"""Logging estructurado con redacción de secretos."""

from __future__ import annotations

import json
import logging
import re
import sys
from datetime import UTC, datetime
from typing import Any

REDACT_PATTERNS = [
    re.compile(r"(authorization\s*[:=]\s*basic\s+)([^\s,]+)", re.I),
    re.compile(r"(password\s*[:=]\s*)([^\s&]+)", re.I),
    re.compile(r"(postgresql\+psycopg://[^:]+:)([^@]+)(@)", re.I),
    re.compile(r"(Basic\s+)([A-Za-z0-9+/=]+)", re.I),
]


def redact_message(message: str) -> str:
    redacted = message
    for pattern in REDACT_PATTERNS:
        if pattern.groups >= 3:
            redacted = pattern.sub(r"\1***\3", redacted)
        else:
            redacted = pattern.sub(r"\1***", redacted)
    return redacted


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": redact_message(record.getMessage()),
        }
        if hasattr(record, "run_id"):
            payload["run_id"] = record.run_id
        if hasattr(record, "resource"):
            payload["resource"] = record.resource
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def setup_logging(level: str = "INFO", json_logs: bool = True) -> None:
    root = logging.getLogger()
    root.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    if json_logs:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
        )
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
