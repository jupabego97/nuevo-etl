"""Diff de payloads JSON para auditoría de webhooks."""

from __future__ import annotations

import json
from typing import Any

# Campos que suelen cambiar sin ser “el” cambio de negocio.
IGNORE_KEYS = frozenset(
    {
        "updatedAt",
        "updated_at",
        "datetime",
        "metadata",
        "stamp",
    }
)

_MAX_VALUE_CHARS = 8_000


def _jsonable(value: Any) -> Any:
    try:
        return json.loads(json.dumps(value, default=str, sort_keys=True))
    except (TypeError, ValueError):
        return str(value)


def _shrink(value: Any) -> Any:
    encoded = json.dumps(value, default=str, ensure_ascii=False)
    if len(encoded) <= _MAX_VALUE_CHARS:
        return value
    if isinstance(value, list):
        return {"_omitted": True, "reason": "too_large", "type": "list", "length": len(value)}
    if isinstance(value, dict):
        return {"_omitted": True, "reason": "too_large", "type": "object", "keys": sorted(value.keys())[:40]}
    return {"_omitted": True, "reason": "too_large", "type": type(value).__name__}


def _walk(
    before: Any,
    after: Any,
    path: str,
    changed: list[str],
    before_out: dict[str, Any],
    after_out: dict[str, Any],
    *,
    depth: int,
) -> None:
    if depth <= 0:
        if before != after:
            key = path or "$"
            changed.append(key)
            before_out[key] = _shrink(_jsonable(before))
            after_out[key] = _shrink(_jsonable(after))
        return

    if isinstance(before, dict) and isinstance(after, dict):
        keys = sorted(set(before) | set(after))
        for key in keys:
            if key in IGNORE_KEYS:
                continue
            child = f"{path}.{key}" if path else key
            if key not in before:
                changed.append(child)
                after_out[child] = _shrink(_jsonable(after[key]))
            elif key not in after:
                changed.append(child)
                before_out[child] = _shrink(_jsonable(before[key]))
            else:
                _walk(
                    before[key],
                    after[key],
                    child,
                    changed,
                    before_out,
                    after_out,
                    depth=depth - 1,
                )
        return

    if isinstance(before, list) and isinstance(after, list):
        if _jsonable(before) != _jsonable(after):
            key = path or "$"
            changed.append(key)
            before_out[key] = _shrink(_jsonable(before))
            after_out[key] = _shrink(_jsonable(after))
        return

    if _jsonable(before) != _jsonable(after):
        key = path or "$"
        changed.append(key)
        before_out[key] = _shrink(_jsonable(before))
        after_out[key] = _shrink(_jsonable(after))


def diff_payloads(
    before: dict[str, Any] | None,
    after: dict[str, Any] | None,
    *,
    max_depth: int = 6,
) -> dict[str, Any]:
    """Compara dos payloads y devuelve campos cambiados con valores before/after."""
    if before is None and after is None:
        return {"kind": "noop", "changed_fields": [], "before": {}, "after": {}}

    if before is None:
        after_dict = after if isinstance(after, dict) else {"value": after}
        fields = sorted(k for k in after_dict if k not in IGNORE_KEYS)
        return {
            "kind": "created",
            "changed_fields": fields,
            "before": {},
            "after": {k: _shrink(_jsonable(after_dict[k])) for k in fields},
        }

    if after is None:
        before_dict = before if isinstance(before, dict) else {"value": before}
        fields = sorted(k for k in before_dict if k not in IGNORE_KEYS)
        return {
            "kind": "deleted",
            "changed_fields": fields,
            "before": {k: _shrink(_jsonable(before_dict[k])) for k in fields},
            "after": {},
        }

    changed: list[str] = []
    before_out: dict[str, Any] = {}
    after_out: dict[str, Any] = {}
    _walk(
        before if isinstance(before, dict) else {"value": before},
        after if isinstance(after, dict) else {"value": after},
        "",
        changed,
        before_out,
        after_out,
        depth=max_depth,
    )
    changed_sorted = sorted(set(changed))
    return {
        "kind": "updated" if changed_sorted else "unchanged",
        "changed_fields": changed_sorted,
        "before": before_out,
        "after": after_out,
    }
