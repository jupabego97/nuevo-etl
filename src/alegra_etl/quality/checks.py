"""Controles de calidad post-carga."""

from __future__ import annotations

import uuid
from typing import Any

from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from alegra_etl.alegra.resources import get_backfill_resources
from alegra_etl.db.models import (
    EtlParseSkip,
    FactIncomePayment,
    FactSalesInvoice,
    FactSalesInvoiceLine,
    QualityCheckResult,
    SourceDocument,
)
from alegra_etl.pipeline.resource_coverage import RESOURCE_TYPED_MAP, count_source_ids, count_typed_ids


def run_quality_checks(session: Session, run_id: uuid.UUID, company_id: int) -> dict[str, Any]:
    results: dict[str, Any] = {}
    checks = [
        ("duplicate_invoice_lines", _check_duplicate_invoice_lines(session, company_id)),
        ("orphan_invoice_lines", _check_orphan_invoice_lines(session, company_id)),
        ("void_invoices_present", _check_void_invoices(session, company_id)),
        ("source_typed_coverage", _check_source_typed_coverage(session, company_id)),
        ("parse_skips", _check_parse_skips(session, company_id)),
        ("payment_type_filter", _check_payment_records(session, company_id)),
        ("resource_coverage_contract", _check_resource_coverage_contract()),
    ]
    for name, status in checks:
        session.add(
            QualityCheckResult(
                run_id=run_id,
                check_name=name,
                status=status["status"],
                details=status,
            )
        )
        results[name] = status
    return results


def _check_duplicate_invoice_lines(session: Session, company_id: int) -> dict[str, Any]:
    sql = text(
        """
        SELECT invoice_alegra_id, line_number, COUNT(*) AS cnt
        FROM fact_sales_invoice_line
        WHERE company_id = :company_id
        GROUP BY invoice_alegra_id, line_number
        HAVING COUNT(*) > 1
        LIMIT 5
        """
    )
    rows = session.execute(sql, {"company_id": company_id}).mappings().all()
    return {"status": "fail" if rows else "pass", "duplicates": [dict(r) for r in rows]}


def _check_orphan_invoice_lines(session: Session, company_id: int) -> dict[str, Any]:
    sql = text(
        """
        SELECT COUNT(*) AS orphan_count
        FROM fact_sales_invoice_line l
        LEFT JOIN fact_sales_invoice h
          ON h.company_id = l.company_id AND h.alegra_id = l.invoice_alegra_id
        WHERE l.company_id = :company_id AND h.id IS NULL
        """
    )
    orphan_count = session.execute(sql, {"company_id": company_id}).scalar() or 0
    return {"status": "fail" if orphan_count else "pass", "orphan_count": orphan_count}


def _check_void_invoices(session: Session, company_id: int) -> dict[str, Any]:
    void_count = session.scalar(
        select(func.count())
        .select_from(FactSalesInvoice)
        .where(FactSalesInvoice.company_id == company_id, FactSalesInvoice.status == "void")
    ) or 0
    return {"status": "pass", "void_count": void_count}


def _check_source_typed_coverage(session: Session, company_id: int) -> dict[str, Any]:
    mismatches: dict[str, dict[str, int]] = {}
    for name in RESOURCE_TYPED_MAP:
        source = count_source_ids(session, company_id, name)
        typed = count_typed_ids(session, company_id, name)
        if source != typed:
            mismatches[name] = {"source": source, "typed": typed}
    return {
        "status": "fail" if mismatches else "pass",
        "mismatches": mismatches,
    }


def _check_parse_skips(session: Session, company_id: int) -> dict[str, Any]:
    rows = (
        session.query(EtlParseSkip.resource_name, func.count())
        .filter_by(company_id=company_id)
        .group_by(EtlParseSkip.resource_name)
        .all()
    )
    by_resource = {name: count for name, count in rows}
    total = sum(by_resource.values())
    return {
        "status": "fail" if total else "pass",
        "total": total,
        "by_resource": by_resource,
    }


def _check_payment_records(session: Session, company_id: int) -> dict[str, Any]:
    """Pagos de ingreso deben existir solo en fact_income_payment."""
    count = session.scalar(
        select(func.count())
        .select_from(FactIncomePayment)
        .where(FactIncomePayment.company_id == company_id)
    ) or 0
    source_payments = session.scalar(
        select(func.count(func.distinct(SourceDocument.alegra_id))).where(
            SourceDocument.company_id == company_id,
            SourceDocument.resource_name == "payments-income",
            SourceDocument.deleted_at.is_(None),
        )
    ) or 0
    aligned = count == source_payments
    return {
        "status": "pass" if aligned or count == 0 else "warn",
        "typed": count,
        "source": source_payments,
    }


def _check_resource_coverage_contract() -> dict[str, Any]:
    from alegra_etl.alegra.resources import validate_resource_coverage

    issues = validate_resource_coverage()
    return {"status": "fail" if issues else "pass", "issues": issues}


def backfill_coverage_manifest(session: Session, company_id: int, settings: Any) -> dict[str, Any]:
    """Manifiesto final de cobertura por recurso de backfill."""
    manifest: dict[str, Any] = {}
    for resource in get_backfill_resources(settings):
        source = count_source_ids(session, company_id, resource.name)
        typed = count_typed_ids(session, company_id, resource.name) if resource.has_typed_loader else None
        manifest[resource.name] = {
            "source_only": resource.source_only,
            "has_typed_loader": resource.has_typed_loader,
            "source_ids": source,
            "typed_ids": typed,
            "aligned": source == typed if resource.has_typed_loader else True,
        }
    return manifest
