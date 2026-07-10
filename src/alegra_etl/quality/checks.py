"""Controles de calidad post-carga."""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from alegra_etl.db.models import FactSalesInvoice, QualityCheckResult


def run_quality_checks(session: Session, run_id: uuid.UUID, company_id: int) -> dict[str, Any]:
    results: dict[str, Any] = {}
    checks = [
        ("duplicate_invoice_lines", _check_duplicate_invoice_lines(session, company_id)),
        ("orphan_invoice_lines", _check_orphan_invoice_lines(session, company_id)),
        ("void_invoices_present", _check_void_invoices(session, company_id)),
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
