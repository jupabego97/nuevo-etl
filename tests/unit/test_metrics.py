from decimal import Decimal

from alegra_etl.alegra.parsers import parse_sales_invoices


def test_invoice_totals_are_not_repeated_per_line(sample_invoice):
    headers, lines, _, _ = parse_sales_invoices([sample_invoice, sample_invoice], company_id=1)
    assert len(headers) == 2
    assert len(lines) == 2
    assert headers[0]["invoice_total"] == Decimal("119")
    assert lines[0]["line_total"] == Decimal("100")
    assert headers[0]["invoice_total"] != lines[0]["line_total"]
