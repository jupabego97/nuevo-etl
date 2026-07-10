from alegra_etl.alegra.parsers import parse_items, parse_purchase_bills, parse_sales_invoices


def test_parse_sales_invoice_splits_header_and_lines(sample_invoice):
    headers, lines, payments, applications = parse_sales_invoices([sample_invoice], company_id=1)
    assert len(headers) == 1
    assert headers[0]["alegra_id"] == "101"
    assert headers[0]["invoice_total"] == 119
    assert headers[0]["total_paid"] == 119
    assert len(lines) == 1
    assert lines[0]["item_alegra_id"] == "501"
    assert lines[0]["line_total"] == 100
    assert len(payments) == 1
    assert len(applications) == 1


def test_parse_items_extracts_inventory_and_prices(sample_item):
    items, prices, inventories = parse_items([sample_item], company_id=1)
    assert items[0]["barcode"] == "123456"
    assert items[0]["family"] == "Hardware"
    assert prices[0]["price"] == 80
    assert inventories[0]["available_quantity"] == 12


def test_parse_purchase_bill_keeps_bill_id(sample_bill):
    headers, lines = parse_purchase_bills([sample_bill], company_id=1)
    assert headers[0]["alegra_id"] == "201"
    assert lines[0]["bill_alegra_id"] == "201"
    assert lines[0]["item_alegra_id"] == "501"
