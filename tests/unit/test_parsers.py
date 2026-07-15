from alegra_etl.alegra.parsers import parse_items, parse_purchase_bills, parse_sales_invoices


def test_parse_sales_invoice_splits_header_and_lines(sample_invoice):
    headers, lines, payments, applications = parse_sales_invoices([sample_invoice], company_id=1)
    assert len(headers) == 1
    assert headers[0]["alegra_id"] == "101"
    assert headers[0]["invoice_total"] == 119
    assert headers[0]["total_paid"] == 119
    assert headers[0]["invoice_number"] == "FE1001"
    assert headers[0]["number_template_id"] == "3"
    assert headers[0]["number_prefix"] == "FE"
    assert headers[0]["number_value"] == "1001"
    assert headers[0]["is_electronic"] is True
    assert headers[0]["cufe"] == "abc123cufe"
    assert len(lines) == 1
    assert lines[0]["item_alegra_id"] == "501"
    assert lines[0]["line_total"] == 100
    assert len(payments) == 1
    assert len(applications) == 1


def test_parse_sales_invoice_ordinary_numbering():
    invoice = {
        "id": "202",
        "date": "2025-06-02",
        "numberTemplate": {
            "id": "1",
            "name": "Factura ordinaria",
            "prefix": "FV-",
            "number": 88,
            "isElectronic": False,
        },
        "total": 10,
        "items": [],
        "retentions": [],
    }
    headers, _, _, _ = parse_sales_invoices([invoice], company_id=1)
    assert headers[0]["is_electronic"] is False
    assert headers[0]["invoice_number"] == "FV-88"
    assert headers[0]["cufe"] is None


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


def test_parse_purchase_bill_mixed_item_and_category_share_keys():
    bill = {
        "id": "301",
        "date": "2025-06-03",
        "status": "open",
        "provider": {"id": "10", "name": "Proveedor"},
        "total": 150,
        "purchases": {
            "items": [{"id": "1", "name": "Item", "quantity": 1, "price": 50, "total": 50}],
            "categories": [{"id": "9", "name": "Gasto", "quantity": 1, "price": 100, "total": 100}],
        },
    }
    _headers, lines = parse_purchase_bills([bill], company_id=1)
    assert len(lines) == 2
    assert set(lines[0].keys()) == set(lines[1].keys())
    assert lines[0]["category_alegra_id"] is None
    assert lines[1]["item_alegra_id"] is None
    assert lines[1]["category_alegra_id"] == "9"
