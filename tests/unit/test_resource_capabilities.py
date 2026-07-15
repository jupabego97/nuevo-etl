"""Pruebas de capacidades por recurso."""

from alegra_etl.alegra.resources import (
    ResourcePriority,
    get_backfill_resources,
    get_daily_sync_resources,
    get_weekly_refresh_resources,
    resource_by_name,
)


def test_invoices_are_critical_and_typed(settings):
    resource = resource_by_name("invoices")
    assert resource is not None
    assert resource.priority == ResourcePriority.CRITICAL
    assert resource.has_typed_loader is True
    assert resource.supports_date_filter is True
    assert resource.include_in_daily_sync is True


def test_estimates_are_optional_and_excluded_from_daily(settings):
    resource = resource_by_name("estimates")
    assert resource is not None
    assert resource.optional is True
    assert resource.include_in_daily_sync is False


def test_daily_sync_excludes_weekly_masters(settings):
    daily = {r.name for r in get_daily_sync_resources(settings)}
    weekly = {r.name for r in get_weekly_refresh_resources(settings)}
    assert "items" not in daily
    assert "items" in weekly
    assert "invoices" in daily


def test_backfill_orders_by_priority(settings):
    resources = get_backfill_resources(settings)
    names = [r.name for r in resources]
    assert names.index("invoices") < names.index("estimates")
    assert names.index("bills") < names.index("terms")


def test_resource_coverage_contract(settings):
    from alegra_etl.alegra.resources import validate_resource_coverage

    issues = validate_resource_coverage()
    assert issues == []


def test_new_typed_loaders_declared(settings):
    for name in (
        "company",
        "currencies",
        "cost-centers",
        "bank-accounts",
        "purchase-orders",
        "inventory-adjustments",
        "warehouse-transfers",
    ):
        resource = resource_by_name(name)
        assert resource is not None
        assert resource.has_typed_loader is True
        assert resource.parser is not None
