"""Registro declarativo de recursos Alegra con capacidades explícitas."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class SyncStrategy(str, Enum):
    FULL = "full"
    DATE_WINDOW = "date_window"
    CHECKPOINT = "checkpoint"


class ResourceGroup(str, Enum):
    MASTER = "master"
    INCOME = "income"
    EXPENSE = "expense"
    INVENTORY = "inventory"
    ACCOUNTING = "accounting"
    BANKS = "banks"
    CONFIG = "config"


class ResourcePriority(str, Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


@dataclass(frozen=True)
class ResourceDefinition:
    name: str
    endpoint: str
    group: ResourceGroup
    strategy: SyncStrategy
    enabled: bool = True
    order_field: str = "id"
    order_direction: str = "ASC"
    extra_params: dict[str, Any] = field(default_factory=dict)
    feature_flag: str | None = None
    detail_endpoint_template: str | None = None
    webhook_events: tuple[str, ...] = ()
    # Capacidades reales del endpoint
    optional: bool = False
    supports_pagination: bool = True
    supports_date_filter: bool = False
    supports_metadata: bool = True
    has_typed_loader: bool = False
    source_only: bool = False
    parser: str | None = None
    priority: ResourcePriority = ResourcePriority.MEDIUM
    include_in_daily_sync: bool = True
    include_in_weekly_refresh: bool = False
    include_in_backfill: bool = True


TransformFn = Callable[[dict[str, Any], int], list[dict[str, Any]]]


def _enabled(settings_flag: str | None, settings: Any) -> bool:
    if not settings_flag:
        return True
    return bool(getattr(settings, settings_flag, True))


RESOURCE_REGISTRY: list[ResourceDefinition] = [
    ResourceDefinition(
        "company",
        "company",
        ResourceGroup.CONFIG,
        SyncStrategy.FULL,
        extra_params={},
        supports_pagination=False,
        supports_metadata=False,
        has_typed_loader=True,
        parser="company",
        optional=True,
        priority=ResourcePriority.LOW,
        include_in_daily_sync=False,
        include_in_backfill=False,
        include_in_weekly_refresh=True,
    ),
    ResourceDefinition(
        "items",
        "items",
        ResourceGroup.MASTER,
        SyncStrategy.FULL,
        extra_params={"mode": "advanced"},
        webhook_events=("new-item", "edit-item", "delete-item"),
        detail_endpoint_template="items/{id}",
        has_typed_loader=True,
        parser="items",
        priority=ResourcePriority.CRITICAL,
        include_in_daily_sync=False,
        include_in_weekly_refresh=True,
    ),
    ResourceDefinition(
        "contacts",
        "contacts",
        ResourceGroup.MASTER,
        SyncStrategy.FULL,
        webhook_events=("new-client", "edit-client", "delete-client"),
        detail_endpoint_template="contacts/{id}",
        has_typed_loader=True,
        parser="contacts",
        priority=ResourcePriority.HIGH,
        include_in_daily_sync=False,
        include_in_weekly_refresh=True,
    ),
    ResourceDefinition(
        "sellers",
        "sellers",
        ResourceGroup.MASTER,
        SyncStrategy.FULL,
        has_typed_loader=True,
        parser="sellers",
        priority=ResourcePriority.MEDIUM,
        include_in_daily_sync=False,
        include_in_weekly_refresh=True,
    ),
    ResourceDefinition(
        "warehouses",
        "warehouses",
        ResourceGroup.INVENTORY,
        SyncStrategy.FULL,
        has_typed_loader=True,
        parser="warehouses",
        priority=ResourcePriority.MEDIUM,
        include_in_daily_sync=False,
        include_in_weekly_refresh=True,
    ),
    ResourceDefinition(
        "item-categories",
        "item-categories",
        ResourceGroup.MASTER,
        SyncStrategy.FULL,
        order_field="",
        optional=True,
        source_only=True,
        priority=ResourcePriority.LOW,
        include_in_daily_sync=False,
        include_in_weekly_refresh=True,
    ),
    ResourceDefinition(
        "price-lists",
        "price-lists",
        ResourceGroup.MASTER,
        SyncStrategy.FULL,
        order_field="",
        optional=True,
        source_only=True,
        priority=ResourcePriority.LOW,
        include_in_daily_sync=False,
        include_in_weekly_refresh=True,
    ),
    ResourceDefinition(
        "variant-attributes",
        "variant-attributes",
        ResourceGroup.MASTER,
        SyncStrategy.FULL,
        order_field="",
        optional=True,
        source_only=True,
        priority=ResourcePriority.LOW,
        include_in_daily_sync=False,
    ),
    ResourceDefinition(
        "taxes",
        "taxes",
        ResourceGroup.CONFIG,
        SyncStrategy.FULL,
        has_typed_loader=True,
        parser="taxes",
        optional=True,
        priority=ResourcePriority.LOW,
        include_in_daily_sync=False,
        include_in_weekly_refresh=True,
    ),
    ResourceDefinition(
        "retentions",
        "retentions",
        ResourceGroup.CONFIG,
        SyncStrategy.FULL,
        optional=True,
        source_only=True,
        priority=ResourcePriority.LOW,
        include_in_daily_sync=False,
    ),
    ResourceDefinition(
        "currencies",
        "currencies",
        ResourceGroup.CONFIG,
        SyncStrategy.FULL,
        has_typed_loader=True,
        parser="currencies",
        optional=True,
        priority=ResourcePriority.LOW,
        include_in_daily_sync=False,
        include_in_weekly_refresh=True,
    ),
    ResourceDefinition(
        "terms",
        "terms",
        ResourceGroup.CONFIG,
        SyncStrategy.FULL,
        optional=True,
        source_only=True,
        priority=ResourcePriority.LOW,
        include_in_daily_sync=False,
    ),
    ResourceDefinition(
        "number-templates",
        "number-templates",
        ResourceGroup.CONFIG,
        SyncStrategy.FULL,
        optional=True,
        source_only=True,
        priority=ResourcePriority.LOW,
        include_in_daily_sync=False,
    ),
    ResourceDefinition(
        "invoices",
        "invoices",
        ResourceGroup.INCOME,
        SyncStrategy.DATE_WINDOW,
        supports_date_filter=True,
        webhook_events=("new-invoice", "edit-invoice", "delete-invoice"),
        detail_endpoint_template="invoices/{id}",
        has_typed_loader=True,
        parser="invoices",
        priority=ResourcePriority.CRITICAL,
    ),
    ResourceDefinition(
        "payments-income",
        "payments",
        ResourceGroup.INCOME,
        SyncStrategy.DATE_WINDOW,
        extra_params={"type": "in"},
        supports_date_filter=True,
        has_typed_loader=True,
        parser="payments_income",
        priority=ResourcePriority.HIGH,
    ),
    ResourceDefinition(
        "credit-notes",
        "credit-notes",
        ResourceGroup.INCOME,
        SyncStrategy.DATE_WINDOW,
        supports_date_filter=True,
        has_typed_loader=True,
        parser="credit_notes",
        priority=ResourcePriority.HIGH,
    ),
    ResourceDefinition(
        "income-debit-notes",
        "income-debit-notes",
        ResourceGroup.INCOME,
        SyncStrategy.DATE_WINDOW,
        supports_date_filter=True,
        optional=True,
        source_only=True,
        priority=ResourcePriority.LOW,
    ),
    ResourceDefinition(
        "estimates",
        "estimates",
        ResourceGroup.INCOME,
        SyncStrategy.DATE_WINDOW,
        supports_date_filter=True,
        optional=True,
        source_only=True,
        priority=ResourcePriority.LOW,
        include_in_daily_sync=False,
    ),
    ResourceDefinition(
        "remissions",
        "remissions",
        ResourceGroup.INCOME,
        SyncStrategy.DATE_WINDOW,
        supports_date_filter=True,
        optional=True,
        source_only=True,
        priority=ResourcePriority.LOW,
        include_in_daily_sync=False,
    ),
    ResourceDefinition(
        "recurring-invoices",
        "recurring-invoices",
        ResourceGroup.INCOME,
        SyncStrategy.FULL,
        optional=True,
        source_only=True,
        priority=ResourcePriority.LOW,
        include_in_daily_sync=False,
    ),
    ResourceDefinition(
        "bills",
        "bills",
        ResourceGroup.EXPENSE,
        SyncStrategy.DATE_WINDOW,
        extra_params={"type": "all"},
        supports_date_filter=True,
        webhook_events=("new-bill", "edit-bill", "delete-bill"),
        detail_endpoint_template="bills/{id}",
        has_typed_loader=True,
        parser="bills",
        priority=ResourcePriority.CRITICAL,
    ),
    ResourceDefinition(
        "payments-expense",
        "payments",
        ResourceGroup.EXPENSE,
        SyncStrategy.DATE_WINDOW,
        extra_params={"type": "out"},
        supports_date_filter=True,
        optional=True,
        source_only=True,
        priority=ResourcePriority.MEDIUM,
        include_in_daily_sync=False,
    ),
    ResourceDefinition(
        "purchase-orders",
        "purchase-orders",
        ResourceGroup.EXPENSE,
        SyncStrategy.DATE_WINDOW,
        supports_date_filter=True,
        has_typed_loader=True,
        parser="purchase_orders",
        optional=True,
        priority=ResourcePriority.LOW,
        include_in_daily_sync=False,
    ),
    ResourceDefinition(
        "debit-notes",
        "debit-notes",
        ResourceGroup.EXPENSE,
        SyncStrategy.DATE_WINDOW,
        supports_date_filter=True,
        optional=True,
        source_only=True,
        priority=ResourcePriority.LOW,
        include_in_daily_sync=False,
    ),
    ResourceDefinition(
        "inventory-adjustments",
        "inventory-adjustments",
        ResourceGroup.INVENTORY,
        SyncStrategy.DATE_WINDOW,
        supports_date_filter=True,
        has_typed_loader=True,
        parser="inventory_adjustments",
        optional=True,
        priority=ResourcePriority.MEDIUM,
        include_in_daily_sync=False,
    ),
    ResourceDefinition(
        "warehouse-transfers",
        "warehouse-transfers",
        ResourceGroup.INVENTORY,
        SyncStrategy.DATE_WINDOW,
        supports_date_filter=True,
        has_typed_loader=True,
        parser="warehouse_transfers",
        optional=True,
        priority=ResourcePriority.LOW,
        include_in_daily_sync=False,
    ),
    ResourceDefinition(
        "categories",
        "categories",
        ResourceGroup.ACCOUNTING,
        SyncStrategy.FULL,
        feature_flag="enable_accounting",
        optional=True,
        source_only=True,
        priority=ResourcePriority.LOW,
        include_in_daily_sync=False,
    ),
    ResourceDefinition(
        "cost-centers",
        "cost-centers",
        ResourceGroup.ACCOUNTING,
        SyncStrategy.FULL,
        feature_flag="enable_accounting",
        has_typed_loader=True,
        parser="cost_centers",
        optional=True,
        priority=ResourcePriority.LOW,
        include_in_daily_sync=False,
    ),
    ResourceDefinition(
        "journals",
        "journals",
        ResourceGroup.ACCOUNTING,
        SyncStrategy.DATE_WINDOW,
        feature_flag="enable_accounting",
        supports_date_filter=True,
        optional=True,
        source_only=True,
        priority=ResourcePriority.LOW,
        include_in_daily_sync=False,
    ),
    ResourceDefinition(
        "bank-accounts",
        "bank-accounts",
        ResourceGroup.BANKS,
        SyncStrategy.FULL,
        feature_flag="enable_banks",
        has_typed_loader=True,
        parser="bank_accounts",
        optional=True,
        priority=ResourcePriority.LOW,
        include_in_daily_sync=False,
        include_in_weekly_refresh=True,
    ),
    ResourceDefinition(
        "conciliations",
        "conciliations",
        ResourceGroup.BANKS,
        SyncStrategy.DATE_WINDOW,
        feature_flag="enable_banks",
        supports_date_filter=True,
        optional=True,
        source_only=True,
        priority=ResourcePriority.LOW,
        include_in_daily_sync=False,
    ),
    ResourceDefinition(
        "global-invoices",
        "global-invoices",
        ResourceGroup.INCOME,
        SyncStrategy.DATE_WINDOW,
        feature_flag="enable_global_invoices",
        supports_date_filter=True,
        optional=True,
        source_only=True,
        priority=ResourcePriority.LOW,
        include_in_daily_sync=False,
    ),
    ResourceDefinition(
        "transportation-receipts",
        "transportation-receipts",
        ResourceGroup.INCOME,
        SyncStrategy.DATE_WINDOW,
        feature_flag="enable_transportation_receipts",
        supports_date_filter=True,
        optional=True,
        source_only=True,
        priority=ResourcePriority.LOW,
        include_in_daily_sync=False,
    ),
]


def get_enabled_resources(settings: Any) -> list[ResourceDefinition]:
    return [r for r in RESOURCE_REGISTRY if r.enabled and _enabled(r.feature_flag, settings)]


def get_daily_sync_resources(settings: Any) -> list[ResourceDefinition]:
    return [
        r
        for r in get_enabled_resources(settings)
        if r.include_in_daily_sync
    ]


def get_weekly_refresh_resources(settings: Any) -> list[ResourceDefinition]:
    return [
        r
        for r in get_enabled_resources(settings)
        if r.include_in_weekly_refresh
    ]


def get_backfill_resources(settings: Any) -> list[ResourceDefinition]:
    priority_order = {
        ResourcePriority.CRITICAL: 0,
        ResourcePriority.HIGH: 1,
        ResourcePriority.MEDIUM: 2,
        ResourcePriority.LOW: 3,
    }
    resources = [
        r
        for r in get_enabled_resources(settings)
        if r.include_in_backfill
    ]
    return sorted(resources, key=lambda r: (priority_order[r.priority], r.name))


def resource_by_name(name: str) -> ResourceDefinition | None:
    for resource in RESOURCE_REGISTRY:
        if resource.name == name:
            return resource
    return None


def resource_for_webhook_event(event_type: str) -> ResourceDefinition | None:
    for resource in RESOURCE_REGISTRY:
        if event_type in resource.webhook_events:
            return resource
    return None


def validate_resource_coverage() -> list[str]:
    """Cada recurso habilitado debe ser typed o source_only explícito."""
    issues: list[str] = []
    for resource in RESOURCE_REGISTRY:
        if not resource.enabled:
            continue
        if resource.has_typed_loader and resource.source_only:
            issues.append(f"{resource.name}: typed y source_only simultáneo")
        if not resource.has_typed_loader and not resource.source_only and resource.include_in_backfill:
            if not resource.optional:
                issues.append(f"{resource.name}: sin loader ni source_only")
    return issues
