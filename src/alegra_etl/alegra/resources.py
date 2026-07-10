"""Registro declarativo de recursos Alegra."""

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


TransformFn = Callable[[dict[str, Any], int], list[dict[str, Any]]]


def _enabled(settings_flag: str | None, settings: Any) -> bool:
    if not settings_flag:
        return True
    return bool(getattr(settings, settings_flag, True))


RESOURCE_REGISTRY: list[ResourceDefinition] = [
    ResourceDefinition("company", "company", ResourceGroup.CONFIG, SyncStrategy.FULL, extra_params={}),
    ResourceDefinition("items", "items", ResourceGroup.MASTER, SyncStrategy.FULL, extra_params={"mode": "advanced"}, webhook_events=("new-item", "edit-item", "delete-item"), detail_endpoint_template="items/{id}"),
    ResourceDefinition("contacts", "contacts", ResourceGroup.MASTER, SyncStrategy.FULL, webhook_events=("new-client", "edit-client", "delete-client"), detail_endpoint_template="contacts/{id}"),
    ResourceDefinition("sellers", "sellers", ResourceGroup.MASTER, SyncStrategy.FULL),
    ResourceDefinition("warehouses", "warehouses", ResourceGroup.INVENTORY, SyncStrategy.FULL),
    ResourceDefinition("item-categories", "item-categories", ResourceGroup.MASTER, SyncStrategy.FULL),
    ResourceDefinition("price-lists", "price-lists", ResourceGroup.MASTER, SyncStrategy.FULL),
    ResourceDefinition("variant-attributes", "variant-attributes", ResourceGroup.MASTER, SyncStrategy.FULL),
    ResourceDefinition("taxes", "taxes", ResourceGroup.CONFIG, SyncStrategy.FULL),
    ResourceDefinition("retentions", "retentions", ResourceGroup.CONFIG, SyncStrategy.FULL),
    ResourceDefinition("currencies", "currencies", ResourceGroup.CONFIG, SyncStrategy.FULL),
    ResourceDefinition("terms", "terms", ResourceGroup.CONFIG, SyncStrategy.FULL),
    ResourceDefinition("number-templates", "number-templates", ResourceGroup.CONFIG, SyncStrategy.FULL),
    ResourceDefinition(
        "invoices",
        "invoices",
        ResourceGroup.INCOME,
        SyncStrategy.DATE_WINDOW,
        webhook_events=("new-invoice", "edit-invoice", "delete-invoice"),
        detail_endpoint_template="invoices/{id}",
    ),
    ResourceDefinition(
        "payments-income",
        "payments",
        ResourceGroup.INCOME,
        SyncStrategy.DATE_WINDOW,
        extra_params={"type": "in"},
    ),
    ResourceDefinition("credit-notes", "credit-notes", ResourceGroup.INCOME, SyncStrategy.DATE_WINDOW),
    ResourceDefinition("income-debit-notes", "income-debit-notes", ResourceGroup.INCOME, SyncStrategy.DATE_WINDOW),
    ResourceDefinition("estimates", "estimates", ResourceGroup.INCOME, SyncStrategy.DATE_WINDOW),
    ResourceDefinition("remissions", "remissions", ResourceGroup.INCOME, SyncStrategy.DATE_WINDOW),
    ResourceDefinition("recurring-invoices", "recurring-invoices", ResourceGroup.INCOME, SyncStrategy.FULL),
    ResourceDefinition(
        "bills",
        "bills",
        ResourceGroup.EXPENSE,
        SyncStrategy.DATE_WINDOW,
        extra_params={"type": "all"},
        webhook_events=("new-bill", "edit-bill", "delete-bill"),
        detail_endpoint_template="bills/{id}",
    ),
    ResourceDefinition(
        "payments-expense",
        "payments",
        ResourceGroup.EXPENSE,
        SyncStrategy.DATE_WINDOW,
        extra_params={"type": "out"},
    ),
    ResourceDefinition("purchase-orders", "purchase-orders", ResourceGroup.EXPENSE, SyncStrategy.DATE_WINDOW),
    ResourceDefinition("debit-notes", "debit-notes", ResourceGroup.EXPENSE, SyncStrategy.DATE_WINDOW),
    ResourceDefinition("inventory-adjustments", "inventory-adjustments", ResourceGroup.INVENTORY, SyncStrategy.DATE_WINDOW),
    ResourceDefinition("warehouse-transfers", "warehouse-transfers", ResourceGroup.INVENTORY, SyncStrategy.DATE_WINDOW),
    ResourceDefinition(
        "categories",
        "categories",
        ResourceGroup.ACCOUNTING,
        SyncStrategy.FULL,
        feature_flag="enable_accounting",
    ),
    ResourceDefinition(
        "cost-centers",
        "cost-centers",
        ResourceGroup.ACCOUNTING,
        SyncStrategy.FULL,
        feature_flag="enable_accounting",
    ),
    ResourceDefinition(
        "journals",
        "journals",
        ResourceGroup.ACCOUNTING,
        SyncStrategy.DATE_WINDOW,
        feature_flag="enable_accounting",
    ),
    ResourceDefinition(
        "bank-accounts",
        "bank-accounts",
        ResourceGroup.BANKS,
        SyncStrategy.FULL,
        feature_flag="enable_banks",
    ),
    ResourceDefinition(
        "conciliations",
        "conciliations",
        ResourceGroup.BANKS,
        SyncStrategy.DATE_WINDOW,
        feature_flag="enable_banks",
    ),
    ResourceDefinition(
        "global-invoices",
        "global-invoices",
        ResourceGroup.INCOME,
        SyncStrategy.DATE_WINDOW,
        feature_flag="enable_global_invoices",
    ),
    ResourceDefinition(
        "transportation-receipts",
        "transportation-receipts",
        ResourceGroup.INCOME,
        SyncStrategy.DATE_WINDOW,
        feature_flag="enable_transportation_receipts",
    ),
]


def get_enabled_resources(settings: Any) -> list[ResourceDefinition]:
    return [r for r in RESOURCE_REGISTRY if r.enabled and _enabled(r.feature_flag, settings)]


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
