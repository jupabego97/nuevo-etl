"""Paquete de integración Alegra."""

from alegra_etl.alegra.client import AlegraClient
from alegra_etl.alegra.resources import RESOURCE_REGISTRY, get_enabled_resources

__all__ = ["AlegraClient", "RESOURCE_REGISTRY", "get_enabled_resources"]
