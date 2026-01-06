"""Utilitários do crawler."""

from .http_client import create_authenticated_client
from .date_utils import get_date_range

__all__ = ["create_authenticated_client", "get_date_range"]
