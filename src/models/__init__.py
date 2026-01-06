"""Modelos Pydantic para o crawler."""

from .sector import Sector, SectorGroup, TemplateGroup
from .service import Service, ServiceRaw

__all__ = ["Sector", "SectorGroup", "TemplateGroup", "Service", "ServiceRaw"]
