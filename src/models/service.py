"""Modelos Pydantic para services/shifts."""

from datetime import datetime

from pydantic import BaseModel, Field


class ServiceRaw(BaseModel):
    """Modelo raw da resposta da API de shifts."""

    service_id: str = Field(alias="ServiceId")
    service_start_date: datetime = Field(alias="ServiceStartDate")
    service_end_date: datetime = Field(alias="ServiceEndDate")
    user_id: str = Field(alias="UserId")
    name: str = Field(alias="Name")
    announcement_type: float = Field(alias="AnnouncementType")
    has_candidates: bool = Field(alias="HasCandidates")
    is_owner: bool = Field(alias="IsOwner")
    professional_id: str = Field(alias="ProfessionalId")
    professional_state: str = Field(alias="ProfessionalState")
    value: float = Field(alias="Value")
    group_name: str = Field(alias="GroupName")
    service_type_id: str | None = Field(default=None, alias="ServiceTypeId")
    color: str = Field(alias="Color")
    service_type_name: str = Field(alias="ServiceTypeName")
    needs_coverage: bool = Field(alias="NeedsCoverage")
    absence_type: float = Field(alias="AbsenceType")
    user_id_in_charge: str | None = Field(default=None, alias="UserIdInCharge")
    view_only: bool = Field(alias="ViewOnly")
    group_id: str = Field(alias="GroupId")
    is_day_off: bool = Field(alias="IsDayOff")
    created_date: datetime = Field(alias="CreatedDate")


class Service(BaseModel):
    """Modelo final de service com campos mapeados."""

    service_id: str
    start_date: datetime
    end_date: datetime
    external_professional_id: str
    location: str
    section: str
    shift_type_id: str | None
    shift_type: str
    needs_coverage: bool
    shift_id: str

    @classmethod
    def from_raw(cls, raw: ServiceRaw) -> "Service":
        """Converte ServiceRaw para Service, parseando GroupName."""
        # GroupName format: "{location} - {section}"
        parts = raw.group_name.split(" - ", 1)
        location = parts[0].strip() if parts else raw.group_name
        section = parts[1].strip() if len(parts) > 1 else ""

        return cls(
            service_id=raw.service_id,
            start_date=raw.service_start_date,
            end_date=raw.service_end_date,
            external_professional_id=raw.user_id,
            location=location,
            section=section,
            shift_type_id=raw.service_type_id,
            shift_type=raw.service_type_name,
            needs_coverage=raw.needs_coverage,
            shift_id=raw.group_id,
        )
