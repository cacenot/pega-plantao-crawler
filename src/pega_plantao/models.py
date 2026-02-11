"""Modelos Pydantic para o Pega Plantão."""

from datetime import datetime

from pydantic import BaseModel, Field


# ── Services / Shifts ──────────────────────────────────────────


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


# ── Sectors ────────────────────────────────────────────────────


class TemplateGroup(BaseModel):
    """Modelo para template de escala."""

    shifts_template_group_id: str = Field(alias="ShiftsTemplateGroupId")
    name: str = Field(alias="Name")
    number_of_weeks: int = Field(alias="NumberOfWeeks")


class Sector(BaseModel):
    """Modelo para setor individual."""

    group_id: str = Field(alias="GroupId")
    name: str = Field(alias="Name")
    group_type: float = Field(alias="GroupType")
    is_active: bool = Field(alias="IsActive")
    internal_code: str | None = Field(default=None, alias="InternalCode")
    template_groups: list[TemplateGroup] = Field(
        default_factory=list, alias="TemplateGroups"
    )
    view_schedule_end_date: datetime | None = Field(
        default=None, alias="ViewScheduleEndDate"
    )
    payment_rule: str | None = Field(default=None, alias="PaymentRule")
    gfa: str | None = Field(default=None, alias="GFA")
    group_parent: str | None = Field(default=None, alias="GroupParent")
    groups_preferences: str | None = Field(default=None, alias="GroupsPreferences")


class SectorGroup(BaseModel):
    """Modelo para grupo de setores (unidade/hospital)."""

    group_id: str = Field(alias="GroupId")
    name: str = Field(alias="Name")
    group_type: float = Field(alias="GroupType")
    is_active: bool = Field(alias="IsActive")
    internal_code: str | None = Field(default=None, alias="InternalCode")
    sectors: list[Sector] = Field(default_factory=list, alias="Sectors")
    template_groups: list[TemplateGroup] | None = Field(
        default=None, alias="TemplateGroups"
    )
    view_schedule_end_date: datetime | None = Field(
        default=None, alias="ViewScheduleEndDate"
    )
    payment_rule: str | None = Field(default=None, alias="PaymentRule")
    gfa: str | None = Field(default=None, alias="GFA")
    group_parent: str | None = Field(default=None, alias="GroupParent")
    groups_preferences: str | None = Field(default=None, alias="GroupsPreferences")

    class Config:
        """Configuração do modelo."""

        populate_by_name = True
