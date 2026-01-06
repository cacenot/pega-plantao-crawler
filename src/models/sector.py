"""Modelos Pydantic para setores e grupos."""

from datetime import datetime

from pydantic import BaseModel, Field


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
