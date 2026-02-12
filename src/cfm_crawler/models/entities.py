"""SQLAlchemy entities para o CFM Crawler.

Define as tabelas: doctors, specialties, captcha_tokens, state_counts.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from ...database.base import Base


class Doctor(Base):
    """Registro de médico do CFM."""

    __tablename__ = "doctors"
    __table_args__ = (
        UniqueConstraint("crm", "state", name="uq_doctors_crm_state"),
        Index("idx_doctors_state", "state"),
        Index("idx_doctors_status", "status"),
        Index("idx_doctors_specialties", "specialties", postgresql_using="gin"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, init=False)
    crm: Mapped[int] = mapped_column(BigInteger, nullable=False)
    raw_crm: Mapped[str] = mapped_column(String(20), nullable=False)
    state: Mapped[str] = mapped_column(String(2), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    crm_natural: Mapped[str | None] = mapped_column(String(20), default=None)
    social_name: Mapped[str | None] = mapped_column(String(255), default=None)
    status: Mapped[str | None] = mapped_column(String(50), default=None)
    specialties: Mapped[dict | list] = mapped_column(JSONB, default_factory=list)
    registration_type: Mapped[str | None] = mapped_column(String(50), default=None)
    registration_date: Mapped[datetime | None] = mapped_column(DateTime, default=None)
    graduation_institution: Mapped[str | None] = mapped_column(
        String(255), default=None
    )
    graduation_date: Mapped[str | None] = mapped_column(String(10), default=None)
    is_foreign: Mapped[bool] = mapped_column(Boolean, default=False)
    security_hash: Mapped[str | None] = mapped_column(String(64), default=None)
    interdicao_obs: Mapped[str | None] = mapped_column(Text, default=None)
    phone: Mapped[str | None] = mapped_column(String(50), default=None)
    address: Mapped[str | None] = mapped_column(Text, default=None)
    photo_url: Mapped[str | None] = mapped_column(Text, default=None)
    raw_data: Mapped[dict] = mapped_column(JSONB, default_factory=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), init=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now(), init=False
    )


class Specialty(Base):
    """Especialidade médica única."""

    __tablename__ = "specialties"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, init=False)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), init=False
    )


class CaptchaToken(Base):
    """Token de reCAPTCHA armazenado com TTL."""

    __tablename__ = "captcha_tokens"
    __table_args__ = (Index("idx_captcha_tokens_expires", "expires_at"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, init=False)
    token: Mapped[str] = mapped_column(Text, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), init=False
    )


class StateCount(Base):
    """Contagem de médicos por estado (API vs banco)."""

    __tablename__ = "state_counts"
    __table_args__ = (Index("idx_state_counts_state", "state"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, init=False)
    state: Mapped[str] = mapped_column(String(2), unique=True, nullable=False)
    api_total: Mapped[int] = mapped_column(Integer, default=0)
    db_total: Mapped[int] = mapped_column(Integer, default=0)
    missing: Mapped[int] = mapped_column(Integer, default=0)
    counted_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), init=False
    )
