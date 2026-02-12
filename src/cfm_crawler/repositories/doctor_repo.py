"""Repositório de médicos — upsert e consulta na tabela doctors."""

from __future__ import annotations

import json
from datetime import date, datetime

from sqlalchemy import text
from sqlalchemy.orm import Session


def _parse_date_br(value: str | None) -> date | None:
    """Converte data DD/MM/YYYY para date. Retorna None se inválido."""
    if not value:
        return None
    try:
        return datetime.strptime(value.strip(), "%d/%m/%Y").date()
    except (ValueError, AttributeError):
        return None


_UPSERT_SQL = text("""
INSERT INTO doctors (
    crm, raw_crm, crm_natural, state, name, social_name, status, specialties,
    registration_type, registration_date, graduation_institution,
    graduation_date, is_foreign, security_hash, interdicao_obs,
    phone, address, photo_url, raw_data
)
VALUES (
    :crm, :raw_crm, :crm_natural, :state, :name, :social_name, :status,
    CAST(:specialties AS jsonb),
    :registration_type, :registration_date, :graduation_institution,
    :graduation_date, :is_foreign, :security_hash, :interdicao_obs,
    :phone, :address, :photo_url, CAST(:raw_data AS jsonb)
)
ON CONFLICT (crm, state) DO UPDATE SET
    raw_crm                = EXCLUDED.raw_crm,
    crm_natural            = EXCLUDED.crm_natural,
    name                   = EXCLUDED.name,
    social_name            = EXCLUDED.social_name,
    status                 = EXCLUDED.status,
    specialties            = EXCLUDED.specialties,
    registration_type      = EXCLUDED.registration_type,
    registration_date      = EXCLUDED.registration_date,
    graduation_institution = EXCLUDED.graduation_institution,
    graduation_date        = EXCLUDED.graduation_date,
    is_foreign             = EXCLUDED.is_foreign,
    security_hash          = EXCLUDED.security_hash,
    interdicao_obs         = EXCLUDED.interdicao_obs,
    phone                  = EXCLUDED.phone,
    address                = EXCLUDED.address,
    photo_url              = EXCLUDED.photo_url,
    raw_data               = EXCLUDED.raw_data,
    updated_at             = NOW();
""")


def _doc_to_params(doc: dict) -> dict:
    """Converte dict de médico para parâmetros do SQL."""
    return {
        "crm": doc["crm"],
        "raw_crm": doc["raw_crm"],
        "crm_natural": doc.get("crm_natural"),
        "state": doc["state"],
        "name": doc["name"],
        "social_name": doc.get("social_name"),
        "status": doc.get("status"),
        "specialties": json.dumps(doc.get("specialties", []), ensure_ascii=False),
        "registration_type": doc.get("registration_type"),
        "registration_date": _parse_date_br(doc.get("registration_date")),
        "graduation_institution": doc.get("graduation_institution"),
        "graduation_date": doc.get("graduation_date"),
        "is_foreign": doc.get("is_foreign", False),
        "security_hash": doc.get("security_hash"),
        "interdicao_obs": doc.get("interdicao_obs"),
        "phone": doc.get("phone"),
        "address": doc.get("address"),
        "photo_url": doc.get("photo_url"),
        "raw_data": json.dumps(doc.get("raw_data", {}), ensure_ascii=False),
    }


def upsert_doctor(session: Session, doc: dict) -> None:
    """Insere ou atualiza um médico no banco.

    Args:
        session: Sessão SQLAlchemy.
        doc: Dict com campos traduzidos para EN (name, state, crm, ...).
    """
    session.execute(_UPSERT_SQL, _doc_to_params(doc))


def upsert_doctors_batch(session: Session, doctors: list[dict]) -> int:
    """Insere ou atualiza um lote de médicos.

    Args:
        session: Sessão SQLAlchemy.
        doctors: Lista de dicts com campos traduzidos para EN.

    Returns:
        Número de registros processados.
    """
    if not doctors:
        return 0

    params_list = [_doc_to_params(doc) for doc in doctors]
    for params in params_list:
        session.execute(_UPSERT_SQL, params)

    return len(params_list)


def get_doctor_by_crm(session: Session, crm: int, state: str) -> dict | None:
    """Busca um médico por CRM e UF.

    Returns:
        Dict com dados do médico ou None.
    """
    result = session.execute(
        text("SELECT * FROM doctors WHERE crm = :crm AND state = :state"),
        {"crm": crm, "state": state},
    ).fetchone()

    if result:
        return dict(result._mapping)
    return None
