"""Operações de persistência para médicos no PostgreSQL."""

from __future__ import annotations

import json
from datetime import date, datetime

import asyncpg


def _parse_date_br(value: str | None) -> date | None:
    """Converte data DD/MM/YYYY para date. Retorna None se inválido."""
    if not value:
        return None
    try:
        return datetime.strptime(value.strip(), "%d/%m/%Y").date()
    except (ValueError, AttributeError):
        return None


UPSERT_SQL = """
INSERT INTO doctors (
    crm, raw_crm, crm_natural, state, name, social_name, status, specialties,
    registration_type, registration_date, graduation_institution,
    graduation_date, is_foreign, security_hash, interdicao_obs,
    phone, address, photo_url, raw_data
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8::jsonb,
    $9, $10, $11,
    $12, $13, $14, $15,
    $16, $17, $18, $19::jsonb
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
    raw_data               = EXCLUDED.raw_data;
"""


async def upsert_doctor(pool: asyncpg.Pool, doc: dict) -> None:
    """Insere ou atualiza um médico no banco.

    Args:
        pool: Pool de conexões asyncpg.
        doc: Dict com campos já traduzidos para EN (name, state, crm, ...).
             Deve conter 'specialties' como list[dict] e 'raw_data' como dict.
    """
    async with pool.acquire() as conn:
        await conn.execute(
            UPSERT_SQL,
            doc["crm"],  # $1  INTEGER
            doc["raw_crm"],  # $2  VARCHAR
            doc.get("crm_natural"),  # $3  VARCHAR
            doc["state"],  # $4  VARCHAR
            doc["name"],  # $5  VARCHAR
            doc.get("social_name"),  # $6  VARCHAR
            doc.get("status"),  # $7  VARCHAR
            json.dumps(doc.get("specialties", []), ensure_ascii=False),  # $8
            doc.get("registration_type"),  # $9  VARCHAR
            _parse_date_br(doc.get("registration_date")),  # $10 DATE
            doc.get("graduation_institution"),  # $11 VARCHAR
            doc.get("graduation_date"),  # $12 VARCHAR
            doc.get("is_foreign", False),  # $13 BOOLEAN
            doc.get("security_hash"),  # $14 VARCHAR
            doc.get("interdicao_obs"),  # $15 TEXT
            doc.get("phone"),  # $16 VARCHAR
            doc.get("address"),  # $17 TEXT
            doc.get("photo_url"),  # $18 TEXT
            json.dumps(doc.get("raw_data", {}), ensure_ascii=False),  # $19
        )


async def upsert_doctors_batch(pool: asyncpg.Pool, doctors: list[dict]) -> int:
    """Insere ou atualiza um lote de médicos via executemany.

    Args:
        pool: Pool de conexões asyncpg.
        doctors: Lista de dicts com campos traduzidos para EN.

    Returns:
        Número de registros processados.
    """
    if not doctors:
        return 0

    rows = [
        (
            doc["crm"],  # $1  INTEGER
            doc["raw_crm"],  # $2  VARCHAR
            doc.get("crm_natural"),  # $3  VARCHAR
            doc["state"],  # $4  VARCHAR
            doc["name"],  # $5  VARCHAR
            doc.get("social_name"),  # $6  VARCHAR
            doc.get("status"),  # $7  VARCHAR
            json.dumps(doc.get("specialties", []), ensure_ascii=False),  # $8
            doc.get("registration_type"),  # $9  VARCHAR
            _parse_date_br(doc.get("registration_date")),  # $10 DATE
            doc.get("graduation_institution"),  # $11 VARCHAR
            doc.get("graduation_date"),  # $12 VARCHAR
            doc.get("is_foreign", False),  # $13 BOOLEAN
            doc.get("security_hash"),  # $14 VARCHAR
            doc.get("interdicao_obs"),  # $15 TEXT
            doc.get("phone"),  # $16 VARCHAR
            doc.get("address"),  # $17 TEXT
            doc.get("photo_url"),  # $18 TEXT
            json.dumps(doc.get("raw_data", {}), ensure_ascii=False),  # $19
        )
        for doc in doctors
    ]

    async with pool.acquire() as conn:
        await conn.executemany(UPSERT_SQL, rows)

    return len(rows)
