"""Repositório de especialidades médicas."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session


def insert_specialties(session: Session, names: set[str] | list[str]) -> int:
    """Insere especialidades no banco, ignorando duplicatas.

    Args:
        session: Sessão SQLAlchemy.
        names: Conjunto ou lista de nomes de especialidades.

    Returns:
        Número de especialidades processadas.
    """
    if not names:
        return 0

    sql = text(
        "INSERT INTO specialties (name) VALUES (:name) ON CONFLICT (name) DO NOTHING"
    )
    for name in sorted(names):
        session.execute(sql, {"name": name})

    return len(names)


def get_all_specialties(session: Session) -> list[dict]:
    """Retorna todas as especialidades do banco."""
    result = session.execute(
        text("SELECT id, name, code, created_at FROM specialties ORDER BY name")
    )
    return [dict(row._mapping) for row in result]


def fetch_specialty_pairs_from_doctors(
    session: Session,
) -> list[dict[str, str]]:
    """Extrai pares (code, name) únicos do JSONB doctors.specialties.

    Returns:
        Lista de dicts com chaves 'code' e 'name'.
    """
    sql = text("""
        SELECT DISTINCT
            elem->>'specialty_code' AS code,
            elem->>'name' AS name
        FROM doctors,
             jsonb_array_elements(specialties) AS elem
        WHERE specialties != '[]'::jsonb
          AND elem->>'specialty_code' IS NOT NULL
        ORDER BY code
    """)
    result = session.execute(sql)
    return [dict(row._mapping) for row in result]


def truncate_and_insert_specialties(
    session: Session,
    specialties: list[dict[str, str]],
) -> int:
    """Limpa a tabela specialties e insere as especialidades fornecidas.

    Args:
        session: Sessão SQLAlchemy.
        specialties: Lista de dicts com chaves 'code' e 'name'.

    Returns:
        Número de especialidades inseridas.
    """
    if not specialties:
        return 0

    session.execute(text("TRUNCATE TABLE specialties RESTART IDENTITY"))

    sql = text("INSERT INTO specialties (code, name) VALUES (:code, :name)")
    for spec in specialties:
        session.execute(sql, {"code": spec["code"], "name": spec["name"]})

    return len(specialties)
