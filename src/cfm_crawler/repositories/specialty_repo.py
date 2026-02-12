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
        text("SELECT id, name, created_at FROM specialties ORDER BY name")
    )
    return [dict(row._mapping) for row in result]
