"""Repositório de contagem de médicos por estado."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session


def upsert_state_counts_batch(session: Session, rows: list[dict]) -> None:
    """Insere ou atualiza contagens de múltiplos estados em batch.

    Args:
        session: Sessão SQLAlchemy.
        rows: Lista de dicts com chaves: state, api_total, db_total, missing.
    """
    sql = text(
        "INSERT INTO state_counts (state, api_total, db_total, missing, counted_at) "
        "VALUES (:state, :api_total, :db_total, :missing, NOW()) "
        "ON CONFLICT (state) DO UPDATE SET "
        "api_total = EXCLUDED.api_total, "
        "db_total = EXCLUDED.db_total, "
        "missing = EXCLUDED.missing, "
        "counted_at = NOW()"
    )
    for row in rows:
        session.execute(
            sql,
            {
                "state": row["state"],
                "api_total": row["api_total"],
                "db_total": row["db_total"],
                "missing": row["missing"],
            },
        )


def get_db_counts_by_state(session: Session) -> dict[str, int]:
    """Conta médicos no banco agrupados por estado."""
    result = session.execute(
        text("SELECT state, COUNT(*)::int AS total FROM doctors GROUP BY state")
    )
    return {row[0]: row[1] for row in result}


def get_natural_counts_by_state(session: Session) -> dict[str, int]:
    """Conta CRMs naturais distintos por estado."""
    result = session.execute(
        text(
            "SELECT state, COUNT(DISTINCT crm_natural)::int AS total "
            "FROM doctors WHERE crm_natural IS NOT NULL GROUP BY state"
        )
    )
    return {row[0]: row[1] for row in result}


def get_total_distinct_natural_count(session: Session) -> int:
    """Conta total de CRMs naturais distintos (médicos únicos)."""
    result = session.execute(
        text(
            "SELECT COUNT(DISTINCT crm_natural)::int "
            "FROM doctors WHERE crm_natural IS NOT NULL"
        )
    ).scalar()
    return result or 0
