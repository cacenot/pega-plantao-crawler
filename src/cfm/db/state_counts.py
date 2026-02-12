"""Operações de persistência para contagem de médicos por estado."""

from __future__ import annotations

import asyncpg


UPSERT_STATE_COUNT_SQL = """
INSERT INTO state_counts (state, api_total, db_total, missing, counted_at)
VALUES ($1, $2, $3, $4, NOW())
ON CONFLICT (state) DO UPDATE SET
    api_total  = EXCLUDED.api_total,
    db_total   = EXCLUDED.db_total,
    missing    = EXCLUDED.missing,
    counted_at = NOW();
"""


async def upsert_state_counts_batch(
    pool: asyncpg.Pool,
    rows: list[dict],
) -> None:
    """Insere ou atualiza contagens de múltiplos estados em batch.

    Args:
        pool: Pool de conexões asyncpg.
        rows: Lista de dicts com chaves: state, api_total, db_total, missing.
    """
    async with pool.acquire() as conn:
        await conn.executemany(
            UPSERT_STATE_COUNT_SQL,
            [(r["state"], r["api_total"], r["db_total"], r["missing"]) for r in rows],
        )


async def get_all_state_counts(pool: asyncpg.Pool) -> list[asyncpg.Record]:
    """Retorna todas as contagens ordenadas por estado."""
    async with pool.acquire() as conn:
        return await conn.fetch(
            "SELECT state, api_total, db_total, missing, counted_at "
            "FROM state_counts ORDER BY state"
        )


async def get_db_counts_by_state(pool: asyncpg.Pool) -> dict[str, int]:
    """Conta médicos no banco agrupados por estado.

    Returns:
        Dict mapeando UF -> quantidade de registros na tabela doctors.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT state, COUNT(*)::int AS total FROM doctors GROUP BY state"
        )
    return {row["state"]: row["total"] for row in rows}
