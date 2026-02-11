"""Operações de persistência para especialidades no PostgreSQL."""

from __future__ import annotations

import asyncpg


INSERT_SPECIALTY_SQL = """
INSERT INTO specialties (name)
VALUES ($1)
ON CONFLICT (name) DO NOTHING;
"""


async def insert_specialties(pool: asyncpg.Pool, names: set[str] | list[str]) -> int:
    """Insere especialidades no banco, ignorando duplicatas.

    Args:
        pool: Pool de conexões asyncpg.
        names: Conjunto ou lista de nomes de especialidades.

    Returns:
        Número de especialidades processadas.
    """
    if not names:
        return 0

    rows = [(name,) for name in sorted(names)]
    async with pool.acquire() as conn:
        await conn.executemany(INSERT_SPECIALTY_SQL, rows)

    return len(rows)


async def get_all_specialties(pool: asyncpg.Pool) -> list[dict]:
    """Retorna todas as especialidades do banco.

    Returns:
        Lista de dicts com id, name e created_at.
    """
    async with pool.acquire() as conn:
        records = await conn.fetch(
            "SELECT id, name, created_at FROM specialties ORDER BY name;"
        )
    return [dict(r) for r in records]
