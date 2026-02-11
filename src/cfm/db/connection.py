"""Gerenciamento do pool de conexões asyncpg."""

from __future__ import annotations

import asyncpg

_pool: asyncpg.Pool | None = None


async def create_pool(dsn: str, **kwargs) -> asyncpg.Pool:
    """Cria e retorna um pool de conexões PostgreSQL.

    Args:
        dsn: Connection string, ex: postgresql://user:pass@host:port/db
        **kwargs: Argumentos extras para asyncpg.create_pool()

    Returns:
        Pool de conexões asyncpg.
    """
    global _pool
    if _pool is not None:
        return _pool

    _pool = await asyncpg.create_pool(
        dsn,
        min_size=2,
        max_size=10,
        **kwargs,
    )
    return _pool


async def get_pool() -> asyncpg.Pool:
    """Retorna o pool de conexões existente.

    Raises:
        RuntimeError: Se o pool não foi criado.
    """
    if _pool is None:
        raise RuntimeError(
            "Pool de conexões não inicializado. Chame create_pool() primeiro."
        )
    return _pool


async def close_pool() -> None:
    """Fecha o pool de conexões."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
