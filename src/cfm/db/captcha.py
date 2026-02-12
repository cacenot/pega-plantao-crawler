"""Gerenciamento de tokens de captcha no PostgreSQL.

Substitui o cache Redis para armazenamento de tokens do reCAPTCHA.
"""

from __future__ import annotations

import asyncpg


async def store_token(pool: asyncpg.Pool, token: str, ttl_seconds: int = 1800) -> None:
    """Armazena um token de captcha com TTL.

    Remove tokens antigos e insere o novo com data de expiração.
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Remove tokens expirados
            await conn.execute("DELETE FROM captcha_tokens WHERE expires_at <= NOW()")
            # Insere novo token
            await conn.execute(
                """
                INSERT INTO captcha_tokens (token, expires_at)
                VALUES ($1, NOW() + make_interval(secs => $2))
                """,
                token,
                float(ttl_seconds),
            )


async def get_token(pool: asyncpg.Pool) -> str | None:
    """Retorna o token válido mais recente, ou None se expirado/inexistente."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT token FROM captcha_tokens
            WHERE expires_at > NOW()
            ORDER BY created_at DESC
            LIMIT 1
            """
        )
    return row["token"] if row else None


async def is_valid(pool: asyncpg.Pool) -> bool:
    """Verifica se existe um token de captcha válido."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT EXISTS(SELECT 1 FROM captcha_tokens WHERE expires_at > NOW()) AS valid"
        )
    return row["valid"] if row else False


async def get_ttl(pool: asyncpg.Pool) -> int:
    """Retorna o TTL restante do token mais recente em segundos.

    Returns:
        Segundos restantes, ou 0 se não houver token válido.
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT GREATEST(0, EXTRACT(EPOCH FROM expires_at - NOW()))::int AS ttl
            FROM captcha_tokens
            WHERE expires_at > NOW()
            ORDER BY created_at DESC
            LIMIT 1
            """
        )
    return row["ttl"] if row else 0


async def delete_token(pool: asyncpg.Pool) -> None:
    """Remove todos os tokens de captcha."""
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM captcha_tokens")


async def cleanup_expired(pool: asyncpg.Pool) -> int:
    """Remove tokens expirados.

    Returns:
        Número de tokens removidos.
    """
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM captcha_tokens WHERE expires_at <= NOW()"
        )
        try:
            return int(result.split()[-1])
        except (ValueError, IndexError):
            return 0
