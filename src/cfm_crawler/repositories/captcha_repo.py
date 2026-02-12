"""Repositório de tokens de captcha."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session


def store_token(session: Session, token: str, ttl_seconds: int = 1800) -> None:
    """Armazena um token de captcha com TTL.

    Remove tokens antigos e insere o novo com data de expiração.
    """
    session.execute(text("DELETE FROM captcha_tokens WHERE expires_at <= NOW()"))
    session.execute(
        text(
            "INSERT INTO captcha_tokens (token, expires_at) "
            "VALUES (:token, NOW() + make_interval(secs => :ttl))"
        ),
        {"token": token, "ttl": float(ttl_seconds)},
    )


def get_token(session: Session) -> str | None:
    """Retorna o token válido mais recente, ou None se expirado/inexistente."""
    result = session.execute(
        text(
            "SELECT token FROM captcha_tokens "
            "WHERE expires_at > NOW() "
            "ORDER BY created_at DESC LIMIT 1"
        )
    ).fetchone()
    return result[0] if result else None


def is_valid(session: Session) -> bool:
    """Verifica se existe um token de captcha válido."""
    result = session.execute(
        text(
            "SELECT EXISTS("
            "SELECT 1 FROM captcha_tokens WHERE expires_at > NOW()"
            ") AS valid"
        )
    ).fetchone()
    return result[0] if result else False


def get_ttl(session: Session) -> int:
    """Retorna o TTL restante do token mais recente em segundos."""
    result = session.execute(
        text(
            "SELECT GREATEST(0, EXTRACT(EPOCH FROM expires_at - NOW()))::int AS ttl "
            "FROM captcha_tokens WHERE expires_at > NOW() "
            "ORDER BY created_at DESC LIMIT 1"
        )
    ).fetchone()
    return result[0] if result else 0


def delete_token(session: Session) -> None:
    """Remove todos os tokens de captcha."""
    session.execute(text("DELETE FROM captcha_tokens"))


def cleanup_expired(session: Session) -> int:
    """Remove tokens expirados.

    Returns:
        Número de tokens removidos.
    """
    result = session.execute(
        text("DELETE FROM captcha_tokens WHERE expires_at <= NOW()")
    )
    return result.rowcount or 0
