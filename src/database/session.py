"""Engine e SessionLocal factory para SQLAlchemy sync."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

_engine = None
_SessionLocal: sessionmaker[Session] | None = None


def init_engine(database_url: str) -> None:
    """Inicializa o engine e a session factory.

    Converte URLs no formato ``postgresql://`` para ``postgresql+psycopg://``
    se necessário, pois o SQLAlchemy precisa de um driver explícito.

    Args:
        database_url: Connection string PostgreSQL.
    """
    global _engine, _SessionLocal

    if _engine is not None:
        return

    # Normaliza driver para psycopg (v3)
    url = database_url
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg://", 1)
    elif url.startswith("postgresql+psycopg2://"):
        url = url.replace("postgresql+psycopg2://", "postgresql+psycopg://", 1)

    _engine = create_engine(url, pool_size=5, max_overflow=10, pool_pre_ping=True)
    _SessionLocal = sessionmaker(bind=_engine)


def get_engine():
    """Retorna o engine inicializado.

    Raises:
        RuntimeError: Se ``init_engine()`` não foi chamado.
    """
    if _engine is None:
        raise RuntimeError("Engine não inicializado. Chame init_engine() primeiro.")
    return _engine


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """Context manager que fornece uma sessão transacional.

    A sessão faz commit ao sair sem erros e rollback em caso de exceção.

    Usage::

        with get_session() as session:
            session.add(entity)
    """
    if _SessionLocal is None:
        raise RuntimeError("Engine não inicializado. Chame init_engine() primeiro.")

    session = _SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def close_engine() -> None:
    """Fecha o engine e limpa recursos."""
    global _engine, _SessionLocal
    if _engine is not None:
        _engine.dispose()
        _engine = None
        _SessionLocal = None
