"""Base declarativa do SQLAlchemy para todas as entities do projeto."""

from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass


class Base(MappedAsDataclass, DeclarativeBase):
    """Base compartilhada para todas as entities SQLAlchemy.

    Combina MappedAsDataclass (interface pythonica) com DeclarativeBase.
    Todas as entities do projeto devem herdar desta classe.
    """

    pass
