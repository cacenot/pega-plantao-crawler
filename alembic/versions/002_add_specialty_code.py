"""Add code column to specialties table.

Revision ID: 002
Revises: 001
Create Date: 2026-02-13
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "002"
down_revision: Union[str, None] = "001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Adicionar coluna code (nullable primeiro para migrar dados existentes)
    op.add_column(
        "specialties",
        sa.Column("code", sa.String(255), nullable=True),
    )

    # Preencher code com UPPER(name) para registros existentes
    op.execute("UPDATE specialties SET code = UPPER(name) WHERE code IS NULL")

    # Tornar NOT NULL e adicionar unique constraint
    op.alter_column("specialties", "code", nullable=False)
    op.create_unique_constraint("uq_specialties_code", "specialties", ["code"])


def downgrade() -> None:
    op.drop_constraint("uq_specialties_code", "specialties", type_="unique")
    op.drop_column("specialties", "code")
