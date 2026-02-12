"""Initial schema — doctors, specialties, captcha_tokens, state_counts.

Revision ID: 001
Revises: None
Create Date: 2026-02-12
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── doctors ────────────────────────────────────────────────
    op.create_table(
        "doctors",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("crm", sa.BigInteger, nullable=False),
        sa.Column("raw_crm", sa.String(20), nullable=False),
        sa.Column("crm_natural", sa.String(20)),
        sa.Column("state", sa.String(2), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("social_name", sa.String(255)),
        sa.Column("status", sa.String(50)),
        sa.Column("specialties", JSONB, server_default="'[]'::jsonb"),
        sa.Column("registration_type", sa.String(50)),
        sa.Column("registration_date", sa.DateTime),
        sa.Column("graduation_institution", sa.String(255)),
        sa.Column("graduation_date", sa.String(10)),
        sa.Column("is_foreign", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("security_hash", sa.String(64)),
        sa.Column("interdicao_obs", sa.Text),
        sa.Column("phone", sa.String(50)),
        sa.Column("address", sa.Text),
        sa.Column("photo_url", sa.Text),
        sa.Column("raw_data", JSONB, nullable=False, server_default="'{}'::jsonb"),
        sa.Column(
            "created_at", sa.DateTime, nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime, nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("crm", "state", name="uq_doctors_crm_state"),
    )

    op.create_index("idx_doctors_state", "doctors", ["state"])
    op.create_index("idx_doctors_status", "doctors", ["status"])
    op.create_index(
        "idx_doctors_specialties",
        "doctors",
        ["specialties"],
        postgresql_using="gin",
    )

    # ── specialties ────────────────────────────────────────────
    op.create_table(
        "specialties",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(255), unique=True, nullable=False),
        sa.Column(
            "created_at", sa.DateTime, nullable=False, server_default=sa.func.now()
        ),
    )

    # ── captcha_tokens ─────────────────────────────────────────
    op.create_table(
        "captcha_tokens",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("token", sa.Text, nullable=False),
        sa.Column("expires_at", sa.DateTime, nullable=False),
        sa.Column(
            "created_at", sa.DateTime, nullable=False, server_default=sa.func.now()
        ),
    )

    op.create_index("idx_captcha_tokens_expires", "captcha_tokens", ["expires_at"])

    # ── state_counts ───────────────────────────────────────────
    op.create_table(
        "state_counts",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("state", sa.String(2), unique=True, nullable=False),
        sa.Column("api_total", sa.Integer, nullable=False, server_default="0"),
        sa.Column("db_total", sa.Integer, nullable=False, server_default="0"),
        sa.Column("missing", sa.Integer, nullable=False, server_default="0"),
        sa.Column(
            "counted_at", sa.DateTime, nullable=False, server_default=sa.func.now()
        ),
    )

    op.create_index("idx_state_counts_state", "state_counts", ["state"])

    # ── Trigger: auto-update updated_at ────────────────────────
    op.execute("""
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = NOW();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """)

    op.execute("""
        CREATE TRIGGER trg_doctors_updated_at
        BEFORE UPDATE ON doctors
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    """)


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_doctors_updated_at ON doctors")
    op.execute("DROP FUNCTION IF EXISTS update_updated_at_column()")
    op.drop_table("state_counts")
    op.drop_table("captcha_tokens")
    op.drop_table("specialties")
    op.drop_table("doctors")
