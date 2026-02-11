"""DDL para criação das tabelas no PostgreSQL."""

from __future__ import annotations

import asyncpg

DOCTORS_TABLE = """
CREATE TABLE IF NOT EXISTS doctors (
    id                      SERIAL PRIMARY KEY,
    crm                     INTEGER      NOT NULL,
    raw_crm                 VARCHAR(20)  NOT NULL,
    crm_natural             VARCHAR(20),
    state                   VARCHAR(2)   NOT NULL,
    name                    VARCHAR(255) NOT NULL,
    social_name             VARCHAR(255),
    status                  VARCHAR(50),
    specialties             JSONB        DEFAULT '[]'::jsonb,
    registration_type       VARCHAR(50),
    registration_date       DATE,
    graduation_institution  VARCHAR(255),
    graduation_date         VARCHAR(10),
    is_foreign              BOOLEAN      NOT NULL DEFAULT FALSE,
    security_hash           VARCHAR(64),
    interdicao_obs          TEXT,
    phone                   VARCHAR(50),
    address                 TEXT,
    photo_url               TEXT,
    raw_data                JSONB        NOT NULL DEFAULT '{}'::jsonb,
    created_at              TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMP    NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_doctors_crm_state UNIQUE (crm, state)
);
"""

DOCTORS_INDEX = """
CREATE INDEX IF NOT EXISTS idx_doctors_state ON doctors (state);
CREATE INDEX IF NOT EXISTS idx_doctors_status ON doctors (status);
CREATE INDEX IF NOT EXISTS idx_doctors_specialties ON doctors USING GIN (specialties);
"""

SPECIALTIES_TABLE = """
CREATE TABLE IF NOT EXISTS specialties (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(255) UNIQUE NOT NULL,
    created_at  TIMESTAMP NOT NULL DEFAULT NOW()
);
"""

UPDATE_TRIGGER = """
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'trg_doctors_updated_at'
    ) THEN
        CREATE TRIGGER trg_doctors_updated_at
            BEFORE UPDATE ON doctors
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
    END IF;
END;
$$;
"""

# Migration para bases existentes que tinham o schema antigo
MIGRATION_SQL = """
DO $$
BEGIN
    -- Adiciona raw_crm se não existir (copia crm original antes da conversão)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'doctors' AND column_name = 'raw_crm'
    ) THEN
        ALTER TABLE doctors ADD COLUMN raw_crm VARCHAR(20);
        UPDATE doctors SET raw_crm = crm WHERE raw_crm IS NULL;
        ALTER TABLE doctors ALTER COLUMN raw_crm SET NOT NULL;
    END IF;

    -- Converte crm para INTEGER se ainda for VARCHAR
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'doctors' AND column_name = 'crm'
        AND data_type IN ('character varying', 'character', 'text')
    ) THEN
        -- Remove a constraint antiga que depende do tipo
        ALTER TABLE doctors DROP CONSTRAINT IF EXISTS uq_doctors_crm_state;
        ALTER TABLE doctors ALTER COLUMN crm TYPE INTEGER USING regexp_replace(crm, '[^0-9]', '', 'g')::integer;
        ALTER TABLE doctors ADD CONSTRAINT uq_doctors_crm_state UNIQUE (crm, state);
    END IF;

    -- Adiciona novas colunas
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'doctors' AND column_name = 'crm_natural'
    ) THEN
        ALTER TABLE doctors ADD COLUMN crm_natural VARCHAR(20);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'doctors' AND column_name = 'is_foreign'
    ) THEN
        ALTER TABLE doctors ADD COLUMN is_foreign BOOLEAN NOT NULL DEFAULT FALSE;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'doctors' AND column_name = 'security_hash'
    ) THEN
        ALTER TABLE doctors ADD COLUMN security_hash VARCHAR(64);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'doctors' AND column_name = 'interdicao_obs'
    ) THEN
        ALTER TABLE doctors ADD COLUMN interdicao_obs TEXT;
    END IF;
END;
$$;
"""


async def ensure_tables(pool: asyncpg.Pool) -> None:
    """Cria as tabelas e índices se não existirem.

    Também executa migrations para atualizar bases existentes.
    """
    async with pool.acquire() as conn:
        await conn.execute(DOCTORS_TABLE)
        await conn.execute(DOCTORS_INDEX)
        await conn.execute(SPECIALTIES_TABLE)
        await conn.execute(UPDATE_TRIGGER)
        await conn.execute(MIGRATION_SQL)
    print("✅ Tabelas verificadas/criadas no PostgreSQL.")
