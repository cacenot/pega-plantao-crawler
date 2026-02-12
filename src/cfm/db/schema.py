"""DDL para criação das tabelas no PostgreSQL."""

from __future__ import annotations

import asyncpg

# ── Tabela de médicos ──────────────────────────────────────────

DOCTORS_TABLE = """
CREATE TABLE IF NOT EXISTS doctors (
    id                      SERIAL PRIMARY KEY,
    crm                     BIGINT       NOT NULL,
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

# ── Tabelas de plano de execução ───────────────────────────────

CRAWL_EXECUTIONS_TABLE = """
CREATE TABLE IF NOT EXISTS crawl_executions (
    id              SERIAL PRIMARY KEY,
    type            VARCHAR(50)  NOT NULL,
    page_size       INTEGER      NOT NULL,
    batch_size      INTEGER      NOT NULL,
    status          VARCHAR(20)  NOT NULL DEFAULT 'pending',
    params          JSONB        NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
    started_at      TIMESTAMP,
    completed_at    TIMESTAMP
);
"""

CRAWL_EXECUTIONS_INDEX = """
CREATE INDEX IF NOT EXISTS idx_crawl_executions_status ON crawl_executions (status);
CREATE INDEX IF NOT EXISTS idx_crawl_executions_type ON crawl_executions (type);
"""

CRAWL_EXECUTION_STATES_TABLE = """
CREATE TABLE IF NOT EXISTS crawl_execution_states (
    id              SERIAL PRIMARY KEY,
    execution_id    INTEGER      NOT NULL
                        REFERENCES crawl_executions(id) ON DELETE CASCADE,
    state           VARCHAR(2)   NOT NULL,
    status          VARCHAR(20)  NOT NULL DEFAULT 'pending',
    total_pages     INTEGER,
    total_records   INTEGER,
    created_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
    started_at      TIMESTAMP,
    completed_at    TIMESTAMP,

    CONSTRAINT uq_exec_state UNIQUE (execution_id, state)
);
"""

CRAWL_EXECUTION_STATES_INDEX = """
CREATE INDEX IF NOT EXISTS idx_crawl_exec_states_exec_id
    ON crawl_execution_states (execution_id);
CREATE INDEX IF NOT EXISTS idx_crawl_exec_states_status
    ON crawl_execution_states (status);
"""

CRAWL_PAGES_TABLE = """
CREATE TABLE IF NOT EXISTS crawl_pages (
    id                  SERIAL PRIMARY KEY,
    execution_state_id  INTEGER      NOT NULL
                            REFERENCES crawl_execution_states(id) ON DELETE CASCADE,
    page_number         INTEGER      NOT NULL,
    status              VARCHAR(20)  NOT NULL DEFAULT 'pending',
    records_count       INTEGER,
    fetched_at          TIMESTAMP,
    error               TEXT,

    CONSTRAINT uq_exec_state_page UNIQUE (execution_state_id, page_number)
);
"""

CRAWL_PAGES_INDEX = """
CREATE INDEX IF NOT EXISTS idx_crawl_pages_state_id_status
    ON crawl_pages (execution_state_id, status);
"""

# ── Tabela de tokens de captcha ────────────────────────────────

CAPTCHA_TOKENS_TABLE = """
CREATE TABLE IF NOT EXISTS captcha_tokens (
    id          SERIAL PRIMARY KEY,
    token       TEXT        NOT NULL,
    expires_at  TIMESTAMP   NOT NULL,
    created_at  TIMESTAMP   NOT NULL DEFAULT NOW()
);
"""

CAPTCHA_TOKENS_INDEX = """
CREATE INDEX IF NOT EXISTS idx_captcha_tokens_expires
    ON captcha_tokens (expires_at);
"""

# ── Triggers ───────────────────────────────────────────────────

UPDATE_TRIGGER = """
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
    _tbl TEXT;
    _trg TEXT;
BEGIN
    FOR _tbl IN
        SELECT unnest(ARRAY[
            'doctors',
            'crawl_executions',
            'crawl_execution_states'
        ])
    LOOP
        _trg := 'trg_' || _tbl || '_updated_at';
        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = _trg) THEN
            EXECUTE format(
                'CREATE TRIGGER %I BEFORE UPDATE ON %I '
                'FOR EACH ROW EXECUTE FUNCTION update_updated_at_column()',
                _trg, _tbl
            );
        END IF;
    END LOOP;
END;
$$;
"""

# ── Migration (bases existentes) ───────────────────────────────

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

    -- Converte crm para BIGINT se ainda for VARCHAR
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'doctors' AND column_name = 'crm'
        AND data_type IN ('character varying', 'character', 'text')
    ) THEN
        ALTER TABLE doctors DROP CONSTRAINT IF EXISTS uq_doctors_crm_state;
        ALTER TABLE doctors ALTER COLUMN crm TYPE BIGINT USING regexp_replace(crm, '[^0-9]', '', 'g')::bigint;

        -- Remove duplicatas antes de recriar a constraint unique
        DELETE FROM doctors d1
        USING doctors d2
        WHERE d1.crm = d2.crm
          AND d1.state = d2.state
          AND d1.id < d2.id;

        ALTER TABLE doctors ADD CONSTRAINT uq_doctors_crm_state UNIQUE (crm, state);
    END IF;

    -- Converte crm de INTEGER para BIGINT se necessário
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'doctors' AND column_name = 'crm'
        AND data_type = 'integer'
    ) THEN
        ALTER TABLE doctors ALTER COLUMN crm TYPE BIGINT;
    END IF;

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
        # Tabelas de dados
        await conn.execute(DOCTORS_TABLE)
        await conn.execute(DOCTORS_INDEX)
        await conn.execute(SPECIALTIES_TABLE)

        # Tabelas de plano de execução
        await conn.execute(CRAWL_EXECUTIONS_TABLE)
        await conn.execute(CRAWL_EXECUTIONS_INDEX)
        await conn.execute(CRAWL_EXECUTION_STATES_TABLE)
        await conn.execute(CRAWL_EXECUTION_STATES_INDEX)
        await conn.execute(CRAWL_PAGES_TABLE)
        await conn.execute(CRAWL_PAGES_INDEX)

        # Captcha tokens
        await conn.execute(CAPTCHA_TOKENS_TABLE)
        await conn.execute(CAPTCHA_TOKENS_INDEX)

        # Triggers e migrations
        await conn.execute(UPDATE_TRIGGER)
        await conn.execute(MIGRATION_SQL)
    print("✅ Tabelas verificadas/criadas no PostgreSQL.")
