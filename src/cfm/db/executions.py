"""Operações de persistência para planos de execução do crawler."""

from __future__ import annotations

import json

import asyncpg


# ── Tipos válidos ──────────────────────────────────────────────

EXECUTION_TYPES = ("doctor",)

EXECUTION_STATUSES = (
    "pending",
    "running",
    "paused",
    "completed",
    "cancelled",
    "failed",
)
STATE_STATUSES = ("pending", "running", "completed", "failed", "skipped")
PAGE_STATUSES = ("pending", "fetched", "failed")


# ── CRUD de execuções ──────────────────────────────────────────


async def create_execution(
    pool: asyncpg.Pool,
    exec_type: str,
    page_size: int,
    batch_size: int,
    params: dict,
    states: list[str],
) -> int:
    """Cria uma execução com seus estados (um por UF).

    Returns:
        ID da execução criada.
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                INSERT INTO crawl_executions (type, page_size, batch_size, params)
                VALUES ($1, $2, $3, $4::jsonb)
                RETURNING id
                """,
                exec_type,
                page_size,
                batch_size,
                json.dumps(params, ensure_ascii=False),
            )
            execution_id = row["id"]

            for state in states:
                await conn.execute(
                    """
                    INSERT INTO crawl_execution_states (execution_id, state)
                    VALUES ($1, $2)
                    """,
                    execution_id,
                    state.upper(),
                )

    return execution_id


async def get_execution(pool: asyncpg.Pool, execution_id: int) -> dict | None:
    """Retorna uma execução com seus estados."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM crawl_executions WHERE id = $1",
            execution_id,
        )
        if not row:
            return None

        states = await conn.fetch(
            """
            SELECT * FROM crawl_execution_states
            WHERE execution_id = $1
            ORDER BY state
            """,
            execution_id,
        )

        execution = dict(row)
        execution["params"] = (
            json.loads(execution["params"])
            if isinstance(execution["params"], str)
            else execution["params"]
        )
        execution["states"] = [dict(s) for s in states]
        return execution


async def list_active_executions(pool: asyncpg.Pool) -> list[dict]:
    """Lista execuções ativas (não finalizadas nem canceladas)."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT e.*,
                   (SELECT COUNT(*) FROM crawl_execution_states
                    WHERE execution_id = e.id) AS total_states,
                   (SELECT COUNT(*) FROM crawl_execution_states
                    WHERE execution_id = e.id AND status = 'completed') AS completed_states
            FROM crawl_executions e
            WHERE e.status NOT IN ('completed', 'cancelled')
            ORDER BY e.created_at DESC
            """,
        )

    result = []
    for row in rows:
        d = dict(row)
        d["params"] = (
            json.loads(d["params"]) if isinstance(d["params"], str) else d["params"]
        )
        result.append(d)
    return result


async def start_execution(pool: asyncpg.Pool, execution_id: int) -> None:
    """Marca uma execução como 'running'."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE crawl_executions
            SET status = 'running', started_at = COALESCE(started_at, NOW())
            WHERE id = $1
            """,
            execution_id,
        )


async def pause_execution(pool: asyncpg.Pool, execution_id: int) -> None:
    """Marca uma execução como 'paused'."""
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE crawl_executions SET status = 'paused' WHERE id = $1",
            execution_id,
        )


async def cancel_execution(pool: asyncpg.Pool, execution_id: int) -> None:
    """Cancela uma execução."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE crawl_executions
            SET status = 'cancelled', completed_at = NOW()
            WHERE id = $1
            """,
            execution_id,
        )


async def fail_execution(pool: asyncpg.Pool, execution_id: int) -> None:
    """Marca uma execução como 'failed'."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE crawl_executions
            SET status = 'failed', completed_at = NOW()
            WHERE id = $1
            """,
            execution_id,
        )


async def complete_execution(pool: asyncpg.Pool, execution_id: int) -> None:
    """Marca uma execução como 'completed'."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE crawl_executions
            SET status = 'completed', completed_at = NOW()
            WHERE id = $1
            """,
            execution_id,
        )


# ── Estados de execução (por UF) ──────────────────────────────


async def get_execution_states(pool: asyncpg.Pool, execution_id: int) -> list[dict]:
    """Retorna os estados de uma execução."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT * FROM crawl_execution_states
            WHERE execution_id = $1
            ORDER BY state
            """,
            execution_id,
        )
    return [dict(r) for r in rows]


async def get_pending_states(pool: asyncpg.Pool, execution_id: int) -> list[dict]:
    """Retorna estados que ainda precisam ser processados."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT * FROM crawl_execution_states
            WHERE execution_id = $1
              AND status NOT IN ('completed', 'skipped')
            ORDER BY state
            """,
            execution_id,
        )
    return [dict(r) for r in rows]


async def start_execution_state(pool: asyncpg.Pool, state_id: int) -> None:
    """Marca um estado como 'running'."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE crawl_execution_states
            SET status = 'running', started_at = COALESCE(started_at, NOW())
            WHERE id = $1
            """,
            state_id,
        )


async def update_execution_state(
    pool: asyncpg.Pool,
    state_id: int,
    *,
    total_pages: int | None = None,
    total_records: int | None = None,
    status: str | None = None,
) -> None:
    """Atualiza campos de um estado de execução."""
    sets: list[str] = []
    args: list = []
    idx = 1

    if total_pages is not None:
        idx += 1
        sets.append(f"total_pages = ${idx}")
        args.append(total_pages)
    if total_records is not None:
        idx += 1
        sets.append(f"total_records = ${idx}")
        args.append(total_records)
    if status is not None:
        idx += 1
        sets.append(f"status = ${idx}")
        args.append(status)
        if status == "completed":
            sets.append("completed_at = NOW()")

    if not sets:
        return

    sql = f"UPDATE crawl_execution_states SET {', '.join(sets)} WHERE id = $1"
    async with pool.acquire() as conn:
        await conn.execute(sql, state_id, *args)


async def fail_execution_state(pool: asyncpg.Pool, state_id: int) -> None:
    """Marca um estado como 'failed'."""
    await update_execution_state(pool, state_id, status="failed")


async def complete_execution_state(pool: asyncpg.Pool, state_id: int) -> None:
    """Marca um estado como 'completed'."""
    await update_execution_state(pool, state_id, status="completed")


# ── Páginas ────────────────────────────────────────────────────


async def initialize_pages(
    pool: asyncpg.Pool,
    execution_state_id: int,
    total_pages: int,
) -> int:
    """Pré-cria todas as páginas como 'pending'.

    Idempotente: usa ON CONFLICT DO NOTHING para suportar re-execução
    quando total_pages muda (novas páginas são adicionadas, existentes mantidas).

    Returns:
        Número de páginas inseridas.
    """
    if total_pages <= 0:
        return 0

    async with pool.acquire() as conn:
        result = await conn.execute(
            """
            INSERT INTO crawl_pages (execution_state_id, page_number)
            SELECT $1, generate_series(1, $2)
            ON CONFLICT (execution_state_id, page_number) DO NOTHING
            """,
            execution_state_id,
            total_pages,
        )
        # result formato: "INSERT 0 N"
        try:
            return int(result.split()[-1])
        except (ValueError, IndexError):
            return 0


async def get_pending_pages(
    pool: asyncpg.Pool,
    execution_state_id: int,
    limit: int | None = None,
) -> list[int]:
    """Retorna números de páginas pendentes (pending ou failed).

    Pages with status 'failed' são incluídas para retry automático.
    Ordenadas por page_number para processamento sequencial.
    """
    sql = """
        SELECT page_number FROM crawl_pages
        WHERE execution_state_id = $1
          AND status IN ('pending', 'failed')
        ORDER BY page_number
    """
    args: list = [execution_state_id]

    if limit is not None:
        sql += " LIMIT $2"
        args.append(limit)

    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *args)

    return [r["page_number"] for r in rows]


async def mark_page_fetched(
    pool: asyncpg.Pool,
    execution_state_id: int,
    page_number: int,
    records_count: int = 0,
) -> None:
    """Marca uma página como buscada com sucesso."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE crawl_pages
            SET status = 'fetched', records_count = $3, fetched_at = NOW(), error = NULL
            WHERE execution_state_id = $1 AND page_number = $2
            """,
            execution_state_id,
            page_number,
            records_count,
        )


async def mark_page_failed(
    pool: asyncpg.Pool,
    execution_state_id: int,
    page_number: int,
    error: str = "",
) -> None:
    """Marca uma página como falha."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE crawl_pages
            SET status = 'failed', error = $3
            WHERE execution_state_id = $1 AND page_number = $2
            """,
            execution_state_id,
            page_number,
            error,
        )


async def mark_pages_fetched_batch(
    pool: asyncpg.Pool,
    execution_state_id: int,
    pages: list[tuple[int, int]],
) -> None:
    """Marca várias páginas como buscadas em batch.

    Args:
        pages: Lista de (page_number, records_count).
    """
    if not pages:
        return

    async with pool.acquire() as conn:
        await conn.executemany(
            """
            UPDATE crawl_pages
            SET status = 'fetched', records_count = $3, fetched_at = NOW(), error = NULL
            WHERE execution_state_id = $1 AND page_number = $2
            """,
            [(execution_state_id, pn, rc) for pn, rc in pages],
        )


async def get_page_stats(
    pool: asyncpg.Pool,
    execution_state_id: int,
) -> dict:
    """Retorna estatísticas de páginas para um estado.

    Returns:
        Dict com total, fetched, pending, failed.
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE status = 'fetched') AS fetched,
                COUNT(*) FILTER (WHERE status = 'pending') AS pending,
                COUNT(*) FILTER (WHERE status = 'failed') AS failed
            FROM crawl_pages
            WHERE execution_state_id = $1
            """,
            execution_state_id,
        )
    return dict(row) if row else {"total": 0, "fetched": 0, "pending": 0, "failed": 0}


async def get_execution_progress(
    pool: asyncpg.Pool,
    execution_id: int,
) -> dict:
    """Retorna o progresso completo de uma execução.

    Returns:
        Dict com informações detalhadas por estado e progresso geral.
    """
    async with pool.acquire() as conn:
        execution = await conn.fetchrow(
            "SELECT * FROM crawl_executions WHERE id = $1",
            execution_id,
        )
        if not execution:
            return {}

        states = await conn.fetch(
            """
            SELECT es.*,
                   COALESCE(ps.total, 0)   AS pages_total,
                   COALESCE(ps.fetched, 0) AS pages_fetched,
                   COALESCE(ps.pending, 0) AS pages_pending,
                   COALESCE(ps.failed, 0)  AS pages_failed
            FROM crawl_execution_states es
            LEFT JOIN LATERAL (
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE status = 'fetched') AS fetched,
                    COUNT(*) FILTER (WHERE status = 'pending') AS pending,
                    COUNT(*) FILTER (WHERE status = 'failed')  AS failed
                FROM crawl_pages
                WHERE execution_state_id = es.id
            ) ps ON TRUE
            WHERE es.execution_id = $1
            ORDER BY es.state
            """,
            execution_id,
        )

    exec_dict = dict(execution)
    exec_dict["params"] = (
        json.loads(exec_dict["params"])
        if isinstance(exec_dict["params"], str)
        else exec_dict["params"]
    )

    states_list = [dict(s) for s in states]

    total_pages_all = sum(s["pages_total"] for s in states_list)
    fetched_pages_all = sum(s["pages_fetched"] for s in states_list)
    percentage = (
        round(fetched_pages_all / total_pages_all * 100, 1)
        if total_pages_all > 0
        else 0.0
    )

    return {
        "execution": exec_dict,
        "states": states_list,
        "total_pages": total_pages_all,
        "fetched_pages": fetched_pages_all,
        "percentage": percentage,
    }


async def check_state_complete(
    pool: asyncpg.Pool,
    execution_state_id: int,
) -> bool:
    """Verifica se todas as páginas de um estado foram buscadas.

    Se sim, marca o estado como 'completed'.

    Returns:
        True se o estado foi marcado como completo.
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE status = 'fetched') AS fetched
            FROM crawl_pages
            WHERE execution_state_id = $1
            """,
            execution_state_id,
        )

    if row and row["total"] > 0 and row["total"] == row["fetched"]:
        await complete_execution_state(pool, execution_state_id)
        return True
    return False


async def check_execution_complete(
    pool: asyncpg.Pool,
    execution_id: int,
) -> bool:
    """Verifica se todos os estados foram processados.

    Se sim, marca a execução como 'completed'.

    Returns:
        True se a execução foi marcada como completa.
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE status IN ('completed', 'skipped')) AS done
            FROM crawl_execution_states
            WHERE execution_id = $1
            """,
            execution_id,
        )

    if row and row["total"] > 0 and row["total"] == row["done"]:
        await complete_execution(pool, execution_id)
        return True
    return False
