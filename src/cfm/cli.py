"""CLI do crawler CFM usando Typer.

Subcomandos:
    create   — Criar um plano de execução (form interativo)
    run      — Iniciar/continuar uma execução
    list     — Listar execuções ativas
    show     — Visualizar detalhes de uma execução
    cancel   — Cancelar uma execução
    token    — Resolver reCAPTCHA manualmente e cachear token
"""

import asyncio
from typing import Annotated

import typer

app = typer.Typer(
    name="cfm",
    help="Crawler do Conselho Federal de Medicina (CFM).",
    no_args_is_help=True,
)

# UFs do Brasil com nomes
UFS_MAP = {
    "AC": "Acre",
    "AL": "Alagoas",
    "AM": "Amazonas",
    "AP": "Amapá",
    "BA": "Bahia",
    "CE": "Ceará",
    "DF": "Distrito Federal",
    "ES": "Espírito Santo",
    "GO": "Goiás",
    "MA": "Maranhão",
    "MG": "Minas Gerais",
    "MS": "Mato Grosso do Sul",
    "MT": "Mato Grosso",
    "PA": "Pará",
    "PB": "Paraíba",
    "PE": "Pernambuco",
    "PI": "Piauí",
    "PR": "Paraná",
    "RJ": "Rio de Janeiro",
    "RN": "Rio Grande do Norte",
    "RO": "Rondônia",
    "RR": "Roraima",
    "RS": "Rio Grande do Sul",
    "SC": "Santa Catarina",
    "SE": "Sergipe",
    "SP": "São Paulo",
    "TO": "Tocantins",
}

UFS = list(UFS_MAP.keys())

EXECUTION_TYPES = {
    "doctor": "Médicos",
    "company": "Empresas Médicas",
}


# ── create ─────────────────────────────────────────────────────


@app.command()
def create() -> None:
    """Criar um novo plano de execução (form interativo)."""
    from InquirerPy import inquirer
    from InquirerPy.base.control import Choice
    from InquirerPy.separator import Separator

    from .config import get_cfm_settings

    settings = get_cfm_settings()

    print("\n" + "=" * 60)
    print("📋 CFM - Criar Plano de Execução")
    print("=" * 60)

    # ── Tipo (radio) ───────────────────────────────────────────
    exec_type = inquirer.select(
        message="Tipo de execução:",
        choices=[
            Choice(value="doctor", name="Médicos"),
            Choice(value="company", name="Empresas Médicas"),
        ],
        default="doctor",
        pointer="❯",
    ).execute()

    if exec_type == "company":
        print("\n🚧 Empresas Médicas ainda não está implementado.")
        print("   Este tipo será disponibilizado em uma versão futura.")
        raise typer.Exit()

    # ── Estados (checkbox) ─────────────────────────────────────
    # Agrupa por região para organizar a lista
    regions = {
        "Norte": ["AC", "AM", "AP", "PA", "RO", "RR", "TO"],
        "Nordeste": ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"],
        "Centro-Oeste": ["DF", "GO", "MS", "MT"],
        "Sudeste": ["ES", "MG", "RJ", "SP"],
        "Sul": ["PR", "RS", "SC"],
    }

    state_choices: list = [
        Choice(value="all", name="✦ Todos os estados (27 UFs)"),
        Separator("─" * 40),
    ]
    for region_name, region_ufs in regions.items():
        state_choices.append(Separator(f"── {region_name} "))
        for uf in region_ufs:
            state_choices.append(Choice(value=uf, name=f"{uf} - {UFS_MAP[uf]}"))

    selected = inquirer.checkbox(
        message="Selecione os estados:",
        choices=state_choices,
        pointer="❯",
        instruction="(Espaço para marcar, Enter para confirmar)",
        validate=lambda result: len(result) > 0,
        invalid_message="Selecione pelo menos um estado.",
    ).execute()

    if "all" in selected:
        states = UFS
    else:
        states = [s for s in selected if s in UFS]

    if not states:
        typer.echo("❌ Nenhum estado selecionado.")
        raise typer.Exit(code=1)

    # ── Page size ──────────────────────────────────────────────
    page_size = int(
        inquirer.number(
            message="Page size (registros por página):",
            default=settings.page_size,
            min_allowed=1,
            max_allowed=10000,
        ).execute()
    )

    # ── Batch size ─────────────────────────────────────────────
    batch_size = int(
        inquirer.number(
            message="Batch size (páginas por batch paralelo):",
            default=settings.batch_size,
            min_allowed=1,
            max_allowed=100,
        ).execute()
    )

    # ── Confirmação ────────────────────────────────────────────
    states_display = ", ".join(states[:6])
    if len(states) > 6:
        states_display += f" +{len(states) - 6}"

    print("\n" + "-" * 60)
    print("📋 Resumo do plano de execução:")
    print(f"   Tipo:       {EXECUTION_TYPES[exec_type]} ({exec_type})")
    print(f"   Estados:    {states_display} ({len(states)} UFs)")
    print(f"   Page size:  {page_size}")
    print(f"   Batch size: {batch_size}")
    print("-" * 60)

    if not inquirer.confirm(message="Confirmar criação?", default=True).execute():
        typer.echo("❌ Cancelado.")
        raise typer.Exit()

    params = {"states": states}
    execution_id = asyncio.run(
        _create_execution(exec_type, page_size, batch_size, params, states)
    )

    print(f"\n✅ Execução #{execution_id} criada com sucesso!")

    if inquirer.confirm(message="🚀 Iniciar execução agora?", default=True).execute():
        asyncio.run(_run_execution(execution_id))


async def _create_execution(
    exec_type: str,
    page_size: int,
    batch_size: int,
    params: dict,
    states: list[str],
) -> int:
    """Cria a execução no banco."""
    from .config import get_cfm_settings
    from .db.connection import close_pool, create_pool
    from .db.executions import create_execution
    from .db.schema import ensure_tables

    settings = get_cfm_settings()
    pool = await create_pool(settings.database_url)
    await ensure_tables(pool)

    execution_id = await create_execution(
        pool=pool,
        exec_type=exec_type,
        page_size=page_size,
        batch_size=batch_size,
        params=params,
        states=states,
    )

    await close_pool()
    return execution_id


# ── run ────────────────────────────────────────────────────────


@app.command()
def run(
    execution_id: Annotated[
        int,
        typer.Argument(help="ID da execução para iniciar/continuar."),
    ],
) -> None:
    """Iniciar ou continuar uma execução existente."""
    asyncio.run(_run_execution(execution_id))


async def _run_execution(execution_id: int) -> None:
    """Lógica async do subcomando run."""
    from playwright.async_api import async_playwright

    from .config import get_cfm_settings
    from .crawler import CFM_PAGE_URL, run_execution
    from .db import captcha as captcha_db
    from .db.connection import close_pool, create_pool
    from .db.executions import get_execution
    from .db.schema import ensure_tables

    settings = get_cfm_settings()

    pool = await create_pool(settings.database_url)
    await ensure_tables(pool)

    # Validar execução
    execution = await get_execution(pool, execution_id)

    if not execution:
        print(f"❌ Execução #{execution_id} não encontrada.")
        await close_pool()
        return

    if execution["status"] in ("completed", "cancelled"):
        print(f"❌ Execução #{execution_id} já está {execution['status']}.")
        await close_pool()
        return

    states = [s["state"] for s in execution["states"]]

    print("=" * 60)
    print(f"🏥 CFM - Execução #{execution_id}")
    print(f"📌 Tipo: {EXECUTION_TYPES.get(execution['type'], execution['type'])}")
    print(f"📋 UFs: {', '.join(states)}")
    print(f"📦 Page size: {execution['page_size']}")
    print(f"⚡ Batch size: {execution['batch_size']}")
    print(
        f"🔗 Database: {settings.database_url.split('@')[-1] if '@' in settings.database_url else settings.database_url}"
    )
    print("=" * 60)

    # Validar token de captcha
    if not await captcha_db.is_valid(pool):
        print("\n❌ Token de captcha não encontrado ou expirado!")
        print("   Execute primeiro: uv run cfm token")
        await close_pool()
        return

    ttl = await captcha_db.get_ttl(pool)
    print(f"\n✅ Token de captcha encontrado (TTL: {ttl}s)")

    # Abrir navegador
    playwright = await async_playwright().start()

    browser = await playwright.chromium.launch(
        headless=settings.headless,
        channel="chrome",
        args=["--disable-blink-features=AutomationControlled"],
    )
    context = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1280, "height": 720},
        locale="pt-BR",
    )

    await context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    """)

    page = await context.new_page()

    print("\n🌐 Abrindo portal do CFM no navegador...")
    await page.goto(CFM_PAGE_URL, wait_until="domcontentloaded", timeout=60000)

    try:
        await page.wait_for_selector("iframe[src*='recaptcha']", timeout=15000)
    except Exception:
        pass

    try:
        total_medicos = await run_execution(
            page=page,
            execution_id=execution_id,
            db_pool=pool,
            page_size=execution["page_size"],
            batch_size=execution["batch_size"],
            delay=settings.delay,
            fetch_fotos=settings.fetch_fotos,
            max_results=settings.max_results,
            request_timeout=settings.request_timeout,
        )

        print("\n" + "=" * 60)
        print(f"✅ Sessão finalizada! Total: {total_medicos} médicos processados")
        print("=" * 60)

    except KeyboardInterrupt:
        print("\n\n🛑 Interrompido. A execução foi pausada e pode ser retomada.")
    except RuntimeError as e:
        if "captcha" in str(e).lower():
            print("\n❌ Token do captcha expirou.")
            print("   Execute: uv run cfm token")
            print(f"   Depois: uv run cfm run {execution_id}")
        else:
            print(f"\n❌ Erro: {e}")
    finally:
        await browser.close()
        await playwright.stop()
        await close_pool()


# ── list ───────────────────────────────────────────────────────


@app.command(name="list")
def list_executions() -> None:
    """Listar execuções ativas (pendentes, em andamento, pausadas ou com falha)."""
    asyncio.run(_list_executions())


async def _list_executions() -> None:
    """Lógica async do subcomando list."""
    from .config import get_cfm_settings
    from .db.connection import close_pool, create_pool
    from .db.executions import list_active_executions
    from .db.schema import ensure_tables

    settings = get_cfm_settings()
    pool = await create_pool(settings.database_url)
    await ensure_tables(pool)

    executions = await list_active_executions(pool)
    await close_pool()

    if not executions:
        print("\nℹ️  Nenhuma execução ativa encontrada.")
        print("   Use 'uv run cfm create' para criar uma nova execução.")
        return

    print("\n" + "=" * 70)
    print("📋 Execuções Ativas")
    print("=" * 70)

    status_icons = {
        "pending": "⏳",
        "running": "🔄",
        "paused": "⏸️",
        "failed": "❌",
    }

    for ex in executions:
        icon = status_icons.get(ex["status"], "❓")
        states_list = ex["params"].get("states", [])
        states_str = ", ".join(states_list[:5])
        if len(states_list) > 5:
            states_str += f" +{len(states_list) - 5}"

        completed = ex.get("completed_states", 0)
        total = ex.get("total_states", 0)
        progress = f"{completed}/{total} UFs" if total > 0 else "—"

        created = ex["created_at"].strftime("%d/%m %H:%M") if ex["created_at"] else "—"

        print(
            f"\n  {icon} #{ex['id']:>3}  │  {ex['type']:<8}  │  {ex['status']:<10}  │  "
            f"{progress:<10}  │  {created}"
        )
        print(f"         │  UFs: {states_str}")
        print(f"         │  Page: {ex['page_size']}  Batch: {ex['batch_size']}")

    print("\n" + "-" * 70)
    print("  Comandos: cfm show <id> │ cfm run <id> │ cfm cancel <id>")
    print("=" * 70)


# ── show ───────────────────────────────────────────────────────


@app.command()
def show(
    execution_id: Annotated[
        int,
        typer.Argument(help="ID da execução para visualizar."),
    ],
) -> None:
    """Visualizar detalhes e progresso de uma execução."""
    asyncio.run(_show_execution(execution_id))


async def _show_execution(execution_id: int) -> None:
    """Lógica async do subcomando show."""
    from .config import get_cfm_settings
    from .db.connection import close_pool, create_pool
    from .db.executions import get_execution_progress
    from .db.schema import ensure_tables

    settings = get_cfm_settings()
    pool = await create_pool(settings.database_url)
    await ensure_tables(pool)

    progress = await get_execution_progress(pool, execution_id)
    await close_pool()

    if not progress:
        print(f"❌ Execução #{execution_id} não encontrada.")
        return

    ex = progress["execution"]
    states = progress["states"]

    status_icons = {
        "pending": "⏳",
        "running": "🔄",
        "paused": "⏸️",
        "completed": "✅",
        "cancelled": "🚫",
        "failed": "❌",
    }

    print("\n" + "=" * 60)
    print(f"📋 Execução #{execution_id}")
    print("=" * 60)

    icon = status_icons.get(ex["status"], "❓")
    print(f"\n  Status:     {icon} {ex['status']}")
    print(f"  Tipo:       {EXECUTION_TYPES.get(ex['type'], ex['type'])}")
    print(f"  Page size:  {ex['page_size']}")
    print(f"  Batch size: {ex['batch_size']}")

    if ex["created_at"]:
        print(f"  Criado em:  {ex['created_at'].strftime('%d/%m/%Y %H:%M:%S')}")
    if ex["started_at"]:
        print(f"  Iniciado:   {ex['started_at'].strftime('%d/%m/%Y %H:%M:%S')}")
    if ex["completed_at"]:
        print(f"  Finalizado: {ex['completed_at'].strftime('%d/%m/%Y %H:%M:%S')}")

    # Progresso geral
    total_p = progress["total_pages"]
    fetched_p = progress["fetched_pages"]
    pct = progress["percentage"]

    print(f"\n  📊 Progresso geral: {fetched_p}/{total_p} páginas ({pct}%)")

    bar_width = 30
    filled = int(bar_width * pct / 100) if pct > 0 else 0
    bar = "█" * filled + "░" * (bar_width - filled)
    print(f"  [{bar}] {pct}%")

    # Progresso por estado
    print(
        f"\n  {'UF':<4} {'Status':<12} {'Páginas':>12}  {'Progresso':>10}  {'Records':>10}"
    )
    print("  " + "-" * 56)

    for s in states:
        s_icon = status_icons.get(s["status"], "❓")
        pages_total = s["pages_total"]
        pages_fetched = s["pages_fetched"]
        pages_failed = s["pages_failed"]

        if pages_total > 0:
            s_pct = round(pages_fetched / pages_total * 100, 1)
            pages_str = f"{pages_fetched}/{pages_total}"
            if pages_failed > 0:
                pages_str += f" ({pages_failed}err)"
        else:
            s_pct = 0
            pages_str = "—"

        records_str = str(s["total_records"]) if s["total_records"] else "—"

        print(
            f"  {s['state']:<4} {s_icon} {s['status']:<10} {pages_str:>12}  "
            f"{s_pct:>8.1f}%  {records_str:>10}"
        )

    print("\n" + "=" * 60)


# ── cancel ─────────────────────────────────────────────────────


@app.command()
def cancel(
    execution_id: Annotated[
        int,
        typer.Argument(help="ID da execução para cancelar."),
    ],
) -> None:
    """Cancelar uma execução."""
    asyncio.run(_cancel_execution(execution_id))


async def _cancel_execution(execution_id: int) -> None:
    """Lógica async do subcomando cancel."""
    from .config import get_cfm_settings
    from .db.connection import close_pool, create_pool
    from .db.executions import cancel_execution, get_execution
    from .db.schema import ensure_tables

    settings = get_cfm_settings()
    pool = await create_pool(settings.database_url)
    await ensure_tables(pool)

    execution = await get_execution(pool, execution_id)

    if not execution:
        print(f"❌ Execução #{execution_id} não encontrada.")
        await close_pool()
        return

    if execution["status"] in ("completed", "cancelled"):
        print(f"ℹ️  Execução #{execution_id} já está {execution['status']}.")
        await close_pool()
        return

    states = [s["state"] for s in execution["states"]]
    print(f"\n⚠️  Cancelar execução #{execution_id}?")
    print(f"   Tipo: {execution['type']} | UFs: {', '.join(states)}")

    await close_pool()

    if not typer.confirm("Confirmar cancelamento?", default=False):
        print("❌ Cancelamento abortado.")
        return

    pool = await create_pool(settings.database_url)
    await cancel_execution(pool, execution_id)
    await close_pool()

    print(f"✅ Execução #{execution_id} cancelada.")


# ── token ──────────────────────────────────────────────────────


@app.command()
def token(
    loop: Annotated[
        bool,
        typer.Option("--loop", help="Modo loop: fica aberto para renovar o token."),
    ] = False,
) -> None:
    """Resolver reCAPTCHA manualmente e armazenar o token no PostgreSQL.

    Abre um navegador na página do CFM para resolução manual.
    O token é salvo no PostgreSQL com TTL configurável.
    """
    asyncio.run(_run_token(loop_mode=loop))


async def _run_token(loop_mode: bool = False) -> None:
    """Lógica async do subcomando token."""
    from playwright.async_api import async_playwright

    from .config import get_cfm_settings
    from .db import captcha as captcha_db
    from .db.connection import close_pool, create_pool
    from .db.schema import ensure_tables

    CFM_PAGE_URL = "https://portal.cfm.org.br/busca-medicos"

    settings = get_cfm_settings()

    pool = await create_pool(settings.database_url)
    await ensure_tables(pool)

    print("=" * 60)
    print("🔑 CFM - Captcha Solver")
    print(
        f"📦 TTL do token: {settings.captcha_ttl}s ({settings.captcha_ttl // 60} min)"
    )
    print(f"🔄 Modo loop: {'Sim' if loop_mode else 'Não'}")
    print("=" * 60)

    playwright = await async_playwright().start()

    browser = await playwright.chromium.launch(
        headless=False,
        channel="chrome",
        args=["--disable-blink-features=AutomationControlled"],
    )
    context = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1280, "height": 720},
        locale="pt-BR",
    )

    await context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    """)

    page = await context.new_page()

    try:
        print("\n🌐 Abrindo portal do CFM...")
        await page.goto(CFM_PAGE_URL, wait_until="domcontentloaded", timeout=60000)

        try:
            await page.wait_for_selector("iframe[src*='recaptcha']", timeout=30000)
            print("✅ Página carregada e reCAPTCHA visível.\n")
        except Exception:
            print("⚠️ reCAPTCHA não encontrado, mas continuando...\n")

        while True:
            token_value = await _wait_for_captcha_token(page)
            await captcha_db.store_token(
                pool, token_value, ttl_seconds=settings.captcha_ttl
            )

            ttl_remaining = await captcha_db.get_ttl(pool)
            print(f"\n✅ Token salvo no PostgreSQL! (TTL: {ttl_remaining}s)")
            print(f"   Token (primeiros 40 chars): {token_value[:40]}...")

            if not loop_mode:
                print("\n🏁 Captcha resolvido. Agora execute o crawler:")
                print("   uv run cfm create")
                break

            print("\n🔄 Modo loop ativo. Aguardando novo captcha...")
            print(
                "   O reCAPTCHA será resetado. Resolva novamente quando quiser renovar."
            )
            print("   Pressione Ctrl+C para sair.\n")

            await page.reload(wait_until="domcontentloaded", timeout=60000)
            try:
                await page.wait_for_selector("iframe[src*='recaptcha']", timeout=30000)
            except Exception:
                pass

    except KeyboardInterrupt:
        print("\n\n🛑 Interrompido pelo usuário.")
    finally:
        await browser.close()
        await playwright.stop()
        await close_pool()


async def _wait_for_captcha_token(page) -> str:
    """Aguarda o usuário resolver o reCAPTCHA e retorna o token."""
    print("\n" + "=" * 60)
    print("⏳ RESOLVA O CAPTCHA MANUALMENTE NO NAVEGADOR")
    print("=" * 60)
    print("1. Clique na checkbox 'Não sou um robô'")
    print("2. Resolva o desafio de imagens se pedido")
    print("3. O token será capturado automaticamente")
    print("=" * 60 + "\n")

    page.set_default_timeout(10 * 60 * 1000)

    token_value = await page.evaluate("""
        () => new Promise((resolve, reject) => {
            const timeout = setTimeout(() => {
                reject(new Error('Timeout aguardando captcha - 10 minutos'));
            }, 10 * 60 * 1000);

            const check = setInterval(() => {
                const el = document.querySelector('#g-recaptcha-response');
                if (el && el.value) {
                    clearInterval(check);
                    clearTimeout(timeout);
                    resolve(el.value);
                }
            }, 500);
        })
    """)

    page.set_default_timeout(30000)
    return token_value
