"""CLI do crawler CFM usando Typer.

Subcomandos:
    create         — Criar um plano de execução (form interativo)
    execute        — Criar e executar de uma vez (form interativo)
    execute-crm    — Buscar médico específico por CRM/UF
    execute-state  — Crawlar um estado inteiro por município (form interativo)
    run            — Iniciar/continuar uma execução
    list           — Listar execuções ativas
    show           — Visualizar detalhes de uma execução
    cancel         — Cancelar uma execução
    token          — Resolver reCAPTCHA manualmente e cachear token
    count          — Totalizar médicos por estado (API vs banco)
    natural_count  — Contar CRMs naturais distintos por estado
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

# Re-exporta opções de filtro para uso no CLI
from .models import SITUACAO_OPTIONS, TIPO_INSCRICAO_OPTIONS


# ── Formulário interativo compartilhado ────────────────────────


def _interactive_form(title: str = "CFM - Configuração") -> dict:
    """Formulário interativo para configurar execução.

    Retorna dict com: exec_type, states, page_size, batch_size,
    tipo_inscricao, situacao, start_page.
    """
    from InquirerPy import inquirer
    from InquirerPy.base.control import Choice
    from InquirerPy.separator import Separator

    from .config import get_cfm_settings

    settings = get_cfm_settings()

    print("\n" + "=" * 60)
    print(f"📋 {title}")
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
            max_allowed=25000,
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

    # ── Tipo de Inscrição ──────────────────────────────────
    tipo_inscricao = inquirer.select(
        message="Tipo de Inscrição:",
        choices=[
            Choice(value=code, name=label)
            for code, label in TIPO_INSCRICAO_OPTIONS.items()
        ],
        default="",
        pointer="❯",
    ).execute()

    # ── Situação ────────────────────────────────────────────
    situacao = inquirer.select(
        message="Situação:",
        choices=[
            Choice(value=code, name=label) for code, label in SITUACAO_OPTIONS.items()
        ],
        default="",
        pointer="❯",
    ).execute()

    # ── Filtro por Cidade ──────────────────────────────────────
    filter_city = inquirer.confirm(
        message="Filtrar por cidade?",
        default=False,
    ).execute()

    city_code = ""
    if filter_city:
        city_code = inquirer.text(
            message="Código da cidade (número):",
            validate=lambda x: x.isdigit() or x == "",
            invalid_message="Digite apenas números ou deixe vazio para cancelar.",
        ).execute()

    # ── Página inicial ─────────────────────────────────────────
    start_page = int(
        inquirer.number(
            message="Página inicial (começar a partir de qual página):",
            default=1,
            min_allowed=1,
        ).execute()
    )

    # ── Resumo ─────────────────────────────────────────────────
    states_display = ", ".join(states[:6])
    if len(states) > 6:
        states_display += f" +{len(states) - 6}"

    print("\n" + "-" * 60)
    print("📋 Resumo:")
    print(f"   Tipo:       {EXECUTION_TYPES[exec_type]} ({exec_type})")
    print(f"   Estados:    {states_display} ({len(states)} UFs)")
    print(f"   Page size:  {page_size}")
    print(f"   Batch size: {batch_size}")
    print(
        f"   Inscrição:  {TIPO_INSCRICAO_OPTIONS.get(tipo_inscricao, tipo_inscricao)}"
    )
    print(f"   Situação:   {SITUACAO_OPTIONS.get(situacao, situacao)}")
    if city_code:
        print(f"   Cidade:     {city_code}")
    if start_page > 1:
        print(f"   Pág. ini.:  {start_page}")
    print("-" * 60)

    return {
        "exec_type": exec_type,
        "states": states,
        "page_size": page_size,
        "batch_size": batch_size,
        "tipo_inscricao": tipo_inscricao,
        "situacao": situacao,
        "city_code": city_code,
        "start_page": start_page,
    }


# ── create ─────────────────────────────────────────────────────


@app.command()
def create() -> None:
    """Criar um novo plano de execução (form interativo)."""
    from InquirerPy import inquirer

    form = _interactive_form(title="CFM - Criar Plano de Execução")

    if not inquirer.confirm(message="Confirmar criação?", default=True).execute():
        typer.echo("❌ Cancelado.")
        raise typer.Exit()

    params = {
        "states": form["states"],
        "tipo_inscricao": form["tipo_inscricao"],
        "situacao": form["situacao"],
        "city_code": form["city_code"],
        "start_page": form["start_page"],
    }
    execution_id = asyncio.run(
        _create_execution(
            form["exec_type"],
            form["page_size"],
            form["batch_size"],
            params,
            form["states"],
        )
    )

    print(f"\n✅ Execução #{execution_id} criada com sucesso!")

    if inquirer.confirm(message="🚀 Iniciar execução agora?", default=True).execute():
        asyncio.run(_run_execution(execution_id))


# ── execute ────────────────────────────────────────────────────


@app.command()
def execute() -> None:
    """Criar e executar de uma vez (form interativo, sem etapa separada)."""
    from InquirerPy import inquirer

    form = _interactive_form(title="CFM - Executar Crawler")

    if not inquirer.confirm(message="🚀 Iniciar execução?", default=True).execute():
        typer.echo("❌ Cancelado.")
        raise typer.Exit()

    params = {
        "states": form["states"],
        "tipo_inscricao": form["tipo_inscricao"],
        "situacao": form["situacao"],
        "city_code": form["city_code"],
        "start_page": form["start_page"],
    }
    execution_id = asyncio.run(
        _create_execution(
            form["exec_type"],
            form["page_size"],
            form["batch_size"],
            params,
            form["states"],
        )
    )

    print(f"\n✅ Execução #{execution_id} criada. Iniciando...")
    asyncio.run(_run_execution(execution_id))


# ── execute-crm ────────────────────────────────────────────────


@app.command(name="execute-crm")
def execute_crm(
    crm: Annotated[
        str,
        typer.Option("--crm", help="Número do CRM do médico."),
    ],
    uf: Annotated[
        str,
        typer.Option("--uf", help="UF do CRM (ex: SC, SP, RJ)."),
    ],
) -> None:
    """Buscar um médico específico por CRM e UF, exibir e salvar no banco."""
    uf = uf.upper()
    if uf not in UFS:
        typer.echo(f"❌ UF inválida: {uf}")
        raise typer.Exit(code=1)

    asyncio.run(_run_execute_crm(crm=crm, uf=uf))


async def _run_execute_crm(crm: str, uf: str) -> None:
    """Lógica async do subcomando execute-crm."""
    from .config import get_cfm_settings
    from .crawler import create_http_client, fetch_medico_by_crm
    from .db import captcha as captcha_db
    from .db.connection import close_pool, create_pool
    from .db.schema import ensure_tables

    settings = get_cfm_settings()

    pool = await create_pool(settings.database_url)
    await ensure_tables(pool)

    print("=" * 60)
    print(f"🔍 CFM - Busca por CRM: {crm} / {uf}")
    print("=" * 60)

    # Validar captcha
    if not await captcha_db.is_valid(pool):
        print("\n❌ Token de captcha não encontrado ou expirado!")
        print("   Execute primeiro: uv run cfm token")
        await close_pool()
        return

    ttl = await captcha_db.get_ttl(pool)
    print(f"✅ Token de captcha encontrado (TTL: {ttl}s)")

    captcha_token = await captcha_db.get_token(pool)
    client = create_http_client(timeout=settings.request_timeout)

    try:
        doc = await fetch_medico_by_crm(
            client=client,
            captcha_token=captcha_token,
            crm=crm,
            uf=uf,
            db_pool=pool,
            request_timeout=settings.request_timeout,
            fetch_foto=settings.fetch_fotos,
        )

        if doc is None:
            print(f"\n❌ Nenhum médico encontrado com CRM {crm}/{uf}.")
            return

        print("\n" + "=" * 60)
        print("✅ Médico encontrado e salvo no banco!")
        print("=" * 60)
        print(f"  Nome:          {doc.get('name', '-')}")
        print(f"  Nome Social:   {doc.get('social_name') or '-'}")
        print(f"  CRM:           {doc.get('crm')}")
        print(f"  UF:            {doc.get('state')}")
        print(f"  Situação:      {doc.get('status', '-')}")
        print(f"  Tipo Inscrição:{doc.get('registration_type', '-')}")
        print(f"  Dt Inscrição:  {doc.get('registration_date', '-')}")
        print(f"  Graduação:     {doc.get('graduation_institution', '-')}")
        print(f"  Dt Graduação:  {doc.get('graduation_date', '-')}")

        specialties = doc.get("specialties", [])
        if specialties:
            nomes = ", ".join(s.get("name", "") for s in specialties)
            print(f"  Especialidades:{nomes}")
        else:
            print("  Especialidades:-")

        print(f"  Telefone:      {doc.get('phone') or '-'}")
        print(f"  Endereço:      {doc.get('address') or '-'}")
        print(f"  Foto URL:      {doc.get('photo_url') or '-'}")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Erro: {e}")
    finally:
        await client.aclose()
        await close_pool()


# ── execute-state ──────────────────────────────────────────────


def _execute_state_form() -> dict:
    """Formulário interativo para configurar crawl por município.

    Retorna dict com: uf, page_size, batch_size.
    """
    from InquirerPy import inquirer
    from InquirerPy.base.control import Choice

    from .config import get_cfm_settings

    settings = get_cfm_settings()

    print("\n" + "=" * 60)
    print("📋 CFM - Executar Estado por Município")
    print("=" * 60)

    # ── UF (select único) ──────────────────────────────────────
    uf = inquirer.select(
        message="Selecione o estado (UF):",
        choices=[
            Choice(value=code, name=f"{code} - {name}")
            for code, name in UFS_MAP.items()
        ],
        pointer="❯",
    ).execute()

    # ── Page size ──────────────────────────────────────────────
    page_size = int(
        inquirer.number(
            message="Page size (registros por página):",
            default=settings.page_size,
            min_allowed=1,
            max_allowed=25000,
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

    # ── Resumo ─────────────────────────────────────────────────
    print("\n" + "-" * 60)
    print("📋 Resumo:")
    print(f"   Estado:     {uf} - {UFS_MAP[uf]}")
    print(f"   Page size:  {page_size}")
    print(f"   Batch size: {batch_size}")
    print("-" * 60)

    return {
        "uf": uf,
        "page_size": page_size,
        "batch_size": batch_size,
    }


@app.command(name="execute-state")
def execute_state() -> None:
    """Crawlar um estado inteiro iterando por todos os municípios."""
    from InquirerPy import inquirer

    form = _execute_state_form()

    if not inquirer.confirm(message="🚀 Iniciar execução?", default=True).execute():
        typer.echo("❌ Cancelado.")
        raise typer.Exit()

    asyncio.run(_run_execute_state(form))


async def _run_execute_state(form: dict) -> None:
    """Lógica async do subcomando execute-state."""
    import time

    from .config import get_cfm_settings
    from .crawler import create_http_client, crawl_state_by_cities, fetch_municipios
    from .db import captcha as captcha_db
    from .db.connection import close_pool, create_pool
    from .db.schema import ensure_tables

    settings = get_cfm_settings()
    uf = form["uf"]
    page_size = form["page_size"]
    batch_size = form["batch_size"]

    pool = await create_pool(settings.database_url)
    await ensure_tables(pool)

    print("=" * 60)
    print(f"🏥 CFM - Crawl por Município: {uf} - {UFS_MAP[uf]}")
    print(f"📦 Page size: {page_size}")
    print(f"⚡ Batch size: {batch_size}")
    print(
        f"🔗 Database: {settings.database_url.split('@')[-1] if '@' in settings.database_url else settings.database_url}"
    )
    print("=" * 60)

    # Validar captcha
    if not await captcha_db.is_valid(pool):
        print("\n❌ Token de captcha não encontrado ou expirado!")
        print("   Execute primeiro: uv run cfm token")
        await close_pool()
        return

    ttl = await captcha_db.get_ttl(pool)
    print(f"\n✅ Token de captcha encontrado (TTL: {ttl}s)")

    client = create_http_client(timeout=settings.request_timeout)

    try:
        # Buscar municípios
        print(f"\n🔍 Buscando municípios de {uf}...")
        cities = await fetch_municipios(client, uf)

        if not cities:
            print(f"❌ Nenhum município encontrado para {uf}.")
            return

        print(f"✅ {len(cities)} municípios encontrados para {uf}")

        start = time.time()
        total_medicos = await crawl_state_by_cities(
            client=client,
            uf=uf,
            cities=cities,
            db_pool=pool,
            page_size=page_size,
            batch_size=batch_size,
            delay=settings.delay,
            request_timeout=settings.request_timeout,
        )
        elapsed = time.time() - start

        print(
            f"\n🎉 Sessão finalizada! {total_medicos} médicos processados em {int(elapsed // 60)}m{int(elapsed % 60)}s"
        )

    except KeyboardInterrupt:
        print("\n\n🛑 Interrompido pelo usuário.")
    except RuntimeError as e:
        if "captcha" in str(e).lower():
            print("\n❌ Token do captcha expirou.")
            print("   Execute: uv run cfm token")
        else:
            print(f"\n❌ Erro: {e}")
    finally:
        await client.aclose()
        await close_pool()


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
    from .config import get_cfm_settings
    from .crawler import create_http_client, run_execution
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
    # Exibir filtros de busca
    exec_params = execution.get("params", {})
    tipo_inscricao = exec_params.get("tipo_inscricao", "")
    situacao = exec_params.get("situacao", "")
    city_code = exec_params.get("city_code", "")
    tipo_label = TIPO_INSCRICAO_OPTIONS.get(tipo_inscricao, tipo_inscricao or "Todas")
    situacao_label = SITUACAO_OPTIONS.get(situacao, situacao or "Todas")
    print(f"📌 Inscrição: {tipo_label}")
    print(f"📌 Situação: {situacao_label}")
    if city_code:
        print(f"📌 Cidade: {city_code}")
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

    client = create_http_client(timeout=settings.request_timeout)

    try:
        total_medicos = await run_execution(
            client=client,
            execution_id=execution_id,
            db_pool=pool,
            page_size=execution["page_size"],
            batch_size=execution["batch_size"],
            delay=settings.delay,
            fetch_fotos=settings.fetch_fotos,
            max_results=settings.max_results,
            request_timeout=settings.request_timeout,
            tipo_inscricao=exec_params.get("tipo_inscricao", ""),
            situacao=exec_params.get("situacao", ""),
            municipio=exec_params.get("city_code", ""),
            start_page=exec_params.get("start_page", 1),
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
        await client.aclose()
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

        # Mostrar filtros se não forem o padrão (Todas)
        list_params = ex.get("params", {})
        list_tipo = list_params.get("tipo_inscricao", "")
        list_situacao = list_params.get("situacao", "")
        filters = []
        if list_tipo:
            filters.append(
                f"Inscrição: {TIPO_INSCRICAO_OPTIONS.get(list_tipo, list_tipo)}"
            )
        if list_situacao:
            filters.append(
                f"Situação: {SITUACAO_OPTIONS.get(list_situacao, list_situacao)}"
            )
        list_start_page = list_params.get("start_page", 1)
        if list_start_page > 1:
            filters.append(f"Pág. ini.: {list_start_page}")
        if filters:
            print(f"         │  {' | '.join(filters)}")

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

    # Filtros de busca
    show_params = ex.get("params", {})
    show_tipo = show_params.get("tipo_inscricao", "")
    show_situacao = show_params.get("situacao", "")
    print(
        f"  Inscrição:  {TIPO_INSCRICAO_OPTIONS.get(show_tipo, show_tipo or 'Todas')}"
    )
    print(
        f"  Situação:   {SITUACAO_OPTIONS.get(show_situacao, show_situacao or 'Todas')}"
    )
    show_start_page = show_params.get("start_page", 1)
    if show_start_page > 1:
        print(f"  Pág. ini.:  {show_start_page}")

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


# ── count ──────────────────────────────────────────────────────


@app.command()
def count() -> None:
    """Totalizar médicos por estado: compara total da API com o banco local."""
    asyncio.run(_run_count())


async def _run_count() -> None:
    """Lógica async do subcomando count."""
    import time

    from .config import get_cfm_settings
    from .crawler import UFS, fetch_all_state_counts
    from .db import captcha as captcha_db
    from .db.connection import close_pool, create_pool
    from .db.schema import ensure_tables
    from .db.state_counts import (
        get_db_counts_by_state,
        upsert_state_counts_batch,
    )

    settings = get_cfm_settings()
    pool = await create_pool(settings.database_url)
    await ensure_tables(pool)

    # Validar captcha
    if not await captcha_db.is_valid(pool):
        print("\n❌ Token de captcha não encontrado ou expirado!")
        print("   Execute primeiro: uv run cfm token")
        await close_pool()
        return

    ttl = await captcha_db.get_ttl(pool)
    captcha_token = await captcha_db.get_token(pool)

    print("\n" + "=" * 70)
    print("📊 CFM - Contagem de Médicos por Estado")
    print("=" * 70)
    print(f"✅ Token de captcha encontrado (TTL: {ttl}s)")

    try:
        print(f"🔍 Consultando {len(UFS)} estados em paralelo...")
        start = time.time()
        api_counts = await fetch_all_state_counts(captcha_token)
        elapsed = time.time() - start
        print(f"✅ Consultas finalizadas em {elapsed:.1f}s")
    except Exception as e:
        print(f"\n❌ Erro ao consultar API: {e}")
        await close_pool()
        return

    # Obter contagens do banco
    db_counts = await get_db_counts_by_state(pool)

    # Montar tabela de resultados
    rows: list[dict] = []
    for uf in sorted(UFS):
        api_total = api_counts.get(uf, 0)
        if api_total < 0:
            api_total = 0  # Erro na consulta
        db_total = db_counts.get(uf, 0)
        missing = max(api_total - db_total, 0)
        rows.append(
            {
                "state": uf,
                "api_total": api_total,
                "db_total": db_total,
                "missing": missing,
            }
        )

    # Persistir contagens
    await upsert_state_counts_batch(pool, rows)
    await close_pool()

    # Exibir tabela
    _print_count_table(rows, api_counts)


def _print_count_table(rows: list[dict], api_counts: dict[str, int]) -> None:
    """Exibe tabela formatada com totais por estado."""
    header = (
        f"  {'UF':<4} │ {'Nome':<22} │ {'API Total':>10} │ "
        f"{'DB Total':>10} │ {'Faltantes':>10} │ {'%':>7}"
    )
    sep = (
        "  "
        + "─" * 4
        + "┼"
        + "─" * 24
        + "┼"
        + "─" * 12
        + "┼"
        + "─" * 12
        + "┼"
        + "─" * 12
        + "┼"
        + "─" * 9
    )

    print("\n" + header)
    print(sep)

    sum_api = 0
    sum_db = 0
    sum_missing = 0
    errors: list[str] = []

    for r in rows:
        uf = r["state"]
        api_total = r["api_total"]
        db_total = r["db_total"]
        missing = r["missing"]
        uf_name = UFS_MAP.get(uf, uf)

        had_error = api_counts.get(uf, 0) < 0

        if had_error:
            pct_str = "ERRO"
            api_str = "?"
            missing_str = "?"
            errors.append(uf)
        else:
            pct = round(db_total / api_total * 100, 1) if api_total > 0 else 0.0
            pct_str = f"{pct}%"
            api_str = f"{api_total:,}".replace(",", ".")
            missing_str = f"{missing:,}".replace(",", ".")

        db_str = f"{db_total:,}".replace(",", ".")

        sum_api += api_total
        sum_db += db_total
        sum_missing += missing

        print(
            f"  {uf:<4} │ {uf_name:<22} │ {api_str:>10} │ "
            f"{db_str:>10} │ {missing_str:>10} │ {pct_str:>7}"
        )

    print(sep)

    total_pct = round(sum_db / sum_api * 100, 1) if sum_api > 0 else 0.0
    sum_api_str = f"{sum_api:,}".replace(",", ".")
    sum_db_str = f"{sum_db:,}".replace(",", ".")
    sum_missing_str = f"{sum_missing:,}".replace(",", ".")

    print(
        f"  {'TOTAL':<4} │ {'':<22} │ {sum_api_str:>10} │ "
        f"{sum_db_str:>10} │ {sum_missing_str:>10} │ {total_pct:>6}%"
    )
    print()

    if errors:
        print(f"  ⚠️  Erro ao consultar: {', '.join(errors)}")
        print()

    print("  💾 Contagens salvas na tabela state_counts.")
    print("=" * 70)


# ── natural_count ──────────────────────────────────────────────


@app.command()
def natural_count() -> None:
    """Contar CRMs naturais distintos por estado (excluindo repetições)."""
    asyncio.run(_run_natural_count())


async def _run_natural_count() -> None:
    """Lógica async do subcomando natural_count."""
    from .config import get_cfm_settings
    from .db.connection import close_pool, create_pool
    from .db.schema import ensure_tables
    from .db.state_counts import (
        get_db_counts_by_state,
        get_total_distinct_natural_count,
    )

    settings = get_cfm_settings()
    pool = await create_pool(settings.database_url)
    await ensure_tables(pool)

    print("\n" + "=" * 70)
    print("📊 CFM - Contagem de Médicos Únicos (CRMs Naturais Distintos)")
    print("=" * 70)

    # Obter contagens do banco
    total_registros = sum((await get_db_counts_by_state(pool)).values())
    total_medicos_unicos = await get_total_distinct_natural_count(pool)
    total_transferencias = total_registros - total_medicos_unicos

    await close_pool()

    if total_registros == 0:
        print("\n⚠️  Nenhum médico encontrado no banco de dados.")
        print("=" * 70)
        return

    # Exibir resumo
    registros_str = f"{total_registros:,}".replace(",", ".")
    unicos_str = f"{total_medicos_unicos:,}".replace(",", ".")
    transferencias_str = f"{total_transferencias:,}".replace(",", ".")

    print()
    print(f"  📋 Total de Registros no Banco:  {registros_str}")
    print(f"  👤 Médicos Únicos (crm_natural):  {unicos_str}")
    print(f"  🔄 Registros de Transferência:    {transferencias_str}")
    print()
    print(
        "  ℹ️  Médicos únicos são contados uma vez, mesmo tendo CRM em múltiplos estados."
    )
    print("=" * 70)


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
