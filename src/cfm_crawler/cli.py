"""CLI do CFM Crawler usando Typer.

Comandos:
    token          — Resolver reCAPTCHA e cachear token no PostgreSQL
    doctors        — Crawlar médicos (todos, por estado ou por CRM)

Exemplos:
    cfm-crawler token
    cfm-crawler token --loop
    cfm-crawler doctors
    cfm-crawler doctors --state SP
    cfm-crawler doctors --crm 12345 --uf SP
    cfm-crawler doctors --count
    cfm-crawler doctors --count --state SP
"""

from __future__ import annotations

from typing import Annotated, Optional

import typer

from ..shared.constants import REGIONS, UFS, UFS_MAP

app = typer.Typer(
    name="cfm-crawler",
    help="Crawler do Conselho Federal de Medicina (CFM).",
    no_args_is_help=True,
)


def _init_db(database_url: str) -> None:
    """Inicializa o engine + cria tabelas se necessário."""
    from ..database.session import init_engine, get_engine
    from ..database.base import Base

    # Importar entities para registrar no metadata
    from .models import entities as _entities  # noqa: F401

    init_engine(database_url)
    Base.metadata.create_all(get_engine())


# ── token ──────────────────────────────────────────────────────


@app.command()
def token(
    loop: Annotated[
        bool,
        typer.Option("--loop", help="Modo loop: fica aberto para renovar o token."),
    ] = False,
) -> None:
    """Resolver reCAPTCHA manualmente e armazenar o token no PostgreSQL."""
    from .config import get_cfm_settings
    from ..database.session import get_session
    from .use_cases.manage_token import ManageTokenUseCase

    settings = get_cfm_settings()
    _init_db(settings.database_url)

    with get_session() as session:
        use_case = ManageTokenUseCase(session, settings)
        use_case.execute(loop=loop)


# ── doctors ────────────────────────────────────────────────────


@app.command()
def doctors(
    state: Annotated[
        Optional[str],
        typer.Option(
            "--state", help="UF para crawlar (ex: SP, RJ). Crawla por município."
        ),
    ] = None,
    crm: Annotated[
        Optional[str],
        typer.Option("--crm", help="Número do CRM para busca individual."),
    ] = None,
    uf: Annotated[
        Optional[str],
        typer.Option("--uf", help="UF do CRM (usado com --crm)."),
    ] = None,
    page_size: Annotated[
        Optional[int],
        typer.Option("--page-size", help="Registros por página."),
    ] = None,
    batch_size: Annotated[
        Optional[int],
        typer.Option("--batch-size", help="Páginas por batch paralelo."),
    ] = None,
    count: Annotated[
        bool,
        typer.Option("--count", help="Exibir contagem de médicos por estado."),
    ] = False,
) -> None:
    """Crawlar médicos do CFM.

    Modos de uso:\n
    - Sem flags: formulário interativo para selecionar estados\n
    - --state SP: crawla um estado por município\n
    - --crm 12345 --uf SP: busca um médico específico\n
    - --count: exibe contagem de médicos por estado\n
    - --count --state SP: contagem de um estado específico
    """
    # Modo: contagem
    if count:
        _run_count(state=state)
        return

    # Modo: busca por CRM
    if crm is not None:
        if uf is None:
            typer.echo("❌ --uf é obrigatório quando --crm é informado.")
            raise typer.Exit(code=1)
        _run_lookup(crm=crm, uf=uf.upper())
        return

    # Modo: crawl por estado (por município)
    if state is not None:
        state_upper = state.upper()
        if state_upper not in UFS:
            typer.echo(f"❌ UF inválida: {state_upper}")
            raise typer.Exit(code=1)
        _run_state_crawl(uf=state_upper, page_size=page_size, batch_size=batch_size)
        return

    # Modo: formulário interativo
    _run_interactive(page_size=page_size, batch_size=batch_size)


# ── Implementações internas ────────────────────────────────────


def _run_count(state: str | None) -> None:
    """Exibe contagem de médicos por estado: API vs banco."""
    from .config import get_cfm_settings
    from ..database.session import get_session
    from .repositories.captcha_repo import get_token
    from .services.cfm_api import CfmApiClient
    from .use_cases.count_doctors import CountDoctorsUseCase

    settings = get_cfm_settings()
    _init_db(settings.database_url)

    with get_session() as session:
        captcha_token = get_token(session)

        if captcha_token is None:
            typer.echo("❌ Nenhum token de captcha válido encontrado.")
            typer.echo("   Execute: cfm-crawler token")
            raise typer.Exit(code=1)

        if state is not None:
            target_ufs = [state.upper()]
            if target_ufs[0] not in UFS:
                typer.echo(f"❌ UF inválida: {target_ufs[0]}")
                raise typer.Exit(code=1)
        else:
            target_ufs = UFS[:]

        print("=" * 80)
        print("📊 CFM - Contagem de médicos por estado (API vs Banco)")
        print("=" * 80)

        with CfmApiClient(timeout=settings.request_timeout) as api:
            use_case = CountDoctorsUseCase(session, settings, api)
            result = use_case.execute(
                captcha_token=captcha_token,
                target_ufs=target_ufs,
            )

    # Formatar e imprimir resultado
    header = (
        f"  {'UF':<6} {'Estado':<22} {'API':>10} {'Banco':>10} {'Diff':>10} {'%':>7}"
    )
    sep = f"  {'─' * 6} {'─' * 22} {'─' * 10} {'─' * 10} {'─' * 10} {'─' * 7}"
    print(f"\n{header}")
    print(sep)

    for row in result["rows"]:
        uf = row["uf"]
        estado_name = row["estado_name"]
        api_count = row["api_count"]
        db_count = row["db_count"]
        diff = row["diff"]
        pct = row["percentage"]

        if api_count < 0:
            api_display = "erro"
            diff_display = "-"
            pct_display = f"{'-':>7}"
        elif api_count == 0:
            api_display = f"{0:>10,}"
            diff_display = f"{'✓':>10}"
            pct_display = f"{'-':>7}"
        else:
            api_display = f"{api_count:>10,}"
            pct_display = f"{pct:>6.1f}%" if pct is not None else f"{'-':>7}"
            if diff == 0:
                diff_display = f"{'✓':>10}"
            else:
                diff_display = f"{diff:>+10,}"

        db_display = f"{db_count:>10,}"

        print(
            f"  {uf:<6} {estado_name:<22} {api_display} "
            f"{db_display} {diff_display} {pct_display}"
        )

    print(sep)

    api_total = result["api_total"]
    db_total = result["db_total"]
    diff_total = result["diff_total"]
    pct_total = result["pct_total"]

    diff_final = f"{diff_total:>+10,}" if diff_total != 0 else f"{'✓':>10}"
    pct_total_display = f"{pct_total:>6.1f}%" if pct_total is not None else "-"

    print(
        f"  {'TOTAL':<6} {'':<22} {api_total:>10,} "
        f"{db_total:>10,} {diff_final} {pct_total_display}"
    )
    print("=" * 80)


def _run_lookup(crm: str, uf: str) -> None:
    """Busca médico por CRM/UF."""
    from .config import get_cfm_settings
    from ..database.session import get_session
    from .services.cfm_api import CfmApiClient
    from .use_cases.lookup_doctor import LookupDoctorUseCase

    settings = get_cfm_settings()
    _init_db(settings.database_url)

    print("=" * 60)
    print(f"🔍 CFM Crawler - Busca por CRM: {crm} / {uf}")
    print("=" * 60)

    with get_session() as session:
        with CfmApiClient(timeout=settings.request_timeout) as api:
            use_case = LookupDoctorUseCase(session, settings, api)
            doc = use_case.execute(crm=crm, uf=uf)

    if doc is None:
        print(f"\n❌ Nenhum médico encontrado com CRM {crm}/{uf}.")
        return

    print("\n" + "=" * 60)
    print("✅ Médico encontrado e salvo no banco!")
    print("=" * 60)
    print(f"  Nome:           {doc.get('name', '-')}")
    print(f"  Nome Social:    {doc.get('social_name') or '-'}")
    print(f"  CRM:            {doc.get('crm')}")
    print(f"  UF:             {doc.get('state')}")
    print(f"  Situação:       {doc.get('status', '-')}")
    print(f"  Tipo Inscrição: {doc.get('registration_type', '-')}")
    print(f"  Dt Inscrição:   {doc.get('registration_date', '-')}")
    print(f"  Graduação:      {doc.get('graduation_institution', '-')}")
    print(f"  Dt Graduação:   {doc.get('graduation_date', '-')}")

    specialties = doc.get("specialties", [])
    if specialties:
        nomes = ", ".join(s.get("name", "") for s in specialties)
        print(f"  Especialidades: {nomes}")
    else:
        print("  Especialidades: -")

    print(f"  Telefone:       {doc.get('phone') or '-'}")
    print(f"  Endereço:       {doc.get('address') or '-'}")
    print(f"  Foto URL:       {doc.get('photo_url') or '-'}")
    print("=" * 60)


def _run_state_crawl(
    uf: str,
    page_size: int | None = None,
    batch_size: int | None = None,
) -> None:
    """Crawla estado por município."""
    import time

    from .config import get_cfm_settings
    from ..database.session import get_session
    from .services.cfm_api import CfmApiClient
    from .use_cases.crawl_state_doctors import CrawlStateDoctorsUseCase

    settings = get_cfm_settings()
    _init_db(settings.database_url)

    print("=" * 60)
    print(f"🏥 CFM Crawler - Crawl por Município: {uf} - {UFS_MAP[uf]}")
    print(f"📦 Page size: {page_size or settings.page_size}")
    print(f"⚡ Batch size: {batch_size or settings.batch_size}")
    print("=" * 60)

    start = time.time()

    try:
        with get_session() as session:
            with CfmApiClient(timeout=settings.request_timeout) as api:
                use_case = CrawlStateDoctorsUseCase(session, settings, api)
                total = use_case.execute(
                    uf=uf, page_size=page_size, batch_size=batch_size
                )

        elapsed = time.time() - start
        print(
            f"\n🎉 Sessão finalizada! {total} médicos processados em "
            f"{int(elapsed // 60)}m{int(elapsed % 60)}s"
        )
    except KeyboardInterrupt:
        print("\n\n🛑 Interrompido pelo usuário.")
    except RuntimeError as e:
        if "captcha" in str(e).lower():
            print("\n❌ Token do captcha expirou.")
            print("   Execute: uv run cfm-crawler token")
        else:
            print(f"\n❌ Erro: {e}")


def _run_interactive(
    page_size: int | None = None,
    batch_size: int | None = None,
) -> None:
    """Formulário interativo para crawl de múltiplos estados."""
    import time

    from InquirerPy import inquirer
    from InquirerPy.base.control import Choice
    from InquirerPy.separator import Separator

    from .config import get_cfm_settings
    from .models.domain import SITUACAO_OPTIONS, TIPO_INSCRICAO_OPTIONS
    from ..database.session import get_session
    from .services.cfm_api import CfmApiClient
    from .use_cases.crawl_all_doctors import CrawlAllDoctorsUseCase

    settings = get_cfm_settings()
    _init_db(settings.database_url)

    print("\n" + "=" * 60)
    print("📋 CFM Crawler - Configuração")
    print("=" * 60)

    # Seleção de estados
    state_choices: list = [
        Choice(value="all", name="✦ Todos os estados (27 UFs)"),
        Separator("─" * 40),
    ]
    for region_name, region_ufs in REGIONS.items():
        state_choices.append(Separator(f"── {region_name} "))
        for region_uf in region_ufs:
            state_choices.append(
                Choice(value=region_uf, name=f"{region_uf} - {UFS_MAP[region_uf]}")
            )

    selected = inquirer.checkbox(
        message="Selecione os estados:",
        choices=state_choices,
        pointer="❯",
        instruction="(Espaço para marcar, Enter para confirmar)",
        validate=lambda result: len(result) > 0,
        invalid_message="Selecione pelo menos um estado.",
    ).execute()

    if "all" in selected:
        states = UFS[:]
    else:
        states = [s for s in selected if s in UFS]

    if not states:
        typer.echo("❌ Nenhum estado selecionado.")
        raise typer.Exit(code=1)

    # Page size
    if page_size is None:
        page_size = int(
            inquirer.number(
                message="Page size (registros por página):",
                default=settings.page_size,
                min_allowed=1,
                max_allowed=25000,
            ).execute()
        )

    # Batch size
    if batch_size is None:
        batch_size = int(
            inquirer.number(
                message="Batch size (páginas por batch):",
                default=settings.batch_size,
                min_allowed=1,
                max_allowed=100,
            ).execute()
        )

    # Tipo de inscrição
    tipo_inscricao = inquirer.select(
        message="Tipo de Inscrição:",
        choices=[
            Choice(value=code, name=label)
            for code, label in TIPO_INSCRICAO_OPTIONS.items()
        ],
        default="",
        pointer="❯",
    ).execute()

    # Situação
    situacao = inquirer.select(
        message="Situação:",
        choices=[
            Choice(value=code, name=label) for code, label in SITUACAO_OPTIONS.items()
        ],
        default="",
        pointer="❯",
    ).execute()

    # Resumo
    states_display = ", ".join(states[:6])
    if len(states) > 6:
        states_display += f" +{len(states) - 6}"

    print("\n" + "-" * 60)
    print("📋 Resumo:")
    print(f"   Estados:    {states_display} ({len(states)} UFs)")
    print(f"   Page size:  {page_size}")
    print(f"   Batch size: {batch_size}")
    print(
        f"   Inscrição:  {TIPO_INSCRICAO_OPTIONS.get(tipo_inscricao, tipo_inscricao)}"
    )
    print(f"   Situação:   {SITUACAO_OPTIONS.get(situacao, situacao)}")
    print("-" * 60)

    if not inquirer.confirm(message="🚀 Iniciar execução?", default=True).execute():
        typer.echo("❌ Cancelado.")
        raise typer.Exit()

    start = time.time()

    try:
        with get_session() as session:
            with CfmApiClient(timeout=settings.request_timeout) as api:
                use_case = CrawlAllDoctorsUseCase(session, settings, api)
                total = use_case.execute(
                    states=states,
                    page_size=page_size,
                    batch_size=batch_size,
                    tipo_inscricao=tipo_inscricao,
                    situacao=situacao,
                )

        elapsed = time.time() - start
        print("\n" + "=" * 60)
        print(
            f"✅ Sessão finalizada! Total: {total} médicos processados em "
            f"{int(elapsed // 60)}m{int(elapsed % 60)}s"
        )
        print("=" * 60)

    except KeyboardInterrupt:
        print("\n\n🛑 Interrompido pelo usuário.")
    except RuntimeError as e:
        if "captcha" in str(e).lower():
            print("\n❌ Token do captcha expirou.")
            print("   Execute: uv run cfm-crawler token")
        else:
            print(f"\n❌ Erro: {e}")
