"""CLI do crawler CFM usando Typer.

Subcomandos:
    doctors  — Crawl de médicos por estado
    token    — Resolver reCAPTCHA manualmente e cachear token
"""

import asyncio
from typing import Annotated, Optional

import typer

app = typer.Typer(
    name="cfm",
    help="Crawler do Conselho Federal de Medicina (CFM).",
    no_args_is_help=True,
)


# ── doctors ────────────────────────────────────────────────────


@app.command()
def doctors(
    state: Annotated[
        Optional[list[str]],
        typer.Option(
            "--state",
            "-s",
            help="Sigla(s) do(s) estado(s) para buscar. Ex: --state RS --state SC",
        ),
    ] = None,
    all_states: Annotated[
        bool,
        typer.Option("--all", help="Buscar médicos de todas as 27 UFs."),
    ] = False,
) -> None:
    """Crawl de médicos no portal do CFM.

    Requer um token de captcha válido no Redis.
    Execute `cfm token` antes para resolver o reCAPTCHA.
    """
    if not state and not all_states:
        typer.echo("❌ Informe --state <UF> ou --all para buscar todas as UFs.")
        raise typer.Exit(code=1)

    asyncio.run(_run_doctors(ufs=[s.upper() for s in state] if state else None))


async def _run_doctors(ufs: list[str] | None = None) -> None:
    """Lógica async do subcomando doctors."""
    from playwright.async_api import async_playwright

    from .cache import CaptchaCache
    from .config import get_cfm_settings
    from .crawler import CFM_PAGE_URL, UFS, crawl_uf
    from .db.connection import close_pool, create_pool
    from .db.schema import ensure_tables

    settings = get_cfm_settings()
    target_ufs = ufs or UFS

    print("=" * 60)
    print("🏥 CFM - Crawler de Médicos")
    print(f"📋 UFs: {', '.join(target_ufs)}")
    print(f"📦 Page size: {settings.page_size}")
    print(f"📸 Buscar fotos: {settings.fetch_fotos}")
    print(
        f"🔗 Database: {settings.database_url.split('@')[-1] if '@' in settings.database_url else settings.database_url}"
    )
    print("=" * 60)

    cache = CaptchaCache(settings.redis_url)
    db_pool = await create_pool(settings.database_url)
    await ensure_tables(db_pool)

    if not await cache.is_valid():
        print("\n❌ Token de captcha não encontrado ou expirado!")
        print("   Execute primeiro: uv run cfm token")
        await cache.close()
        await close_pool()
        return

    ttl = await cache._redis.ttl(cache._CAPTCHA_KEY)
    print(f"\n✅ Token de captcha encontrado (TTL: {ttl}s)")

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

    total_medicos = 0

    try:
        for uf in target_ufs:
            progress = await cache.get_progress(uf)
            start_page = 1

            if progress:
                if progress["status"] == "complete":
                    print(f"\n⏭️  UF {uf} já concluída. Pulando...")
                    continue
                elif progress["status"] in ("running", "failed"):
                    start_page = progress["last_page"] + 1
                    print(
                        f"\n🔄 UF {uf}: retomando da página {start_page} "
                        f"(progresso anterior: {progress['last_page']}/{progress['total_pages']})"
                    )

            try:
                count = await crawl_uf(
                    page=page,
                    uf=uf,
                    cache=cache,
                    db_pool=db_pool,
                    page_size=settings.page_size,
                    delay=settings.delay,
                    fetch_fotos=settings.fetch_fotos,
                    max_results=settings.max_results,
                    start_page=start_page,
                    request_timeout=settings.request_timeout,
                    batch_size=settings.batch_size,
                )
                total_medicos += count

            except RuntimeError as e:
                if "captcha" in str(e).lower():
                    print(f"\n❌ Token do captcha expirou durante o crawl de {uf}.")
                    print("   Execute: uv run cfm token")
                    break
                print(f"❌ Erro ao processar UF {uf}: {e}")
                continue
            except Exception as e:
                print(f"❌ Erro ao processar UF {uf}: {e}")
                await cache.mark_failed(uf)
                continue

        print("\n" + "=" * 60)
        print(f"✅ Crawler finalizado! Total: {total_medicos} médicos persistidos")
        print("=" * 60)

        all_progress = await cache.get_all_progress()
        if all_progress:
            print("\n📊 Resumo por UF:")
            for uf_key, prog in sorted(all_progress.items()):
                status_icon = {
                    "complete": "✅",
                    "running": "🔄",
                    "failed": "❌",
                }.get(prog["status"], "❓")
                print(
                    f"   {status_icon} {uf_key}: {prog['status']} "
                    f"(página {prog['last_page']}/{prog['total_pages']}, "
                    f"{prog['total_records']} total)"
                )

    finally:
        await browser.close()
        await playwright.stop()
        await cache.close()
        await close_pool()


# ── token ──────────────────────────────────────────────────────


@app.command()
def token(
    loop: Annotated[
        bool,
        typer.Option("--loop", help="Modo loop: fica aberto para renovar o token."),
    ] = False,
) -> None:
    """Resolver reCAPTCHA manualmente e armazenar o token no Redis.

    Abre um navegador na página do CFM para resolução manual.
    O token é salvo no Redis com TTL configurável.
    """
    asyncio.run(_run_token(loop_mode=loop))


async def _run_token(loop_mode: bool = False) -> None:
    """Lógica async do subcomando token."""
    from playwright.async_api import async_playwright

    from .cache import CaptchaCache
    from .config import get_cfm_settings

    CFM_PAGE_URL = "https://portal.cfm.org.br/busca-medicos"

    settings = get_cfm_settings()
    cache = CaptchaCache(settings.redis_url)

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
            await cache.store_token(token_value, ttl=settings.captcha_ttl)

            ttl_remaining = await cache._redis.ttl(cache._CAPTCHA_KEY)
            print(f"\n✅ Token salvo no Redis! (TTL: {ttl_remaining}s)")
            print(f"   Token (primeiros 40 chars): {token_value[:40]}...")

            if not loop_mode:
                print("\n🏁 Captcha resolvido. Agora execute o crawler:")
                print("   uv run cfm doctors --state <UF>")
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
        await cache.close()


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
