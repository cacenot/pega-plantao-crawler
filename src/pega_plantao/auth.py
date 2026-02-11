"""Handler de login via Playwright para o Pega Plantão."""

from playwright.async_api import BrowserContext, Page, async_playwright

from .config import Settings


async def login_and_get_context(settings: Settings) -> tuple[BrowserContext, Page]:
    """Realiza login no PegaPlantão e retorna o contexto autenticado.

    Args:
        settings: Configurações com credenciais e URLs.

    Returns:
        Tupla com (BrowserContext, Page) autenticados.

    Raises:
        Exception: Se o login falhar.
    """
    playwright = await async_playwright().start()

    browser = await playwright.chromium.launch(headless=settings.headless)
    context = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/143.0.0.0 Safari/537.36"
        )
    )

    page = await context.new_page()
    page.set_default_timeout(settings.timeout)

    print(f"🔐 Navegando para {settings.login_url}...")
    await page.goto(settings.login_url)

    await page.wait_for_selector("#MainContent_LoginUser_UserName")

    print("📝 Preenchendo credenciais...")
    await page.fill("#MainContent_LoginUser_UserName", settings.email)
    await page.fill("#Password", settings.password)

    print("🚀 Efetuando login...")
    await page.click("#MainContent_LoginUser_btnLogin")

    await page.wait_for_load_state("networkidle")

    current_url = page.url
    if "/Login" in current_url:
        raise Exception("❌ Falha no login. Verifique suas credenciais.")

    print(f"✅ Login realizado com sucesso! URL atual: {current_url}")

    return context, page


async def navigate_to_escala_mensal(page: Page, settings: Settings) -> None:
    """Navega para a página de Escala Mensal."""
    print(f"📅 Navegando para {settings.escala_mensal_url}...")
    await page.goto(settings.escala_mensal_url)
    await page.wait_for_load_state("networkidle")
    print("✅ Página de Escala Mensal carregada.")
