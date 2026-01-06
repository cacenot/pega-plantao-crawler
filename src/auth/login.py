"""Handler de login via Playwright."""

from playwright.async_api import BrowserContext, Page, async_playwright

from config import Settings


async def login_and_get_context(settings: Settings) -> tuple[BrowserContext, Page]:
    """
    Realiza login no PegaPlantão e retorna o contexto autenticado.

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

    # Navega para a página de login
    print(f"🔐 Navegando para {settings.login_url}...")
    await page.goto(settings.login_url)

    # Aguarda o formulário carregar
    await page.wait_for_selector("#MainContent_LoginUser_UserName")

    # Preenche credenciais
    print("📝 Preenchendo credenciais...")
    await page.fill("#MainContent_LoginUser_UserName", settings.email)
    await page.fill("#Password", settings.password)

    # Clica no botão de login
    print("🚀 Efetuando login...")
    await page.click("#MainContent_LoginUser_btnLogin")

    # Aguarda navegação pós-login
    await page.wait_for_load_state("networkidle")

    # Verifica se o login foi bem-sucedido (não está mais na página de login)
    current_url = page.url
    if "/Login" in current_url:
        raise Exception("❌ Falha no login. Verifique suas credenciais.")

    print(f"✅ Login realizado com sucesso! URL atual: {current_url}")

    return context, page


async def navigate_to_escala_mensal(page: Page, settings: Settings) -> None:
    """
    Navega para a página de Escala Mensal.

    Args:
        page: Página do Playwright.
        settings: Configurações com URLs.
    """
    print(f"📅 Navegando para {settings.escala_mensal_url}...")
    await page.goto(settings.escala_mensal_url)
    await page.wait_for_load_state("networkidle")
    print("✅ Página de Escala Mensal carregada.")
