"""Use case: Gerenciar token de captcha.

Abre navegador para resolução manual do reCAPTCHA e armazena o token no banco.
"""

from __future__ import annotations

import asyncio

from sqlalchemy.orm import Session

from ..config import CfmSettings
from ..repositories import captcha_repo
from ..services.cfm_api import CFM_PAGE_URL


class ManageTokenUseCase:
    """Resolve reCAPTCHA manualmente e armazena token no banco."""

    def __init__(self, session: Session, settings: CfmSettings) -> None:
        self._session = session
        self._settings = settings

    def execute(self, loop: bool = False) -> None:
        """Executa o fluxo de resolução de captcha.

        Args:
            loop: Se True, fica aberto para renovar o token continuamente.
        """
        asyncio.run(self._run(loop))

    async def _run(self, loop: bool) -> None:
        """Lógica async — usa Playwright para abrir o browser."""
        from playwright.async_api import async_playwright

        print("=" * 60)
        print("🔑 CFM Crawler - Captcha Solver")
        print(
            f"📦 TTL do token: {self._settings.captcha_ttl}s "
            f"({self._settings.captcha_ttl // 60} min)"
        )
        print(f"🔄 Modo loop: {'Sim' if loop else 'Não'}")
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

        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
        )

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
                token_value = await self._wait_for_captcha_token(page)
                captcha_repo.store_token(
                    self._session, token_value, self._settings.captcha_ttl
                )
                self._session.commit()

                ttl = captcha_repo.get_ttl(self._session)
                print(f"\n✅ Token salvo no PostgreSQL! (TTL: {ttl}s)")
                print(f"   Token (primeiros 40 chars): {token_value[:40]}...")

                if not loop:
                    print("\n🏁 Captcha resolvido. Agora execute o crawler:")
                    print("   uv run cfm-crawler doctors")
                    break

                print("\n🔄 Modo loop ativo. Aguardando novo captcha...")
                print("   Pressione Ctrl+C para sair.\n")

                await page.reload(wait_until="domcontentloaded", timeout=60000)
                try:
                    await page.wait_for_selector(
                        "iframe[src*='recaptcha']", timeout=30000
                    )
                except Exception:
                    pass

        except KeyboardInterrupt:
            print("\n\n🛑 Interrompido pelo usuário.")
        finally:
            await browser.close()
            await playwright.stop()

    async def _wait_for_captcha_token(self, page) -> str:
        """Aguarda o usuário resolver o reCAPTCHA e retorna o token."""
        print("\n" + "=" * 60)
        print("⏳ RESOLVA O CAPTCHA MANUALMENTE NO NAVEGADOR")
        print("=" * 60)
        print("1. Clique na checkbox 'Não sou um robô'")
        print("2. Resolva o desafio de imagens se pedido")
        print("3. O token será capturado automaticamente")
        print("=" * 60 + "\n")

        page.set_default_timeout(10 * 60 * 1000)

        token_value = await page.evaluate(
            """
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
            """
        )

        page.set_default_timeout(30000)
        return token_value
