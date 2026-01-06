"""Cliente HTTP autenticado usando httpx."""

import httpx
from playwright.async_api import BrowserContext

from config import Settings


async def create_authenticated_client(
    context: BrowserContext,
    settings: Settings,
) -> httpx.AsyncClient:
    """
    Cria um cliente httpx com os cookies do Playwright.

    Args:
        context: Contexto do Playwright com sessão autenticada.
        settings: Configurações do crawler.

    Returns:
        Cliente httpx configurado com cookies e headers.
    """
    # Extrai cookies do Playwright
    playwright_cookies = await context.cookies()

    # Converte para formato httpx
    cookies = {}
    for cookie in playwright_cookies:
        cookies[cookie["name"]] = cookie["value"]

    # Headers necessários para a API
    headers = {
        "accept": "*/*",
        "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "content-type": "application/json",
        "referer": settings.escala_mensal_url,
        "sec-ch-ua": '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/143.0.0.0 Safari/537.36"
        ),
        "x-requested-with": "XMLHttpRequest",
    }

    # Cria cliente httpx
    client = httpx.AsyncClient(
        base_url=settings.base_url,
        cookies=cookies,
        headers=headers,
        timeout=httpx.Timeout(settings.timeout / 1000),  # Converte para segundos
        follow_redirects=True,
    )

    print(f"🍪 Cliente httpx criado com {len(cookies)} cookies.")

    return client
