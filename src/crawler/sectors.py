"""Crawler para capturar setores da API."""

import json
import re
from pathlib import Path

import httpx
from playwright.async_api import Page, Response
from pydantic import TypeAdapter

from config import Settings
from models.sector import SectorGroup


async def intercept_sectors_api(page: Page, settings: Settings) -> str:
    """
    Navega para EscalaMensal e intercepta a URL da API de setores.

    Args:
        page: Página do Playwright autenticada.
        settings: Configurações do crawler.

    Returns:
        URL completa da API com parâmetro v= dinâmico.
    """
    captured_url: str | None = None

    # Regex para capturar a URL da API
    api_pattern = re.compile(rf".*{re.escape(settings.sectors_api_path)}\?v=.*")

    async def handle_response(response: Response) -> None:
        nonlocal captured_url
        if api_pattern.match(response.url):
            captured_url = response.url
            print(f"🎯 API interceptada: {captured_url}")

    # Registra listener para respostas
    page.on("response", handle_response)

    # Navega para a página de Escala Mensal
    print(f"📅 Navegando para {settings.escala_mensal_url}...")
    await page.goto(settings.escala_mensal_url)

    # Aguarda a página carregar completamente
    await page.wait_for_load_state("networkidle")

    # Remove o listener
    page.remove_listener("response", handle_response)

    if not captured_url:
        raise Exception("❌ Não foi possível interceptar a URL da API de setores.")

    return captured_url


async def fetch_sectors(client: httpx.AsyncClient, api_url: str) -> list[SectorGroup]:
    """
    Faz request à API de setores e retorna os dados validados.

    Args:
        client: Cliente httpx autenticado.
        api_url: URL completa da API de setores.

    Returns:
        Lista de SectorGroup validados.
    """
    print(f"📡 Fazendo request para API de setores...")

    response = await client.get(api_url)
    response.raise_for_status()

    data = response.json()
    print(f"✅ Recebidos {len(data)} grupos de setores.")

    # Valida com Pydantic
    adapter = TypeAdapter(list[SectorGroup])
    sectors = adapter.validate_python(data)

    print(f"✅ Dados validados com Pydantic: {len(sectors)} grupos.")

    return sectors


def save_sectors_to_json(sectors: list[SectorGroup], output_dir: str) -> Path:
    """
    Salva os setores em arquivo JSON.

    Args:
        sectors: Lista de setores validados.
        output_dir: Diretório de saída.

    Returns:
        Path do arquivo salvo.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    file_path = output_path / "sectors.json"

    # Converte para JSON mantendo aliases originais
    data = [sector.model_dump(by_alias=True, mode="json") for sector in sectors]

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"💾 Dados salvos em {file_path}")

    return file_path
