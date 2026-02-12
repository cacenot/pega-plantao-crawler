"""Cliente HTTP e funções de crawling para a API Pega Plantão (síncrono)."""

import json
import time
from calendar import monthrange
from datetime import datetime
from pathlib import Path

import httpx
from playwright.sync_api import BrowserContext
from pydantic import TypeAdapter

from ..config import Settings
from ..models.domain import Service, ServiceRaw


# ── Date utils ─────────────────────────────────────────────────


def get_date_range() -> tuple[str, str]:
    """Retorna o range de datas: hoje até o fim do próximo mês.

    Returns:
        Tupla com (start_date, end_date) nos formatos esperados pela API.
        - start_date: "YYYY-MM-DD"
        - end_date: "YYYY-MM-DDTHH:MM:SS"
    """
    today = datetime.now()

    if today.month == 12:
        next_month = 1
        next_year = today.year + 1
    else:
        next_month = today.month + 1
        next_year = today.year

    last_day = monthrange(next_year, next_month)[1]
    end_of_next_month = datetime(next_year, next_month, last_day)

    start_date = today.strftime("%Y-%m-%d")
    end_date = end_of_next_month.strftime("%Y-%m-%dT00:00:00")

    return start_date, end_date


# ── Authenticated HTTP client ──────────────────────────────────


def create_authenticated_client(
    context: BrowserContext,
    settings: Settings,
) -> httpx.Client:
    """Cria um cliente httpx síncrono com os cookies do Playwright.

    Args:
        context: Contexto do Playwright com sessão autenticada.
        settings: Configurações do crawler.

    Returns:
        Cliente httpx configurado com cookies e headers.
    """
    playwright_cookies = context.cookies()

    cookies = {cookie["name"]: cookie["value"] for cookie in playwright_cookies}

    headers = {
        "accept": "*/*",
        "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "content-type": "application/json",
        "referer": settings.escala_mensal_url,
        "sec-ch-ua": (
            '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"'
        ),
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

    client = httpx.Client(
        base_url=settings.base_url,
        cookies=cookies,
        headers=headers,
        timeout=httpx.Timeout(settings.timeout / 1000),
        follow_redirects=True,
    )

    print(f"🍪 Cliente httpx criado com {len(cookies)} cookies.")

    return client


# ── Fetch services ─────────────────────────────────────────────


def fetch_all_services(
    client: httpx.Client,
    settings: Settings,
    page_size: int = 50,
    delay: float = 1.0,
    max_retries: int = 3,
    retry_delay: float = 2.0,
) -> list[Service]:
    """Faz requests paginadas à API de shifts e retorna todos os services."""
    start_date, end_date = get_date_range()
    print(f"📅 Buscando services de {start_date} até {end_date}")

    all_services: list[Service] = []
    page = 1
    api_url = f"{settings.base_url}/api/v1/shifts/forlist"

    while True:
        payload = {
            "ServiceStartDate": start_date,
            "ServiceEndDate": end_date,
            "ServiceStartTime": "",
            "Page": page,
            "PageSize": page_size,
            "SelectedProfessionals": [],
            "SelectedSectors": [],
            "FilterType": ["3"],
            "ServiceTypeId": [],
            "WeekDay": -1,
            "WeekDays": [1, 2, 3, 4, 5, 6, 7],
            "ProfessionalToViewId": "incharge",
        }

        response_data = None
        for attempt in range(1, max_retries + 1):
            try:
                print(f"📡 Página {page} (tentativa {attempt}/{max_retries})...")
                response = client.post(api_url, json=payload)
                response.raise_for_status()
                response_data = response.json()
                break
            except httpx.HTTPStatusError as e:
                print(f"⚠️ Erro HTTP {e.response.status_code} na página {page}")
                if attempt < max_retries:
                    print(f"🔄 Aguardando {retry_delay}s antes de retry...")
                    time.sleep(retry_delay)
                else:
                    raise
            except httpx.RequestError as e:
                print(f"⚠️ Erro de conexão na página {page}: {e}")
                if attempt < max_retries:
                    print(f"🔄 Aguardando {retry_delay}s antes de retry...")
                    time.sleep(retry_delay)
                else:
                    raise

        if response_data is None:
            raise Exception(
                f"Falha ao buscar página {page} após {max_retries} tentativas"
            )

        services_list = response_data.get("Services", [])

        adapter = TypeAdapter(list[ServiceRaw])
        raw_services = adapter.validate_python(services_list)

        services = [Service.from_raw(raw) for raw in raw_services]
        all_services.extend(services)

        print(
            f"✅ Página {page}: {len(services)} services (total: {len(all_services)})"
        )

        if len(raw_services) < page_size:
            print("📄 Última página alcançada.")
            break

        page += 1
        time.sleep(delay)

    return all_services


# ── Save to JSON ───────────────────────────────────────────────


def save_services_to_json(services: list[Service], output_dir: str) -> Path:
    """Salva os services em arquivo JSON."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    file_path = output_path / "services.json"

    data = [service.model_dump(mode="json") for service in services]

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"💾 Dados salvos em {file_path}")

    return file_path
