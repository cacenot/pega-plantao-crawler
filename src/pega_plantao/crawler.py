"""Crawler para capturar services/shifts da API Pega Plantão."""

import asyncio
import json
from pathlib import Path

import httpx
from pydantic import TypeAdapter

from .config import Settings
from .models import Service, ServiceRaw
from .utils.date_utils import get_date_range


async def fetch_all_services(
    client: httpx.AsyncClient,
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
                response = await client.post(api_url, json=payload)
                response.raise_for_status()
                response_data = response.json()
                break
            except httpx.HTTPStatusError as e:
                print(f"⚠️ Erro HTTP {e.response.status_code} na página {page}")
                if attempt < max_retries:
                    print(f"🔄 Aguardando {retry_delay}s antes de retry...")
                    await asyncio.sleep(retry_delay)
                else:
                    raise
            except httpx.RequestError as e:
                print(f"⚠️ Erro de conexão na página {page}: {e}")
                if attempt < max_retries:
                    print(f"🔄 Aguardando {retry_delay}s antes de retry...")
                    await asyncio.sleep(retry_delay)
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
        await asyncio.sleep(delay)

    return all_services


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
