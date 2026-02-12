"""Use case: Crawlar médicos de um estado iterando por município.

Alternativa ao crawl por paginação global — faz um sub-crawl por cidade,
útil para estados grandes ou quando o servidor limita resultados.
"""

from __future__ import annotations

import math
import time

from sqlalchemy.orm import Session

from ..config import CfmSettings
from ..models.domain import MedicoRaw, Medico, translate_keys_to_en
from ..repositories import captcha_repo, doctor_repo
from ..services.cfm_api import CfmApiClient
from ...shared.specialty_parser import parse_specialties
from ...shared.text_utils import title_case_br


def _format_doctor_for_db(raw: MedicoRaw, raw_data: dict, foto=None) -> dict:
    """Formata um médico para persistência no banco."""
    medico = Medico.from_raw(raw, foto=foto)
    medico_dict = medico.model_dump(mode="json")

    specialties_json = parse_specialties(medico_dict.get("especialidade"))
    doc = translate_keys_to_en(medico_dict)

    doc["name"] = title_case_br(doc.get("name"))
    doc["social_name"] = title_case_br(doc.get("social_name"))
    doc["graduation_institution"] = title_case_br(doc.get("graduation_institution"))

    for spec in specialties_json:
        spec["name"] = title_case_br(spec.get("name"))

    doc["specialties"] = specialties_json
    doc["raw_data"] = raw_data

    return doc


class CrawlStateDoctorsUseCase:
    """Crawla médicos de uma UF iterando por todos os municípios."""

    def __init__(
        self,
        session: Session,
        settings: CfmSettings,
        api_client: CfmApiClient,
    ) -> None:
        self._session = session
        self._settings = settings
        self._api = api_client

    def execute(
        self,
        uf: str,
        page_size: int | None = None,
        batch_size: int | None = None,
    ) -> int:
        """Executa crawl de um estado por município.

        Args:
            uf: UF para crawlar.
            page_size: Registros por página (usa config se None).
            batch_size: Páginas por batch (usa config se None).

        Returns:
            Total de médicos processados.
        """
        page_size = page_size or self._settings.page_size
        batch_size = batch_size or self._settings.batch_size

        # Validar captcha
        if not captcha_repo.is_valid(self._session):
            print("\n❌ Token de captcha não encontrado ou expirado!")
            print("   Execute primeiro: uv run cfm-crawler token")
            return 0

        ttl = captcha_repo.get_ttl(self._session)
        print(f"✅ Token de captcha encontrado (TTL: {ttl}s)")

        # Buscar municípios
        print(f"\n🔍 Buscando municípios de {uf}...")
        cities = self._api.fetch_municipios(uf)

        if not cities:
            print(f"❌ Nenhum município encontrado para {uf}.")
            return 0

        print(f"✅ {len(cities)} municípios encontrados para {uf}")

        return self._crawl_by_cities(
            uf=uf,
            cities=cities,
            page_size=page_size,
            batch_size=batch_size,
        )

    def _crawl_by_cities(
        self,
        uf: str,
        cities: list[dict],
        page_size: int,
        batch_size: int,
    ) -> int:
        """Itera por município, buscando todos os médicos de cada um."""
        total_medicos = 0
        total_start = time.time()
        skipped_cities = 0

        captcha_token = self._get_captcha_token()

        for city_idx, city in enumerate(cities, 1):
            city_id = city["id"]
            city_name = city["name"]

            # Revalidar captcha
            captcha_token = self._refresh_token(captcha_token)

            # Página 1 para descobrir total
            try:
                first_page, total_count = self._api.fetch_page(
                    captcha_token=captcha_token,
                    uf=uf,
                    municipio=city_id,
                    page=1,
                    page_size=page_size,
                    request_timeout=self._settings.request_timeout,
                )
            except Exception as e:
                print(
                    f"⚠️ [{city_idx}/{len(cities)}] Erro ao consultar {city_name}: {e}"
                )
                continue

            if total_count == 0:
                skipped_cities += 1
                continue

            total_pages = math.ceil(total_count / page_size)

            # Processar página 1
            city_medicos = 0
            if first_page:
                batch_docs = []
                for raw in first_page:
                    raw_data = raw.model_dump(mode="json", by_alias=True)
                    doc = _format_doctor_for_db(raw, raw_data)
                    batch_docs.append(doc)
                if batch_docs:
                    doctor_repo.upsert_doctors_batch(self._session, batch_docs)
                    self._session.commit()
                    city_medicos += len(batch_docs)

            # Páginas restantes
            for page_num in range(2, total_pages + 1):
                captcha_token = self._refresh_token(captcha_token)

                try:
                    raw_medicos, _ = self._api.fetch_page(
                        captcha_token=captcha_token,
                        uf=uf,
                        municipio=city_id,
                        page=page_num,
                        page_size=page_size,
                        request_timeout=self._settings.request_timeout,
                    )
                except Exception as e:
                    print(f"⚠️ Erro na página {page_num} de {city_name}: {e}")
                    continue

                if raw_medicos:
                    batch_docs = []
                    for raw in raw_medicos:
                        raw_data = raw.model_dump(mode="json", by_alias=True)
                        doc = _format_doctor_for_db(raw, raw_data)
                        batch_docs.append(doc)
                    if batch_docs:
                        doctor_repo.upsert_doctors_batch(self._session, batch_docs)
                        self._session.commit()
                        city_medicos += len(batch_docs)

                time.sleep(self._settings.delay)

            total_medicos += city_medicos
            elapsed = time.time() - total_start
            elapsed_str = f"{int(elapsed // 60)}m{int(elapsed % 60)}s"

            print(
                f"📡 [{city_idx}/{len(cities)}] {city_name}: "
                f"{city_medicos} médicos ({total_pages}pg) | "
                f"Total: {total_medicos} | ⏱️ {elapsed_str}"
            )

        total_time = time.time() - total_start
        total_min = int(total_time / 60)
        total_sec = int(total_time % 60)

        print(f"\n{'=' * 60}")
        print(f"✅ Crawl de {uf} por cidades finalizado!")
        print(
            f"   🏙️  Cidades com médicos: {len(cities) - skipped_cities}/{len(cities)}"
        )
        print(f"   🔹 Cidades sem registros: {skipped_cities}")
        print(f"   👤 Total de médicos: {total_medicos}")
        print(f"   ⏱️  Tempo total: {total_min}m {total_sec}s")
        print(f"{'=' * 60}")

        return total_medicos

    def _get_captcha_token(self) -> str:
        """Obtém token válido do banco."""
        token = captcha_repo.get_token(self._session)
        if not token:
            raise RuntimeError(
                "❌ Token do captcha não encontrado ou expirado!\n"
                "   Execute primeiro: uv run cfm-crawler token"
            )
        ttl = captcha_repo.get_ttl(self._session)
        print(f"✅ Token do captcha obtido (TTL restante: {ttl}s)")
        return token

    def _refresh_token(self, current_token: str) -> str:
        """Revalida e retorna o token."""
        if not captcha_repo.is_valid(self._session):
            raise RuntimeError("Token do captcha expirado durante o crawl.")
        token = captcha_repo.get_token(self._session)
        if not token:
            raise RuntimeError(
                "❌ Token do captcha não encontrado!\n"
                "   Execute: uv run cfm-crawler token"
            )
        return token
