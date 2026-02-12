"""Use case: Crawlar todos os médicos de um ou mais estados.

Itera estados selecionados, pagina em batches e persiste no banco.
Simplificação: sem execution plans — crawla do zero a cada execução.
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


class CrawlAllDoctorsUseCase:
    """Crawla médicos de um ou mais estados via API do CFM.

    Para cada estado, descobre o total de registros, pagina em batches
    e faz upsert no banco.
    """

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
        states: list[str],
        page_size: int | None = None,
        batch_size: int | None = None,
        tipo_inscricao: str = "",
        situacao: str = "",
    ) -> int:
        """Executa crawl de todos os estados selecionados.

        Args:
            states: Lista de UFs para crawlar.
            page_size: Registros por página (usa config se None).
            batch_size: Páginas por batch (usa config se None).
            tipo_inscricao: Filtro de tipo de inscrição.
            situacao: Filtro de situação.

        Returns:
            Total de médicos processados.
        """
        page_size = page_size or self._settings.page_size
        batch_size = batch_size or self._settings.batch_size

        total_medicos = 0

        print(f"📋 Estados a processar: {', '.join(states)}")

        for uf in states:
            try:
                count = self._crawl_state(
                    uf=uf,
                    page_size=page_size,
                    batch_size=batch_size,
                    tipo_inscricao=tipo_inscricao,
                    situacao=situacao,
                )
                total_medicos += count
            except RuntimeError as e:
                if "captcha" in str(e).lower():
                    print(f"\n❌ Token do captcha expirou durante o crawl de {uf}.")
                    print("   Execute: uv run cfm-crawler token")
                    raise
                print(f"❌ Erro ao processar UF {uf}: {e}")
                continue
            except Exception as e:
                print(f"❌ Erro ao processar UF {uf}: {e}")
                continue

        return total_medicos

    def _crawl_state(
        self,
        uf: str,
        page_size: int,
        batch_size: int,
        tipo_inscricao: str = "",
        situacao: str = "",
    ) -> int:
        """Crawla todos os médicos de uma UF.

        Returns:
            Total de médicos processados.
        """
        print(f"\n{'=' * 60}")
        print(f"🏥 Iniciando crawl da UF: {uf}")
        print(f"⚡ Batch size: {batch_size} | Page size: {page_size}")
        print(f"{'=' * 60}")

        captcha_token = self._get_captcha_token()

        total_medicos = 0
        total_count = 0
        total_pages = None
        consecutive_empty = 0
        max_empty = 2
        batch_times: list[float] = []
        total_start = time.time()

        current_page = 1

        while True:
            # Validar token
            if not captcha_repo.is_valid(self._session):
                raise RuntimeError("Token do captcha expirado durante o crawl.")

            # Determinar páginas do batch
            if total_pages is not None:
                remaining = list(range(current_page, total_pages + 1))
                pages = remaining[:batch_size]
            else:
                pages = [current_page]

            if not pages:
                break

            batch_start = time.time()

            # Fetch batch
            batch_medicos: list[dict] = []
            successful_pages = 0

            for p in pages:
                try:
                    raw_medicos, page_total = self._api.fetch_page(
                        captcha_token=captcha_token,
                        uf=uf,
                        page=p,
                        page_size=page_size,
                        request_timeout=self._settings.request_timeout,
                        tipo_inscricao=tipo_inscricao,
                        situacao=situacao,
                    )

                    if page_total > 0:
                        total_count = page_total
                        total_pages = math.ceil(total_count / page_size)

                    for raw in raw_medicos:
                        raw_data = raw.model_dump(mode="json", by_alias=True)
                        doc = _format_doctor_for_db(raw, raw_data)
                        batch_medicos.append(doc)

                    successful_pages += 1

                except Exception as e:
                    print(f"⚠️ Erro na página {p}: {e}")
                    # Retry individual
                    try:
                        time.sleep(2)
                        raw_medicos, page_total = self._api.fetch_page(
                            captcha_token=captcha_token,
                            uf=uf,
                            page=p,
                            page_size=page_size,
                            request_timeout=self._settings.request_timeout,
                            tipo_inscricao=tipo_inscricao,
                            situacao=situacao,
                        )
                        if page_total > 0:
                            total_count = page_total
                            total_pages = math.ceil(total_count / page_size)
                        for raw in raw_medicos:
                            raw_data = raw.model_dump(mode="json", by_alias=True)
                            doc = _format_doctor_for_db(raw, raw_data)
                            batch_medicos.append(doc)
                        successful_pages += 1
                        print(f"   ✅ Página {p} recuperada no retry")
                    except Exception as e2:
                        print(f"   ❌ Página {p} falhou no retry: {e2}")

            batch_time = time.time() - batch_start
            batch_times.append(batch_time)

            # Detectar bloqueio
            if batch_medicos:
                consecutive_empty = 0
            elif total_count > 0:
                consecutive_empty += 1
                if consecutive_empty >= max_empty:
                    print(
                        f"\n🚫 Servidor bloqueou! {consecutive_empty} batches "
                        f"consecutivos com 0 médicos."
                    )
                    raise RuntimeError(
                        "Servidor bloqueou a requisição. "
                        "Resolva novo captcha: uv run cfm-crawler token"
                    )

            # Persistir
            if batch_medicos:
                process_start = time.time()
                doctor_repo.upsert_doctors_batch(self._session, batch_medicos)
                self._session.commit()
                process_time = time.time() - process_start

                if process_time > 1.0:
                    print(f"   💾 Insert: {process_time:.2f}s")

                total_medicos += len(batch_medicos)

            # Progresso
            current_page = max(pages) + 1
            if total_pages:
                fetched = min(current_page - 1, total_pages)
                pct = round(fetched / total_pages * 100, 1)

                avg_time = sum(batch_times) / len(batch_times)
                remaining_batches = math.ceil((total_pages - fetched) / batch_size)
                eta_s = remaining_batches * (avg_time + self._settings.delay)
                eta_m = int(eta_s / 60)

                eta = ""
                if eta_m > 60:
                    eta = f" | ETA: ~{eta_m // 60}h{eta_m % 60}m"
                elif eta_m > 0:
                    eta = f" | ETA: ~{eta_m}m"

                page_range = f"{min(pages)}-{max(pages)}"
                print(
                    f"📡 Páginas {page_range}/{total_pages}: "
                    f"{len(batch_medicos)} médicos ({pct}%) | "
                    f"{batch_time:.2f}s{eta}"
                )

            # Limite de teste
            if (
                self._settings.max_results > 0
                and total_medicos >= self._settings.max_results
            ):
                print(
                    f"🛑 Limite de teste atingido: {total_medicos}/{self._settings.max_results}"
                )
                break

            # Verificar se terminou
            if total_pages and current_page > total_pages:
                break

            time.sleep(self._settings.delay)

        total_time = time.time() - total_start
        total_min = int(total_time / 60)
        total_sec = int(total_time % 60)

        print(f"\n{'=' * 60}")
        print(f"✅ {total_medicos} médicos processados para UF {uf}.")
        print(f"⏱️  Tempo total: {total_min}m {total_sec}s")
        if batch_times:
            avg = sum(batch_times) / len(batch_times)
            print(f"⚡ Tempo médio por batch ({batch_size}pg): {avg:.2f}s")
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
