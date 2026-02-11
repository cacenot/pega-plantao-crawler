"""Crawler para buscar médicos no portal do CFM usando Playwright."""

import asyncio
import json
import math
import time
from pathlib import Path

from playwright.async_api import Page, Response
from pydantic import TypeAdapter

from .cache import CaptchaCache
from .db.doctors import upsert_doctors_batch
from .models import Medico, MedicoFotoRaw, MedicoRaw, translate_keys_to_en
from ..shared.specialty_parser import parse_specialties
from ..shared.text_utils import title_case_br

# UFs do Brasil
UFS = [
    "AC",
    "AL",
    "AM",
    "AP",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MG",
    "MS",
    "MT",
    "PA",
    "PB",
    "PE",
    "PI",
    "PR",
    "RJ",
    "RN",
    "RO",
    "RR",
    "RS",
    "SC",
    "SE",
    "SP",
    "TO",
]

CFM_BASE_URL = "https://portal.cfm.org.br"
CFM_BUSCA_URL = f"{CFM_BASE_URL}/api_rest_php/api/v2/medicos/buscar_medicos"
CFM_FOTO_URL = f"{CFM_BASE_URL}/api_rest_php/api/v2/medicos/buscar_foto/"
CFM_PAGE_URL = f"{CFM_BASE_URL}/busca-medicos"


def _build_search_payload(
    captcha_token: str,
    uf: str,
    municipio: str = "",
    page: int = 1,
    page_size: int = 100,
) -> list[dict]:
    """Monta o payload de busca de médicos."""
    return [
        {
            "useCaptchav2": True,
            "captcha": captcha_token,
            "medico": {
                "nome": "",
                "ufMedico": uf,
                "crmMedico": "",
                "municipioMedico": municipio,
                "tipoInscricaoMedico": "",
                "situacaoMedico": "",
                "detalheSituacaoMedico": "",
                "especialidadeMedico": "",
                "areaAtuacaoMedico": "",
            },
            "page": page,
            "pageNumber": page,
            "pageSize": page_size,
        }
    ]


async def _get_captcha_token(cache: CaptchaCache) -> str:
    """Obtém o token do captcha do cache Redis.

    Raises:
        RuntimeError: Se não houver token válido no cache.
    """
    token = await cache.get_token()
    if not token:
        raise RuntimeError(
            "❌ Token do captcha não encontrado ou expirado!\n"
            "   Execute primeiro: uv run cfm token"
        )

    ttl = await cache._redis.ttl(cache._CAPTCHA_KEY)
    print(f"✅ Token do captcha obtido do Redis (TTL restante: {ttl}s)")
    return token


async def _validate_captcha_token(cache: CaptchaCache) -> bool:
    """Verifica se o token do captcha ainda é válido no cache."""
    return await cache.is_valid()


async def _get_captcha_ttl(cache: CaptchaCache) -> int:
    """Retorna o TTL restante do token do captcha em segundos."""
    return await cache._redis.ttl(cache._CAPTCHA_KEY)


async def _intercept_search_response(page: Page) -> dict:
    """Intercepta a resposta da API buscar_medicos disparada pelo formulário."""
    future: asyncio.Future[dict] = asyncio.get_event_loop().create_future()

    async def _on_response(response: Response) -> None:
        if "buscar_medicos" in response.url:
            try:
                data = await response.json()
                if not future.done():
                    future.set_result(data)
            except Exception:
                pass

    page.on("response", _on_response)

    result = await future
    page.remove_listener("response", _on_response)
    return result


async def _select_uf_and_search(page: Page, uf: str) -> None:
    """Seleciona a UF no formulário de busca."""
    await page.select_option('select[name="uf"]', uf)
    await page.wait_for_timeout(500)


async def fetch_medicos_page(
    page: Page,
    captcha_token: str,
    uf: str,
    municipio: str = "",
    current_page: int = 1,
    page_size: int = 100,
    request_timeout: int = 120,
) -> tuple[list[MedicoRaw], int]:
    """Busca uma página de médicos via fetch no contexto do browser.

    Returns:
        Tupla com (lista de MedicoRaw, total de registros).
    """
    payload = _build_search_payload(
        captcha_token=captcha_token,
        uf=uf,
        municipio=municipio,
        page=current_page,
        page_size=page_size,
    )

    js_timeout_ms = (request_timeout - 5) * 1000

    try:
        data = await asyncio.wait_for(
            page.evaluate(
                """async ([url, payload, timeoutMs]) => {
                    const controller = new AbortController();
                    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

                    try {
                        const resp = await fetch(url, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify(payload),
                            signal: controller.signal
                        });
                        clearTimeout(timeoutId);
                        return await resp.json();
                    } catch (error) {
                        clearTimeout(timeoutId);
                        if (error.name === 'AbortError') {
                            throw new Error(`Request timeout (${timeoutMs}ms)`);
                        }
                        throw error;
                    }
                }""",
                [CFM_BUSCA_URL, payload, js_timeout_ms],
            ),
            timeout=request_timeout,
        )
    except asyncio.TimeoutError:
        raise Exception(
            f"Timeout de {request_timeout}s ao buscar página {current_page} da UF {uf}"
        )

    if data.get("status") != "sucesso":
        raise Exception(f"API retornou erro: {data}")

    dados = data.get("dados", [])
    if not dados:
        return [], 0

    total_count = int(dados[0].get("COUNT", 0))

    adapter = TypeAdapter(list[MedicoRaw])
    medicos = adapter.validate_python(dados)

    return medicos, total_count


async def fetch_foto_medico(
    page: Page,
    crm: str,
    uf: str,
    security_hash: str,
) -> MedicoFotoRaw | None:
    """Busca os detalhes/foto de um médico via POST no contexto do browser."""
    try:
        payload = [{"securityHash": security_hash, "crm": crm, "uf": uf}]
        data = await page.evaluate(
            """async ([url, payload, timeoutMs]) => {
                const controller = new AbortController();
                const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

                try {
                    const resp = await fetch(url, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(payload),
                        signal: controller.signal
                    });
                    clearTimeout(timeoutId);
                    const text = await resp.text();
                    try {
                        return JSON.parse(text);
                    } catch {
                        return { status: 'erro', mensagem: text };
                    }
                } catch (error) {
                    clearTimeout(timeoutId);
                    if (error.name === 'AbortError') {
                        return { status: 'erro', mensagem: `Timeout após ${timeoutMs}ms` };
                    }
                    throw error;
                }
            }""",
            [CFM_FOTO_URL, payload, 30000],
        )

        if data.get("status") == "sucesso" and data.get("dados"):
            return MedicoFotoRaw(**data["dados"][0])
    except Exception as e:
        print(f"⚠️ Erro ao buscar foto CRM {crm}/{uf}: {e}")

    return None


def _format_doctor_for_db(
    raw: MedicoRaw, raw_data: dict, foto: MedicoFotoRaw | None = None
) -> dict:
    """Formata um médico para persistência no banco.

    Aplica title_case_br, parseia especialidades, traduz chaves para EN
    e inclui raw_data.
    """
    medico = Medico.from_raw(raw, foto=foto)
    medico_dict = medico.model_dump(mode="json")

    # Parseia especialidades de string para lista de dicts
    specialties_json = parse_specialties(medico_dict.get("especialidade"))

    # Traduz chaves para EN
    doc = translate_keys_to_en(medico_dict)

    # Formata campos com title_case_br
    doc["name"] = title_case_br(doc.get("name"))
    doc["social_name"] = title_case_br(doc.get("social_name"))
    doc["graduation_institution"] = title_case_br(doc.get("graduation_institution"))

    # Formata nomes das especialidades
    for spec in specialties_json:
        spec["name"] = title_case_br(spec.get("name"))

    doc["specialties"] = specialties_json
    doc["raw_data"] = raw_data

    return doc


async def _fetch_batch(
    page: Page,
    captcha_token: str,
    uf: str,
    pages: list[int],
    page_size: int,
    request_timeout: int = 120,
) -> list[tuple[int, list[MedicoRaw], int]]:
    """Busca várias páginas em paralelo.

    Returns:
        Lista de tuplas (page_number, medicos, total_count).
    """
    tasks = [
        fetch_medicos_page(
            page=page,
            captcha_token=captcha_token,
            uf=uf,
            current_page=p,
            page_size=page_size,
            request_timeout=request_timeout,
        )
        for p in pages
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    successful: list[tuple[int, list[MedicoRaw], int]] = []
    failed_pages: list[int] = []

    for p, result in zip(pages, results):
        if isinstance(result, Exception):
            print(f"⚠️ Erro na página {p}: {result}")
            failed_pages.append(p)
        else:
            medicos, total_count = result
            successful.append((p, medicos, total_count))

    # Retry das páginas que falharam (até 2 tentativas individuais)
    for retry_attempt in range(1, 3):
        if not failed_pages:
            break

        print(
            f"🔄 Retry {retry_attempt}/2 de {len(failed_pages)} página(s): {failed_pages}"
        )
        await asyncio.sleep(2)

        still_failed: list[int] = []
        for p in failed_pages:
            try:
                medicos, total_count = await fetch_medicos_page(
                    page=page,
                    captcha_token=captcha_token,
                    uf=uf,
                    current_page=p,
                    page_size=page_size,
                    request_timeout=request_timeout,
                )
                successful.append((p, medicos, total_count))
                print(f"   ✅ Página {p} recuperada no retry {retry_attempt}")
            except Exception as e:
                print(f"   ⚠️ Página {p} falhou novamente: {e}")
                still_failed.append(p)

        failed_pages = still_failed

    if failed_pages:
        print(f"❌ Páginas não recuperadas após retries: {failed_pages}")

    return successful


async def crawl_uf(
    page: Page,
    uf: str,
    cache: CaptchaCache,
    db_pool,
    page_size: int = 200,
    delay: float = 2.0,
    fetch_fotos: bool = True,
    max_results: int = 0,
    start_page: int = 1,
    request_timeout: int = 120,
    batch_size: int = 5,
) -> int:
    """Crawla todos os médicos de uma UF, paginando até o fim.

    Returns:
        Total de médicos processados para esta UF.
    """
    print(f"\n{'=' * 60}")
    print(f"🏥 Iniciando crawl da UF: {uf}")
    if start_page > 1:
        print(f"🔄 Retomando a partir da página {start_page}")
    print(f"⚡ Batch size: {batch_size} | Timeout: {request_timeout}s")
    print(f"{'=' * 60}")

    captcha_token = await _get_captcha_token(cache)

    medicos_anteriores = (start_page - 1) * page_size
    total_medicos = 0
    current_page = start_page
    total_pages = None
    total_count = 0
    retries = 0
    max_retries = 3

    batch_times: list[float] = []
    total_start_time = time.time()

    while True:
        if not await _validate_captcha_token(cache):
            print("❌ Token do captcha expirou! Execute: uv run cfm token")
            await cache.mark_failed(uf)
            raise RuntimeError("Token do captcha expirado durante o crawl.")

        if total_pages is not None:
            remaining_pages = total_pages - current_page + 1
            n_pages = min(batch_size, remaining_pages)
        else:
            n_pages = batch_size

        pages_to_fetch = list(range(current_page, current_page + n_pages))

        batch_start = time.time()

        try:
            results = await _fetch_batch(
                page=page,
                captcha_token=captcha_token,
                uf=uf,
                pages=pages_to_fetch,
                page_size=page_size,
                request_timeout=request_timeout,
            )
            batch_time = time.time() - batch_start
            batch_times.append(batch_time)
            retries = 0
        except Exception as e:
            retries += 1
            print(f"⚠️ Erro no batch (tentativa {retries}/{max_retries}): {e}")

            if retries >= max_retries:
                if await _validate_captcha_token(cache):
                    captcha_token = await _get_captcha_token(cache)
                    retries = 0
                    continue
                else:
                    print("❌ Token expirado e máximo de retries atingido.")
                    await cache.mark_failed(uf)
                    raise RuntimeError(
                        f"Falha após {max_retries} tentativas para UF {uf}. "
                        "Execute: uv run cfm token"
                    )

            await asyncio.sleep(delay)
            continue

        if not results:
            retries += 1
            if retries >= max_retries:
                if await _validate_captcha_token(cache):
                    captcha_token = await _get_captcha_token(cache)
                    retries = 0
                    continue
                else:
                    await cache.mark_failed(uf)
                    raise RuntimeError(
                        f"Todas as {len(pages_to_fetch)} requests falharam para UF {uf}."
                    )
            await asyncio.sleep(delay)
            continue

        results.sort(key=lambda r: r[0])

        batch_medicos: list[dict] = []
        last_page_in_batch = current_page
        finished = False

        for page_num, raw_medicos, page_total_count in results:
            if not raw_medicos:
                finished = True
                break

            total_count = page_total_count
            total_pages = math.ceil(total_count / page_size) if total_count > 0 else 1
            last_page_in_batch = page_num

            for raw in raw_medicos:
                raw_data = raw.model_dump(mode="json", by_alias=True)
                doc = _format_doctor_for_db(raw, raw_data=raw_data, foto=None)
                batch_medicos.append(doc)

            if len(raw_medicos) < page_size:
                finished = True
                break

        total_acumulado = medicos_anteriores + total_medicos + len(batch_medicos)
        pages_in_batch = last_page_in_batch - current_page + 1
        successful_pages = len(results)
        failed_pages_count = len(pages_to_fetch) - successful_pages

        avg_batch_time = sum(batch_times) / len(batch_times) if batch_times else 0
        if total_pages and total_pages > last_page_in_batch:
            batches_remaining = math.ceil(
                (total_pages - last_page_in_batch) / batch_size
            )
            eta_seconds = batches_remaining * (avg_batch_time + delay)
            eta_minutes = int(eta_seconds / 60)
        else:
            eta_minutes = 0

        eta_info = ""
        if eta_minutes > 60:
            eta_hours = eta_minutes // 60
            eta_mins = eta_minutes % 60
            eta_info = f" | ETA: ~{eta_hours}h{eta_mins}m"
        elif eta_minutes > 0:
            eta_info = f" | ETA: ~{eta_minutes}m"

        ttl_info = ""
        if current_page == start_page or last_page_in_batch % 20 == 0:
            ttl = await _get_captcha_ttl(cache)
            if ttl > 0:
                ttl_info = f" | TTL: {ttl}s"
                if ttl < 120:
                    ttl_info += " ⚠️"

        fail_info = f" ({failed_pages_count} falhas)" if failed_pages_count > 0 else ""

        print(
            f"📡 Páginas {current_page}-{last_page_in_batch}"
            f"/{total_pages or '?'}: {len(batch_medicos)} médicos "
            f"(parcial: {total_acumulado}/{total_count}) | "
            f"{batch_time:.2f}s ({successful_pages}pg){fail_info}{eta_info}{ttl_info}"
        )

        if batch_medicos:
            process_start = time.time()
            await upsert_doctors_batch(db_pool, batch_medicos)
            process_time = time.time() - process_start

            if process_time > 1.0:
                print(f"   💾 Insert: {process_time:.2f}s")

            total_medicos += len(batch_medicos)

        if total_pages:
            await cache.store_progress(
                uf=uf,
                last_page=last_page_in_batch,
                total_pages=total_pages,
                total_records=total_count,
                status="running",
            )

        if max_results > 0 and total_medicos >= max_results:
            print(f"🛑 Limite de teste atingido: {total_medicos}/{max_results}")
            break

        if finished or total_acumulado >= total_count:
            print(f"✅ {total_acumulado}/{total_count} médicos coletados para {uf}.")
            break

        current_page = last_page_in_batch + 1
        await asyncio.sleep(delay)

    await cache.mark_complete(uf)
    total_final = medicos_anteriores + total_medicos

    total_time = time.time() - total_start_time
    total_minutes = int(total_time / 60)
    total_seconds = int(total_time % 60)
    pages_processed = (last_page_in_batch - start_page + 1) if results else 0

    print(f"\n{'=' * 60}")
    print(
        f"💾 {total_medicos} médicos processados nesta sessão ({total_final} total) de {uf} persistidos no PostgreSQL."
    )
    print(f"⏱️  Tempo total: {total_minutes}m {total_seconds}s")
    print(f"📄 Páginas processadas: {pages_processed}")
    if batch_times:
        avg_time = sum(batch_times) / len(batch_times)
        print(f"⚡ Tempo médio por batch ({batch_size}pg): {avg_time:.2f}s")
    print(f"{'=' * 60}")

    return total_medicos


def save_medicos_to_json(medicos: list[Medico], output_dir: str, uf: str) -> Path:
    """Salva médicos em arquivo JSON (legacy/backup).

    .. deprecated::
        Use a persistência no PostgreSQL via crawl_uf().
    """
    output_path = Path(output_dir) / "cfm"
    output_path.mkdir(parents=True, exist_ok=True)

    file_path = output_path / f"medicos_{uf.lower()}.json"

    data = [medico.model_dump(mode="json") for medico in medicos]

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"💾 {len(medicos)} médicos salvos em {file_path}")
    return file_path
