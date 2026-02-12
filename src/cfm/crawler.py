"""Crawler para buscar médicos no portal do CFM usando httpx."""

import asyncio
import json
import math
import time
from pathlib import Path

import asyncpg
import httpx
from pydantic import TypeAdapter

from .db import captcha as captcha_db
from .db.doctors import upsert_doctors_batch
from .db.executions import (
    check_execution_complete,
    check_state_complete,
    complete_execution_state,
    fail_execution_state,
    get_pending_pages,
    get_pending_states,
    initialize_pages,
    mark_page_failed,
    mark_pages_fetched_batch,
    start_execution,
    start_execution_state,
    pause_execution,
    update_execution_state,
)
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
    tipo_inscricao: str = "",
    situacao: str = "",
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
                "tipoInscricaoMedico": tipo_inscricao,
                "situacaoMedico": situacao,
                "detalheSituacaoMedico": "",
                "especialidadeMedico": "",
                "areaAtuacaoMedico": "",
            },
            "page": page,
            "pageNumber": page,
            "pageSize": page_size,
        }
    ]


async def _get_captcha_token(db_pool: asyncpg.Pool) -> str:
    """Obtém o token do captcha do banco.

    Raises:
        RuntimeError: Se não houver token válido.
    """
    token = await captcha_db.get_token(db_pool)
    if not token:
        raise RuntimeError(
            "❌ Token do captcha não encontrado ou expirado!\n"
            "   Execute primeiro: uv run cfm token"
        )

    ttl = await captcha_db.get_ttl(db_pool)
    print(f"✅ Token do captcha obtido (TTL restante: {ttl}s)")
    return token


async def _validate_captcha_token(db_pool: asyncpg.Pool) -> bool:
    """Verifica se o token do captcha ainda é válido."""
    return await captcha_db.is_valid(db_pool)


async def _get_captcha_ttl(db_pool: asyncpg.Pool) -> int:
    """Retorna o TTL restante do token do captcha em segundos."""
    return await captcha_db.get_ttl(db_pool)


_HTTP_HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Origin": CFM_BASE_URL,
    "Referer": CFM_PAGE_URL,
}


def create_http_client(timeout: int = 120) -> httpx.AsyncClient:
    """Cria um httpx.AsyncClient configurado para o portal do CFM."""
    return httpx.AsyncClient(
        headers=_HTTP_HEADERS,
        timeout=httpx.Timeout(timeout, connect=15),
    )


async def fetch_medicos_page(
    client: httpx.AsyncClient,
    captcha_token: str,
    uf: str,
    municipio: str = "",
    current_page: int = 1,
    page_size: int = 100,
    request_timeout: int = 120,
    tipo_inscricao: str = "",
    situacao: str = "",
) -> tuple[list[MedicoRaw], int]:
    """Busca uma página de médicos via httpx POST.

    Returns:
        Tupla com (lista de MedicoRaw, total de registros).
    """
    payload = _build_search_payload(
        captcha_token=captcha_token,
        uf=uf,
        municipio=municipio,
        page=current_page,
        page_size=page_size,
        tipo_inscricao=tipo_inscricao,
        situacao=situacao,
    )

    try:
        resp = await client.post(CFM_BUSCA_URL, json=payload, timeout=request_timeout)
        data = resp.json()
    except httpx.TimeoutException:
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
    client: httpx.AsyncClient,
    crm: str,
    uf: str,
    security_hash: str,
) -> MedicoFotoRaw | None:
    """Busca os detalhes/foto de um médico via httpx POST."""
    try:
        payload = [{"securityHash": security_hash, "crm": crm, "uf": uf}]
        resp = await client.post(CFM_FOTO_URL, json=payload, timeout=30)
        data = resp.json()

        if data.get("status") == "sucesso" and data.get("dados"):
            return MedicoFotoRaw(**data["dados"][0])
    except Exception as e:
        print(f"⚠️ Erro ao buscar foto CRM {crm}/{uf}: {e}")

    return None


async def fetch_all_state_counts(
    captcha_token: str,
) -> dict[str, int]:
    """Busca o total de médicos de cada UF em paralelo via httpx (sem browser).

    Dispara 27 requests simultâneas — uma por estado, pageSize=1 —
    apenas para obter o campo COUNT da API.

    Returns:
        Dict mapeando UF -> total de registros na API.
    """
    import httpx

    headers = {
        "Content-Type": "application/json",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        ),
        "Origin": CFM_BASE_URL,
        "Referer": CFM_PAGE_URL,
    }

    async def _fetch_count(client: httpx.AsyncClient, uf: str) -> tuple[str, int]:
        payload = _build_search_payload(
            captcha_token=captcha_token,
            uf=uf,
            page=1,
            page_size=1,
        )
        try:
            resp = await client.post(CFM_BUSCA_URL, json=payload, timeout=30)
            data = resp.json()
            if data.get("status") != "sucesso":
                print(f"⚠️  API erro para UF {uf}: {data}")
                return uf, -1
            dados = data.get("dados", [])
            total = int(dados[0].get("COUNT", 0)) if dados else 0
            return uf, total
        except Exception as e:
            print(f"⚠️  Erro ao contar UF {uf}: {e}")
            return uf, -1

    async with httpx.AsyncClient(headers=headers) as client:
        results = await asyncio.gather(*[_fetch_count(client, uf) for uf in UFS])
    return {uf: total for uf, total in results}


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
    client: httpx.AsyncClient,
    captcha_token: str,
    uf: str,
    pages: list[int],
    page_size: int,
    request_timeout: int = 120,
    tipo_inscricao: str = "",
    situacao: str = "",
) -> list[tuple[int, list[MedicoRaw], int]]:
    """Busca várias páginas em paralelo.

    Returns:
        Lista de tuplas (page_number, medicos, total_count).
    """
    tasks = [
        fetch_medicos_page(
            client=client,
            captcha_token=captcha_token,
            uf=uf,
            current_page=p,
            page_size=page_size,
            request_timeout=request_timeout,
            tipo_inscricao=tipo_inscricao,
            situacao=situacao,
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
                    client=client,
                    captcha_token=captcha_token,
                    uf=uf,
                    current_page=p,
                    page_size=page_size,
                    request_timeout=request_timeout,
                    tipo_inscricao=tipo_inscricao,
                    situacao=situacao,
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


async def crawl_state(
    client: httpx.AsyncClient,
    uf: str,
    execution_state_id: int,
    db_pool: asyncpg.Pool,
    page_size: int = 1000,
    delay: float = 0.8,
    fetch_fotos: bool = True,
    max_results: int = 0,
    request_timeout: int = 120,
    batch_size: int = 5,
    tipo_inscricao: str = "",
    situacao: str = "",
    start_page: int = 1,
) -> int:
    """Crawla todos os médicos de uma UF usando o plano de execução.

    Usa a tabela crawl_pages para saber exatamente quais páginas faltam.
    Ao descobrir total_pages, pré-cria todas como 'pending'.
    Retoma de onde parou buscando apenas páginas pending/failed.

    Returns:
        Total de médicos processados para esta UF nesta sessão.
    """
    print(f"\n{'=' * 60}")
    print(f"🏥 Iniciando crawl da UF: {uf}")
    print(f"⚡ Batch size: {batch_size} | Timeout: {request_timeout}s")
    print(f"{'=' * 60}")

    await start_execution_state(db_pool, execution_state_id)

    captcha_token = await _get_captcha_token(db_pool)

    total_medicos = 0
    total_count = 0
    total_pages = None
    retries = 0
    max_retries = 3
    consecutive_empty_batches = 0
    max_empty_batches = 2  # Pausa após N batches consecutivos com 0 médicos

    batch_times: list[float] = []
    total_start_time = time.time()

    # Carregar total_pages/total_records do banco (para retomada)
    state_row = await db_pool.fetchrow(
        "SELECT total_pages, total_records FROM crawl_execution_states WHERE id = $1",
        execution_state_id,
    )
    if state_row and state_row["total_pages"]:
        total_pages = state_row["total_pages"]
        total_count = state_row["total_records"] or 0

    # Busca a primeira página para descobrir total_count e total_pages
    pending = await get_pending_pages(db_pool, execution_state_id, limit=1)

    # Se não há páginas pendentes no banco, precisamos fazer a descoberta inicial
    first_page = pending[0] if pending else 1
    discovery_needed = not pending  # Se não há nenhuma página, precisa descobrir

    if not discovery_needed:
        # Verifica se já inicializou (tem páginas no banco)
        all_pending = await get_pending_pages(db_pool, execution_state_id)
        if all_pending:
            print(f"🔄 Retomando: {len(all_pending)} páginas pendentes")
        else:
            # Todas fetched — verificar completude
            if await check_state_complete(db_pool, execution_state_id):
                print(f"✅ UF {uf} já está completa.")
                return 0

    while True:
        if not await _validate_captcha_token(db_pool):
            print("❌ Token do captcha expirou! Execute: uv run cfm token")
            await fail_execution_state(db_pool, execution_state_id)
            raise RuntimeError("Token do captcha expirado durante o crawl.")

        # Busca páginas pendentes do banco
        pending_pages = await get_pending_pages(
            db_pool, execution_state_id, limit=batch_size
        )

        if not pending_pages:
            # Sem mais páginas pendentes
            if total_pages is None:
                # Primeira execução — busca página 1 para descobrir total
                pending_pages = [1]
            else:
                break

        batch_start = time.time()

        try:
            results = await _fetch_batch(
                client=client,
                captcha_token=captcha_token,
                uf=uf,
                pages=pending_pages,
                page_size=page_size,
                request_timeout=request_timeout,
                tipo_inscricao=tipo_inscricao,
                situacao=situacao,
            )
            batch_time = time.time() - batch_start
            batch_times.append(batch_time)
            retries = 0
        except Exception as e:
            retries += 1
            print(f"⚠️ Erro no batch (tentativa {retries}/{max_retries}): {e}")

            if retries >= max_retries:
                if await _validate_captcha_token(db_pool):
                    captcha_token = await _get_captcha_token(db_pool)
                    retries = 0
                    continue
                else:
                    print("❌ Token expirado e máximo de retries atingido.")
                    await fail_execution_state(db_pool, execution_state_id)
                    raise RuntimeError(
                        f"Falha após {max_retries} tentativas para UF {uf}. "
                        "Execute: uv run cfm token"
                    )

            await asyncio.sleep(delay)
            continue

        if not results:
            retries += 1
            if retries >= max_retries:
                if await _validate_captcha_token(db_pool):
                    captcha_token = await _get_captcha_token(db_pool)
                    retries = 0
                    continue
                else:
                    await fail_execution_state(db_pool, execution_state_id)
                    raise RuntimeError(
                        f"Todas as {len(pending_pages)} requests falharam para UF {uf}."
                    )
            await asyncio.sleep(delay)
            continue

        results.sort(key=lambda r: r[0])

        batch_medicos: list[dict] = []
        fetched_page_records: list[tuple[int, int]] = []  # (page_number, count)
        blocked_page_numbers: list[
            int
        ] = []  # páginas com 0 médicos (servidor bloqueou)
        failed_page_numbers: list[int] = set(pending_pages) - {r[0] for r in results}

        for page_num, raw_medicos, page_total_count in results:
            if page_total_count > 0:
                total_count = page_total_count
                new_total_pages = math.ceil(total_count / page_size)

                if total_pages is None or new_total_pages != total_pages:
                    total_pages = new_total_pages
                    # Pré-criar/atualizar páginas no banco
                    inserted = await initialize_pages(
                        db_pool,
                        execution_state_id,
                        total_pages,
                        start_page=start_page,
                    )
                    await update_execution_state(
                        db_pool,
                        execution_state_id,
                        total_pages=total_pages,
                        total_records=total_count,
                    )
                    if inserted > 0:
                        print(
                            f"📋 Total: {total_count} médicos em {total_pages} páginas "
                            f"({inserted} novas páginas criadas)"
                        )

            record_count = len(raw_medicos) if raw_medicos else 0

            # Detectar bloqueio do servidor: página retorna 0 médicos
            # mas sabemos que deveria ter (total_count já conhecido)
            if record_count == 0 and total_count > 0:
                blocked_page_numbers.append(page_num)
            else:
                fetched_page_records.append((page_num, record_count))

            if raw_medicos:
                for raw in raw_medicos:
                    raw_data = raw.model_dump(mode="json", by_alias=True)
                    doc = _format_doctor_for_db(raw, raw_data=raw_data, foto=None)
                    batch_medicos.append(doc)

        # Marcar páginas no banco
        if fetched_page_records:
            await mark_pages_fetched_batch(
                db_pool, execution_state_id, fetched_page_records
            )
        for fp in failed_page_numbers:
            await mark_page_failed(
                db_pool, execution_state_id, fp, "Sem resultado no batch"
            )
        for bp in blocked_page_numbers:
            await mark_page_failed(
                db_pool,
                execution_state_id,
                bp,
                "Servidor retornou 0 médicos (possível bloqueio)",
            )

        # Detectar bloqueio consecutivo do servidor
        if batch_medicos:
            consecutive_empty_batches = 0
        elif total_count > 0:
            # Batch inteiro retornou 0 médicos — possível bloqueio
            consecutive_empty_batches += 1
            if consecutive_empty_batches >= max_empty_batches:
                print(
                    f"\n🚫 Servidor bloqueou! {consecutive_empty_batches} batches "
                    f"consecutivos retornaram 0 médicos."
                )
                print(
                    f"   {len(blocked_page_numbers)} páginas marcadas como falha para retry."
                )
                print(f"   Pausando execução. Resolva novo captcha e retome:")
                print(f"   uv run cfm token && uv run cfm run <ID>")
                await fail_execution_state(db_pool, execution_state_id)
                raise RuntimeError(
                    "Servidor bloqueou a requisição (0 médicos retornados). "
                    "Resolva novo captcha: uv run cfm token"
                )

        # Calcular progresso
        remaining_pages = await get_pending_pages(db_pool, execution_state_id)
        remaining_count = len(remaining_pages)

        fetched_count = (total_pages or 0) - remaining_count if total_pages else 0
        percentage = round(fetched_count / total_pages * 100, 1) if total_pages else 0

        avg_batch_time = sum(batch_times) / len(batch_times) if batch_times else 0
        if remaining_count > 0:
            batches_remaining = math.ceil(remaining_count / batch_size)
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
        if fetched_count <= batch_size or fetched_count % 20 == 0:
            ttl = await _get_captcha_ttl(db_pool)
            if ttl > 0:
                ttl_info = f" | TTL: {ttl}s"
                if ttl < 120:
                    ttl_info += " ⚠️"

        page_range = (
            f"{min(pending_pages)}-{max(pending_pages)}" if pending_pages else "?"
        )
        successful = len(results)
        fail_count = len(failed_page_numbers)
        fail_info = f" ({fail_count} falhas)" if fail_count > 0 else ""

        print(
            f"📡 Páginas {page_range}"
            f"/{total_pages or '?'}: {len(batch_medicos)} médicos "
            f"({percentage}%) | "
            f"{batch_time:.2f}s ({successful}pg){fail_info}{eta_info}{ttl_info}"
        )

        if batch_medicos:
            process_start = time.time()
            await upsert_doctors_batch(db_pool, batch_medicos)
            process_time = time.time() - process_start

            if process_time > 1.0:
                print(f"   💾 Insert: {process_time:.2f}s")

            total_medicos += len(batch_medicos)

        if max_results > 0 and total_medicos >= max_results:
            print(f"🛑 Limite de teste atingido: {total_medicos}/{max_results}")
            break

        # Verificar se terminou
        if remaining_count == 0:
            break

        await asyncio.sleep(delay)

    # Verificar completude do estado
    is_complete = await check_state_complete(db_pool, execution_state_id)

    total_time = time.time() - total_start_time
    total_minutes = int(total_time / 60)
    total_seconds = int(total_time % 60)

    status_icon = "✅" if is_complete else "⏸️"
    print(f"\n{'=' * 60}")
    print(
        f"{status_icon} {total_medicos} médicos processados nesta sessão para UF {uf}."
    )
    print(f"⏱️  Tempo total: {total_minutes}m {total_seconds}s")
    if batch_times:
        avg_time = sum(batch_times) / len(batch_times)
        print(f"⚡ Tempo médio por batch ({batch_size}pg): {avg_time:.2f}s")
    print(f"{'=' * 60}")

    return total_medicos


async def run_execution(
    client: httpx.AsyncClient,
    execution_id: int,
    db_pool: asyncpg.Pool,
    page_size: int = 1000,
    batch_size: int = 5,
    delay: float = 0.8,
    fetch_fotos: bool = True,
    max_results: int = 0,
    request_timeout: int = 120,
    tipo_inscricao: str = "",
    situacao: str = "",
    start_page: int = 1,
) -> int:
    """Executa um plano de execução completo.

    Itera sobre os estados pendentes da execução, chamando crawl_state para cada.
    Trata interrupções e atualiza status.

    Returns:
        Total de médicos processados na sessão.
    """
    await start_execution(db_pool, execution_id)
    total_medicos = 0

    try:
        pending_states = await get_pending_states(db_pool, execution_id)

        if not pending_states:
            print("ℹ️  Todos os estados já foram processados.")
            await check_execution_complete(db_pool, execution_id)
            return 0

        print(f"📋 Estados pendentes: {', '.join(s['state'] for s in pending_states)}")

        for state_record in pending_states:
            uf = state_record["state"]
            state_id = state_record["id"]

            try:
                count = await crawl_state(
                    client=client,
                    uf=uf,
                    execution_state_id=state_id,
                    db_pool=db_pool,
                    page_size=page_size,
                    delay=delay,
                    fetch_fotos=fetch_fotos,
                    max_results=max_results,
                    request_timeout=request_timeout,
                    batch_size=batch_size,
                    tipo_inscricao=tipo_inscricao,
                    situacao=situacao,
                    start_page=start_page,
                )
                total_medicos += count

            except RuntimeError as e:
                if "captcha" in str(e).lower():
                    print(f"\n❌ Token do captcha expirou durante o crawl de {uf}.")
                    print("   Execute: uv run cfm token")
                    await pause_execution(db_pool, execution_id)
                    raise
                print(f"❌ Erro ao processar UF {uf}: {e}")
                continue
            except Exception as e:
                print(f"❌ Erro ao processar UF {uf}: {e}")
                await fail_execution_state(db_pool, state_id)
                continue

        # Verificar se completou tudo
        is_complete = await check_execution_complete(db_pool, execution_id)
        if is_complete:
            print(f"\n🎉 Execução #{execution_id} concluída com sucesso!")
        else:
            print(
                f"\n⏸️ Execução #{execution_id} pausada. Use 'cfm run {execution_id}' para continuar."
            )
            await pause_execution(db_pool, execution_id)

    except KeyboardInterrupt:
        print(f"\n\n🛑 Interrompido pelo usuário. Execução #{execution_id} pausada.")
        await pause_execution(db_pool, execution_id)
        raise

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
