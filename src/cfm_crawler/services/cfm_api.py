"""Cliente HTTP para a API do CFM.

Encapsula toda a comunicação com o portal do CFM usando httpx (sync).
"""

from __future__ import annotations

import httpx
from pydantic import TypeAdapter

from ..models.domain import MedicoFotoRaw, MedicoRaw

CFM_BASE_URL = "https://portal.cfm.org.br"
CFM_BUSCA_URL = f"{CFM_BASE_URL}/api_rest_php/api/v2/medicos/buscar_medicos"
CFM_FOTO_URL = f"{CFM_BASE_URL}/api_rest_php/api/v2/medicos/buscar_foto/"
CFM_MUNICIPIOS_URL = f"{CFM_BASE_URL}/api_rest_php/api/v2/medicos/listar_municipios"
CFM_PAGE_URL = f"{CFM_BASE_URL}/busca-medicos"

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


def _build_search_payload(
    captcha_token: str,
    uf: str,
    municipio: str = "",
    crm: str = "",
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
                "crmMedico": crm,
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


class CfmApiClient:
    """Cliente HTTP para comunicação com a API do CFM.

    Encapsula fetch de páginas, fotos, contagens e municípios.
    """

    def __init__(self, timeout: int = 120) -> None:
        self._client = httpx.Client(
            headers=_HTTP_HEADERS,
            timeout=httpx.Timeout(timeout, connect=15),
        )

    def close(self) -> None:
        """Fecha o client HTTP."""
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def fetch_page(
        self,
        captcha_token: str,
        uf: str,
        municipio: str = "",
        crm: str = "",
        page: int = 1,
        page_size: int = 100,
        request_timeout: int = 120,
        tipo_inscricao: str = "",
        situacao: str = "",
    ) -> tuple[list[MedicoRaw], int]:
        """Busca uma página de médicos via POST.

        Returns:
            Tupla com (lista de MedicoRaw, total de registros).
        """
        payload = _build_search_payload(
            captcha_token=captcha_token,
            uf=uf,
            municipio=municipio,
            crm=crm,
            page=page,
            page_size=page_size,
            tipo_inscricao=tipo_inscricao,
            situacao=situacao,
        )

        try:
            resp = self._client.post(
                CFM_BUSCA_URL, json=payload, timeout=request_timeout
            )
            data = resp.json()
        except httpx.TimeoutException:
            raise Exception(
                f"Timeout de {request_timeout}s ao buscar página {page} da UF {uf}"
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

    def fetch_doctor_detail(
        self,
        crm: str,
        uf: str,
        security_hash: str,
    ) -> MedicoFotoRaw | None:
        """Busca os detalhes/foto de um médico via POST."""
        try:
            payload = [{"securityHash": security_hash, "crm": crm, "uf": uf}]
            resp = self._client.post(CFM_FOTO_URL, json=payload, timeout=30)
            data = resp.json()

            if data.get("status") == "sucesso" and data.get("dados"):
                return MedicoFotoRaw(**data["dados"][0])
        except Exception as e:
            print(f"⚠️ Erro ao buscar foto CRM {crm}/{uf}: {e}")

        return None

    def fetch_state_counts(
        self,
        captcha_token: str,
        ufs: list[str],
    ) -> dict[str, int]:
        """Busca o total de médicos de cada UF concorrentemente via async.

        Returns:
            Dict mapeando UF -> total de registros na API.
        """
        import asyncio

        async def _fetch_all() -> dict[str, int]:
            async with httpx.AsyncClient(
                headers=_HTTP_HEADERS,
                timeout=httpx.Timeout(30, connect=15),
            ) as client:
                tasks = {
                    uf: client.post(
                        CFM_BUSCA_URL,
                        json=_build_search_payload(
                            captcha_token=captcha_token,
                            uf=uf,
                            page=1,
                            page_size=1,
                        ),
                    )
                    for uf in ufs
                }

                results: dict[str, int] = {}
                responses = await asyncio.gather(
                    *[tasks[uf] for uf in ufs], return_exceptions=True
                )

                for uf, resp in zip(ufs, responses):
                    if isinstance(resp, Exception):
                        print(f"⚠️ Erro ao contar UF {uf}: {resp}")
                        results[uf] = -1
                        continue
                    try:
                        data = resp.json()
                        if data.get("status") != "sucesso":
                            results[uf] = -1
                            continue
                        dados = data.get("dados", [])
                        results[uf] = int(dados[0].get("COUNT", 0)) if dados else 0
                    except Exception as e:
                        print(f"⚠️ Erro ao processar UF {uf}: {e}")
                        results[uf] = -1

                return results

        return asyncio.run(_fetch_all())

    def fetch_municipios(self, uf: str) -> list[dict]:
        """Busca a lista de municípios de uma UF.

        Returns:
            Lista de dicts com 'id' e 'name'.
        """
        url = f"{CFM_MUNICIPIOS_URL}/{uf}"
        try:
            resp = self._client.get(url, timeout=30)
            data = resp.json()
        except Exception as e:
            raise Exception(f"Erro ao buscar municípios de {uf}: {e}")

        dados = data.get("dados", [])
        return [
            {"id": m["ID_MUNICIPIO"], "name": m["DS_MUNICIPIO"]}
            for m in dados
            if "ID_MUNICIPIO" in m and "DS_MUNICIPIO" in m
        ]
