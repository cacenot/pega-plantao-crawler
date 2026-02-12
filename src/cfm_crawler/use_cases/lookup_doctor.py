"""Use case: Buscar médico específico por CRM/UF.

Busca um médico na API do CFM, persiste no banco e exibe na tela.
"""

from __future__ import annotations

from sqlalchemy.orm import Session

from ..config import CfmSettings
from ..models.domain import Medico, MedicoRaw, translate_keys_to_en
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


class LookupDoctorUseCase:
    """Busca um médico por CRM/UF na API do CFM e persiste no banco."""

    def __init__(
        self,
        session: Session,
        settings: CfmSettings,
        api_client: CfmApiClient,
    ) -> None:
        self._session = session
        self._settings = settings
        self._api = api_client

    def execute(self, crm: str, uf: str) -> dict | None:
        """Busca médico por CRM/UF, salva no banco e retorna dict formatado.

        Args:
            crm: Número do CRM.
            uf: UF do CRM.

        Returns:
            Dict com dados do médico formatado para o banco, ou None.
        """
        # Validar captcha
        if not captcha_repo.is_valid(self._session):
            print("\n❌ Token de captcha não encontrado ou expirado!")
            print("   Execute primeiro: uv run cfm-crawler token")
            return None

        ttl = captcha_repo.get_ttl(self._session)
        captcha_token = captcha_repo.get_token(self._session)
        print(f"✅ Token de captcha encontrado (TTL: {ttl}s)")

        # Buscar na API
        from pydantic import TypeAdapter

        medicos, _ = self._api.fetch_page(
            captcha_token=captcha_token,
            uf=uf,
            crm=crm,
            page=1,
            page_size=10,
            request_timeout=self._settings.request_timeout,
        )

        if not medicos:
            return None

        raw = medicos[0]
        raw_data = raw.model_dump(mode="json", by_alias=True)

        # Buscar foto se disponível
        foto = None
        if self._settings.fetch_fotos and raw.security_hash:
            foto = self._api.fetch_doctor_detail(
                raw.nu_crm, raw.sg_uf, raw.security_hash
            )

        # Formatar e persistir
        doc = _format_doctor_for_db(raw, raw_data, foto)
        doctor_repo.upsert_doctor(self._session, doc)
        self._session.commit()

        return doc
