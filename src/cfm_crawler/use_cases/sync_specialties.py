"""Use case: sincronizar especialidades da tabela doctors para specialties."""

from __future__ import annotations

from sqlalchemy.orm import Session

from ..config import CfmSettings
from ..repositories.specialty_repo import (
    fetch_specialty_pairs_from_doctors,
    truncate_and_insert_specialties,
)
from ...shared.text_utils import title_case_br


class SyncSpecialtiesUseCase:
    """Extrai especialidades únicas de doctors e popula a tabela specialties."""

    def __init__(self, session: Session, settings: CfmSettings) -> None:
        self.session = session
        self.settings = settings

    def execute(self) -> int:
        """Executa a sincronização.

        1. Busca pares (code, name) distintos do JSONB doctors.specialties
        2. Formata os nomes com title_case_br (Title Case, preposições minúsculas)
        3. Deduplica por code (mantém último nome encontrado)
        4. TRUNCATE + INSERT na tabela specialties

        Returns:
            Número de especialidades inseridas.
        """
        raw_pairs = fetch_specialty_pairs_from_doctors(self.session)

        if not raw_pairs:
            return 0

        # Deduplica por code, aplicando title_case_br ao name
        seen: dict[str, str] = {}
        for pair in raw_pairs:
            code = pair["code"].strip().upper()
            name = title_case_br(pair["name"]) or code
            seen[code] = name

        specialties = [
            {"code": code, "name": name}
            for code, name in sorted(seen.items(), key=lambda x: x[1])
        ]

        count = truncate_and_insert_specialties(self.session, specialties)
        return count
