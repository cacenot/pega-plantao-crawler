"""Use case: contar médicos por estado (API vs Banco)."""

from __future__ import annotations

from typing import TypedDict

from sqlalchemy.orm import Session

from ..config import CfmSettings
from ..repositories.state_count_repo import get_db_counts_by_state
from ..services.cfm_api import CfmApiClient
from ...shared.constants import UFS_MAP


class StateCountRow(TypedDict):
    """Resultado de contagem por estado."""

    uf: str
    estado_name: str
    api_count: int
    db_count: int
    diff: int
    percentage: float | None


class CountResult(TypedDict):
    """Resultado completo da contagem."""

    rows: list[StateCountRow]
    api_total: int
    db_total: int
    diff_total: int
    pct_total: float | None


class CountDoctorsUseCase:
    """Realiza contagem de médicos: API vs banco de dados."""

    def __init__(
        self,
        session: Session,
        settings: CfmSettings,
        api: CfmApiClient,
    ) -> None:
        self.session = session
        self.settings = settings
        self.api = api

    def execute(
        self,
        captcha_token: str,
        target_ufs: list[str],
    ) -> CountResult:
        """Busca contagens da API e do banco para os estados especificados.

        Args:
            captcha_token: Token válido do reCAPTCHA.
            target_ufs: Lista de UFs para contar.

        Returns:
            Resultado estruturado com linhas por estado e totais.
        """
        # Buscar contagens
        db_counts = get_db_counts_by_state(self.session)
        api_counts = self.api.fetch_state_counts(
            captcha_token=captcha_token,
            ufs=target_ufs,
        )

        # Processar cada estado
        rows: list[StateCountRow] = []
        api_total = 0
        db_total = 0
        diff_total = 0

        for uf in target_ufs:
            api_count = api_counts.get(uf, 0)
            db_count = db_counts.get(uf, 0)
            diff = api_count - db_count if api_count >= 0 else 0

            estado_name = UFS_MAP.get(uf, uf)

            if api_count < 0:
                percentage = None
            elif api_count == 0:
                percentage = None
            else:
                percentage = (db_count / api_count) * 100
                api_total += api_count

            db_total += db_count
            diff_total += diff

            rows.append(
                StateCountRow(
                    uf=uf,
                    estado_name=estado_name,
                    api_count=api_count,
                    db_count=db_count,
                    diff=diff,
                    percentage=percentage,
                )
            )

        # Calcular percentual total
        pct_total = (db_total / api_total * 100) if api_total > 0 else None

        return CountResult(
            rows=rows,
            api_total=api_total,
            db_total=db_total,
            diff_total=diff_total,
            pct_total=pct_total,
        )
