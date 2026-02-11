"""Parser de especialidades médicas do CFM."""

from __future__ import annotations

import re

from .text_utils import title_case_br


def parse_specialties(raw: str | None) -> list[dict[str, str | None]]:
    """Parseia a string de especialidades do CFM em lista de dicts.

    Formato de entrada:
        "&CARDIOLOGIA - RQE Nº: 12345&PEDIATRIA - RQE Nº: 67890"
        "&CIRURGIA GERAL - RQE Nº: 123 (Cirurgia do Trauma)"

    Retorna:
        [{"name": "Cardiologia", "rqe": "12345"}, ...]
    """
    if not raw or not raw.strip():
        return []

    specialties: list[dict[str, str | None]] = []

    # Divide por '&' e remove vazios
    parts = [p.strip() for p in raw.split("&") if p.strip()]

    for part in parts:
        # Extrai RQE se presente
        rqe_match = re.search(r"RQE\s*N[ºo°]?\s*:?\s*(\d+)", part, re.IGNORECASE)
        rqe = rqe_match.group(1) if rqe_match else None

        # Remove o RQE e parênteses (área de atuação) do nome
        name = re.sub(
            r"\s*-?\s*RQE\s*N[ºo°]?\s*:?\s*\d+", "", part, flags=re.IGNORECASE
        )
        name = re.sub(r"\s*\(.*?\)\s*", "", name).strip(" -")
        # Remove parênteses soltos
        name = name.strip("() ")

        if name:
            specialties.append(
                {
                    "name": title_case_br(name),
                    "specialty_code": name.strip().upper(),
                    "rqe": rqe,
                }
            )

    return specialties


def extract_unique_specialty_names(raw_values: list[str | None]) -> set[str]:
    """Extrai nomes únicos de especialidades a partir de uma lista de valores raw.

    Args:
        raw_values: Lista de strings de especialidades no formato CFM.

    Returns:
        Conjunto de nomes únicos de especialidades formatados em Title Case.
    """
    names: set[str] = set()
    for raw in raw_values:
        for spec in parse_specialties(raw):
            if spec["name"]:
                names.add(spec["name"])
    return names
