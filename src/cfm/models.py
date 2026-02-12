"""Modelos Pydantic e mapeamento de campos para médicos do CFM."""

from __future__ import annotations

import logging
import re

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


# ── Opções de filtro da API ────────────────────────────────────

TIPO_INSCRICAO_OPTIONS: dict[str, str] = {
    "": "Todas",
    "P": "Principal",
    "S": "Secundária",
    "V": "Provisória",
    "R": "Provisória Secundária",
    "E": "Estudante Médico Estrangeiro",
}

SITUACAO_OPTIONS: dict[str, str] = {
    "": "Todas",
    "A": "Ativo",
    "I": "Inativo",
}


# ── Modelos Raw (API) ─────────────────────────────────────────


class MedicoRaw(BaseModel):
    """Modelo raw da resposta da API buscar_medicos do CFM."""

    count: str = Field(alias="COUNT")
    sg_uf: str = Field(alias="SG_UF")
    nu_crm: str = Field(alias="NU_CRM")
    nu_crm_natural: str = Field(alias="NU_CRM_NATURAL")
    nm_medico: str = Field(alias="NM_MEDICO")
    cod_situacao: str = Field(alias="COD_SITUACAO")
    nm_social: str | None = Field(default=None, alias="NM_SOCIAL")
    dt_inscricao: str | None = Field(default=None, alias="DT_INSCRICAO")
    in_tipo_inscricao: str | None = Field(default=None, alias="IN_TIPO_INSCRICAO")
    tipo_inscricao: str | None = Field(default=None, alias="TIPO_INSCRICAO")
    situacao: str | None = Field(default=None, alias="SITUACAO")
    especialidade: str | None = Field(default=None, alias="ESPECIALIDADE")
    prim_inscricao_uf: str | None = Field(default=None, alias="PRIM_INSCRICAO_UF")
    periodo_i: str | None = Field(default=None, alias="PERIODO_I")
    periodo_f: str | None = Field(default=None, alias="PERIODO_F")
    obs_interdicao: str | None = Field(default=None, alias="OBS_INTERDICAO")
    nm_instituicao_graduacao: str | None = Field(
        default=None, alias="NM_INSTITUICAO_GRADUACAO"
    )
    dt_graduacao: str | None = Field(default=None, alias="DT_GRADUACAO")
    id_tipo_formacao: str | None = Field(default=None, alias="ID_TIPO_FORMACAO")
    nm_faculdade_estrangeira_graduacao: str | None = Field(
        default=None, alias="NM_FACULDADE_ESTRANGEIRA_GRADUACAO"
    )
    has_pos_graduacao: str | None = Field(default=None, alias="HAS_POS_GRADUACAO")
    rnum: str | None = Field(default=None, alias="RNUM")
    security_hash: str | None = Field(default=None, alias="SECURITYHASH")


class MedicoFotoRaw(BaseModel):
    """Modelo raw da resposta da API buscar_foto do CFM."""

    id_solicitante: str = Field(alias="ID_SOLICITANTE")
    nome: str = Field(alias="NOME")
    crm: str = Field(alias="CRM")
    uf_crm: str = Field(alias="UF_CRM")
    situacao: str = Field(alias="SITUACAO")
    endereco: str | None = Field(default=None, alias="ENDERECO")
    telefone: str | None = Field(default=None, alias="TELEFONE")
    inscricao: str | None = Field(default=None, alias="INSCRICAO")
    autorizacao_imagem: str | None = Field(default=None, alias="AUTORIZACAO_IMAGEM")
    autorizacao_endereco: str | None = Field(default=None, alias="AUTORIZACAO_ENDERECO")
    vp_destino: str | None = Field(default=None, alias="VP_DESTINO")
    vp_inicio: str | None = Field(default=None, alias="VP_INICIO")
    vp_fim: str | None = Field(default=None, alias="VP_FIM")
    hash: str | None = Field(default=None, alias="HASH")
    conflito_interesse: list = Field(default_factory=list, alias="CONFLITO_INTERESSE")


# ── Funções auxiliares ─────────────────────────────────────────


def clean_crm(raw_crm: str) -> int:
    """Extrai apenas dígitos do CRM e retorna como inteiro.

    Se o valor contiver algo além de dígitos, loga um warning.

    Args:
        raw_crm: Valor original de NU_CRM (ex: "EMFE-95660", "31840").

    Returns:
        CRM como inteiro (apenas dígitos).

    Raises:
        ValueError: Se não houver dígitos no valor.
    """
    digits = re.sub(r"\D", "", raw_crm)

    if not digits:
        raise ValueError(f"CRM sem dígitos: {raw_crm!r}")

    if digits != raw_crm:
        logger.warning("CRM com prefixo não-numérico: %s → %s", raw_crm, digits)

    return int(digits)


# ── Modelo Consolidado ─────────────────────────────────────────


class Medico(BaseModel):
    """Modelo final consolidado de médico do CFM."""

    crm: int
    raw_crm: str
    crm_natural: str | None = None
    uf: str
    nome: str
    nome_social: str | None = None
    situacao: str | None = None
    especialidade: str | None = None
    tipo_inscricao: str | None = None
    dt_inscricao: str | None = None
    instituicao_graduacao: str | None = None
    dt_graduacao: str | None = None
    is_foreign: bool = False
    security_hash: str | None = None
    interdicao_obs: str | None = None
    telefone: str | None = None
    endereco: str | None = None
    foto_url: str | None = None

    @classmethod
    def from_raw(cls, raw: MedicoRaw, foto: MedicoFotoRaw | None = None) -> "Medico":
        """Converte MedicoRaw + MedicoFotoRaw para Medico."""
        foto_url = None
        if foto and foto.autorizacao_imagem == "S" and foto.hash:
            foto_url = (
                f"https://portal.cfm.org.br/wp-content/themes/portalcfm/"
                f"assets/php/foto_medico.php?crm={foto.crm}&uf={foto.uf_crm}&hash={foto.hash}"
            )

        # Prioriza instituição nacional; fallback para estrangeira
        instituicao = (
            raw.nm_instituicao_graduacao or raw.nm_faculdade_estrangeira_graduacao
        )

        return cls(
            crm=clean_crm(raw.nu_crm),
            raw_crm=raw.nu_crm,
            crm_natural=raw.nu_crm_natural,
            uf=raw.sg_uf,
            nome=raw.nm_medico,
            nome_social=raw.nm_social,
            situacao=raw.situacao or raw.cod_situacao,
            especialidade=raw.especialidade,
            tipo_inscricao=raw.tipo_inscricao,
            dt_inscricao=raw.dt_inscricao,
            instituicao_graduacao=instituicao,
            dt_graduacao=raw.dt_graduacao,
            is_foreign=(raw.tipo_inscricao == "Estudante Medico Formado no Exterior"),
            security_hash=raw.security_hash,
            interdicao_obs=raw.obs_interdicao,
            telefone=foto.telefone if foto else None,
            endereco=(
                foto.endereco if foto and foto.autorizacao_endereco == "S" else None
            ),
            foto_url=foto_url,
        )


# ── Mapeamento de campos PT-BR → EN ───────────────────────────

FIELD_MAP: dict[str, str] = {
    "crm": "crm",
    "raw_crm": "raw_crm",
    "crm_natural": "crm_natural",
    "uf": "state",
    "nome": "name",
    "nome_social": "social_name",
    "situacao": "status",
    "especialidade": "specialties",
    "tipo_inscricao": "registration_type",
    "dt_inscricao": "registration_date",
    "instituicao_graduacao": "graduation_institution",
    "dt_graduacao": "graduation_date",
    "is_foreign": "is_foreign",
    "security_hash": "security_hash",
    "interdicao_obs": "interdicao_obs",
    "telefone": "phone",
    "endereco": "address",
    "foto_url": "photo_url",
}

FIELD_MAP_REVERSE: dict[str, str] = {v: k for k, v in FIELD_MAP.items()}


def translate_keys_to_en(data: dict) -> dict:
    """Traduz chaves de um dict de PT-BR para EN usando o FIELD_MAP.

    Chaves não mapeadas são mantidas como estão.
    """
    return {FIELD_MAP.get(k, k): v for k, v in data.items()}


def translate_keys_to_pt(data: dict) -> dict:
    """Traduz chaves de um dict de EN para PT-BR usando o FIELD_MAP_REVERSE.

    Chaves não mapeadas são mantidas como estão.
    """
    return {FIELD_MAP_REVERSE.get(k, k): v for k, v in data.items()}
