"""Configuração do crawler CFM."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class CfmSettings(BaseSettings):
    """Configurações do crawler CFM carregadas do .env."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="CFM_",
        case_sensitive=False,
        extra="ignore",
    )

    # Playwright
    headless: bool = False
    timeout: int = 40000  # milliseconds

    # Paginação
    page_size: int = 1000

    # Rate limiting
    delay: float = 0.8
    foto_delay: float = 0.3

    # Request
    request_timeout: int = 120
    batch_size: int = 5

    # Buscar fotos/detalhes dos médicos
    fetch_fotos: bool = True

    # Limite de resultados (0 = sem limite, útil para testes)
    max_results: int = 0

    # Output
    output_dir: str = "data"

    # PostgreSQL
    database_url: str = "postgresql://postgres:postgres@localhost:5432/qp_crawler"

    # Captcha
    captcha_ttl: int = 1800


def get_cfm_settings() -> CfmSettings:
    """Retorna instância das configurações do CFM."""
    return CfmSettings()
