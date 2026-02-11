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
    headless: bool = False  # False para resolver captcha manualmente
    timeout: int = 40000  # milliseconds

    # Paginação
    page_size: int = 10

    # Rate limiting
    delay: float = 0.8  # Delay entre páginas em segundos
    foto_delay: float = 0.3  # Delay entre requests de foto

    # Request
    request_timeout: int = 120  # Timeout por request em segundos (2 min)
    batch_size: int = 50  # Quantidade de requests paralelas por batch

    # Buscar fotos/detalhes dos médicos
    fetch_fotos: bool = False

    # Limite de resultados (0 = sem limite, útil para testes)
    max_results: int = 0

    # Output
    output_dir: str = "data"

    # PostgreSQL
    database_url: str = "postgresql://postgres:postgres@localhost:5432/qp_crawler"

    # Redis
    redis_url: str = "redis://localhost:6379/0"
    captcha_ttl: int = 1800  # TTL do token do captcha em segundos


def get_cfm_settings() -> CfmSettings:
    """Retorna instância das configurações do CFM."""
    return CfmSettings()
