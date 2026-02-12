"""Configuração do crawler Pega Plantão usando Pydantic Settings."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configurações do crawler carregadas do .env."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="PP_",
        case_sensitive=False,
        extra="ignore",
    )

    # Credenciais
    email: str
    password: str

    # URLs
    base_url: str = "https://www.pegaplantao.com.br"
    login_url: str = "https://www.pegaplantao.com.br/Login"
    escala_mensal_url: str = "https://www.pegaplantao.com.br/EscalaMensal"
    sectors_api_path: str = "/api/v1/groups/sectorsformattedandgroupped"

    # Playwright
    headless: bool = True
    timeout: int = 30000  # milliseconds

    # Output
    output_dir: str = "data"

    @property
    def sectors_api_pattern(self) -> str:
        """Regex pattern para interceptar a API de setores."""
        return rf"{self.sectors_api_path}\?v=.*"


def get_settings() -> Settings:
    """Retorna instância das configurações."""
    return Settings()
