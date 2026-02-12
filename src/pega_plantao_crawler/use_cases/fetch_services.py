"""Use case: buscar services/shifts do Pega Plantão."""

from ..config import Settings
from ..models.domain import Service
from ..services.api_client import (
    create_authenticated_client,
    fetch_all_services,
    save_services_to_json,
)
from ..services.auth import login_and_get_context


class FetchServicesUseCase:
    """Realiza login, busca services e salva em JSON."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def execute(self) -> list[Service]:
        """Executa o fluxo completo: login → fetch → save."""
        context, page = login_and_get_context(self.settings)

        try:
            client = create_authenticated_client(context, self.settings)
        finally:
            # Fecha o browser após extrair cookies
            browser = context.browser
            if browser:
                browser.close()

        try:
            services = fetch_all_services(client, self.settings)
            save_services_to_json(services, self.settings.output_dir)
            return services
        finally:
            client.close()
