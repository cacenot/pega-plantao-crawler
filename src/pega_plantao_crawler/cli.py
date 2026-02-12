"""Entry point do crawler PegaPlantão."""

import sys

from .config import get_settings
from .use_cases.fetch_services import FetchServicesUseCase


def run() -> None:
    """Função principal do crawler."""
    print("=" * 60)
    print("🏥 Pega Plantão Crawler")
    print("=" * 60)

    try:
        settings = get_settings()
    except Exception as e:
        print(f"❌ Erro ao carregar configurações: {e}")
        print("💡 Certifique-se de criar o arquivo .env com PP_EMAIL e PP_PASSWORD")
        sys.exit(1)

    try:
        use_case = FetchServicesUseCase(settings)
        services = use_case.execute()

        print("=" * 60)
        print("✅ Crawler finalizado com sucesso!")
        print(f"📊 Total de services: {len(services)}")
        print("=" * 60)

    except Exception as e:
        print(f"❌ Erro durante a execução: {e}")
        raise


if __name__ == "__main__":
    run()
