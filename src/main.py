"""Entry point do crawler PegaPlantão."""

import asyncio
import sys

from auth.login import login_and_get_context
from config import get_settings
from crawler.services import fetch_all_services, save_services_to_json
from utils.http_client import create_authenticated_client


async def main() -> None:
    """Função principal do crawler."""
    print("=" * 60)
    print("🏥 Pega Plantão Crawler")
    print("=" * 60)

    # Carrega configurações
    try:
        settings = get_settings()
    except Exception as e:
        print(f"❌ Erro ao carregar configurações: {e}")
        print("💡 Certifique-se de criar o arquivo .env com PP_EMAIL e PP_PASSWORD")
        sys.exit(1)

    context = None
    client = None

    try:
        # 1. Login via Playwright
        context, page = await login_and_get_context(settings)

        # 2. Cria cliente httpx com cookies autenticados
        client = await create_authenticated_client(context, settings)

        # 3. Fecha o navegador (não é mais necessário)
        await context.browser.close()
        context = None

        # 4. Busca todos os services via API
        services = await fetch_all_services(client, settings)

        # 5. Salva os dados em JSON
        output_file = save_services_to_json(services, settings.output_dir)

        print("=" * 60)
        print("✅ Crawler finalizado com sucesso!")
        print(f"📁 Arquivo salvo: {output_file}")
        print(f"📊 Total de services: {len(services)}")
        print("=" * 60)

    except Exception as e:
        print(f"❌ Erro durante a execução: {e}")
        raise

    finally:
        # Cleanup
        if client:
            await client.aclose()
        if context:
            await context.browser.close()


def run() -> None:
    """Wrapper síncrono para executar via CLI."""
    asyncio.run(main())


if __name__ == "__main__":
    run()
