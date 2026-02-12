# qp-data-service

Serviço de coleta de dados médicos para o **QueroPlantão**. Composto por dois módulos independentes:

| Módulo | Comando | Descrição |
|---|---|---|
| `cfm_crawler` | `cfm-crawler` | Crawler de médicos do portal do CFM (Conselho Federal de Medicina) |
| `pega_plantao_crawler` | `pega-plantao` | Crawler de plantões do Pega Plantão |

## Requisitos

- Python ≥ 3.11
- PostgreSQL (para o `cfm_crawler`)
- [uv](https://docs.astral.sh/uv/) (recomendado) ou pip

## Instalação

```bash
# Clonar e instalar
uv sync
playwright install chromium
```

## Configuração

```bash
cp .env.example .env
# Editar .env com suas credenciais
```

## Uso

### CFM Crawler

```bash
# Resolver captcha e armazenar token
cfm-crawler token

# Buscar todos os médicos (selecione estados interativamente)
cfm-crawler doctors

# Buscar médicos de um estado específico
cfm-crawler doctors --state SP

# Consultar médico por CRM
cfm-crawler doctors --crm 123456 --uf SP
```

### Pega Plantão

```bash
# Buscar plantões disponíveis
pega-plantao
```

## Banco de dados

```bash
# Subir PostgreSQL via Docker
docker compose up -d

# Rodar migrations
alembic upgrade head
```

## Estrutura

```
src/
├── database/           # SQLAlchemy base + session
├── shared/             # Constantes, parsers, utils compartilhados
├── cfm_crawler/        # Módulo CFM
│   ├── models/         # Domain (Pydantic) + Entities (SQLAlchemy)
│   ├── repositories/   # Acesso a dados
│   ├── services/       # API client
│   ├── use_cases/      # Lógica de negócio
│   └── cli.py          # Interface CLI (Typer)
└── pega_plantao_crawler/ # Módulo Pega Plantão
    ├── models/         # Domain models
    ├── services/       # Auth + API client
    ├── use_cases/      # Lógica de negócio
    └── cli.py          # Entry point
```
