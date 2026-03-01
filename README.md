# Price Extractor - Client

MVP para transformar o pipeline atual (notebook) em um sistema multi-cliente com:

- execucao automatizada por cliente
- painel local de acompanhamento e reprocessamento
- telas web para review, mappings e catalogo globais
- SQLite como fonte principal de cadastro (sem dependencia operacional de Excel)
- scheduler diario (07:00)

## Conceito de escala (cadastro global)

- `Catalog`, `Mappings` e `Review` sao compartilhados por **todos os clientes**.
- Se um fingerprint for aprovado/mapeado uma vez, ele vale para todo o portfolio.
- Cada cliente continua com `Compras` e `Alertas` proprios (input/output separados).
- O "grupo economico" fica como metadado de agrupamento de lojas/visao.
- Arquivo compartilhado global:
  - `data/shared_state/global/pipeline_editor_shared.xlsx`

## 1) Requisitos

- Python 3.11+
- notebook legado `nfe_sku_pipeline_v10.ipynb` acessivel no disco

## 2) Instalacao

No diretorio `price-extractor-client`:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -U pip
pip install -e .
```

## 3) Configuracao basica

Variaveis de ambiente suportadas:

- `PEC_NOTEBOOK_PATH`: caminho do notebook legado
- `PEC_CLIENTS_DIR`: raiz dos clientes (default: `./clientes`)
- `PEC_DB_PATH`: caminho do SQLite (default: `./data/app.db`)
- `PEC_TIMEZONE`: timezone do scheduler (default: `America/Sao_Paulo`)
- `PEC_SCHEDULE_HOUR`: hora do scheduler (default: `7`)
- `PEC_SCHEDULE_MINUTE`: minuto do scheduler (default: `0`)

Exemplo PowerShell:

```powershell
$env:PEC_NOTEBOOK_PATH="C:\Users\USER\Documents\Price Extractor\nfe_sku_pipeline_v10.ipynb"
```

## 4) Cadastro de cliente

Via CLI:

```bash
price-client init-db
price-client add-client --name "Cliente A" --slug cliente-a --economic-group ferreirinha --store 49223665000153=LOJA_LEBLON --store 54293878000191=LOJA_JB
```

Estrutura criada:

```text
clientes/
  cliente-a/
    input/
    outputs/
```

O pipeline espera XMLs em:

```text
clientes/cliente-a/input/<CNPJ>/xml/*.xml
```

Alternativa para operacao de cliente unico (separado do legado): defina o `root_dir` do cliente como `price-extractor-client`.
Nesse caso, o pipeline usa:

```text
price-extractor-client/input/<CNPJ>/xml/*.xml
price-extractor-client/outputs/
```

## 5) Rodar pipeline

Todos os clientes ativos:

```bash
price-client run-all
```

Um cliente:

```bash
price-client run-client --slug cliente-a
```

Saidas por cliente:

- `outputs/pipeline_editor.xlsx`
- `outputs/whatsapp_alertas/*.txt`
- `outputs/whatsapp_alertas/html_reports/*.html`

## 6) Painel web local

```bash
price-client serve --host 127.0.0.1 --port 8000 --reload
```

Esse comando agora exige FastAPI/Uvicorn por padrao (mesmo comportamento esperado em cloud).

Se voce quiser usar fallback localmente em caso de falta de dependencias, rode:

```bash
price-client serve --host 127.0.0.1 --port 8000 --reload --allow-fallback
```

Abrir:

- `http://127.0.0.1:8000`

Funcionalidades:

- lista de clientes e status do ultimo run
- botao de reprocessamento por cliente
- status ativo/inativo
- review global (aprovar/ignorar fingerprint pendente)
- tela de mappings globais (buscar, criar e editar)
- tela de catalogo global (buscar, criar e editar)
- importacao/exportacao opcional com o Excel compartilhado (bridge com legado)

## 6.1) Painel V3 (layout React do prototipo)

Backend FastAPI (API):

```bash
.\.venv\Scripts\python.exe -m app.cli serve --host 127.0.0.1 --port 8001 --reload
```

Frontend React/Vite (na pasta `frontend`):

```bash
cd frontend
npm install
npm run dev
```

Abrir:

- `http://127.0.0.1:5173`

Endpoints usados pelo frontend:

- `GET /api/review?status=ALL|PENDING|APPROVED|IGNORED&loja=&search=`
- `POST /api/review/{id}/approve` (JSON: `sku_id`, `base_measure_override`, `base_qty_per_purchase_unit_override`)
- `POST /api/review/{id}/ignore`
- `GET /api/mappings?status=ALL|ACTIVE|IGNORE&search=`
- `PUT /api/mappings/{id}` (JSON: `sku_id`, `base_measure`, `qty`, `status`)
- `DELETE /api/mappings/{id}`
- `GET /api/catalog?search=&category=`
- `GET /api/catalog/{sku_id}/fingerprints`

## 7) Scheduler diario

Rodar continuamente local:

```bash
price-client schedule
```

Rodar uma vez e depois agendar:

```bash
price-client schedule --run-now
```

## 8) GitHub Actions (opcional)

Workflow pronto em `.github/workflows/daily-run.yml`.

Para usar no GitHub Actions, configure no repositorio:

- secret `PEC_NOTEBOOK_PATH` (se o notebook nao estiver no root do repo)
- secrets/variaveis necessarias do seu ambiente

## Observacoes importantes

- A logica principal de negocio vem do notebook legado, carregado dinamicamente.
- Nesta etapa, a entrega gera os arquivos de WhatsApp, mas nao envia por API.
- A consolidacao de aprovacoes do review acontece no proximo processamento do cliente.

