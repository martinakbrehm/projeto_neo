# 📊 Dashboard de Aproveitamento

Dashboard interativo para monitoramento de resultados de automações de extração de dados, com foco em análise de desempenho, identificação de erros e acompanhamento operacional.

A aplicação consolida informações a partir de arquivos Excel e permite visualizar métricas e padrões de falha de forma rápida e intuitiva.

---

## 🎯 Funcionalidades

* 📅 Resumo diário

  * Total de registros
  * Sucessos
  * Erros de requisição
  * Status (ativos / inativos)

* ⚠️ Distribuição de erros

  * Mensagens mais frequentes
  * Identificação de padrões de falha

* 📈 Gráfico de erros

  * Por dia
  * Por hora

* 🔎 Filtros dinâmicos

  * Por data
  * Por empresa (quando disponível)

---

## 🧠 Arquitetura

O projeto segue uma estrutura em camadas para melhor organização e manutenção:

relatorio_aproveitamento/

├── dashboard.py              # Interface web (Dash)
├── data/
│   └── loader.py            # Leitura dos arquivos Excel
├── processing/
│   └── processing.py        # Regras e métricas
├── service/
│   └── orchestrator.py      # Orquestra os dados para o dashboard
├── run_dashboard_launcher.py
├── run_dashboard.bat
└── requirements.txt

### 🔄 Fluxo de dados

Arquivos Excel → Data Layer → Processing → Service → Dashboard

Essa arquitetura permite evoluir o projeto futuramente (ex: uso de banco de dados) sem alterar a interface.

---

## 📂 Regras de entrada

* Apenas arquivos cujo nome começa com "saida" são considerados (case-insensitive)

Exemplos:
saida-2026-03.xlsx
saida-consultas.xls

* Extensões aceitas:

  * .xlsx
  * .xls

* Arquivos temporários (~$...) são ignorados automaticamente

---

## ▶️ Como executar

### 🟢 Opção recomendada (Windows)

1. Execute:
   run_dashboard.bat

ou:

python run_dashboard_launcher.py

2. Selecione a pasta com os arquivos "saida-*.xlsx"

3. O sistema abrirá automaticamente em:
   http://127.0.0.1:8050

---

### ⚙️ Linha de comando

Passando a pasta diretamente:

python relatorio_aproveitamento\dashboard.py --pasta-saidas "C:\caminho\para\Arquivos"

Ou usando variável de ambiente:

$env:DASHBOARD_PASTA_SAIDAS = "C:\caminho\para\Arquivos"
python relatorio_aproveitamento\dashboard.py

---

## 📦 Dependências

Instale com:

python -m pip install -r relatorio_aproveitamento\requirements.txt

(Recomenda-se usar ambiente virtual)

---

## ⚠️ Troubleshooting

* O navegador não abriu?
  → Acesse manualmente: http://127.0.0.1:8050

* Nenhum dado apareceu?
  → Verifique:

  * nome começando com "saida"
  * extensão .xlsx ou .xls
  * pasta correta selecionada

* Erro de biblioteca?
  → Reinstale as dependências

---

## 🚀 Evoluções futuras

* Persistência em banco de dados
* Pipeline ETL automatizado
* Empacotamento como executável (.exe)
* Deploy em cloud

---

## 💡 Contexto

Este projeto foi desenvolvido para acompanhar execuções de automações de coleta e consulta de dados, permitindo:

* monitorar desempenho operacional
* identificar falhas rapidamente
* analisar padrões de erro ao longo do tempo

Funciona como uma camada de observabilidade para processos baseados em arquivos Excel.
