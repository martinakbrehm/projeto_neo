# Documentação — Dashboard de Aproveitamento das Macros

Dashboard interativo para monitoramento de resultados das automações de consulta (macros). Lê dados diretamente do banco MySQL e exibe métricas de aproveitamento por data, empresa, fornecedor e arquivo de origem.

---

## Como executar

```powershell
# Na raiz do projeto (projeto_banco_neo/)
python -m dashboard_macros
```

Acesse em: http://127.0.0.1:8050

Para parar um processo travado na porta 8050 e reiniciar:

```powershell
Get-NetTCPConnection -LocalPort 8050 -ErrorAction SilentlyContinue |
  ForEach-Object { Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue }
Start-Sleep 2
python -m dashboard_macros
```

---

## Dependências

```powershell
pip install -r dashboard_macros/requirements.txt
```

Principais: `dash`, `pandas`, `pymysql`

---

## Estrutura de arquivos

```
dashboard_macros/
├── __main__.py           # ponto de entrada: python -m dashboard_macros
├── dashboard.py          # layout Dash + callbacks
├── data/
│   └── loader.py         # SQL queries + cache em memória
├── service/
│   └── orchestrator.py   # lógica de negócio, filtros, agregações
├── processing/
│   └── processing.py     # utilitários auxiliares (legado)
├── requirements.txt
└── DOCUMENTACAO_DASHBOARD.md  # este arquivo
```

---

## Tabelas do banco utilizadas

| Tabela                | Papel                                                      |
|-----------------------|------------------------------------------------------------|
| `tabela_macros`       | Resultado de cada consulta da macro (status, resposta_id)  |
| `respostas`           | Catálogo de mensagens de retorno (id, mensagem, status)    |
| `distribuidoras`      | Nome da empresa associada                                  |
| `cliente_origem`      | Fornecedor de origem do CPF (fornecedor2 / contatus)       |
| `staging_imports`     | Arquivo de importação (filename = `data/arquivo.csv`)      |
| `staging_import_rows` | Linhas do staging com CPF normalizado                      |
| `clientes`            | CPF do cliente                                             |

---

## Status dos registros em `tabela_macros`

| status        | Significado                                             |
|---------------|---------------------------------------------------------|
| `pendente`    | Aguardando processamento pela macro                     |
| `consolidado` | Resposta definitiva positiva                            |
| `reprocessar` | Resposta provisória — será consultado novamente         |
| `excluido`    | Resposta definitiva negativa                            |

**Invariante crítica:** `reprocessar` com `resposta_id = NULL` é estado inválido — deve ser tratado como `pendente`.

---

## Filtros disponíveis

- **Filtrar dias** — datas de processamento (`DATE(data_update)`)
- **Filtrar empresa** — distribuidora associada
- **Filtrar arquivo** — arquivo de staging de origem (`data/arquivo.csv`)
- **Fornecedor** — fornecedor2 ou contatus
- **Tipo de macro** — `macro` (tabela_macros) ou `api` (tabela_macro_api)

---

## Métricas exibidas

### Resumo por data
- Total, Ativos (`consolidado`) e Inativos (`excluido` + `reprocessar`) com percentuais
- Linha "Total" somada ao final da tabela

### Distribuição de respostas
Contagem por mensagem — apenas registros com `resposta_id IS NOT NULL` e `resposta_status != 'pendente'`.

### Registros por arquivo de origem (top 10)
Usa CTE `latest_arquivo` para associar o arquivo de staging mais recente ao CPF via `normalized_cpf`.

---

## Query SQL principal (loader.py)

```sql
WITH latest_arquivo AS (
    SELECT sir.normalized_cpf, si.filename,
           ROW_NUMBER() OVER (PARTITION BY sir.normalized_cpf ORDER BY sir.id DESC) AS rn
    FROM staging_import_rows sir
    JOIN staging_imports si ON si.id = sir.staging_id
    WHERE sir.validation_status = 'valid'
)
SELECT
    m.id,
    DATE(m.data_update)                          AS dia,
    m.status,
    m.resposta_id,
    r.mensagem,
    r.status                                     AS resposta_status,
    d.nome                                       AS empresa,
    COALESCE(co.fornecedor, 'fornecedor2')       AS fornecedor,
    CASE
        WHEN m.data_extracao IS NULL THEN 'Dados históricos'
        ELSE COALESCE(la.filename, 'operacional')
    END                                          AS arquivo_origem
FROM tabela_macros m
LEFT JOIN respostas      r  ON r.id  = m.resposta_id
LEFT JOIN distribuidoras d  ON d.id  = m.distribuidora_id
LEFT JOIN cliente_origem co ON co.cliente_id = m.cliente_id
LEFT JOIN clientes       cl ON cl.id = m.cliente_id
LEFT JOIN latest_arquivo la ON la.normalized_cpf = cl.cpf AND la.rn = 1
WHERE m.status != 'pendente'
  AND m.resposta_id IS NOT NULL
```

---

## Decisões de projeto

### Cache em memória
`loader.py` mantém um `_CACHE` dict por tipo de query. Para forçar recarga após migration: reiniciar o servidor. Não há TTL automático.

### STATUS_ATIVO / STATUS_INATIVO
```python
STATUS_ATIVO   = {"consolidado"}
STATUS_INATIVO = {"excluido", "reprocessar"}
```

### Coluna `arquivo_origem`
Calculada no SQL via CASE — `data_extracao IS NULL` indica importação histórica manual. O `filename` usa o formato `YYYY-MM-DD/arquivo.csv`.

### Sem coluna `campanha`
Removida de `cliente_origem` (migration `20260409_drop_campanha_cliente_origem`). O rastreamento de arquivo é feito exclusivamente via `staging_imports.filename`.

---

## Migrations aplicadas

| Migration                                       | O que fez                                                         |
|-------------------------------------------------|-------------------------------------------------------------------|
| `20260409_backfill_campanha_filename`           | Backfill com formato `data/arquivo.csv`                           |
| `20260409_drop_campanha_cliente_origem`         | Removeu coluna `campanha`, recriou 12 views                       |
| `20260409_truncar_filename_staging`             | Truncou `filename` de path Windows para `data/arquivo.csv`        |
| `20260409_corrigir_status_respostas`            | Corrigiu `respostas.status = 'excluir'` → `'excluido'`            |
| `20260409_corrigir_reprocessar_sem_resposta`    | 11.173 registros `reprocessar` sem `resposta_id` → `pendente`     |

---

## Problemas já resolvidos

| Problema                                        | Causa raiz                                        | Solução                                                    |
|-------------------------------------------------|---------------------------------------------------|------------------------------------------------------------|
| "(sem resposta)" na distribuição               | `reprocessar` com `resposta_id=NULL`              | Migration + `AND m.resposta_id IS NOT NULL` no SQL         |
| "Aguardando processamento" na distribuição      | `resposta_id=6` (pendente) vinculado a reprocessar | Migration + filtro `resposta_status != 'pendente'`        |
| Percentuais não somam 100%                      | `STATUS_INATIVO = {"excluir"}` (typo)             | Corrigido para `{"excluido", "reprocessar"}`               |
| Dashboard mostrando dados antigos               | Processo antigo rodando na porta 8050             | `Stop-Process` pelo PID antes de reiniciar                 |
| `arquivo_origem` mostrando caminho completo     | `filename` tinha path Windows absoluto            | Migration `20260409_truncar_filename_staging`              |

---

## Teste de sanidade

```python
import sys; sys.path.insert(0, '.')
from dashboard_macros.data import loader
from dashboard_macros.service.orchestrator import build_dashboard_data

loader.invalidar_cache()
resumo, mensagens, origens, _ = build_dashboard_data([], None, 'macro', None, None)

assert all(m['mensagem'] for m in mensagens), "Mensagens vazias encontradas!"
for r in resumo:
    if r['dia'] == 'Total': continue
    assert r['ativos'] + r['inativos'] == r['total'], f"Soma incorreta em {r['dia']}"
print("OK")
```
