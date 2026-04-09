# Padrão: Dashboard Analítico sobre Pipeline de Automação com Estados

Este documento descreve um padrão genérico e replicável para construir dashboards de monitoramento de pipelines que processam registros com estados discretos. Foi derivado da implementação `dashboard_macros/` deste projeto.

---

## 1. Quando usar este padrão

Use este padrão quando você tiver:

- Uma tabela principal com registros que possuem **status de processamento** (pendente, sucesso, falha, reprocessar)
- Um **catálogo de respostas/mensagens** associado (FK da tabela principal para o catálogo)
- Necessidade de monitorar **distribuição de resultados ao longo do tempo**
- Volume médio/alto (10k–1M registros) que inviabiliza inspeção manual

---

## 2. Estrutura de dados necessária

### Tabela principal (obrigatória)

```sql
tabela_processamento (
    id              INT PK,
    entidade_id     INT FK,          -- o "quem" sendo processado
    status          VARCHAR,         -- estados discretos do pipeline
    resposta_id     INT FK NULLABLE, -- NULL = ainda não processado
    data_update     DATETIME,        -- quando foi processado
    data_entrada    DATETIME         -- NULL = importação histórica manual
)
```

### Catálogo de respostas (obrigatório)

```sql
respostas (
    id       INT PK,
    mensagem VARCHAR,    -- texto legível para o usuário
    status   VARCHAR     -- deve espelhar o status da tabela principal
)
```

### Regra de ouro
> `resposta_id = NULL` sempre significa "não processado ainda". Qualquer status diferente de "pendente" COM `resposta_id = NULL` é um estado inválido que corrompe métricas.

---

## 3. Semântica dos status — modelo de 4 estados

| Estado     | Semântica                                                 | Aparece no dashboard? |
|------------|-----------------------------------------------------------|-----------------------|
| `pendente` | Aguardando processamento                                  | Não (excluído via SQL)|
| ativo      | Resultado final positivo (ex: `consolidado`)              | Sim — coluna "Ativos" |
| provisório | Resultado temporário, será reprocessado (ex:`reprocessar`)| Sim — coluna "Inativos"|
| negativo   | Resultado final negativo (ex: `excluido`)                 | Sim — coluna "Inativos"|

**Agrupamentos no orchestrator:**
```python
STATUS_ATIVO   = {"consolidado"}               # adaptar para seu contexto
STATUS_INATIVO = {"excluido", "reprocessar"}   # adaptar para seu contexto
```

---

## 4. Arquitetura de código — separação de camadas

```
meu_dashboard/
├── __main__.py          # python -m meu_dashboard
├── dashboard.py         # layout Dash + callbacks (só UI, sem lógica de dado)
├── data/
│   └── loader.py        # SQL → DataFrame, cache em memória
├── service/
│   └── orchestrator.py  # filtros + agregações → listas de dicts para Dash
└── requirements.txt
```

### Responsabilidades de cada camada

| Módulo           | Faz                                            | Não faz                      |
|------------------|------------------------------------------------|------------------------------|
| `loader.py`      | Executa SQL, armazena em `_CACHE` dict         | Lógica de negócio, filtros   |
| `orchestrator.py`| Filtra DataFrame, agrega, retorna listas       | Queries SQL                  |
| `dashboard.py`   | Define layout, callbacks e estilos             | Queries, lógica de dado      |

### Cache em memória

```python
_CACHE: dict = {}  # chave = tipo da query ('macro', 'api', etc.)

def carregar_dados(tipo):
    if tipo in _CACHE:
        return _CACHE[tipo].copy()  # cópia para thread safety
    # executa SQL, salva no cache...
    _CACHE[tipo] = df
    return df.copy()

def invalidar_cache(tipo=None):
    if tipo:
        _CACHE.pop(tipo, None)
    else:
        _CACHE.clear()
```

**Trade-off:** dados novos no banco só aparecem após reiniciar o servidor (sem TTL). Para adicionar refresh sem restart, exponha um endpoint que chama `invalidar_cache()`.

---

## 5. Design visual — padrão de cores e estilos

### Paleta de cores

| Uso                       | Cor hex    | Onde usar                          |
|---------------------------|------------|------------------------------------|
| Azul principal            | `#2980b9`  | Header de tabelas, títulos         |
| Azul escuro (texto)       | `#2c3e50`  | Títulos, labels                    |
| Verde (origens)           | `#27ae60`  | Header da tabela de arquivos       |
| Cinza claro (fundo)       | `#f4f6f8`  | Background do bloco de conteúdo    |
| Azul pastel (seletor)     | `#eaf4fb`  | Background do seletor de tipo      |
| Verde pastel (seletor)    | `#f0f7f0`  | Background do seletor de fornecedor|
| Branco                    | `#fff`      | Cards individuais                  |
| Linha azul clara          | `#d6eaf8`  | Linha "Total" na tabela de resumo  |
| Linha zebra               | `#fafafa`  | Linhas ímpares das tabelas         |

### Fontes e tamanhos

```python
# Importadas via external_stylesheets
"https://fonts.googleapis.com/css?family=Roboto:400,700&display=swap"
"https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css"

TITLE_STYLE         = {"fontFamily": "Roboto", "color": "#2c3e50", "fontWeight": "700", "fontSize": "22px"}
SECTION_TITLE_STYLE = {"fontFamily": "Roboto", "color": "#2980b9", "fontWeight": "700", "fontSize": "18px"}
SUBTITLE_STYLE      = {"fontFamily": "Roboto", "color": "#2c3e50", "fontWeight": "700", "fontSize": "16px"}
```

### Sombra e borda de cards

```python
"boxShadow": "0 2px 8px #e0e0e0"   # cards principais
"boxShadow": "0 1px 6px rgba(44,62,80,0.06)"  # cards de filtro
"borderRadius": "8px"
```

### Largura máxima do layout

```python
style={"maxWidth": "1100px", "margin": "0 auto"}
```

---

## 6. Estrutura do layout — seções em ordem

```
[Cabeçalho com ícone + título]
[Seletor RadioItems: tipo de fonte (Macro / API)]    ← fundo azul pastel
[Seletor RadioItems: fornecedor (Todos / F2 / C)]   ← fundo verde pastel
[Info bar: contagem de registros visíveis]
[Filtros em flex-row: dias | empresa | arquivo]      ← 3 dropdowns multi-select
[dcc.Loading > bloco cinza com cards]:
    [Card: Resumo por data]
    [Card: Distribuição de respostas]
    [Card: Registros por arquivo de origem]
```

---

## 7. Controles de seleção — RadioItems vs Dropdown

### RadioItems (seleção única, sempre visível)
Usar para **dimensões de contexto** que alteram a fonte de dados ou o escopo global:
- Tipo de macro (`macro` vs `api`) → muda a query SQL usada
- Fornecedor (`todos` / `fornecedor2` / `contatus`) → filtra antes de popular os dropdowns

```python
dcc.RadioItems(
    id="selector-tipo-macro",
    options=[{"label": "  Macro", "value": "macro"}, ...],
    value="macro",
    inline=True,
)
```

### Dropdowns multi-select (seleção múltipla, opcional)
Usar para **filtros ad-hoc** que o usuário pode ou não aplicar:
- Dias, Empresa, Arquivo de origem

```python
dcc.Dropdown(
    id="filtro-arquivo-dropdown",
    multi=True,
    clearable=True,
    placeholder="Todos os arquivos",
)
```

**Regra:** `value=None` = sem filtro (todos). Opções populadas dinamicamente pelo callback `atualizar_opcoes_filtros`.

---

## 8. Callbacks — dois callbacks principais

### Callback 1: `atualizar_opcoes_filtros`
**Gatilho:** mudança no seletor de tipo ou fornecedor  
**Propósito:** repopular as opções dos 3 dropdowns e a info bar

```
Inputs:  selector-tipo-macro, selector-fornecedor
Outputs: resumo-dia-dropdown (options+value),
         filtro-empresa-dropdown (options+value),
         filtro-arquivo-dropdown (options+value),
         info-registros (children)
```

- Ao mudar o fornecedor, os dropdowns são resetados (value=None)
- A info bar mostra: `Registros: X | Dias: Y | Empresas: Z | Arquivos: W`

### Callback 2: `atualizar_dashboard`
**Gatilho:** qualquer filtro (dias, empresa, arquivo, tipo, fornecedor)  
**Propósito:** atualizar as 3 tabelas de dados

```
Inputs:  resumo-dia-dropdown, filtro-empresa-dropdown,
         filtro-arquivo-dropdown, selector-tipo-macro, selector-fornecedor
Outputs: tabela-resumo (data), tabela-mensagens (data), tabela-origens (data)
```

- Chama `orchestrator.build_dashboard_data(...)` com todos os filtros
- Em caso de exceção retorna listas vazias (dashboard não quebra)

### Endpoint de debug (Flask)
```python
@app.server.route("/_debug/data")
def debug_data():
    # retorna JSON com os dados brutos para inspeção via browser
```

---

## 9. Tabelas — especificações

### Tabela de resumo por data (`tabela-resumo`)

| Coluna       | Label     | Alinhamento | Tipo     |
|--------------|-----------|-------------|----------|
| `dia`        | Data      | center      | string   |
| `total`      | Total     | center      | int      |
| `ativos`     | Ativos    | center      | int      |
| `pct_ativos` | % Ativos  | center      | string   |
| `inativos`   | Inativos  | center      | int      |
| `pct_inativos`| % Inativos| center     | string   |

- Linha "Total" destacada em azul claro (`#d6eaf8`, bold)
- `page_size=20`

### Tabela de distribuição de respostas (`tabela-mensagens`)

| Coluna      | Label      | Alinhamento |
|-------------|------------|-------------|
| `mensagem`  | Resposta   | left        |
| `quantidade`| Quantidade | left        |

- Ordenada por quantidade decrescente
- `page_size=12`

### Tabela de arquivos de origem (`tabela-origens`)

| Coluna          | Label          | Alinhamento |
|-----------------|----------------|-------------|
| `arquivo_origem`| Arquivo/Origem | left        |
| `quantidade`    | Quantidade     | left        |

- Header verde (`#27ae60`)
- Linha "Dados históricos" em amarelo pastel (`#fef9e7`, itálico)
- Limitada a top 10 (no orchestrator, não no `page_size`)
- `page_size=10`

---

## 10. Query SQL — padrão com CTE de rastreamento de origem

```sql
WITH latest_lote AS (
    SELECT
        entidade_cpf,
        nome_arquivo,
        ROW_NUMBER() OVER (
            PARTITION BY entidade_cpf
            ORDER BY id DESC        -- mais recente primeiro
        ) AS rn
    FROM lote_importacao_rows
    WHERE status_validacao = 'valid'
)
SELECT
    m.*,
    r.mensagem,
    r.status AS resposta_status,
    CASE
        WHEN m.data_entrada IS NULL THEN 'Histórico'    -- importação manual
        ELSE COALESCE(l.nome_arquivo, 'desconhecido')   -- lote automático
    END AS origem
FROM tabela_processamento m
LEFT JOIN respostas r ON r.id = m.resposta_id
LEFT JOIN entidades  e ON e.id = m.entidade_id
LEFT JOIN latest_lote l ON l.entidade_cpf = e.cpf AND l.rn = 1
WHERE m.status != 'pendente'
  AND m.resposta_id IS NOT NULL    -- crítico: evita "(sem resposta)"
```

---

## 11. Métricas — como calcular percentuais corretos

```python
g        = dff.groupby(dff["dia"].astype(str))
total_s  = g.size()
ativo_s  = dff["status"].isin(STATUS_ATIVO).groupby(dff["dia"].astype(str)).sum()
inativo_s = dff["status"].isin(STATUS_INATIVO).groupby(dff["dia"].astype(str)).sum()

resumo = pd.DataFrame({
    "dia":      total_s.index,
    "total":    total_s.values,
    "ativos":   ativo_s.reindex(total_s.index, fill_value=0).values,
    "inativos": inativo_s.reindex(total_s.index, fill_value=0).values,
}).sort_values("dia")

resumo["pct_ativos"]   = (resumo["ativos"]   / resumo["total"] * 100).round(1).astype(str) + "%"
resumo["pct_inativos"] = (resumo["inativos"] / resumo["total"] * 100).round(1).astype(str) + "%"

# Linha total (só se houver mais de 1 dia)
if len(resumo) > 1:
    total_sum = int(resumo["total"].sum())
    # ... pd.concat com linha de soma
```

**Condição para percentuais somarem 100%:** `STATUS_ATIVO ∪ STATUS_INATIVO` deve cobrir exatamente os status presentes no DataFrame.

---

## 12. Armadilhas comuns e como evitar

| Armadilha                                              | Como evitar                                                    |
|--------------------------------------------------------|----------------------------------------------------------------|
| "(sem resposta)" na distribuição                       | `AND m.resposta_id IS NOT NULL` no SQL                         |
| Percentuais não somam 100%                             | Confirmar que STATUS_ATIVO ∪ STATUS_INATIVO cobre tudo         |
| Dashboard não atualiza após migration                  | Matar o PID explicitamente (`Stop-Process`), não só Ctrl+C     |
| status com typo (`'excluir'` vs `'excluido'`)          | `SELECT DISTINCT status FROM tabela` antes de codar            |
| Estado inválido: status ≠ pendente + resposta NULL     | Migration para marcar como `pendente` + invariante no INSERT   |
| Dois processos na mesma porta                          | `Get-NetTCPConnection -LocalPort 8050` para listar todos       |
| Opções de filtro não atualizam ao trocar fornecedor    | Callback 1 deve retornar `value=None` junto com as opções      |

---

## 13. Checklist para adaptar em novo contexto

- [ ] Mapear status do pipeline → equivalentes de pendente/ativo/provisório/negativo
- [ ] Confirmar invariante: status ≠ pendente com `resposta_id = NULL` não existe
- [ ] Adaptar `STATUS_ATIVO` e `STATUS_INATIVO` no orchestrator
- [ ] Adaptar `WHERE status != 'pendente'` para o nome real do status neutro
- [ ] Definir como calcular `origem` (arquivo, lote, canal) no SQL
- [ ] Verificar que o identificador de origem está em formato legível (não path absoluto)
- [ ] Ajustar paleta de cores se necessário (seção 5)
- [ ] Ajustar os RadioItems de segmentação para as dimensões do novo contexto
- [ ] Rodar teste de sanidade (seção 14)

---

## 14. Testes de sanidade e verificação de valores

Execute na raiz do projeto (`python -c "..." ` ou salve como script). Todos os testes comparam o que o dashboard exibe com o que está no banco diretamente.

### 14.1 Teste estrutural (sem DB)

Verifica que o pipeline de dados internamente é consistente.

```python
import sys; sys.path.insert(0, '.')
from dashboard_macros.data import loader
from dashboard_macros.service.orchestrator import build_dashboard_data, STATUS_ATIVO, STATUS_INATIVO

loader.invalidar_cache()
resumo, mensagens, origens, _ = build_dashboard_data([], None, 'macro', None, None)

# 1. Sem mensagens vazias na distribuicao
assert all(m['mensagem'] for m in mensagens), \
    "FALHA: mensagens vazias — checar AND m.resposta_id IS NOT NULL no SQL"

# 2. Percentuais consistentes por dia (ativos + inativos == total)
for r in resumo:
    if r['dia'] == 'Total': continue
    soma = r['ativos'] + r['inativos']
    assert soma == r['total'], \
        f"FALHA em {r['dia']}: ativos({r['ativos']}) + inativos({r['inativos']}) = {soma} != total({r['total']})"

# 3. Nenhum status inesperado no DataFrame
df = loader.carregar_dados('macro')
nao_cobertos = set(df['status'].unique()) - (STATUS_ATIVO | STATUS_INATIVO)
assert not nao_cobertos, f"FALHA: status sem mapeamento em STATUS_ATIVO/INATIVO: {nao_cobertos}"

# 4. Linha Total bate com a soma das linhas de dia
linha_total = next((r for r in resumo if r['dia'] == 'Total'), None)
if linha_total:
    soma_dias = sum(r['total'] for r in resumo if r['dia'] != 'Total')
    assert linha_total['total'] == soma_dias, \
        f"FALHA: linha Total({linha_total['total']}) != soma dos dias({soma_dias})"

print("OK — estrutura do dashboard consistente")
```

---

### 14.2 Teste de valores contra o banco (ground truth)

Verifica que os totais exibidos no dashboard batem exatamente com queries diretas no banco.

```python
import sys; sys.path.insert(0, '.')
import pymysql
from dashboard_macros.data import loader
from dashboard_macros.service.orchestrator import build_dashboard_data, STATUS_ATIVO, STATUS_INATIVO
from config import db_destino

loader.invalidar_cache()
resumo, mensagens, origens, _ = build_dashboard_data([], None, 'macro', None, None)
df = loader.carregar_dados('macro')

conn = pymysql.connect(**db_destino())
cur = conn.cursor()

# --- Teste A: total geral ---
cur.execute("""
    SELECT COUNT(*) FROM tabela_macros
    WHERE status != 'pendente' AND resposta_id IS NOT NULL
""")
total_banco = cur.fetchone()[0]
total_dash  = sum(r['total'] for r in resumo if r['dia'] != 'Total')
assert total_banco == total_dash, \
    f"FALHA total geral: banco={total_banco}, dashboard={total_dash}"
print(f"OK total geral: {total_banco:,}")

# --- Teste B: contagem por status ---
cur.execute("""
    SELECT status, COUNT(*) FROM tabela_macros
    WHERE status != 'pendente' AND resposta_id IS NOT NULL
    GROUP BY status
""")
contagem_banco = dict(cur.fetchall())

ativos_banco   = sum(v for k, v in contagem_banco.items() if k in STATUS_ATIVO)
inativos_banco = sum(v for k, v in contagem_banco.items() if k in STATUS_INATIVO)

linha_total = next((r for r in resumo if r['dia'] == 'Total'), resumo[-1] if resumo else None)
if linha_total:
    assert ativos_banco == linha_total['ativos'], \
        f"FALHA ativos: banco={ativos_banco}, dashboard={linha_total['ativos']}"
    assert inativos_banco == linha_total['inativos'], \
        f"FALHA inativos: banco={inativos_banco}, dashboard={linha_total['inativos']}"
print(f"OK status: ativos={ativos_banco:,}, inativos={inativos_banco:,}")

# --- Teste C: distribuicao de mensagens ---
cur.execute("""
    SELECT r.mensagem, COUNT(*) as qtd
    FROM tabela_macros m
    JOIN respostas r ON r.id = m.resposta_id
    WHERE m.status != 'pendente' AND m.resposta_id IS NOT NULL
    GROUP BY r.mensagem
    ORDER BY qtd DESC
""")
dist_banco = {row[0]: row[1] for row in cur.fetchall()}
dist_dash  = {m['mensagem']: m['quantidade'] for m in mensagens}

for msg, qtd_banco in dist_banco.items():
    qtd_dash = dist_dash.get(msg)
    assert qtd_dash == qtd_banco, \
        f"FALHA mensagem '{msg}': banco={qtd_banco}, dashboard={qtd_dash}"
print(f"OK distribuicao de mensagens: {len(dist_banco)} tipos")

# --- Teste D: totais por dia ---
cur.execute("""
    SELECT DATE(data_update) AS dia, COUNT(*) as qtd
    FROM tabela_macros
    WHERE status != 'pendente' AND resposta_id IS NOT NULL
    GROUP BY dia
    ORDER BY dia
""")
dias_banco = {str(row[0]): row[1] for row in cur.fetchall()}
dias_dash  = {r['dia']: r['total'] for r in resumo if r['dia'] != 'Total'}

for dia, qtd_banco in dias_banco.items():
    qtd_dash = dias_dash.get(dia)
    assert qtd_dash == qtd_banco, \
        f"FALHA dia {dia}: banco={qtd_banco}, dashboard={qtd_dash}"
print(f"OK totais por dia: {len(dias_banco)} dias")

# --- Teste E: invariante — sem reprocessar com resposta_id NULL ---
cur.execute("""
    SELECT COUNT(*) FROM tabela_macros
    WHERE status != 'pendente' AND resposta_id IS NULL
""")
invalidos = cur.fetchone()[0]
assert invalidos == 0, \
    f"FALHA invariante: {invalidos} registros com status != pendente e resposta_id NULL"
print(f"OK invariante resposta_id: 0 registros invalidos")

conn.close()
print("\nTODOS OS TESTES PASSARAM")
```

---

### 14.3 Teste de filtros (verifica que os filtros reduzem os dados corretamente)

```python
import sys; sys.path.insert(0, '.')
import pymysql
from dashboard_macros.data import loader
from dashboard_macros.service.orchestrator import build_dashboard_data
from config import db_destino

loader.invalidar_cache()
conn = pymysql.connect(**db_destino())
cur = conn.cursor()

# Pegar um dia real para testar
df = loader.carregar_dados('macro')
dia_teste = str(df['dia'].dropna().unique()[0])

# Dashboard com filtro de dia
resumo_filtrado, _, _, _ = build_dashboard_data([dia_teste], None, 'macro', None, None)
total_filtrado = sum(r['total'] for r in resumo_filtrado if r['dia'] != 'Total')

# Banco com mesmo filtro
cur.execute("""
    SELECT COUNT(*) FROM tabela_macros
    WHERE status != 'pendente'
      AND resposta_id IS NOT NULL
      AND DATE(data_update) = %s
""", (dia_teste,))
total_banco_dia = cur.fetchone()[0]

assert total_filtrado == total_banco_dia, \
    f"FALHA filtro dia {dia_teste}: dashboard={total_filtrado}, banco={total_banco_dia}"
print(f"OK filtro por dia ({dia_teste}): {total_filtrado:,} registros")

# Pegar uma empresa real para testar
empresa_teste = str(df['empresa'].dropna().unique()[0])
resumo_emp, _, _, _ = build_dashboard_data([], [empresa_teste], 'macro', None, None)
total_emp = sum(r['total'] for r in resumo_emp if r['dia'] != 'Total')

cur.execute("""
    SELECT COUNT(*) FROM tabela_macros m
    JOIN distribuidoras d ON d.id = m.distribuidora_id
    WHERE m.status != 'pendente'
      AND m.resposta_id IS NOT NULL
      AND d.nome = %s
""", (empresa_teste,))
total_banco_emp = cur.fetchone()[0]

assert total_emp == total_banco_emp, \
    f"FALHA filtro empresa '{empresa_teste}': dashboard={total_emp}, banco={total_banco_emp}"
print(f"OK filtro por empresa ({empresa_teste}): {total_emp:,} registros")

conn.close()
print("\nTODOS OS TESTES DE FILTRO PASSARAM")
```

---

## 15. Prompt para LLM replicar este padrão

Forneça à LLM:

1. O schema das tabelas (seção 2 adaptada ao seu contexto)
2. Os status do pipeline e o que cada um significa
3. Os arquivos `loader.py` e `orchestrator.py` deste projeto como referência
4. Peça: *"Adapte o padrão de dashboard_macro.md para o meu contexto, respeitando as mesmas invariantes, arquitetura de camadas, design visual e lógica de filtros."*
