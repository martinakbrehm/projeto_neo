# Migração 001 — `cliente_origem`, correção da unique key de `cliente_uc` e views por fornecedor

**Data:** 2026-04-06  
**Contexto:** Introdução do fornecedor contatus ao pipeline, com dados históricos que podem conter ou não UC.

---

## Problema que motivou esta migração

O pipeline foi construído inicialmente para o **fornecedor2**, que sempre fornece CPF + UC + distribuidora. O **contatus** apresenta uma particularidade: parte dos registros históricos possui UC (capturada por uma macro anterior), e parte não possui. Isso cria três necessidades simultâneas:

1. **Roteamento diferenciado**: registros com UC devem entrar na fila da `tabela_macros` (macro roda em segundo plano por CPF+UC); registros sem UC devem entrar na `tabela_macro_api`.
2. **Identificação de origem**: é necessário saber que um cliente veio do contatus para gerar views e extrações separadas, sem poluir as tabelas de processamento.
3. **Deduplicação inteligente**: se o mesmo CPF+UC+distribuidora já existir de qualquer fonte, não duplicar. Se o CPF existe sem UC no contatus e já tem UC de outra fonte, promover direto para `tabela_macros`.

---

## O que o `migration.py` aplica no banco

### Passo 1 — Correção da UNIQUE KEY de `cliente_uc`

**Antes:**
```sql
UNIQUE KEY ux_cliente_uc (cliente_id, uc)
```

**Depois:**
```sql
UNIQUE KEY ux_cliente_uc (cliente_id, uc, distribuidora_id)
```

**Por quê:** O contatus pode trazer a mesma UC de uma distribuidora diferente da que o fornecedor2 já cadastrou. Sem `distribuidora_id` na chave, o `INSERT IGNORE` silenciaria esse dado. Com a chave incluindo `distribuidora_id`, a tripla (cliente, uc, distribuidora) fica univocamente identificada.

**Impacto em dados existentes:** Nenhum — todos os dados atuais são do fornecedor2, que sempre informa distribuidora. A mudança é retrocompatível.

---

### Passo 2 — Atualização de `proc_macro_api_link_uc`

A procedure de vínculo UC era:
```sql
SELECT id INTO v_cliente_uc_id
FROM cliente_uc
WHERE cliente_id = v_cliente_id AND uc = v_uc
LIMIT 1;
```

Após a mudança da unique key, a mesma (cliente_id, uc) pode existir para duas distribuidoras distintas. O `LIMIT 1` poderia retornar a distribuidora errada. A procedure foi atualizada para incluir:
```sql
AND distribuidora_id = p_distribuidora_id
```

---

### Passo 3 — Criação da tabela `cliente_origem`

```sql
CREATE TABLE cliente_origem (
  id           INT AUTO_INCREMENT,
  cliente_id   INT NOT NULL,
  fornecedor   VARCHAR(50) NOT NULL,   -- 'fornecedor2', 'contatus', ...
  campanha     VARCHAR(100),           -- 'operacional', 'periodo_historico', ...
  data_ref     DATE,                   -- data de referência do dado na origem
  data_import  DATETIME DEFAULT NOW(),
  PRIMARY KEY (id),
  UNIQUE KEY ux_cliente_fornecedor (cliente_id, fornecedor),
  FOREIGN KEY (cliente_id) REFERENCES clientes (id) ON DELETE CASCADE
)
```

**Por que não colocar `fornecedor` em `tabela_macros` ou `tabela_macro_api`?**  
Essas tabelas são de **estado de processamento** — acumulam múltiplos registros por cliente ao longo do tempo. Colocar `fornecedor` lá mistura proveniência com estado e se perde no histórico. `clientes` e `cliente_uc` também são inadequadas: um CPF é uma identidade global e pode chegar de fontes múltiplas. A `cliente_origem` é a tabela correta: registra *de onde vieram* os clientes, separado de quem eles são e como estão sendo processados.

---

### Passo 4 — Backfill fornecedor2

Todos os clientes existentes no banco no momento da migração vieram do fornecedor2. O script insere:

```sql
INSERT IGNORE INTO cliente_origem (cliente_id, fornecedor, campanha, data_import)
SELECT id, 'fornecedor2', 'operacional', data_criacao FROM clientes;
```

---

### Passo 5 — Views por fornecedor (12 views)

| View | Descrição |
|---|---|
| `view_fornecedor2_macro` | Todos os registros de `tabela_macros` do fornecedor2 |
| `view_contatus_macro` | Todos os registros de `tabela_macros` do contatus |
| `view_fornecedor2_macro_automacao` | Fila ativa (pendente/reprocessar) de `tabela_macros` — fornecedor2 |
| `view_contatus_macro_automacao` | Fila ativa (pendente/reprocessar) de `tabela_macros` — contatus |
| `view_fornecedor2_macro_consolidados` | Consolidados de `tabela_macros` — fornecedor2 (para extração) |
| `view_contatus_macro_consolidados` | Consolidados de `tabela_macros` — contatus (para extração) |
| `view_fornecedor2_api` | Todos os registros de `tabela_macro_api` do fornecedor2 |
| `view_contatus_api` | Todos os registros de `tabela_macro_api` do contatus |
| `view_fornecedor2_api_automacao` | Fila ativa de `tabela_macro_api` — fornecedor2 |
| `view_contatus_api_automacao` | Fila ativa de `tabela_macro_api` — contatus |
| `view_fornecedor2_api_consolidados` | Consolidados de `tabela_macro_api` — fornecedor2 (para extração) |
| `view_contatus_api_consolidados` | Consolidados de `tabela_macro_api` — contatus (para extração) |

Todas as views de automação por fornecedor são **derivadas das views globais existentes** via JOIN com `cliente_origem`, mantendo a lógica de deduplicação por CPF+UC que já existe.

---

## Alterações de código necessárias (não aplicadas pelo migration.py)

### 1. `etl/load/macro/02_processar_staging.py` — função `carregar_maps`

A chave do `uc_map` precisa incluir `distribuidora_id` para ser consistente com a nova unique key.

**Antes:**
```python
cur.execute("SELECT cliente_id, uc, id FROM cliente_uc")
uc_map = {(r[0], r[1]): r[2] for r in cur.fetchall()}
```

**Depois:**
```python
cur.execute("SELECT cliente_id, uc, distribuidora_id, id FROM cliente_uc")
uc_map = {(r[0], r[1], r[2]): r[3] for r in cur.fetchall()}
```

---

A chave usada para lookup e o SELECT de fallback também mudam:

**Antes:**
```python
chave_uc = (cliente_id if cliente_id > 0 else 0, uc)
# ...
cur.execute("SELECT id FROM cliente_uc WHERE cliente_id=%s AND uc=%s",
            (cliente_id, uc))
```

**Depois:**
```python
chave_uc = (cliente_id if cliente_id > 0 else 0, uc, distrib_id)
# ...
cur.execute("SELECT id FROM cliente_uc WHERE cliente_id=%s AND uc=%s AND distribuidora_id=%s",
            (cliente_id, uc, distrib_id))
```

---

Após o INSERT de `cliente_id` (novo ou existente), registrar a origem:

```python
# Após confirmar/criar cliente_id, registrar proveniência (INSERT IGNORE para idempotência)
if not dry_run:
    cur.execute(
        "INSERT IGNORE INTO cliente_origem (cliente_id, fornecedor, campanha)"
        " VALUES (%s, 'fornecedor2', 'operacional')",
        (cliente_id,)
    )
```

---

### 2. `dashboard_macros/data/loader.py` — dicionário `SQLs`

Adicionar as seguintes entradas para habilitar filtro por fornecedor no dashboard:

```python
"fornecedor2_macro": """
    SELECT m.id, DATE(m.data_update) AS dia, m.data_update, m.status,
           m.resposta_id, r.mensagem, r.status AS resposta_status, d.nome AS empresa
    FROM view_fornecedor2_macro m
    LEFT JOIN respostas      r ON r.id = m.resposta_id
    LEFT JOIN distribuidoras d ON d.id = m.distribuidora_id
""",
"fornecedor2_api": """
    SELECT m.id, DATE(m.data_update) AS dia, m.data_update, m.status,
           m.resposta_id, r.mensagem, r.status AS resposta_status, d.nome AS empresa
    FROM view_fornecedor2_api m
    LEFT JOIN respostas      r ON r.id = m.resposta_id
    LEFT JOIN distribuidoras d ON d.id = m.distribuidora_id
""",
"contatus_macro": """
    SELECT m.id, DATE(m.data_update) AS dia, m.data_update, m.status,
           m.resposta_id, r.mensagem, r.status AS resposta_status, d.nome AS empresa
    FROM view_contatus_macro m
    LEFT JOIN respostas      r ON r.id = m.resposta_id
    LEFT JOIN distribuidoras d ON d.id = m.distribuidora_id
""",
"contatus_api": """
    SELECT m.id, DATE(m.data_update) AS dia, m.data_update, m.status,
           m.resposta_id, r.mensagem, r.status AS resposta_status, d.nome AS empresa
    FROM view_contatus_api m
    LEFT JOIN respostas      r ON r.id = m.resposta_id
    LEFT JOIN distribuidoras d ON d.id = m.distribuidora_id
""",
```

---

## Lógica de roteamento na migration do contatus

O script de importação do contatus (`etl/migration/contatus/`) deve seguir esta ordem para cada linha:

```
Para cada CPF no arquivo do contatus:
  1. Upsert em clientes → obtém cliente_id
  2. INSERT IGNORE em cliente_origem (fornecedor='contatus', campanha='periodo_historico')

  Se a linha TEM UC:
    3. Verifica se já existe registro ativo em tabela_macros
       (cliente_id + distribuidora_id, status NOT IN ('excluido'))
       → Se SIM: skip (não duplicar)
       → Se NÃO: INSERT em cliente_uc + INSERT pendente em tabela_macros

  Se a linha NÃO TEM UC:
    3. Verifica se o CPF já tem UC em cliente_uc (de qualquer fonte)
       → Se SIM: INSERT pendente em tabela_macros com essa UC (promove para macro)
       → Se NÃO: Verifica se já existe registro ativo em tabela_macro_api
                 → Se SIM: skip
                 → Se NÃO: INSERT pendente em tabela_macro_api
```

Quando um novo fornecedor trouxer UC para um CPF que está em `tabela_macro_api` sem UC:
- Criar/reutilizar `cliente_uc`
- INSERT pendente em `tabela_macros`
- UPDATE em `tabela_macro_api` SET status='excluido' para os registros pendentes/reprocessar desse cliente

---

## Como aplicar

```bash
# Simulação (sem alterar nada)
python db/migrations/001_cliente_origem_views_fornecedor/migration.py --dry-run

# Aplicação real
python db/migrations/001_cliente_origem_views_fornecedor/migration.py
```

## Rollback

```sql
-- Desfazer unique key (voltar ao estado original)
ALTER TABLE cliente_uc DROP INDEX ux_cliente_uc;
ALTER TABLE cliente_uc ADD UNIQUE KEY ux_cliente_uc (cliente_id, uc);

-- Remover tabela e views criadas
DROP TABLE IF EXISTS cliente_origem;
DROP VIEW IF EXISTS view_fornecedor2_macro;
DROP VIEW IF EXISTS view_contatus_macro;
DROP VIEW IF EXISTS view_fornecedor2_macro_automacao;
DROP VIEW IF EXISTS view_contatus_macro_automacao;
DROP VIEW IF EXISTS view_fornecedor2_macro_consolidados;
DROP VIEW IF EXISTS view_contatus_macro_consolidados;
DROP VIEW IF EXISTS view_fornecedor2_api;
DROP VIEW IF EXISTS view_contatus_api;
DROP VIEW IF EXISTS view_fornecedor2_api_automacao;
DROP VIEW IF EXISTS view_contatus_api_automacao;
DROP VIEW IF EXISTS view_fornecedor2_api_consolidados;
DROP VIEW IF EXISTS view_contatus_api_consolidados;

-- Restaurar proc original (ver schema.sql versão anterior)
```
