# Subagente: Auditoria de Discrepância no Dashboard (CPF vs CPF+UC)

## Contexto
Investiga casos onde arquivos aparecem com 100% pendente no resumo do dashboard,
mas ao selecioná-los o detalhe mostra combos processadas em dias anteriores.

## Prompt

```
Você é um agente de auditoria de banco de dados e código Python. Tarefa: investigar discrepância no dashboard.

**PROBLEMA REPORTADO:** No dashboard, alguns arquivos aparecem com 100% pendente (todas as combos como pendentes). Mas quando o usuário seleciona aquele arquivo específico, as primeiras tabelas mostram combos processadas em dias anteriores. A suspeita é que a lógica das primeiras tabelas é por CPF (cliente), enquanto a lógica resumida é por cliente_uc_id ou algo diferente.

**PROJETO:** `C:\Users\marti\Desktop\Bases fornecedor novo\pipeline_bases_neo\projeto_banco_neo`

**TAREFA:** Faça as seguintes pesquisas e retorne um relatório completo:

1. Leia o arquivo `dashboard_macros/data/loader.py` completo — identifique todas as queries SQL usadas, especialmente:
   - `carregar_stats_por_arquivo()` (query do resumo/agregado)
   - `carregar_detalhe_arquivo(arquivo)` (query do detalhe por arquivo)
   - Veja se um usa `cliente_uc_id` e outro usa `cpf` ou `cliente_id`

2. Leia `dashboard_macros/dashboard.py` — identifique as callbacks que populam:
   - A tabela agregada (lista de arquivos com colunas pendente/processado/excluido)
   - As tabelas do detalhe do arquivo selecionado

3. Conecte ao banco MySQL:
   - host: `integracoes-assisty.ccr0wsmgsayo.us-east-1.rds.amazonaws.com`
   - port: 3306
   - user: `time_dados`
   - password: `Assisty@2025!`
   - database: `bd_Automacoes_time_dadosV2`
   
   Execute as queries:
   ```sql
   -- Ver estrutura da view/tabela materializada usada no resumo
   SHOW CREATE TABLE dashboard_macros_agg;
   -- ou
   SHOW CREATE TABLE dashboard_arquivos_agg;
   
   -- Verificar se existem arquivos que têm combos pendentes no agg mas processadas no detalhe
   SELECT 
     arquivo,
     SUM(combos_pendentes) as pendentes,
     SUM(combos_processadas) as processadas
   FROM dashboard_macros_agg
   GROUP BY arquivo
   HAVING pendentes > 0 AND processadas = 0
   LIMIT 10;
   
   -- Para um desses arquivos, verificar na tabela_macros quantos têm resposta != NULL
   -- Use o primeiro arquivo encontrado e faça:
   SELECT tm.resposta_id, COUNT(*) 
   FROM tabela_macros tm
   JOIN staging_imports si ON si.id = tm.staging_id
   WHERE si.filename LIKE '%<arquivo_encontrado>%'
   GROUP BY tm.resposta_id;
   ```

4. Verifique a tabela `dashboard_macros_agg` — qual é a grain (granularidade)? É por `(arquivo, cliente_uc_id)`? Ou por `(arquivo, cpf)`?

5. Verifique se há uma diferença entre:
   - Combos onde `cliente_uc_id IS NOT NULL` com resposta
   - Combos onde a lógica de "CPF já foi chamado antes em outro arquivo" (cross-arquivo)

**RETORNE:**
- O texto das queries SQL principais do loader.py (resumo vs detalhe)
- A estrutura da tabela `dashboard_macros_agg` (SHOW CREATE TABLE)  
- Exemplos concretos de arquivos com discrepância (se encontrados via SQL)
- Sua conclusão sobre a causa raiz: por que no resumo aparece pendente mas no detalhe aparece processado
- Se a hipótese do CPF cross-arquivo está correta ou não

Use o arquivo `config.py` do projeto para obter as credenciais se necessário (função `db_destino()`).
```

## Resultado obtido

### Causa raiz identificada
As duas stored procedures usam **lógicas opostas** de atribuição de CPF ao arquivo:

| | SP | Lógica | Efeito |
|---|---|---|---|
| **Resumo** (`sp_refresh_dashboard_arquivos_agg`) | `ROW_NUMBER() OVER ORDER BY created_at ASC` → `WHERE rn=1` | CPF pertence ao **primeiro** arquivo (first-seen) | Arquivo novo = 0 combos processadas |
| **Detalhe** (`sp_refresh_dashboard_macros_agg`) | `MAX(staging_id)` por CPF | CPF pertence ao **último** arquivo (last-seen) | Arquivo novo herda histórico antigo |

### Arquivos afetados (exemplos)
```
27-04-2026/cosern_15000_segundo_lote.csv   ucs_ineditas=14946, combos_processadas=0
23-04-2026/coelba_15000.csv                ucs_ineditas=14721, combos_processadas=0
23-04-2026/cosern_15000.csv                ucs_ineditas=13778, combos_processadas=0
27-04-2026/celpe_15000_segundo_lote.csv    ucs_ineditas=12260, combos_processadas=0
27-04-2026/coelba_15000_segundo_lote.csv   ucs_ineditas=5399,  combos_processadas=0
```

### Solução implementada
Toggle de granularidade na tabela de arquivos do dashboard:
- **CPF+UC (combo)**: visão original por pares CPF+UC inéditos (first-seen)
- **Só CPF**: visão deduplicada por CPF, mostrando `cpfs_ineditos`, `ineditos_processados`, `ineditos_pendentes`

Arquivos modificados:
- `dashboard_macros/dashboard.py` — toggle `selector-granularidade` + callback atualizado
- `dashboard_macros/service/orchestrator.py` — `build_tabela_arquivos(granularidade)` com lógicas CPF e CPF+UC
