# TODO — Projeto Banco Neo

**Data:** 2026-04-06  
**Status:** Em desenvolvimento ativo

---

## 📊 Migrações/Importações de Dados Históricos

### 🔄 Fornecedor1 — Histórico completo
**Status:** Não importado  
**Local:** `etl/migration/fornecedor1/periodo_ate_20260312/`

- ❌ **Pipeline completo** — **PENDENTE**
  - Status: Scripts existem mas não foram executados
  - Tipo: Dados históricos (não operacionais)
  - Próximo passo: Executar pipeline completo de migração do fornecedor1

### 🔄 Fornecedor2 — Período até 2026-03-12 (histórico)
**Status:** Dados importados, falta enriquecimento  
**Local:** `etl/migration/fornecedor2/periodo_ate_20260312/`

- ✅ **01_normalizar_historico.py** — Executado
- ✅ **02_importar_historico_csv.py** — Executado
- ❌ **03_enriquecer_clientes.py** — **PENDENTE**
  - Arquivo: `etl/migration/fornecedor2/periodo_ate_20260312/03_enriquecer_clientes.py`
  - Status: Falha na execução (Exit Code: 1)
  - Tipo: Dados históricos (não operacionais)
  - Próximo passo: Debugar e corrigir o script de enriquecimento
  - Arquivo de progresso: `etl/migration/fornecedor2/periodo_ate_20260312/import_log.txt`

### 🔄 Contatus — Histórico completo
**Status:** Pendente  
**Local:** `etl/migration/contatus/periodo_historico/`

- ❌ **Pipeline completo** — **PENDENTE**
  - Status: Estrutura de pastas criada, scripts não implementados
  - Tipo: Dados históricos (não operacionais)
  - Requisitos especiais: Roteamento UC → macro, sem UC → macro_api
  - Próximo passo: Implementar pipeline de migração do contatus

---

## 🛠️ Melhorias no Banco de Dados

### ✅ Identificação de Fornecedor (cliente_origem + views)
**Status:** Concluído (2026-04-07)  
**Local:** `db/improvements/20260406_cliente_origem_views_fornecedor/`

- ✅ **migration.py** — Executado com sucesso
  - Unique key de `cliente_uc` corrigida para `(cliente_id, uc, distribuidora_id)`
  - Tabela `cliente_origem` criada e backfillada com 122.168 clientes como `'fornecedor2'`
  - 12 views criadas por fornecedor (`view_fornecedor2_macro_consolidados`, etc.)
- ✅ **Alterações de código pós-migração** — Aplicadas em `02_processar_staging.py`
  - `uc_map` atualizado para chave `(cliente_id, uc, distribuidora_id)`
  - INSERT em `cliente_origem` adicionado para novos clientes
- ❌ **Queries por fornecedor no loader.py** — **PENDENTE**
  - Arquivo: `dashboard_macros/data/loader.py`
  - Próximo passo: Adicionar 4 queries por fornecedor no dict `SQLs`

---

## 🔄 Pipelines Operacionais

### 🔄 Fornecedor2 — Operacional (diário)
**Status:** Staging carregado, processamento em andamento (07-04-2026)  
**Local:** `etl/load/macro/`

- ✅ **01_staging_import.py** — Staging IDs 1-4 carregados (132.717 linhas válidas)
- ✅ **02_processar_staging.py** — Adaptado à migration 20260406 (uc_map + cliente_origem)
- ✅ **pipeline_carga_operacional_fornecedor2.py** — Funcionando
- 🔄 **Processamento staging → produção** — **EM ANDAMENTO**
  - 8.500 linhas do staging_id=1 já inseridas antes da interrupção
  - Reiniciado em 07-04-2026; processa as ~124.217 linhas restantes (IDs 1-4)
- ❌ **Integração com macro** — **PENDENTE**
  - Status: Pipeline carrega dados → `tabela_macros` com status='pendente'; macro configurada e validada
  - Próximo passo: Rodar `EXECUTAR.bat` após carga diária e validar ciclo completo

---

## ⚙️ Configuração da Macro

### ✅ Macro Neo Energia
**Status:** Configurada e validada (dry-run OK)
**Local:** `macro/macro/`

- ✅ **executar_automatico.py** — Orquestrador ETL + SSH + macro
- ✅ **consulta_contrato.py** — Aceita `--arquivo` e `--saida` (sem dialog)
- ✅ **CONFIGURACAO.md** — Guia completo Windows + Linux
- ✅ **setup_venv.bat / setup_venv.sh** — Setup portável para nova máquina
- ✅ **venv recriado** — `pymysql` adicionado ao `requirements.txt`
- ✅ **SSH_HOST_KEY** — Fingerprint configurada no `.env` (plink sem prompt)
- ✅ **Conexão SSH testada** — `plink echo conectado` retornou Exit Code 0
- ✅ **Dry-run validado** — Buscou 2.000 registros do banco com sucesso
- ✅ **Fallback de tabela** — `03_buscar_lote_macro.py` detecta automaticamente se `cliente_origem` existe
- ❌ **Execução real da macro** — **PENDENTE**
  - Pré-requisitos: túnel SSH ativo + `tabela_macros` com registros `pendente`
  - Comando: `macro\macro\EXECUTAR.bat` ou `.venv\Scripts\python.exe executar_automatico.py`

---

## 📈 Dashboard e Extrações

### 🔄 Dashboard Macros
**Status:** Scripts criados, pendente integração  
**Local:** `dashboard_macros/`

- ✅ **loader.py** — Criado
- ✅ **dashboard.py** — Criado
- ✅ **service/orchestrator.py** — Criado
- ❌ **Queries por fornecedor** — **PENDENTE**
  - Status: Após executar `db/improvements/20260406_cliente_origem_views_fornecedor/migration.py`
  - Próximo passo: Adicionar queries no `loader.py` para filtrar por fornecedor

### 🔄 Extração de Consolidados
**Status:** Scripts criados  
**Local:** `etl/extraction/macro/`

- ✅ **extrair_consolidados.py** — Criado
- ✅ **03_buscar_lote_macro.py** — Movido para camada correta (`extraction`)
  - Fallback automático: detecta se `cliente_origem` existe, senão trata tudo como `fornecedor2`
- ❌ **Views por fornecedor** — **PENDENTE**
  - Status: Após executar melhoria do banco
  - Próximo passo: Atualizar script para usar views `view_*_consolidados`

---

## 🎯 Ordem Recomendada de Execução

### Fase 1: Melhorias na Infraestrutura
1. ✅ **Melhoria do banco executada** — `migration.py` aplicado (2026-04-07)
   - 122.168 clientes backfillados como `'fornecedor2'`; 12 views criadas
   - `02_processar_staging.py` adaptado

2. 🔄 **Pipeline operacional em execução** — staging → produção (132.717 linhas)

3. **Rodar ciclo completo da macro:**
   ```
   macro\macro\EXECUTAR.bat
   ```

### Fase 2: Migrações Históricas (segundo plano)
4. **Corrigir migração fornecedor2 (histórico):**
   - Debugar `03_enriquecer_clientes.py` (período até 2026-03-12)

5. **Implementar período pós fornecedor2 (histórico):**
   - Criar pipeline em `etl/migration/fornecedor2/periodo_pos_20260312/`

6. **🔄 MIGRAÇÕES EM SEGUNDO PLANO (não críticas para operacional):**
   - **Fornecedor1 (histórico):** Executar pipeline em `etl/migration/fornecedor1/periodo_ate_20260312/`
   - **Contatus (histórico):** Criar pipeline em `etl/migration/contatus/`

### Fase 3: Operacional
7. **Integração pipeline operacional + macro:**
   - Configurar execução automática da macro após carga diária

---

## 📝 Notas Gerais

- **Repositório Git:** `https://github.com/martinakbrehm/projeto_neo.git`
- **Dados sensíveis:** Nunca versionar `dados/`, `config.py`, `.env`
- **Ambientes virtuais:** Usar `.venv` em cada pasta que tem `requirements.txt`
- **Testes:** Sempre usar `--dry-run` antes de executar mudanças no banco
- **Logs:** Verificar `import_log.txt` para progresso das migrações</content>
<parameter name="filePath">c:\Users\marti\Desktop\Bases fornecedor novo\pipeline_bases_neo\projeto_banco_neo\TODO.md