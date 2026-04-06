# TODO — Projeto Banco Neo

**Data:** 2026-04-06  
**Status:** Em desenvolvimento ativo

---

## 📊 Migrações/Importações de Dados Históricos

### 🔄 Fornecedor1 — Período até 2026-03-12
**Status:** Não importado  
**Local:** `etl/migration/fornecedor1/periodo_ate_20260312/`

- ❌ **Nenhum dado importado** — **PENDENTE**
  - Status: Scripts existem mas não foram executados
  - Próximo passo: Executar pipeline completo de migração do fornecedor1

### 🔄 Fornecedor2 — Período até 2026-03-12
**Status:** Dados importados, falta enriquecimento  
**Local:** `etl/migration/fornecedor2/periodo_ate_20260312/`

- ✅ **01_normalizar_historico.py** — Executado
- ✅ **02_importar_historico_csv.py** — Executado
- ❌ **03_enriquecer_clientes.py** — **PENDENTE**
  - Arquivo: `etl/migration/fornecedor2/periodo_ate_20260312/03_enriquecer_clientes.py`
  - Status: Falha na execução (Exit Code: 1)
  - Próximo passo: Debugar e corrigir o script de enriquecimento
  - Arquivo de progresso: `etl/migration/fornecedor2/periodo_ate_20260312/import_log.txt`

### 🔄 Fornecedor2 — Período pós 2026-03-12
**Status:** Pendente  
**Local:** `etl/migration/fornecedor2/periodo_pos_20260312/`

- ❌ **Pipeline completo** — **PENDENTE**
  - Arquivo: `etl/migration/fornecedor2/periodo_pos_20260312/pipeline.py`
  - Status: Não implementado
  - Próximo passo: Criar pipeline similar ao período anterior

### 🔄 Contatus — Período histórico
**Status:** Pendente  
**Local:** `etl/migration/contatus/periodo_historico/`

- ❌ **Pipeline completo** — **PENDENTE**
  - Status: Estrutura de pastas criada, scripts não implementados
  - Próximo passo: Implementar pipeline de migração do contatus
  - Requisitos especiais: Roteamento UC → macro, sem UC → macro_api

---

## 🛠️ Melhorias no Banco de Dados

### 🔄 Identificação de Fornecedor (cliente_origem + views)
**Status:** Scripts criados, pendente execução  
**Local:** `db/improvements/20260406_cliente_origem_views_fornecedor/`

- ✅ **migration.py** — Criado
- ✅ **README.md** — Criado
- ❌ **Execução da melhoria** — **PENDENTE**
  - Comando: `python db/improvements/20260406_cliente_origem_views_fornecedor/migration.py`
  - Impacto: Adiciona tabela `cliente_origem`, corrige unique key de `cliente_uc`, cria 12 views por fornecedor
  - Próximo passo: Executar com `--dry-run` primeiro, depois aplicar

- ❌ **Alterações de código pós-migração** — **PENDENTE**
  - Arquivo: `etl/load/macro/02_processar_staging.py`
    - [Linha 168](etl/load/macro/02_processar_staging.py#L168): Atualizar `uc_map` para incluir `distribuidora_id`
    - [Linha 289](etl/load/macro/02_processar_staging.py#L289): Adicionar INSERT em `cliente_origem`
    - [Linha 303](etl/load/macro/02_processar_staging.py#L303): Atualizar `chave_uc` tuple
    - [Linha 318](etl/load/macro/02_processar_staging.py#L318): Adicionar `AND distribuidora_id=%s` no WHERE

  - Arquivo: `dashboard_macros/data/loader.py`
    - [Linha 34](dashboard_macros/data/loader.py#L34): Adicionar 4 queries por fornecedor no dict `SQLs`

---

## 🔄 Pipelines Operacionais

### 🔄 Fornecedor2 — Operacional (diário)
**Status:** Em andamento  
**Local:** `etl/load/macro/`

- ✅ **01_staging_import.py** — Funcionando
- ✅ **02_processar_staging.py** — Funcionando
- ✅ **pipeline_carga_operacional_fornecedor2.py** — Renomeado e funcionando
- ❌ **Integração com macro** — **PENDENTE**
  - Status: Pipeline carrega dados → `tabela_macros` com status='pendente'
  - Próximo passo: Configurar execução automática da macro após carga

---

## ⚙️ Configuração da Macro

### 🔄 Macro Neo Energia
**Status:** Scripts criados, configuração pendente  
**Local:** `macro/macro/`

- ✅ **executar_automatico.py** — Criado (orquestrador ETL + SSH + macro)
- ✅ **consulta_contrato.py** — Modificado para aceitar `--arquivo` e `--saida`
- ✅ **CONFIGURACAO.md** — Criado (Windows + Linux)
- ❌ **Configuração inicial** — **PENDENTE**
  - Arquivo: `macro/macro/CONFIGURACAO.md`
  - Passos: Instalar PuTTY (Windows) ou sshpass (Linux), configurar `.env`, aceitar chave SSH
  - Próximo passo: Seguir o guia passo a passo no CONFIGURACAO.md

- ❌ **Teste de túnel** — **PENDENTE**
  - Comando: `plink -pw SUA_SENHA -L 5000:REMOTE_HOST:80 USUARIO@SERVIDOR -N` (Windows)
  - Ou: `sshpass -p SUA_SENHA ssh -N -f -L 5000:REMOTE_HOST:80 USUARIO@SERVIDOR` (Linux)
  - Próximo passo: Validar conectividade com API Neo Energia

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
- ❌ **Views por fornecedor** — **PENDENTE**
  - Status: Após executar melhoria do banco
  - Próximo passo: Atualizar script para usar views `view_*_consolidados`

---

## 🎯 Ordem Recomendada de Execução

1. **Executar melhoria do banco:**
   ```bash
   python db/improvements/20260406_cliente_origem_views_fornecedor/migration.py --dry-run
   python db/improvements/20260406_cliente_origem_views_fornecedor/migration.py
   ```

2. **Aplicar alterações de código:**
   - Modificar `02_processar_staging.py` (4 pontos)
   - Modificar `loader.py` (1 ponto)

3. **Configurar macro:**
   - Seguir `macro/macro/CONFIGURACAO.md`
   - Testar túnel SSH

4. **Corrigir migração fornecedor2:**
   - Debugar `03_enriquecer_clientes.py` (fornecedor2 período pré)

5. **Implementar migração fornecedor1:**
   - Executar pipeline completo em `etl/migration/fornecedor1/periodo_ate_20260312/`

6. **Implementar migração contatus:**
   - Criar pipeline em `etl/migration/contatus/`

7. **Implementar período pós fornecedor2:**
   - Criar pipeline em `etl/migration/fornecedor2/periodo_pos_20260312/`

---

## 📝 Notas Gerais

- **Repositório Git:** `https://github.com/martinakbrehm/projeto_neo.git`
- **Dados sensíveis:** Nunca versionar `dados/`, `config.py`, `.env`
- **Ambientes virtuais:** Usar `.venv` em cada pasta que tem `requirements.txt`
- **Testes:** Sempre usar `--dry-run` antes de executar mudanças no banco
- **Logs:** Verificar `import_log.txt` para progresso das migrações</content>
<parameter name="filePath">c:\Users\marti\Desktop\Bases fornecedor novo\pipeline_bases_neo\projeto_banco_neo\TODO.md