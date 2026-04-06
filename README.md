Objetivo
- Fornecer esquema relacional para armazenamento de clientes (dados imutáveis), endereços, telefones e uma tabela "macros" de retornos equivalente à `tabela_retornos_2`.

Arquivos gerados
- `schema.sql`: esquema SQL (MySQL/InnoDB, utf8mb4).
- `create_database.py`: script Python para aplicar o `schema.sql` (usa `pymysql`).

Decisões importantes
- `clientes`: tabela de dados imutáveis (cpf como `CHAR(11)` único; `nome`, `data_nascimento`).
- `telefones`: tabela 1:N para telefones; valores armazenados como `BIGINT UNSIGNED` (apenas dígitos). Garantir limpeza dos dados de entrada (apenas dígitos) antes da inserção.
- `enderecos`: tabela separada para endereços e histórico de endereços por cliente.
- `tabela_macros`: equivalente a `tabela_retornos_2`, possui `cliente_id`, `distribuidora_id` e campos de retorno/processamento.

Staging
------
O schema inclui duas tabelas de staging simplificadas:

- `staging_imports`: registra cada importação de arquivo (metadata, status, contadores e timestamps).
- `staging_import_rows`: contém as linhas brutas do arquivo, campos `raw_*`, o `normalized_cpf`, status de validação e mensagem.
# pipeline_banco_neo — README

Este repositório contém o esquema e utilitários para o pipeline de ingestão/processamento de retornos (macros).

**Visão Geral**
- **Arquivos-chave:** `schema.sql`, `create_database.py`, `scripts/update_extracted_records.py`.
- **Objetivo:** armazenar clientes, UCs, endereços, telefones e dois fluxos de retorno: `tabela_macros` (batch) e `tabela_macro_api` (API/eventos).

**Estrutura de dados**
- **clientes:** dados imutáveis por CPF (PK `id`, `cpf` unique).
- **cliente_uc:** UC por cliente+distribuidora (uniqueness por `(cliente_id, uc)`).
- **distribuidoras / respostas:** catálogos de apoio.
- **tabela_macros:** histórico append-only para cargas batch (por cliente+distribuidora).
- **tabela_macro_api:** registros vindos da API/eventos; campos específicos financeiros/contratuais foram adicionados.
- **staging_imports / staging_import_rows:** ingestão e validação de arquivos antes da carga final.

**Fluxo atual (end-to-end)**
1. Ingestão
	- Arquivo é criado/registrado em `staging_imports`; linhas são inseridas em `staging_import_rows` com `raw_*`.
	- Processo de validação normaliza CPF/UC e popula `normalized_cpf` e `validation_status`.
2. Carga
	- Linhas validadas são transformadas e inseridas em `tabela_macros` (batch) ou em `tabela_macro_api` (API/eventos).
3. Enriquecimento / Linkagem
	- Procedure `proc_macro_api_link_uc` associa um `tabela_macro_api` a um `cliente_uc` (cria `cliente_uc` se necessário), define `distribuidora_id` e `resposta_id` quando ausentes.
4. Seleção para processamento (orquestração)
	- Views `view_macros_automacao` e `view_macro_api_automacao` usam `ROW_NUMBER() OVER (PARTITION BY c.cpf, cu.uc ...)` para, por CPF+UC, retornar o registro mais relevante (prefere `pendente` e mais recente por `data_update`).
	- Procedures `get_macros_automacao_batch` e `get_macro_api_batch` entregam lotes (padrão 2000) para workers.
5. Processamento pelo worker
	- Worker obtém lote via `get_*_batch()`, marca os IDs (ex.: `processed=1` / `extraido=1`) para evitar duplicação, executa chamadas externas e atualiza `status`, `data_extracao` e `data_update`.
6. Consolidação/Consumo
	- `view_consolidados_unificado` agrega registros consolidados de ambas as tabelas (`tabela_macros` e `tabela_macro_api`) para relatórios/consumo downstream.

**Mecanismos de sincronização e segurança**
- **Triggers**: `before_insert_clientes` e `before_update_clientes` padronizam CPF (LPAD).
- **Procedures**: loteamento (`get_*_batch`), link/normalização (`proc_macro_api_link_uc`), extração de finalizados.
- **Flags de controle:** `status` (enum), `processed` / `extraido`, `data_extracao`, `data_update`.

**Índices e performance**
- Índices adicionados para acelerar seleção de batches e joins: colunas `cliente_id`, `cliente_uc_id`, `distribuidora_id`, `status`, `data_update`, `data_extracao`, `resposta_id`, `codigo`.
- Recomenda-se dropar índices grandes antes de grandes loads e recriá-los após (script a gerar).
- Evitar `OFFSET` em paginação — use id-range ou marcação para evitar inconsistências/concorrência.

**Como aplicar o schema (resumo rápido)**
1. Instale dependências (ex.: `pymysql`) e configure variáveis de ambiente.
2. Use `python create_database.py` para aplicar `schema.sql` (ver [create_database.py](create_database.py)).
3. Teste em ambiente de staging antes de aplicar em produção.

Exemplo de comando:
```bash
pip install -r requirements.txt  # ou pip install pymysql
python create_database.py --apply
```

**Arquivos úteis**
- `schema.sql` — esquema principal (views, procedures, índices).
- `create_database.py` — utilitário para aplicar o schema.
- `scripts/update_extracted_records.py` — script de exemplo que atualiza flags `extraido` (rever paginação OFFSET).

**Boas práticas operacionais**
- Worker: obter lote → marcar registros → processar → atualizar status (idempotência).
- Prefer paginação por `id` ranges ou marcação; evite `OFFSET` para grandes volumes.
- Fazer backup antes de alterações estruturais; testar `ALTER TABLE` em staging.

**Próximos passos recomendados**
- Gerar scripts para drop/create índices antes/depois de cargas em massa.
- Criar um snippet de worker (Python) que implemente marcação segura por lote.
- Implementar checks com `EXPLAIN` e `SHOW INDEX` para validar ganhos.
- (Opcional) avaliar particionamento ou table-per-period se o volume e retenção justificarem.

Se quiser, eu gero agora o snippet de worker seguro ou os scripts SQL para dropar/recriar índices. Escolha qual prefere.