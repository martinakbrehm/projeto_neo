# Migração 20260409 — Limpar associações de registros históricos

## Problema

Registros históricos (inseridos/processados antes de 2026-01-01) estão sendo incorretamente associados a arquivos de importação posteriores através do sistema de staging.

### Como acontece:
1. Registro histórico é criado/processado (ex: em dezembro de 2025)
2. Arquivo de importação é enviado em janeiro de 2026
3. Sistema faz JOIN entre `tabela_macros` e `staging_imports` por CPF
4. Registro histórico aparece associado ao arquivo de janeiro, mesmo tendo sido processado em dezembro

### Resultado:
- Relatórios mostram registros históricos associados a arquivos incorretos
- Estatísticas de processamento por arquivo ficam distorcidas
- Dificulta análise de performance por período

## Solução

Limpar `data_extracao` de registros históricos que foram processados, fazendo com que apareçam como "histórico" nos relatórios em vez de associados a arquivos específicos.

### Lógica:
- Registros criados/processados antes de 2026-01-01 são considerados históricos
- Para esses registros, `data_extracao` é definida como NULL
- Sistema identifica registros com `data_extracao IS NULL` como "histórico"

## Execução

```bash
python db/improvements/20260409_limpar_associacoes_historicas/migration.py
```

## Verificação

Após execução, registros históricos aparecerão como "histórico" nos relatórios, não associados a arquivos específicos.

## Impacto

- Registros históricos: passam a aparecer corretamente como "histórico"
- Relatórios: Estatísticas por arquivo ficam mais precisas
- Performance: Não afeta processamento futuro