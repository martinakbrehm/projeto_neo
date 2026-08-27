# Consolidados Locais - 27/08/2026

## Descricao
Migração de dados consolidados que foram rodados localmente (macro).

Esses dados representam as respostas da macro após processar clientes, com status ATIVO/INATIVO.

## Dados
- **Arquivo**: RESULTADO_COMPLETO_CONSOLIDADO.csv
- **Registros**: ~870k linhas
- **Colunas**: cpf, codigo cliente, empresa, resposta (JSON)
- **Data**: 2026-08-27 (data de carga retroativa)

## Execucao
```bash
python 01_carregar_consolidados.py
```

O script irá:
1. Ler o CSV consolidado (com suporte a múltiplos encodings)
2. Extrair Status (ATIVO/INATIVO) do JSON da resposta
3. Linkear com clientes e cliente_uc
4. Inserir em tabela_macros com status='consolidado'
5. Usar data retroativa (extraída do nome do arquivo ou hoje)

## Resultado Esperado
- ~870k registros inseridos em tabela_macros
- Status marcado como 'consolidado'
- Resposta JSON preservada para auditoria
