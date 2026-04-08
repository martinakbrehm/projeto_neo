# _legado — Arquivos Descontinuados

Pasta de arquivos que existiam no desenvolvimento inicial da macro, mantidos apenas para referência histórica. **Não são usados pelo pipeline atual.**

| Arquivo | Descrição | Substituído por |
|---|---|---|
| `executar_automatico_backup.py` | Backup do orquestrador antes da integração com ETL | `executar_automatico.py` |
| `muito_rapido_tratado.py` | Protótipo de consulta sem controle de taxa (requisições muito rápidas) | `consulta_contrato.py` (com throttling) |
| `demora.py` | Teste de latência / protótipo com delay excessivo | `consulta_contrato.py` |
| `cli.txt` | Rascunho de comandos CLI usados durante o desenvolvimento | `README.md` e `CONFIGURACAO.md` |
