# Esta pasta contém os arquivos de interface entre o banco e a macro:
#
#   lote_pendente.csv    → gerado por etl/load/macro/03_buscar_lote_macro.py
#                          lido por macro/macro/consulta_contrato.py
#
#   resultado_lote.csv   → gerado por macro/macro/consulta_contrato.py
#                          lido por etl/load/macro/04_processar_retorno_macro.py
#
#   lote_meta.json       → mapa de correlação macro_id ↔ cpf+uc
#                          gerado pelo passo 03, consumido pelo passo 04
#
#   arquivo/             → histórico arquivado com timestamp (gerado automaticamente)
