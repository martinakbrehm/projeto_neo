"""
checks/salvamento.py
====================
Verifica se o processo de salvamento no banco esta funcionando corretamente.
Inspeciona os arquivos de lote arquivados e cruza com o banco.
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

# Diretorio de arquivos de lote gerados pela macro
MACRO_DADOS = Path(__file__).resolve().parents[2] / "macro" / "dados"
ARQUIVO_DIR = MACRO_DADOS / "arquivo"
LOTE_META   = MACRO_DADOS / "lote_meta.json"
RESULTADO_CSV = MACRO_DADOS / "resultado_lote.csv"


def rodar(cur) -> dict[str, Any]:
    resultado = {}

    # --- Arquivos de lote arquivados (historico) ---
    metas = sorted(ARQUIVO_DIR.glob("lote_meta_*.json"), reverse=True) if ARQUIVO_DIR.exists() else []
    resultado["total_lotes_arquivados"] = len(metas)
    resultado["ultimo_lote_arquivo"] = metas[0].name if metas else None

    # --- Lote pendente atual ---
    resultado["lote_meta_existe"] = LOTE_META.exists()
    resultado["resultado_csv_existe"] = RESULTADO_CSV.exists()

    if RESULTADO_CSV.exists():
        stat = RESULTADO_CSV.stat()
        resultado["resultado_csv_bytes"] = stat.st_size
        resultado["resultado_csv_modificado"] = datetime.fromtimestamp(stat.st_mtime)
    else:
        resultado["resultado_csv_bytes"] = 0
        resultado["resultado_csv_modificado"] = None

    # --- Analise dos ultimos 5 lotes arquivados ---
    lotes_analise = []
    for meta_path in metas[:5]:
        try:
            with open(meta_path, encoding="utf-8") as f:
                meta = json.load(f)
            if isinstance(meta, dict):
                # formato: {"gerado_em": ..., "total": N, "dry_run": bool, "registros": [...]}
                n = meta.get("total") or len(meta.get("registros", [])) or "?"
            elif isinstance(meta, list):
                n = len(meta)
            else:
                n = "?"
            ts = meta_path.stem.replace("lote_meta_", "")
            lotes_analise.append({
                "arquivo": meta_path.name,
                "timestamp": ts,
                "n_registros": n,
            })
        except Exception as e:
            lotes_analise.append({"arquivo": meta_path.name, "erro": str(e)})

    resultado["lotes_recentes"] = lotes_analise

    # --- Verificar se ha atividade recente no banco (ultima 1h) ---
    cur.execute("""
        SELECT COUNT(*), MAX(data_update)
        FROM tabela_macros
        WHERE data_update >= NOW() - INTERVAL 1 HOUR
          AND status IN ('consolidado','excluido','reprocessar')
    """)
    row = cur.fetchone()
    resultado["atualizados_ultima_hora"] = row[0]
    resultado["data_update_maxima_1h"] = row[1]

    # --- Verificar se ha atividade nas ultimas 6h ---
    cur.execute("""
        SELECT COUNT(*)
        FROM tabela_macros
        WHERE data_update >= NOW() - INTERVAL 6 HOUR
          AND status IN ('consolidado','excluido','reprocessar')
    """)
    resultado["atualizados_ultimas_6h"] = cur.fetchone()[0]

    # --- IDs que ficaram em 'processando' por mais de 30 min (lote incompleto) ---
    cur.execute("""
        SELECT COUNT(*)
        FROM tabela_macros
        WHERE status = 'processando'
          AND data_update < NOW() - INTERVAL 30 MINUTE
    """)
    resultado["processando_30min"] = cur.fetchone()[0]

    return resultado


def formatar(r: dict[str, Any]) -> list[str]:
    linhas = []
    linhas.append("== SALVAMENTO E ARQUIVOS DE LOTE ==")

    linhas.append(f"  Lotes arquivados no disco: {r['total_lotes_arquivados']}")
    if r["ultimo_lote_arquivo"]:
        linhas.append(f"  Ultimo lote arquivado:     {r['ultimo_lote_arquivo']}")

    # CSV de resultado
    if r["resultado_csv_existe"]:
        mod = r["resultado_csv_modificado"]
        delta = datetime.now() - mod if mod else None
        idade = f"ha {int(delta.total_seconds() / 60)} min" if delta else "?"
        linhas.append(f"\n  resultado_lote.csv: EXISTS  {r['resultado_csv_bytes']:,} bytes  (modificado {idade})")
    else:
        linhas.append("\n  resultado_lote.csv: nao existe (normal se nao ha lote pendente)")

    # Atividade no banco
    ah = r["atualizados_ultima_hora"]
    a6h = r["atualizados_ultimas_6h"]
    flag = " [ATENCAO - macro pode estar parada]" if a6h == 0 else ""
    linhas.append(f"\n  Atualizacoes no banco:")
    linhas.append(f"    Ultima hora: {ah:,}")
    linhas.append(f"    Ultimas 6h:  {a6h:,}{flag}")

    if r["data_update_maxima_1h"]:
        linhas.append(f"    Ultimo update: {r['data_update_maxima_1h'].strftime('%H:%M:%S')}")

    p30 = r["processando_30min"]
    if p30 > 0:
        linhas.append(f"\n  [ATENCAO] Presos em 'processando' por mais de 30 min: {p30:,}")
        linhas.append("            (podem ser orfaos de um ciclo interrompido)")

    # Lotes recentes
    linhas.append("\n  Lotes recentes (metadados):")
    if r["lotes_recentes"]:
        for lote in r["lotes_recentes"]:
            if "erro" in lote:
                linhas.append(f"    {lote['arquivo']}  [ERRO: {lote['erro']}]")
            else:
                linhas.append(f"    {lote['arquivo']}  n={lote['n_registros']}")
    else:
        linhas.append("    (nenhum lote arquivado encontrado)")

    return linhas
