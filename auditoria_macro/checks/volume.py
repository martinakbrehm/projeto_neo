"""
checks/volume.py
================
Verifica volume de registros por status na tabela_macros
e tendencia de processamento nos ultimos dias.
"""

from datetime import datetime, timedelta
from typing import Any


def rodar(cur) -> dict[str, Any]:
    """Retorna metricas de volume. cur = cursor pymysql ja conectado."""
    resultado = {}

    # --- Totais por status ---
    cur.execute("""
        SELECT status, COUNT(*) AS total
        FROM tabela_macros
        GROUP BY status
        ORDER BY FIELD(status,'pendente','processando','reprocessar','consolidado','excluido')
    """)
    por_status = {row[0]: row[1] for row in cur.fetchall()}
    resultado["por_status"] = por_status
    resultado["total_geral"] = sum(por_status.values())

    # --- Processados nas ultimas 24 h (data_update) ---
    cur.execute("""
        SELECT status, COUNT(*) AS total
        FROM tabela_macros
        WHERE data_update >= NOW() - INTERVAL 24 HOUR
          AND status IN ('consolidado','excluido','reprocessar')
        GROUP BY status
    """)
    ultimas_24h = {row[0]: row[1] for row in cur.fetchall()}
    resultado["ultimas_24h"] = ultimas_24h
    resultado["processados_24h"] = sum(ultimas_24h.values())

    # --- Tendencia diaria dos ultimos 7 dias (finalizados = consolidado+excluido) ---
    cur.execute("""
        SELECT DATE(data_update) AS dia, status, COUNT(*) AS total
        FROM tabela_macros
        WHERE data_update >= NOW() - INTERVAL 7 DAY
          AND status IN ('consolidado','excluido')
        GROUP BY dia, status
        ORDER BY dia
    """)
    tendencia_raw = cur.fetchall()
    tendencia: dict[str, dict[str, int]] = {}
    for dia, status, total in tendencia_raw:
        chave = str(dia)
        if chave not in tendencia:
            tendencia[chave] = {}
        tendencia[chave][status] = total
    resultado["tendencia_7d"] = tendencia

    # --- Ritmo atual: registros processados por hora (media das ultimas 6h) ---
    cur.execute("""
        SELECT COUNT(*) AS total
        FROM tabela_macros
        WHERE data_update >= NOW() - INTERVAL 6 HOUR
          AND status IN ('consolidado','excluido','reprocessar')
    """)
    processados_6h = cur.fetchone()[0]
    resultado["ritmo_por_hora"] = round(processados_6h / 6, 1)

    # --- Registros que nunca foram atualizados (data_update = data_criacao, pendentes antigos) ---
    cur.execute("""
        SELECT COUNT(*) FROM tabela_macros
        WHERE status = 'pendente'
          AND data_criacao < NOW() - INTERVAL 7 DAY
    """)
    resultado["pendentes_antigos_7d"] = cur.fetchone()[0]

    return resultado


def formatar(r: dict[str, Any]) -> list[str]:
    linhas = []
    linhas.append("== VOLUME ==")

    ps = r["por_status"]
    total = r["total_geral"]
    linhas.append(f"  Total de registros: {total:,}")
    for status, n in ps.items():
        pct = (n / total * 100) if total else 0
        linhas.append(f"    {status:<15} {n:>8,}  ({pct:5.1f}%)")

    linhas.append(f"\n  Processados nas ultimas 24h: {r['processados_24h']:,}")
    for status, n in r["ultimas_24h"].items():
        linhas.append(f"    {status:<15} {n:>8,}")

    linhas.append(f"\n  Ritmo medio (ultimas 6h): {r['ritmo_por_hora']} registros/hora")

    if r["pendentes_antigos_7d"] > 0:
        linhas.append(f"\n  [ATENCAO] Pendentes com mais de 7 dias: {r['pendentes_antigos_7d']:,}")

    linhas.append("\n  Tendencia (ultimos 7 dias):")
    if r["tendencia_7d"]:
        for dia, statuses in sorted(r["tendencia_7d"].items()):
            c = statuses.get("consolidado", 0)
            e = statuses.get("excluido", 0)
            linhas.append(f"    {dia}  consolidado={c:>6,}  excluido={e:>6,}  total={c+e:>6,}")
    else:
        linhas.append("    (sem dados nos ultimos 7 dias)")

    return linhas
