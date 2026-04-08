"""
checks/status.py
================
Verifica integridade das transicoes de status:
- Registros presos em 'processando'
- Orfaos (processando ha mais de X minutos)
- Taxa de reprocessar (indica problemas na API)
- Ciclos recentes (atividade nos ultimos intervalos)
"""

from typing import Any


def rodar(cur) -> dict[str, Any]:
    resultado = {}

    # --- Presos em 'processando' ---
    cur.execute("""
        SELECT COUNT(*) FROM tabela_macros WHERE status = 'processando'
    """)
    resultado["presos_processando"] = cur.fetchone()[0]

    # --- Orfaos: processando ha mais de 2 horas ---
    cur.execute("""
        SELECT id, cliente_id, distribuidora_id, data_update
        FROM tabela_macros
        WHERE status = 'processando'
          AND data_update < NOW() - INTERVAL 2 HOUR
        ORDER BY data_update
        LIMIT 10
    """)
    resultado["orfaos_2h"] = cur.fetchall()
    resultado["total_orfaos_2h"] = len(resultado["orfaos_2h"])

    # --- Taxa de reprocessar nos ultimos 7 dias ---
    cur.execute("""
        SELECT
            COUNT(*) AS total_finalizados,
            SUM(status = 'reprocessar') AS total_reprocessar
        FROM tabela_macros
        WHERE data_update >= NOW() - INTERVAL 7 DAY
          AND status IN ('consolidado','excluido','reprocessar')
    """)
    row = cur.fetchone()
    total_fin = row[0] or 0
    total_rep = row[1] or 0
    resultado["reprocessar_7d"] = total_rep
    resultado["taxa_reprocessar_7d"] = round(total_rep / total_fin * 100, 2) if total_fin else 0.0

    # --- Ultima atividade no banco (data_update mais recente de consolidado/excluido) ---
    cur.execute("""
        SELECT MAX(data_update)
        FROM tabela_macros
        WHERE status IN ('consolidado','excluido')
    """)
    resultado["ultima_atividade"] = cur.fetchone()[0]

    # --- Ciclos recentes: quantos registros foram finalizados por hora nas ultimas 12h ---
    cur.execute("""
        SELECT DATE_FORMAT(data_update, '%Y-%m-%d %H:00') AS hora,
               COUNT(*) AS total
        FROM tabela_macros
        WHERE status IN ('consolidado','excluido')
          AND data_update >= NOW() - INTERVAL 12 HOUR
        GROUP BY hora
        ORDER BY hora
    """)
    resultado["ciclos_12h"] = cur.fetchall()

    # --- Consistencia: consolidados sem resposta_id ---
    cur.execute("""
        SELECT COUNT(*)
        FROM tabela_macros
        WHERE status = 'consolidado'
          AND (resposta_id IS NULL OR resposta_id = 6)
    """)
    resultado["consolidados_sem_resposta"] = cur.fetchone()[0]

    # --- Excluidos sem resposta_id ---
    cur.execute("""
        SELECT COUNT(*)
        FROM tabela_macros
        WHERE status = 'excluido'
          AND (resposta_id IS NULL OR resposta_id = 6)
    """)
    resultado["excluidos_sem_resposta"] = cur.fetchone()[0]

    return resultado


def formatar(r: dict[str, Any]) -> list[str]:
    linhas = []
    linhas.append("== STATUS E INTEGRIDADE ==")

    # Presos
    presos = r["presos_processando"]
    flag = " [ATENCAO]" if presos > 0 else ""
    linhas.append(f"  Presos em 'processando': {presos:,}{flag}")

    orfaos = r["total_orfaos_2h"]
    flag2 = " [ATENCAO]" if orfaos > 0 else ""
    linhas.append(f"  Orfaos (processando > 2h): {orfaos:,}{flag2}")
    if orfaos > 0:
        for row in r["orfaos_2h"]:
            linhas.append(f"    id={row[0]}  cliente_id={row[1]}  distrib={row[2]}  data_update={row[3]}")

    # Taxa reprocessar
    taxa = r["taxa_reprocessar_7d"]
    flag3 = " [ATENCAO - alta taxa]" if taxa > 15 else ""
    linhas.append(f"\n  Taxa reprocessar (7 dias): {taxa:.1f}% ({r['reprocessar_7d']:,} registros){flag3}")

    # Ultima atividade
    ua = r["ultima_atividade"]
    if ua:
        from datetime import datetime
        delta = datetime.now() - ua
        horas = delta.total_seconds() / 3600
        flag4 = " [ATENCAO - macro pode estar parada]" if horas > 4 else ""
        linhas.append(f"\n  Ultima atividade (consolidado/excluido): {ua.strftime('%d/%m/%Y %H:%M:%S')}{flag4}")
    else:
        linhas.append("\n  Ultima atividade: nenhum registro consolidado/excluido encontrado [ATENCAO]")

    # Ciclos recentes
    linhas.append("\n  Atividade por hora (ultimas 12h):")
    if r["ciclos_12h"]:
        for hora, total in r["ciclos_12h"]:
            bar = "#" * min(int(total / 10), 50)
            linhas.append(f"    {hora}  {total:>6,}  {bar}")
    else:
        linhas.append("    (sem atividade)")

    # Consistencia
    csm = r["consolidados_sem_resposta"]
    esm = r["excluidos_sem_resposta"]
    if csm > 0 or esm > 0:
        linhas.append(f"\n  [ATENCAO] Consolidados sem resposta valida: {csm:,}")
        linhas.append(f"  [ATENCAO] Excluidos sem resposta valida:    {esm:,}")
    else:
        linhas.append("\n  Consistencia resposta_id: OK")

    return linhas
