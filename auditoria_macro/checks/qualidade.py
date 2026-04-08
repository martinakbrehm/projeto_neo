"""
checks/qualidade.py
===================
Verifica qualidade dos dados salvos:
- Duplicatas (mesmo cliente_id + distribuidora_id com status ativo)
- Registros com campos nulos inesperados
- Distribuicao por distribuidora (detecta distorcoes)
- Respostas mais frequentes (identifica padrao de retorno da API)

Alertas com justificativa documentada em requisitos.json (ignorar=true)
sao exibidos como [INFO] em vez de [ATENCAO].
"""

import json
from pathlib import Path
from typing import Any

_REQUISITOS_PATH = Path(__file__).resolve().parents[1] / "requisitos.json"

def _carregar_requisitos() -> dict:
    try:
        return json.loads(_REQUISITOS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _flag(chave: str, requisitos: dict) -> str:
    """Retorna '[INFO]' se o requisito tem ignorar=true, senao '[ATENCAO]'."""
    req = requisitos.get(chave, {})
    return "[INFO]" if req.get("ignorar") else "[ATENCAO]"

def _nota(chave: str, requisitos: dict) -> str | None:
    """Retorna a justificativa documentada, se existir."""
    return requisitos.get(chave, {}).get("justificativa")


def rodar(cur) -> dict[str, Any]:
    resultado = {}

    # --- Duplicatas ativas: mesmo cliente + distribuidora pendente/processando/reprocessar ---
    cur.execute("""
        SELECT cliente_id, distribuidora_id, COUNT(*) AS qtd
        FROM tabela_macros
        WHERE status IN ('pendente','processando','reprocessar')
        GROUP BY cliente_id, distribuidora_id
        HAVING qtd > 1
        ORDER BY qtd DESC
        LIMIT 10
    """)
    resultado["duplicatas"] = cur.fetchall()
    resultado["total_duplicatas"] = len(resultado["duplicatas"])

    # --- Consolidados com data_extracao NULL ---
    cur.execute("""
        SELECT COUNT(*) FROM tabela_macros
        WHERE status = 'consolidado'
          AND data_extracao IS NULL
    """)
    resultado["consolidados_sem_data_extracao"] = cur.fetchone()[0]

    # --- Distribuicao por distribuidora (consolidados) ---
    cur.execute("""
        SELECT d.nome, tm.status, COUNT(*) AS total
        FROM tabela_macros tm
        JOIN distribuidoras d ON tm.distribuidora_id = d.id
        WHERE tm.status IN ('consolidado','excluido','pendente','reprocessar')
        GROUP BY d.nome, tm.status
        ORDER BY d.nome, tm.status
    """)
    resultado["por_distribuidora"] = cur.fetchall()

    # --- Respostas mais frequentes (ultimos 7 dias) ---
    cur.execute("""
        SELECT r.mensagem, r.status AS tipo_resposta, COUNT(*) AS total
        FROM tabela_macros tm
        JOIN respostas r ON tm.resposta_id = r.id
        WHERE tm.data_update >= NOW() - INTERVAL 7 DAY
          AND tm.status IN ('consolidado','excluido')
        GROUP BY r.id, r.mensagem, r.status
        ORDER BY total DESC
        LIMIT 10
    """)
    resultado["top_respostas_7d"] = cur.fetchall()

    # --- Registros com distribuidora nao mapeada ---
    cur.execute("""
        SELECT COUNT(*) FROM tabela_macros tm
        LEFT JOIN distribuidoras d ON tm.distribuidora_id = d.id
        WHERE d.id IS NULL
    """)
    resultado["sem_distribuidora"] = cur.fetchone()[0]

    # --- Registros excluidos hoje (pode indicar problema se numero for muito alto) ---
    cur.execute("""
        SELECT COUNT(*) FROM tabela_macros
        WHERE status = 'excluido'
          AND DATE(data_update) = CURDATE()
    """)
    resultado["excluidos_hoje"] = cur.fetchone()[0]

    # --- Registros consolidados hoje ---
    cur.execute("""
        SELECT COUNT(*) FROM tabela_macros
        WHERE status = 'consolidado'
          AND DATE(data_update) = CURDATE()
    """)
    resultado["consolidados_hoje"] = cur.fetchone()[0]

    return resultado


def formatar(r: dict[str, Any]) -> list[str]:
    linhas = []
    requisitos = _carregar_requisitos()
    linhas.append("== QUALIDADE DOS DADOS ==")

    # Hoje
    ch = r["consolidados_hoje"]
    eh = r["excluidos_hoje"]
    linhas.append(f"  Hoje: consolidados={ch:,}  excluidos={eh:,}  total={ch+eh:,}")

    taxa_ex = (eh / (ch + eh) * 100) if (ch + eh) > 0 else 0
    if taxa_ex > 60:
        flag = _flag("taxa_exclusao_alta", requisitos)
        nota = _nota("taxa_exclusao_alta", requisitos)
        linhas.append(f"  {flag} Taxa de exclusao hoje muito alta: {taxa_ex:.1f}%")
        if nota:
            linhas.append(f"         Justificativa: {nota}")

    # Duplicatas
    dup = r["total_duplicatas"]
    flag = " [ATENCAO]" if dup > 0 else ""
    linhas.append(f"\n  Duplicatas ativas (pendente/processando/reprocessar): {dup:,}{flag}")
    if dup > 0:
        for cliente_id, distrib_id, qtd in r["duplicatas"][:5]:
            linhas.append(f"    cliente_id={cliente_id}  distrib={distrib_id}  qtd={qtd}")

    # Campos nulos
    csde = r["consolidados_sem_data_extracao"]
    flag_csde = _flag("consolidados_sem_data_extracao", requisitos)
    nota_csde = _nota("consolidados_sem_data_extracao", requisitos)
    if csde > 0:
        linhas.append(f"\n  {flag_csde} Consolidados sem data_extracao: {csde:,}")
        if nota_csde:
            linhas.append(f"         Justificativa: {nota_csde}")
    else:
        linhas.append("\n  data_extracao em consolidados: OK")

    sd = r["sem_distribuidora"]
    if sd > 0:
        linhas.append(f"  [ATENCAO] Registros sem distribuidora valida: {sd:,}")

    # Por distribuidora
    linhas.append("\n  Por distribuidora:")
    dist_agrup: dict[str, dict[str, int]] = {}
    for nome, status, total in r["por_distribuidora"]:
        if nome not in dist_agrup:
            dist_agrup[nome] = {}
        dist_agrup[nome][status] = total
    for nome, statuses in sorted(dist_agrup.items()):
        partes = "  ".join(f"{s}={n:,}" for s, n in sorted(statuses.items()))
        linhas.append(f"    {nome:<20} {partes}")

    # Top respostas
    linhas.append("\n  Top respostas API (ultimos 7 dias):")
    if r["top_respostas_7d"]:
        for msg, tipo, total in r["top_respostas_7d"]:
            linhas.append(f"    [{tipo:<12}] {total:>7,}  {msg}")
    else:
        linhas.append("    (sem dados)")

    return linhas
