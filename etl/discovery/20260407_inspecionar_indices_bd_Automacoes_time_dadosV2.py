"""
20260407_inspecionar_indices_bd_Automacoes_time_dadosV2.py
===========================================================
Captura o estado real do banco de destino: bd_Automacoes_time_dadosV2.
  1. Estrutura de cada tabela (SHOW CREATE TABLE)
  2. Índices existentes por tabela (SHOW INDEX)
  3. Contagem de linhas por tabela
  4. EXPLAIN das queries críticas dos pipelines
  5. Processamento pendente na staging

Salva resultados em: etl/discovery/20260407_indices_bd_Automacoes_time_dadosV2.txt

Uso:
    python etl/discovery/20260407_inspecionar_indices_bd_Automacoes_time_dadosV2.py
"""

import sys
from datetime import datetime
from pathlib import Path

import pymysql

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from config import db_destino  # noqa: E402

OUTPUT_FILE = Path(__file__).parent / "20260407_indices_bd_Automacoes_time_dadosV2.txt"
SEP  = "=" * 70
SEP2 = "-" * 70

TABELAS = [
    "clientes",
    "cliente_uc",
    "cliente_origem",
    "tabela_macros",
    "telefones",
    "enderecos",
    "staging_imports",
    "staging_import_rows",
    "respostas",
    "distribuidoras",
]

# Queries críticas dos pipelines — (label, sql)
QUERIES_CRITICAS = [
    (
        "02_processar_staging — SELECT linhas válidas pendentes por staging_id",
        "EXPLAIN SELECT row_idx, normalized_cpf FROM staging_import_rows "
        "WHERE staging_id=1 AND validation_status='valid' AND processed_at IS NULL",
    ),
    (
        "02_processar_staging — UPDATE batch de processed_at por row_idx",
        "EXPLAIN SELECT id FROM staging_import_rows "
        "WHERE staging_id=1 AND row_idx IN (1,2,3,4,5)",
    ),
    (
        "02_processar_staging — carregar_maps: dedup macros hoje por data_criacao",
        "EXPLAIN SELECT cliente_id, distribuidora_id FROM tabela_macros "
        "WHERE DATE(data_criacao) = CURDATE()",
    ),
    (
        "02_processar_staging — carregar_maps: todos clientes (cpf -> id)",
        "EXPLAIN SELECT id, cpf FROM clientes",
    ),
    (
        "02_processar_staging — carregar_maps: todas as UCs",
        "EXPLAIN SELECT id, cliente_id, uc, distribuidora_id FROM cliente_uc",
    ),
    (
        "02_processar_staging — carregar_maps: todos os telefones",
        "EXPLAIN SELECT cliente_id, telefone FROM telefones WHERE telefone IS NOT NULL",
    ),
    (
        "02_processar_staging — carregar_maps: todos os enderecos",
        "EXPLAIN SELECT cliente_uc_id, COALESCE(cep,'') FROM enderecos",
    ),
    (
        "02_processar_staging — INSERT IGNORE cliente_uc (inline SELECT fallback)",
        "EXPLAIN SELECT id FROM cliente_uc "
        "WHERE cliente_id=1 AND uc='0000000001' AND distribuidora_id=3",
    ),
    (
        "04_processar_retorno_macro — SELECT lote pendente/reprocessar com JOIN",
        "EXPLAIN SELECT tm.id, c.cpf, cu.uc, tm.distribuidora_id "
        "FROM tabela_macros tm "
        "JOIN clientes c ON c.id = tm.cliente_id "
        "JOIN cliente_uc cu ON cu.cliente_id = tm.cliente_id "
        "  AND cu.distribuidora_id = tm.distribuidora_id "
        "WHERE tm.status IN ('pendente','reprocessar') "
        "ORDER BY tm.id LIMIT 200",
    ),
    (
        "04_processar_retorno_macro — UPDATE status por id",
        "EXPLAIN SELECT id FROM tabela_macros WHERE id=1",
    ),
    (
        "executar_automatico — SELECT pendente/reprocessar LIMIT N",
        "EXPLAIN SELECT tm.id, c.cpf, cu.uc, d.nome "
        "FROM tabela_macros tm "
        "JOIN clientes c ON c.id = tm.cliente_id "
        "JOIN cliente_uc cu ON cu.cliente_id = tm.cliente_id "
        "  AND cu.distribuidora_id = tm.distribuidora_id "
        "JOIN distribuidoras d ON d.id = tm.distribuidora_id "
        "WHERE tm.status IN ('pendente','reprocessar') "
        "LIMIT 200",
    ),
    (
        "staging_imports — SELECT pendentes para processar",
        "EXPLAIN SELECT id, filename, distribuidora_nome FROM staging_imports "
        "WHERE status='pending' ORDER BY created_at",
    ),
    (
        "cliente_origem — INSERT/check por cliente_id+fornecedor",
        "EXPLAIN SELECT id FROM cliente_origem "
        "WHERE cliente_id=1 AND fornecedor='fornecedor2'",
    ),
]


def run(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    return rows


def main():
    conn = pymysql.connect(**db_destino())
    cur = conn.cursor()

    lines = []
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines.append(SEP)
    lines.append(f"DISCOVERY — Índices e estrutura do banco de destino")
    lines.append(f"Executado em: {ts}")
    lines.append(SEP)

    # ------------------------------------------------------------------
    # 1. Contagem de linhas por tabela
    # ------------------------------------------------------------------
    lines.append("")
    lines.append(SEP2)
    lines.append("1. CONTAGEM DE LINHAS")
    lines.append(SEP2)
    for t in TABELAS:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            cnt = cur.fetchone()[0]
            lines.append(f"  {t:<30} {cnt:>12,} linhas")
        except Exception as e:
            lines.append(f"  {t:<30} ERRO: {e}")

    # ------------------------------------------------------------------
    # 2. Índices existentes por tabela
    # ------------------------------------------------------------------
    lines.append("")
    lines.append(SEP2)
    lines.append("2. ÍNDICES POR TABELA (SHOW INDEX)")
    lines.append(SEP2)
    for t in TABELAS:
        try:
            cur.execute(f"SHOW INDEX FROM {t}")
            rows = cur.fetchall()
            lines.append(f"\n  >>> {t}")
            last_key = None
            for r in rows:
                key_name  = r[2]
                col_name  = r[4]
                non_uniq  = r[1]    # 0=unique, 1=not unique
                seq       = r[3]
                idx_type  = r[10]   # BTREE, HASH, FULLTEXT
                uniq_label = "UNIQUE" if non_uniq == 0 else "INDEX"
                if key_name != last_key:
                    lines.append(f"    [{uniq_label}] {key_name}  ({idx_type})")
                    last_key = key_name
                lines.append(f"      seq={seq}  col={col_name}")
        except Exception as e:
            lines.append(f"  {t}: ERRO: {e}")

    # ------------------------------------------------------------------
    # 3. EXPLAIN das queries críticas
    # ------------------------------------------------------------------
    lines.append("")
    lines.append(SEP2)
    lines.append("3. EXPLAIN DAS QUERIES CRÍTICAS DOS PIPELINES")
    lines.append(SEP2)
    for label, sql in QUERIES_CRITICAS:
        lines.append(f"\n  >>> {label}")
        lines.append(f"      SQL: {sql[:120]}...")
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            for r in rows:
                # EXPLAIN cols: id, select_type, table, type, possible_keys, key, key_len, ref, rows, Extra
                t_name = r[2] if r[2] else "-"
                t_type = r[3] if r[3] else "ALL"
                key    = r[5] if r[5] else "NENHUM"
                est_rows = r[8] if r[8] else "?"
                extra  = r[9] if len(r) > 9 and r[9] else ""
                alerta = " *** FULL SCAN ***" if t_type in ("ALL", "index") and key == "NENHUM" else ""
                lines.append(f"      table={t_name:<25} type={t_type:<10} key={key:<50} rows={est_rows}{alerta}")
                if extra:
                    lines.append(f"      Extra: {extra}")
        except Exception as e:
            lines.append(f"      ERRO ao executar EXPLAIN: {e}")

    # ------------------------------------------------------------------
    # 4. Staging pendente
    # ------------------------------------------------------------------
    lines.append("")
    lines.append(SEP2)
    lines.append("4. ESTADO DA STAGING")
    lines.append(SEP2)
    try:
        cur.execute("SELECT id, filename, status, total_rows, rows_success FROM staging_imports ORDER BY id")
        rows = cur.fetchall()
        for r in rows:
            lines.append(f"  id={r[0]}  status={r[2]:<12} total={r[3]}  ok={r[4]}  file={Path(r[1]).name}")
    except Exception as e:
        lines.append(f"  ERRO: {e}")

    try:
        cur.execute(
            "SELECT staging_id, validation_status, "
            "SUM(CASE WHEN processed_at IS NULL THEN 1 ELSE 0 END) AS pendentes, "
            "COUNT(*) AS total "
            "FROM staging_import_rows GROUP BY staging_id, validation_status"
        )
        rows = cur.fetchall()
        lines.append("")
        lines.append("  staging_import_rows por staging_id + validation_status:")
        for r in rows:
            lines.append(f"    staging_id={r[0]}  status={r[1]:<10}  pendentes={r[2]}  total={r[3]}")
    except Exception as e:
        lines.append(f"  ERRO: {e}")

    cur.close()
    conn.close()

    # Escreve para arquivo e imprime no console
    output = "\n".join(lines)
    OUTPUT_FILE.write_text(output, encoding="utf-8")
    print(output)
    print(f"\n[OK] Salvo em: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
