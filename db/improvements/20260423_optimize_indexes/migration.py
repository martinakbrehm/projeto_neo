"""
Migration: Otimizar indexes para performance dos SPs e macros
Data: 2026-04-23

Problemas identificados:
  1. tabela_macros tem 15 indexes (muitos redundantes), pesando INSERTs/UPDATEs
  2. SP sp_refresh_dashboard_arquivos_agg faz full scan em 911k rows (GROUP BY CASE)
  3. staging_import_rows faz GROUP BY cpf,uc com 'Using temporary'

Ações:
  - DROP 5 indexes redundantes em tabela_macros (15 → 10)
  - ADD 1 covering index em staging_import_rows para GROUP BY cpf+uc+staging_id
  - ADD 1 covering index em tabela_macros para o GROUP BY do SP arquivos
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
import config
import pymysql

SEP = "=" * 60


def idx_exists(cur, table, idx_name):
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.STATISTICS "
        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s AND INDEX_NAME=%s",
        (table, idx_name),
    )
    return cur.fetchone()[0] > 0


def drop_idx(cur, conn, table, idx_name):
    if not idx_exists(cur, table, idx_name):
        print(f"  [SKIP] {idx_name} ja nao existe")
        return
    t0 = time.time()
    try:
        cur.execute(f"ALTER TABLE {table} DROP INDEX {idx_name}")
        conn.commit()
        print(f"  [DROP] {idx_name} ({time.time()-t0:.1f}s)")
    except Exception as e:
        conn.rollback()
        print(f"  [WARN] {idx_name} nao pode ser removido: {e}")


def add_idx(cur, conn, table, idx_name, cols):
    if idx_exists(cur, table, idx_name):
        print(f"  [SKIP] {idx_name} ja existe")
        return
    col_str = ", ".join(cols)
    t0 = time.time()
    cur.execute(f"ALTER TABLE {table} ADD INDEX {idx_name} ({col_str})")
    conn.commit()
    print(f"  [ADD]  {idx_name} ({col_str}) ({time.time()-t0:.1f}s)")


def main():
    conn = pymysql.connect(**config.db_destino(), read_timeout=600, write_timeout=600)
    cur = conn.cursor()

    print(SEP)
    print("MIGRATION: Otimizar indexes para performance")
    print(SEP)

    # ---------------------------------------------------------------
    # PASSO 1: Drop indexes redundantes em tabela_macros (15 → 10)
    # ---------------------------------------------------------------
    print("\n[PASSO 1] Remover indexes redundantes de tabela_macros")
    print("-" * 40)

    # idx_tm_status_resposta = idx_tabela_macros_status_resposta (duplicata exata)
    drop_idx(cur, conn, "tabela_macros", "idx_tm_status_resposta")

    # idx_tm_datas (data_extracao, data_update) — coberto por agg_cover
    drop_idx(cur, conn, "tabela_macros", "idx_tm_datas")

    # idx_tabela_macros_status_id (status, id) — coberto por status_data_cliente_distrib
    drop_idx(cur, conn, "tabela_macros", "idx_tabela_macros_status_id")

    # idx_tabela_macros_data_extracao (data_extracao) — coberto por agg_cover
    drop_idx(cur, conn, "tabela_macros", "idx_tabela_macros_data_extracao")

    # idx_tabela_macros_distrib_data_evento (distribuidora_id, data_update)
    # — coberto por cliente_distrib_data e status_data_cliente_distrib
    drop_idx(cur, conn, "tabela_macros", "idx_tabela_macros_distrib_data_evento")

    # Verificação
    cur.execute(
        "SELECT COUNT(DISTINCT INDEX_NAME) FROM information_schema.STATISTICS "
        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='tabela_macros'"
    )
    cnt = cur.fetchone()[0]
    print(f"\n  tabela_macros: {cnt} indexes restantes")

    # ---------------------------------------------------------------
    # PASSO 2: Covering index em staging_import_rows para GROUP BY
    # ---------------------------------------------------------------
    print("\n[PASSO 2] Adicionar covering index em staging_import_rows")
    print("-" * 40)

    # Para GROUP BY (normalized_cpf, normalized_uc) WHERE validation_status='valid'
    # Cobre as queries de macros_agg, arquivos_agg e cobertura_agg
    add_idx(
        cur, conn, "staging_import_rows",
        "idx_sir_valid_cpf_uc_stg",
        ["validation_status", "normalized_cpf", "normalized_uc", "staging_id"],
    )

    # ---------------------------------------------------------------
    # PASSO 3: Index para o GROUP BY CASE do SP arquivos
    # ---------------------------------------------------------------
    print("\n[PASSO 3] Adicionar index composto em tabela_macros para SPs")
    print("-" * 40)

    # O GROUP BY do SP faz: WHERE status!='pendente' AND resposta_id IS NOT NULL
    # GROUP BY cliente_uc_id, cliente_id, distribuidora_id
    # Precisa de: (status, resposta_id, cliente_uc_id, cliente_id, distribuidora_id, id)
    add_idx(
        cur, conn, "tabela_macros",
        "idx_tm_sp_arquivos",
        ["status", "resposta_id", "cliente_uc_id", "cliente_id", "distribuidora_id", "id"],
    )

    # ---------------------------------------------------------------
    # Verificação final
    # ---------------------------------------------------------------
    print(f"\n{SEP}")
    print("VERIFICAÇÃO FINAL")
    print(SEP)

    for tbl in ["tabela_macros", "staging_import_rows"]:
        cur.execute(
            "SELECT COUNT(DISTINCT INDEX_NAME) FROM information_schema.STATISTICS "
            "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s", (tbl,)
        )
        print(f"  {tbl}: {cur.fetchone()[0]} indexes")

    cur.close()
    conn.close()
    print(f"\n{SEP}")
    print("Migration concluída")
    print(SEP)


if __name__ == "__main__":
    main()
