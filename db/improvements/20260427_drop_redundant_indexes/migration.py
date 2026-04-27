"""
Migration: Drop 13 redundant / unused indexes
==============================================
Objetivo: reduzir overhead de INSERT nas tabelas principais (~400 MB de índices inúteis).

Análise por tabela:

tabela_macros (334 MB indexes → ~210 MB estimado após remoção):
  DROP idx_tabela_macros_status_resposta         — prefixo EXATO de idx_tabela_macros_agg_cover
  DROP idx_tabela_macros_distrib_data_evento     — nenhuma query filtra distrib_id como coluna líder
  DROP idx_tabela_macros_status_cliente_update   — coberto por agg_cover (status) + cliente_distrib_data (cliente_id)
  DROP idx_tabela_macros_data_criacao_data       — usado 1x para mapa dedup no passo 2, cardinality=18, não justifica

  MANTIDOS:
  - PRIMARY (id)                                — PK
  - idx_tabela_macros_cliente_distrib_data       — dedup maps + JOINs por (cliente_id, distribuidora_id)
  - idx_tabela_macros_status_data_cliente_distrib — batch fetch ORDER BY data_update + auditorias
  - idx_tabela_macros_extraido_status_data       — extrair_consolidados WHERE extraido=0
  - idx_tabela_macros_resposta                   — FK para respostas (obrigatório)
  - idx_tabela_macros_cliente_uc_id              — FK para cliente_uc (obrigatório)
  - idx_tabela_macros_agg_cover                  — SP refresh dashboard_macros_agg (covering)
  - idx_tm_sp_arquivos                           — SP refresh dashboard_arquivos_agg (covering)

staging_import_rows (355 MB indexes → ~140 MB estimado):
  DROP idx_staging_rows_staging                  — prefixo de idx_staging_rows_pendentes E staging_rowidx
  DROP idx_staging_rows_normcpf                  — nenhuma query filtra CPF isolado nesta tabela
  DROP idx_staging_rows_normuc                   — nenhuma query filtra UC isolado nesta tabela
  DROP idx_sir_valid_cpf_id                      — query obsoleta, não referenciado
  DROP idx_sir_valid_cpf_uc_stg                  — staging_id é última col, inutilizável para filtros

  MANTIDOS:
  - PRIMARY (id)                                — PK
  - idx_staging_rows_pendentes                   — WHERE staging_id+validation_status+processed_at
  - idx_staging_rows_staging_rowidx              — UPDATE WHERE staging_id+row_idx

telefones (103 MB indexes → ~53 MB estimado):
  DROP idx_telefones_cliente                     — prefixo EXATO de idx_telefones_cliente_numero
  DROP idx_telefones_numero                      — nenhuma query busca telefone isolado

  MANTIDOS:
  - PRIMARY (id)                                — PK
  - idx_telefones_cliente_numero                 — dedup + FK coverage (cliente_id é prefixo)

clientes (40 MB indexes → ~27 MB estimado):
  DROP idx_clientes_nome                         — só usado por audit infrequente, full scan OK

  MANTIDOS:
  - PRIMARY (id)                                — PK
  - ux_clientes_cpf                              — UNIQUE constraint para dedup

cliente_uc (90 MB indexes → ~73 MB estimado):
  DROP idx_cliente_uc_uc                         — nenhuma query filtra UC sem cliente_id

  MANTIDOS:
  - PRIMARY (id)                                — PK
  - ux_cliente_uc                                — UNIQUE constraint (cliente_id, uc, distribuidora_id)
  - distribuidora_id                             — FK para distribuidoras (obrigatório)
  - idx_cliente_uc_cliente_distrib_uc            — WHERE cliente_id + distribuidora_id (02_processar_staging)
"""

import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
import pymysql
from config import db_destino

DROPS = [
    # (tabela, nome_indice, motivo_curto)
    # ── tabela_macros ──
    ("tabela_macros", "idx_tabela_macros_status_resposta",
     "prefixo de idx_tabela_macros_agg_cover"),
    # idx_tabela_macros_distrib_data_evento — MANTIDO: FK fk_tabela_macros_distribuidora exige
    ("tabela_macros", "idx_tabela_macros_status_cliente_update",
     "coberto por agg_cover + cliente_distrib_data"),
    ("tabela_macros", "idx_tabela_macros_data_criacao_data",
     "cardinality=18, usado 1x para mapa dedup"),

    # ── staging_import_rows ──
    ("staging_import_rows", "idx_staging_rows_staging",
     "prefixo de pendentes e staging_rowidx"),
    ("staging_import_rows", "idx_staging_rows_normcpf",
     "nenhuma query filtra CPF isolado"),
    ("staging_import_rows", "idx_staging_rows_normuc",
     "nenhuma query filtra UC isolado"),
    ("staging_import_rows", "idx_sir_valid_cpf_id",
     "query obsoleta, não referenciado"),
    ("staging_import_rows", "idx_sir_valid_cpf_uc_stg",
     "staging_id é última col, inútil"),

    # ── telefones ──
    ("telefones", "idx_telefones_cliente",
     "prefixo de idx_telefones_cliente_numero"),
    ("telefones", "idx_telefones_numero",
     "nenhuma query busca telefone isolado"),

    # ── clientes ──
    ("clientes", "idx_clientes_nome",
     "só audit infrequente, full scan OK"),

    # ── cliente_uc ──
    ("cliente_uc", "idx_cliente_uc_uc",
     "nenhuma query filtra UC sem cliente_id"),
]


def index_exists(cur, table: str, index_name: str) -> bool:
    cur.execute(
        "SELECT COUNT(1) FROM information_schema.statistics "
        "WHERE table_schema = DATABASE() AND table_name = %s AND index_name = %s",
        (table, index_name),
    )
    return cur.fetchone()[0] > 0


def get_index_size_mb(cur, table: str) -> float:
    cur.execute(
        "SELECT ROUND(index_length / 1024 / 1024, 1) "
        "FROM information_schema.tables "
        "WHERE table_schema = DATABASE() AND table_name = %s",
        (table,),
    )
    row = cur.fetchone()
    return float(row[0]) if row else 0.0


def main():
    conn = pymysql.connect(**db_destino())
    cur = conn.cursor()

    # Medir tamanho dos índices ANTES
    tabelas = sorted({t for t, _, _ in DROPS})
    print("=" * 60)
    print("TAMANHO DOS ÍNDICES — ANTES")
    print("=" * 60)
    antes = {}
    for tbl in tabelas:
        sz = get_index_size_mb(cur, tbl)
        antes[tbl] = sz
        print(f"  {tbl:<25} {sz:>8.1f} MB")
    total_antes = sum(antes.values())
    print(f"  {'TOTAL':<25} {total_antes:>8.1f} MB")

    # Executar drops
    print(f"\n{'=' * 60}")
    print(f"REMOVENDO {len(DROPS)} ÍNDICES REDUNDANTES")
    print(f"{'=' * 60}")

    ok = 0
    skip = 0
    for table, idx_name, motivo in DROPS:
        if not index_exists(cur, table, idx_name):
            print(f"  [SKIP] {table}.{idx_name} — já não existe")
            skip += 1
            continue
        print(f"  DROP INDEX {idx_name} ON {table}  ({motivo})")
        t0 = time.time()
        try:
            cur.execute(f"ALTER TABLE {table} DROP INDEX {idx_name}")
            conn.commit()
            dt = time.time() - t0
            print(f"         OK ({dt:.1f}s)")
            ok += 1
        except pymysql.err.OperationalError as e:
            conn.rollback()
            dt = time.time() - t0
            print(f"         FALHOU ({dt:.1f}s): {e.args[1]}")
            skip += 1

    # Medir DEPOIS
    print(f"\n{'=' * 60}")
    print("TAMANHO DOS ÍNDICES — DEPOIS")
    print("=" * 60)
    depois = {}
    for tbl in tabelas:
        sz = get_index_size_mb(cur, tbl)
        depois[tbl] = sz
        diff = antes[tbl] - sz
        print(f"  {tbl:<25} {sz:>8.1f} MB  (−{diff:>6.1f} MB)")
    total_depois = sum(depois.values())
    total_diff = total_antes - total_depois
    print(f"  {'TOTAL':<25} {total_depois:>8.1f} MB  (−{total_diff:>6.1f} MB)")

    print(f"\n{'=' * 60}")
    print(f"RESUMO: {ok} removidos, {skip} já ausentes")
    print(f"Economia de índices: −{total_diff:.0f} MB")
    print("=" * 60)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
