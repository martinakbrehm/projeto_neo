"""
migration.py
============
Melhoria 20260407 — Indices de performance para os pipelines de carga e macro

Lacunas identificadas via EXPLAIN em 07/04/2026
(ver etl/discovery/20260407_indices_bd_Automacoes_time_dadosV2.txt):

PROBLEMA 1 — staging_import_rows: SELECT pendentes faz scan de 65k linhas
  Existe: idx_staging_rows_staging (staging_id)
  Faltava: filtro adicional por validation_status + processed_at IS NULL
  Fix: idx_staging_rows_pendentes (staging_id, validation_status, processed_at)

PROBLEMA 2 — staging_import_rows: UPDATE batch por row_idx sem indice composto
  Existe: idx_staging_rows_staging (staging_id)
  Faltava: lookup por staging_id + row_idx para o UPDATE em lote
  Fix: idx_staging_rows_staging_rowidx (staging_id, row_idx)

PROBLEMA 3 — tabela_macros: DATE(data_criacao) causa full scan de 136k linhas
  Existe: indices em data_update e data_extracao, mas nao em data_criacao
  Causa: funcao DATE() envolve a coluna, tornando qualquer indice inutilizavel
  Fix: coluna gerada data_criacao_data DATE GENERATED ALWAYS AS (DATE(data_criacao)) STORED
       + indice idx_tabela_macros_data_criacao_data (data_criacao_data, cliente_id, distribuidora_id)

PROBLEMA 4 — tabela_macros: SELECT lote macro usa ORDER BY id sem indice otimizado
  Existe: idx_tabela_macros_status_data_cliente_distrib (status, data_update, ...)
  Faltava: cobertura de (status, id) para o padrao WHERE status IN (...) ORDER BY id LIMIT N
  Fix: idx_tabela_macros_status_id (status, id)

PROBLEMA 5 — telefones: dedup carrega 266k linhas sem indice composto (cliente_id, telefone)
  Existe: idx_telefones_cliente (cliente_id) separado de idx_telefones_numero (telefone)
  Faltava: indice composto para o par (cliente_id, telefone) do set de dedup
  Fix: idx_telefones_cliente_numero (cliente_id, telefone)

PROBLEMA 6 — enderecos: dedup carrega 73k linhas sem indice em (cliente_uc_id, cep)
  Existe: fk_enderecos_cliente_uc (cliente_uc_id) isolado
  Faltava: indice cobrindo cep junto para o par do set de dedup
  Fix: idx_enderecos_uc_cep (cliente_uc_id, cep)

Uso:
    python db/improvements/20260407_indices_performance/migration.py
    python db/improvements/20260407_indices_performance/migration.py --dry-run
"""

import argparse
import sys
from pathlib import Path

import pymysql

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from config import db_destino  # noqa: E402

SEP  = "=" * 70
SEP2 = "-" * 70


def log(msg: str):
    print(msg)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def indice_existe(cur, tabela: str, nome_indice: str) -> bool:
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.STATISTICS "
        "WHERE TABLE_SCHEMA = DATABASE() "
        "  AND TABLE_NAME = %s "
        "  AND INDEX_NAME = %s",
        (tabela, nome_indice),
    )
    return cur.fetchone()[0] > 0


def coluna_existe(cur, tabela: str, coluna: str) -> bool:
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() "
        "  AND TABLE_NAME = %s "
        "  AND COLUMN_NAME = %s",
        (tabela, coluna),
    )
    return cur.fetchone()[0] > 0


def executar(cur, conn, sql: str, descricao: str, dry_run: bool):
    if dry_run:
        log(f"  [DRY-RUN] {descricao}")
        log(f"            {sql[:120]}")
    else:
        log(f"  Executando: {descricao}...")
        try:
            cur.execute(sql)
            conn.commit()
            log(f"  [OK] {descricao}")
        except pymysql.err.OperationalError as e:
            if e.args[0] in (1061, 1060):  # duplicate key name / duplicate column
                log(f"  [SKIP] Ja existe: {descricao}")
            else:
                log(f"  [ERRO] {descricao}: {e}")
                raise


# ---------------------------------------------------------------------------
# PASSO 1 — staging_import_rows: indice composto para SELECT pendentes
# ---------------------------------------------------------------------------

def passo1_staging_rows_pendentes(cur, conn, dry_run: bool):
    log(f"\n{SEP2}")
    log("PASSO 1 — staging_import_rows: indice para SELECT pendentes")
    log(SEP2)

    NOME = "idx_staging_rows_pendentes"
    if not dry_run and indice_existe(cur, "staging_import_rows", NOME):
        log(f"  [SKIP] {NOME} ja existe.")
        return

    executar(
        cur, conn,
        "ALTER TABLE staging_import_rows "
        "  ADD INDEX idx_staging_rows_pendentes (staging_id, validation_status, processed_at)",
        f"ADD INDEX {NOME} (staging_id, validation_status, processed_at)",
        dry_run,
    )
    log("  Impacto: query principal do processar_staging passa de full scan "
        "65k linhas para lookup direto nos pendentes.")


# ---------------------------------------------------------------------------
# PASSO 2 — staging_import_rows: indice para UPDATE batch por row_idx
# ---------------------------------------------------------------------------

def passo2_staging_rows_rowidx(cur, conn, dry_run: bool):
    log(f"\n{SEP2}")
    log("PASSO 2 — staging_import_rows: indice para UPDATE batch por row_idx")
    log(SEP2)

    NOME = "idx_staging_rows_staging_rowidx"
    if not dry_run and indice_existe(cur, "staging_import_rows", NOME):
        log(f"  [SKIP] {NOME} ja existe.")
        return

    executar(
        cur, conn,
        "ALTER TABLE staging_import_rows "
        "  ADD INDEX idx_staging_rows_staging_rowidx (staging_id, row_idx)",
        f"ADD INDEX {NOME} (staging_id, row_idx)",
        dry_run,
    )
    log("  Impacto: commit batch de processed_at nao precisa mais resolver "
        "row_idx pos scan de staging_id.")


# ---------------------------------------------------------------------------
# PASSO 3 — tabela_macros: coluna gerada + indice para DATE(data_criacao)
# ---------------------------------------------------------------------------

def passo3_macros_data_criacao(cur, conn, dry_run: bool):
    log(f"\n{SEP2}")
    log("PASSO 3 — tabela_macros: coluna gerada para DATE(data_criacao)")
    log(SEP2)

    # 3a — coluna gerada
    COLUNA = "data_criacao_data"
    if not dry_run and coluna_existe(cur, "tabela_macros", COLUNA):
        log(f"  [SKIP] Coluna {COLUNA} ja existe.")
    else:
        executar(
            cur, conn,
            "ALTER TABLE tabela_macros "
            "  ADD COLUMN data_criacao_data DATE "
            "  GENERATED ALWAYS AS (DATE(data_criacao)) STORED",
            f"ADD COLUMN {COLUNA} DATE GENERATED ALWAYS AS (DATE(data_criacao)) STORED",
            dry_run,
        )

    # 3b — indice na coluna gerada
    NOME = "idx_tabela_macros_data_criacao_data"
    if not dry_run and indice_existe(cur, "tabela_macros", NOME):
        log(f"  [SKIP] {NOME} ja existe.")
        return

    executar(
        cur, conn,
        "ALTER TABLE tabela_macros "
        "  ADD INDEX idx_tabela_macros_data_criacao_data "
        "  (data_criacao_data, cliente_id, distribuidora_id)",
        f"ADD INDEX {NOME} (data_criacao_data, cliente_id, distribuidora_id)",
        dry_run,
    )
    log("  Impacto: carregar_maps() em processar_staging deixa de fazer full "
        "scan de 136k linhas para filtrar macros do dia.")
    log("  ATENCAO: a query em 02_processar_staging.py deve ser atualizada de")
    log("    WHERE DATE(data_criacao) = CURDATE()")
    log("  para:")
    log("    WHERE data_criacao_data = CURDATE()")


# ---------------------------------------------------------------------------
# PASSO 4 — tabela_macros: indice (status, id) para lote da macro
# ---------------------------------------------------------------------------

def passo4_macros_status_id(cur, conn, dry_run: bool):
    log(f"\n{SEP2}")
    log("PASSO 4 — tabela_macros: indice (status, id) para lote da macro")
    log(SEP2)

    NOME = "idx_tabela_macros_status_id"
    if not dry_run and indice_existe(cur, "tabela_macros", NOME):
        log(f"  [SKIP] {NOME} ja existe.")
        return

    executar(
        cur, conn,
        "ALTER TABLE tabela_macros "
        "  ADD INDEX idx_tabela_macros_status_id (status, id)",
        f"ADD INDEX {NOME} (status, id)",
        dry_run,
    )
    log("  Impacto: SELECT ... WHERE status IN ('pendente','reprocessar') ORDER BY id LIMIT N")
    log("  passa a usar index range scan em vez de filesort sobre 136k linhas.")


# ---------------------------------------------------------------------------
# PASSO 5 — telefones: indice composto (cliente_id, telefone) para dedup
# ---------------------------------------------------------------------------

def passo5_telefones_dedup(cur, conn, dry_run: bool):
    log(f"\n{SEP2}")
    log("PASSO 5 — telefones: indice composto (cliente_id, telefone) para dedup")
    log(SEP2)

    NOME = "idx_telefones_cliente_numero"
    if not dry_run and indice_existe(cur, "telefones", NOME):
        log(f"  [SKIP] {NOME} ja existe.")
        return

    executar(
        cur, conn,
        "ALTER TABLE telefones "
        "  ADD INDEX idx_telefones_cliente_numero (cliente_id, telefone)",
        f"ADD INDEX {NOME} (cliente_id, telefone)",
        dry_run,
    )
    log("  Impacto: dedup de telefones em carregar_maps() carrega 266k linhas "
        "uma unica vez; index ajuda inserts pontuais de verificacao.")


# ---------------------------------------------------------------------------
# PASSO 6 — enderecos: indice composto (cliente_uc_id, cep) para dedup
# ---------------------------------------------------------------------------

def passo6_enderecos_dedup(cur, conn, dry_run: bool):
    log(f"\n{SEP2}")
    log("PASSO 6 — enderecos: indice composto (cliente_uc_id, cep) para dedup")
    log(SEP2)

    NOME = "idx_enderecos_uc_cep"
    if not dry_run and indice_existe(cur, "enderecos", NOME):
        log(f"  [SKIP] {NOME} ja existe.")
        return

    executar(
        cur, conn,
        "ALTER TABLE enderecos "
        "  ADD INDEX idx_enderecos_uc_cep (cliente_uc_id, cep)",
        f"ADD INDEX {NOME} (cliente_uc_id, cep)",
        dry_run,
    )
    log("  Impacto: dedup de enderecos em carregar_maps() passa a permitir "
        "lookup pontual por (uc_id, cep) em vez de full scan de 73k linhas.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Migration 20260407 — Indices de performance"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Exibe o que seria feito sem executar")
    args = parser.parse_args()

    conn = pymysql.connect(**db_destino(autocommit=False))
    cur  = conn.cursor()

    log(SEP)
    log("MIGRATION 20260407 — Indices de performance para pipelines")
    if args.dry_run:
        log("  [DRY-RUN] nenhuma alteracao sera aplicada")
    log(SEP)

    passos = [
        passo1_staging_rows_pendentes,
        passo2_staging_rows_rowidx,
        passo3_macros_data_criacao,
        passo4_macros_status_id,
        passo5_telefones_dedup,
        passo6_enderecos_dedup,
    ]

    for passo in passos:
        passo(cur, conn, args.dry_run)

    cur.close()
    conn.close()

    log(f"\n{SEP}")
    log("Migration concluida." + (" [DRY-RUN]" if args.dry_run else ""))
    log(SEP)

    if not args.dry_run:
        log("")
        log("PROXIMOS PASSOS:")
        log("  - Atualizar 02_processar_staging.py linha ~170:")
        log("    WHERE DATE(data_criacao) = CURDATE()")
        log("    => WHERE data_criacao_data = CURDATE()")


if __name__ == "__main__":
    main()
