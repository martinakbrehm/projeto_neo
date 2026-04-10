"""
migration.py
============
Melhoria 20260410 — Vincular tabela_macros à tabela cliente_uc

Problema:
    `tabela_macros` registra CPF + distribuidora, mas não a UC específica.
    Clientes com múltiplas UCs na mesma distribuidora geram ambiguidade:
    não é possível saber qual UC cada registro da macro se refere.
    Adicionalmente, `staging_import_rows` não armazenava a UC normalizada,
    impossibilitando rastrear de qual arquivo/staging veio uma combinação CPF+UC.

O que este script faz:
    1. Adiciona `cliente_uc_id INT NULL` em `tabela_macros` com FK para `cliente_uc`.
    2. Adiciona `normalized_uc CHAR(10) NULL` em `staging_import_rows`.
    3. Backfill de `tabela_macros.cliente_uc_id`:
       a) Clientes com 1 única UC na distribuidora → preenchimento direto (100% seguro).
       b) Clientes com múltiplas UCs → tenta casar pela data de criação mais próxima +
          correspondência por distribuidora. Registros ambíguos ficam com NULL.

Impacto em produção:
    - Alter table leve (adiciona coluna nullable + índice) — sem locks longos.
    - FKs adicionadas com ON DELETE SET NULL (sem risco de perda de dados).
    - Registros históricos sem UC identificável ficam NULL — não quebra nada.

Rollback:
    ALTER TABLE tabela_macros DROP FOREIGN KEY fk_tabela_macros_cliente_uc;
    ALTER TABLE tabela_macros DROP INDEX idx_tabela_macros_cliente_uc_id;
    ALTER TABLE tabela_macros DROP COLUMN cliente_uc_id;
    ALTER TABLE staging_import_rows DROP INDEX idx_staging_rows_normuc;
    ALTER TABLE staging_import_rows DROP COLUMN normalized_uc;

Uso:
    python db/improvements/20260410_cliente_uc_id_tabela_macros/migration.py
    python db/improvements/20260410_cliente_uc_id_tabela_macros/migration.py --dry-run
"""

import argparse
import sys
from pathlib import Path

import pymysql

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from config import db_destino  # noqa: E402

SEP = "=" * 70


def log(msg: str):
    print(msg)


# ---------------------------------------------------------------------------
# Verificações de estado atual
# ---------------------------------------------------------------------------

SQL_CHECK_COL_TM = """
    SELECT COUNT(*) FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME   = 'tabela_macros'
      AND COLUMN_NAME  = 'cliente_uc_id'
"""

SQL_CHECK_COL_SIR = """
    SELECT COUNT(*) FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME   = 'staging_import_rows'
      AND COLUMN_NAME  = 'normalized_uc'
"""

# ---------------------------------------------------------------------------
# DDL: adicionar colunas
# ---------------------------------------------------------------------------

SQL_ADD_COL_TM = """
    ALTER TABLE tabela_macros
      ADD COLUMN cliente_uc_id INT NULL AFTER distribuidora_id
"""

SQL_ADD_IDX_TM = """
    ALTER TABLE tabela_macros
      ADD INDEX idx_tabela_macros_cliente_uc_id (cliente_uc_id)
"""

SQL_ADD_FK_TM = """
    ALTER TABLE tabela_macros
      ADD CONSTRAINT fk_tabela_macros_cliente_uc
        FOREIGN KEY (cliente_uc_id) REFERENCES cliente_uc (id) ON DELETE SET NULL
"""

SQL_ADD_COL_SIR = """
    ALTER TABLE staging_import_rows
      ADD COLUMN normalized_uc CHAR(10) NULL AFTER normalized_cpf
"""

SQL_ADD_IDX_SIR = """
    ALTER TABLE staging_import_rows
      ADD INDEX idx_staging_rows_normuc (normalized_uc)
"""

# ---------------------------------------------------------------------------
# Backfill — Fase A: clientes com exatamente 1 UC por distribuidora (seguro)
# ---------------------------------------------------------------------------

SQL_COUNT_SINGLE_UC = """
    SELECT COUNT(*)
    FROM tabela_macros tm
    JOIN (
        SELECT cliente_id, distribuidora_id, MIN(id) AS uc_id
        FROM cliente_uc
        GROUP BY cliente_id, distribuidora_id
        HAVING COUNT(*) = 1
    ) single ON single.cliente_id = tm.cliente_id
            AND single.distribuidora_id = tm.distribuidora_id
    WHERE tm.cliente_uc_id IS NULL
"""

SQL_BACKFILL_SINGLE_UC = """
    UPDATE tabela_macros tm
    JOIN (
        SELECT cliente_id, distribuidora_id, MIN(id) AS uc_id
        FROM cliente_uc
        GROUP BY cliente_id, distribuidora_id
        HAVING COUNT(*) = 1
    ) single ON single.cliente_id = tm.cliente_id
            AND single.distribuidora_id = tm.distribuidora_id
    SET tm.cliente_uc_id = single.uc_id
    WHERE tm.cliente_uc_id IS NULL
"""

# ---------------------------------------------------------------------------
# Backfill — Fase B: clientes com múltiplas UCs — tentar casar por data
# Estratégia: para cada (cliente_id, distribuidora_id) com N UCs,
# casa cada registro de tabela_macros com a UC cuja data_criacao
# está mais próxima da data_criacao do registro, desde que seja único
# naquele dia (evita ambiguidade por data).
# Feito em Python para controle fino.
# ---------------------------------------------------------------------------

SQL_MULTI_UC_PAIRS = """
    SELECT tm.id AS tm_id,
           tm.cliente_id,
           tm.distribuidora_id,
           DATE(tm.data_criacao) AS tm_data,
           cu.id   AS uc_id,
           DATE(cu.data_criacao) AS uc_data
    FROM tabela_macros tm
    JOIN cliente_uc cu
        ON cu.cliente_id = tm.cliente_id
       AND cu.distribuidora_id = tm.distribuidora_id
    WHERE tm.cliente_uc_id IS NULL
    ORDER BY tm.id, cu.id
"""


def backfill_multi_uc(cur, dry_run: bool) -> int:
    """
    Para clientes com múltiplas UCs: se a data_criacao do registro da macro
    coincide com a data_criacao de exatamente 1 UC desse cliente+distribuidora,
    faz o vínculo. Caso haja empate (>1 UC na mesma data), deixa NULL.
    Usa UPDATE com JOIN via tabela temporária para eficiência.
    Retorna quantidade de registros atualizados.
    """
    log("\n[4/5] Backfill multi-UC (por correspondência de datas)...")
    cur.execute(SQL_MULTI_UC_PAIRS)
    rows = cur.fetchall()

    # Agrupa por tm_id: {tm_id -> {tm_data, [(uc_id, uc_data), ...]}}
    from collections import defaultdict
    tm_candidates: dict = defaultdict(lambda: {"tm_data": None, "ucs": []})
    for tm_id, cliente_id, distrib_id, tm_data, uc_id, uc_data in rows:
        tm_candidates[tm_id]["tm_data"] = tm_data
        tm_candidates[tm_id]["ucs"].append((uc_id, uc_data))

    updates = []
    for tm_id, info in tm_candidates.items():
        tm_data = info["tm_data"]
        ucs = info["ucs"]
        # Filtra UCs com data_criacao == tm_data (mesmo dia)
        matching = [uc_id for uc_id, uc_data in ucs if uc_data == tm_data]
        if len(matching) == 1:
            # Exatamente 1 UC chegou no mesmo dia → vínculo seguro
            updates.append((matching[0], tm_id))

    log(f"    {len(tm_candidates):,} registros com múltiplas UCs analisados")
    log(f"    {len(updates):,} vínculos resolúveis por data")

    if updates and not dry_run:
        # Cria tabela temporária e INSERT em batch → UPDATE com JOIN (muito mais rápido)
        cur.execute("""
            CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_uc_backfill (
                tm_id INT NOT NULL PRIMARY KEY,
                uc_id INT NOT NULL
            )
        """)
        # Limpa caso já exista de execução anterior
        cur.execute("TRUNCATE TABLE _tmp_uc_backfill")

        BATCH = 1000
        for i in range(0, len(updates), BATCH):
            chunk = updates[i:i + BATCH]
            ph = ",".join(["(%s,%s)"] * len(chunk))
            flat = [v for pair in chunk for v in pair]
            cur.execute(f"INSERT INTO _tmp_uc_backfill (uc_id, tm_id) VALUES {ph}", flat)

        cur.execute("""
            UPDATE tabela_macros tm
            JOIN _tmp_uc_backfill t ON t.tm_id = tm.id
            SET tm.cliente_uc_id = t.uc_id
            WHERE tm.cliente_uc_id IS NULL
        """)
        updated = cur.rowcount
        log(f"    Atualizados: {updated:,}")
        cur.execute("DROP TEMPORARY TABLE IF EXISTS _tmp_uc_backfill")

    return len(updates)


# ---------------------------------------------------------------------------
# Resumo final
# ---------------------------------------------------------------------------

SQL_RESUMO = """
    SELECT
        COUNT(*)                                          AS total,
        SUM(cliente_uc_id IS NOT NULL)                   AS vinculados,
        SUM(cliente_uc_id IS NULL)                       AS nao_vinculados,
        ROUND(SUM(cliente_uc_id IS NOT NULL) / COUNT(*) * 100, 1) AS pct_vinculado
    FROM tabela_macros
"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(dry_run: bool):
    log(SEP)
    log("MIGRATION 20260410 -- cliente_uc_id em tabela_macros + normalized_uc em staging")
    log("Modo: DRY-RUN (sem alteracoes)" if dry_run else "Modo: EXECUCAO REAL")
    log(SEP)

    conn = pymysql.connect(**db_destino(), connect_timeout=30)
    cur = conn.cursor()

    # ── Passo 1: verificar estado atual ─────────────────────────────────
    log("\n[1/5] Verificando estado atual das colunas...")
    cur.execute(SQL_CHECK_COL_TM)
    col_tm_existe = cur.fetchone()[0] > 0

    cur.execute(SQL_CHECK_COL_SIR)
    col_sir_existe = cur.fetchone()[0] > 0

    log(f"    tabela_macros.cliente_uc_id   : {'JÁ EXISTE' if col_tm_existe else 'AUSENTE (será criada)'}")
    log(f"    staging_import_rows.normalized_uc: {'JÁ EXISTE' if col_sir_existe else 'AUSENTE (será criada)'}")

    # ── Passo 2: DDL em tabela_macros ────────────────────────────────────
    if not col_tm_existe:
        log("\n[2/5] Adicionando cliente_uc_id em tabela_macros...")
        if not dry_run:
            cur.execute(SQL_ADD_COL_TM)
            log("    Coluna adicionada.")
            cur.execute(SQL_ADD_IDX_TM)
            log("    Índice adicionado.")
            cur.execute(SQL_ADD_FK_TM)
            log("    FK adicionada.")
            conn.commit()
        else:
            log("    [DRY] ALTER TABLE tabela_macros ADD COLUMN cliente_uc_id INT NULL ...")
    else:
        log("\n[2/5] Coluna cliente_uc_id já existe em tabela_macros — pulando DDL.")

    # ── Passo 3: DDL em staging_import_rows ──────────────────────────────
    if not col_sir_existe:
        log("\n[3/5] Adicionando normalized_uc em staging_import_rows...")
        if not dry_run:
            cur.execute(SQL_ADD_COL_SIR)
            log("    Coluna adicionada.")
            cur.execute(SQL_ADD_IDX_SIR)
            log("    Índice adicionado.")
            conn.commit()
        else:
            log("    [DRY] ALTER TABLE staging_import_rows ADD COLUMN normalized_uc CHAR(10) NULL ...")
    else:
        log("\n[3/5] Coluna normalized_uc já existe em staging_import_rows — pulando DDL.")

    # ── Passo 4: Backfill fase A — clientes com 1 única UC ───────────────
    log("\n[4/5a] Backfill fase A: clientes com 1 única UC por distribuidora...")
    if dry_run and col_tm_existe is False:
        # Coluna ainda não existe no banco (dry-run pulou DDL) — estima via cliente_uc
        cur.execute("""
            SELECT COUNT(*)
            FROM tabela_macros tm
            JOIN (
                SELECT cliente_id, distribuidora_id
                FROM cliente_uc
                GROUP BY cliente_id, distribuidora_id
                HAVING COUNT(*) = 1
            ) single ON single.cliente_id = tm.cliente_id
                    AND single.distribuidora_id = tm.distribuidora_id
        """)
        n_single = cur.fetchone()[0]
        log(f"    [DRY] Registros elegíveis estimados (single-UC): {n_single:,}")
        log(f"    [DRY] Atualizaria {n_single:,} registros.")
    else:
        cur.execute(SQL_COUNT_SINGLE_UC)
        n_single = cur.fetchone()[0]
        log(f"    Registros elegíveis (single-UC): {n_single:,}")

    if n_single > 0 and not dry_run:
        cur.execute(SQL_BACKFILL_SINGLE_UC)
        log(f"    Atualizados: {cur.rowcount:,}")
        conn.commit()
    elif n_single > 0 and dry_run and not col_tm_existe:
        pass  # já logado acima
    elif n_single > 0 and dry_run:
        log(f"    [DRY] Atualizaria {n_single:,} registros.")

    # ── Passo 5: Backfill fase B — clientes com múltiplas UCs ────────────
    if dry_run and not col_tm_existe:
        log("\n[4/5] Backfill multi-UC (por correspondência de datas)...")
        log("    [DRY] Coluna ainda não existe — análise estimada após criação.")
        n_multi = 0
    else:
        n_multi = backfill_multi_uc(cur, dry_run)
    if n_multi > 0 and not dry_run:
        conn.commit()

    # ── Resumo ────────────────────────────────────────────────────────────
    log("\n[5/5] Resumo final de tabela_macros...")
    if not dry_run or col_tm_existe:
        cur.execute(SQL_RESUMO)
        r = cur.fetchone()
        log(f"    Total registros : {r[0]:,}")
        log(f"    Vinculados (%)  : {r[1]:,}  ({r[3]}%)")
        log(f"    Não vinculados  : {r[2]:,}  (histórico ambíguo — ficam NULL)")
    else:
        cur.execute("SELECT COUNT(*) FROM tabela_macros")
        total = cur.fetchone()[0]
        log(f"    Total registros : {total:,}")
        log(f"    [DRY] Vinculados estimados: ~{n_single + n_multi:,}")

    cur.close()
    conn.close()

    log(f"\n{SEP}")
    if dry_run:
        log("DRY-RUN concluído — nenhuma alteração foi feita.")
    else:
        log("Migração concluída com sucesso.")
    log(SEP)


def main():
    parser = argparse.ArgumentParser(
        description="Migration 20260410 — cliente_uc_id em tabela_macros"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Simula sem alterar o banco")
    args = parser.parse_args()
    run(args.dry_run)


if __name__ == "__main__":
    main()
