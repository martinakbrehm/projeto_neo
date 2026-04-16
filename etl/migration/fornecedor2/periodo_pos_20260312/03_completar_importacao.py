"""
03_completar_importacao.py  —  Completa a importação histórica
================================================================
Identifica os registros do CSV que ainda NÃO estão em tabela_macros
(por CPF) e importa apenas esses, evitando duplicatas.

Uso:
    python etl/migration/fornecedor2/periodo_pos_20260312/03_completar_importacao.py
"""

import sys
import time
from pathlib import Path
from datetime import datetime

import pandas as pd
import pymysql

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))
from config import db_destino  # noqa: E402

# ---------------------------------------------------------------------------
BASE_DIR       = Path(__file__).resolve().parents[4]
PERIODO        = "migration_periodo_pos_20260312"
CSV_PATH       = BASE_DIR / "dados" / "fornecedor2" / PERIODO / "processed" / "historico_normalizado_para_importar.csv"
DATA_IMPORTACAO = "2026-03-23"
STAGING_ID     = 8
BATCH_SIZE     = 300
MAX_RETRIES    = 5
VALID_RESPOSTA_IDS = set(range(12))


def get_imported_cpfs(cursor) -> set:
    """Retorna CPFs que já possuem macros para data_criacao=2026-03-23."""
    cursor.execute(
        """SELECT DISTINCT cl.cpf
           FROM tabela_macros tm
           JOIN clientes cl ON cl.id = tm.cliente_id
           WHERE tm.data_criacao = %s""",
        (DATA_IMPORTACAO,),
    )
    return set(r[0] for r in cursor.fetchall())


def bulk_upsert_clientes(cursor, registros):
    if not registros:
        return {}
    ph = ", ".join(["(%s, %s, %s, %s)"] * len(registros))
    vals = []
    for cpf, nome, dt in registros:
        vals.extend([cpf, nome, dt, dt])
    cursor.execute(
        f"INSERT IGNORE INTO clientes (cpf, nome, data_criacao, data_update) VALUES {ph}",
        vals,
    )
    cpfs = [r[0] for r in registros]
    fmt = ", ".join(["%s"] * len(cpfs))
    cursor.execute(f"SELECT id, cpf FROM clientes WHERE cpf IN ({fmt})", cpfs)
    return {row[1]: row[0] for row in cursor.fetchall()}


def bulk_upsert_cliente_uc(cursor, registros):
    if not registros:
        return {}
    ph = ", ".join(["(%s, %s, %s, %s)"] * len(registros))
    vals = []
    for cid, uc, did, dc in registros:
        vals.extend([cid, uc, did, dc])
    cursor.execute(
        f"INSERT IGNORE INTO cliente_uc (cliente_id, uc, distribuidora_id, data_criacao) VALUES {ph}",
        vals,
    )
    conds = " OR ".join(["(cliente_id = %s AND uc = %s)"] * len(registros))
    params = []
    for cid, uc, _, _ in registros:
        params.extend([cid, uc])
    cursor.execute(f"SELECT id, cliente_id, uc FROM cliente_uc WHERE {conds}", params)
    return {(row[1], row[2]): row[0] for row in cursor.fetchall()}


def bulk_inserir_macros(cursor, registros):
    if not registros:
        return
    ph = ", ".join(["(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"] * len(registros))
    vals = []
    for r in registros:
        vals.extend(r)
    cursor.execute(
        f"""INSERT INTO tabela_macros
            (cliente_id, distribuidora_id, cliente_uc_id,
             resposta_id, qtd_faturas, valor_debito, valor_credito,
             data_update, data_criacao, status, extraido,
             data_inic_parc, qtd_parcelas, valor_parcelas)
            VALUES {ph}""",
        vals,
    )


def processar_lote(cursor, lote, cpf_cache, uc_cache):
    ok = 0
    skipped = 0

    # 1) Clientes
    novos = []
    for _, row in lote.iterrows():
        cpf = str(row["cpf"]).strip()
        if not cpf or cpf in cpf_cache:
            continue
        nome = str(row["nome"]).strip() if row.get("nome") else None
        novos.append((cpf, nome, row["data_importacao"]))
    if novos:
        seen = set()
        dedup = [c for c in novos if c[0] not in seen and not seen.add(c[0])]
        cpf_cache.update(bulk_upsert_clientes(cursor, dedup))

    # 2) Cliente UC
    ucs = []
    for _, row in lote.iterrows():
        cpf = str(row["cpf"]).strip()
        uc = str(row["uc"]).strip()
        did = row.get("distribuidora_id")
        if not cpf or not uc or not did or cpf not in cpf_cache:
            continue
        cid = cpf_cache[cpf]
        try:
            did = int(float(did))
        except (ValueError, TypeError):
            continue
        if (cid, uc) in uc_cache:
            continue
        ucs.append((cid, uc, did, row["data_importacao"]))
    if ucs:
        seen = set()
        dedup = [u for u in ucs if (u[0], u[1]) not in seen and not seen.add((u[0], u[1]))]
        uc_cache.update(bulk_upsert_cliente_uc(cursor, dedup))

    # 3) Macros
    macros = []
    for _, row in lote.iterrows():
        cpf = str(row["cpf"]).strip()
        if not cpf or cpf not in cpf_cache:
            skipped += 1
            continue
        cid = cpf_cache[cpf]
        uc = str(row["uc"]).strip()
        try:
            did = int(float(row["distribuidora_id"]))
        except (ValueError, TypeError):
            skipped += 1
            continue
        try:
            rid = int(float(row["resposta_id"]))
            if rid not in VALID_RESPOSTA_IDS:
                rid = 6
        except (ValueError, TypeError):
            rid = 6
        status = str(row.get("status", "pendente")).strip()
        if status not in ("pendente", "processando", "reprocessar", "consolidado", "excluido"):
            status = "pendente"
        uc_id = uc_cache.get((cid, uc))

        qtd_fat = None
        try:
            v = row.get("qtd_faturas", "")
            if v != "" and not pd.isna(v):
                qtd_fat = int(float(v))
        except (ValueError, TypeError):
            pass

        val_deb = None
        try:
            v = row.get("valor_debito", "")
            if v != "" and not pd.isna(v):
                val_deb = round(float(v), 2)
        except (ValueError, TypeError):
            pass

        val_cred = None
        try:
            v = row.get("valor_credito", "")
            if v != "" and not pd.isna(v):
                val_cred = round(float(v), 2)
        except (ValueError, TypeError):
            pass

        data_proc = row.get("data_processamento", "")
        if pd.isna(data_proc) or str(data_proc).strip() == "":
            data_update = row["data_importacao"]
        else:
            data_update = str(data_proc).strip()

        data_criacao = str(row["data_importacao"]).strip()

        d_inic = None
        v_p = row.get("data_inic_parc", "")
        if not pd.isna(v_p) and str(v_p).strip():
            d_inic = str(v_p).strip()

        qtd_parc = None
        v = row.get("qtd_parcelas", "")
        if not pd.isna(v) and str(v).strip():
            try:
                qtd_parc = int(float(v))
            except (ValueError, TypeError):
                pass

        val_parc = None
        v = row.get("valor_parcelas", "")
        if not pd.isna(v) and str(v).strip():
            try:
                val_parc = round(float(v), 2)
            except (ValueError, TypeError):
                pass

        macros.append((
            cid, did, uc_id,
            rid, qtd_fat, val_deb, val_cred,
            data_update, data_criacao, status, 1,
            d_inic, qtd_parc, val_parc,
        ))
        ok += 1

    if macros:
        bulk_inserir_macros(cursor, macros)

    return ok, skipped


def main():
    print("=" * 70)
    print("COMPLETAR IMPORTAÇÃO HISTÓRICA — 23/03/300k.csv")
    print("=" * 70)

    df = pd.read_csv(CSV_PATH, sep=";", dtype=str, encoding="utf-8-sig")
    print(f"CSV total: {len(df):,} linhas")

    conn = pymysql.connect(**db_destino(autocommit=False))
    cursor = conn.cursor()

    # 1) Identificar CPFs já importados
    print("\n[1/3] Carregando CPFs já importados...")
    imported_cpfs = get_imported_cpfs(cursor)
    print(f"  CPFs já no banco: {len(imported_cpfs):,}")

    # 2) Filtrar apenas linhas pendentes
    df_missing = df[~df["cpf"].isin(imported_cpfs)].copy()
    print(f"  Linhas pendentes: {len(df_missing):,}")

    if len(df_missing) == 0:
        print("\n[OK] Todos os registros já foram importados!")
        cursor.execute(
            "UPDATE staging_imports SET status='completed', rows_success=%s, finished_at=NOW() WHERE id=%s",
            (len(df), STAGING_ID),
        )
        conn.commit()
        conn.close()
        return

    # 3) Importar
    print(f"\n[2/3] Importando {len(df_missing):,} linhas em lotes de {BATCH_SIZE}...")
    cpf_cache = {}
    uc_cache = {}
    ok_total = 0
    skip_total = 0
    total = len(df_missing)

    for batch_start in range(0, total, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total)
        lote = df_missing.iloc[batch_start:batch_end]

        for attempt in range(MAX_RETRIES):
            try:
                ok, skipped = processar_lote(cursor, lote, cpf_cache, uc_cache)
                conn.commit()
                break
            except pymysql.err.OperationalError as e:
                if "Deadlock found" in str(e) and attempt < MAX_RETRIES - 1:
                    conn.rollback()
                    time.sleep(1 + attempt)
                    continue
                elif "Lock wait timeout" in str(e) and attempt < MAX_RETRIES - 1:
                    conn.rollback()
                    time.sleep(2 + attempt)
                    continue
                else:
                    raise

        ok_total += ok
        skip_total += skipped

        if batch_end % 5000 < BATCH_SIZE:
            pct = batch_end / total * 100
            elapsed_cpfs = len(imported_cpfs) + ok_total
            print(f"  ... {batch_end:,}/{total:,} ({pct:.1f}%)  "
                  f"ok={ok_total:,}  skip={skip_total}  "
                  f"total_macros_23mar~{263130 + ok_total:,}")

    # 4) Finalizar staging
    print("\n[3/3] Atualizando staging_imports...")
    cursor.execute(
        "SELECT COUNT(*) FROM tabela_macros WHERE data_criacao = %s",
        (DATA_IMPORTACAO,),
    )
    final_macros = cursor.fetchone()[0]

    cursor.execute(
        """UPDATE staging_imports
           SET status='completed', rows_success=%s, finished_at=NOW()
           WHERE id=%s""",
        (final_macros, STAGING_ID),
    )
    conn.commit()

    print("\n" + "=" * 70)
    print("IMPORTAÇÃO CONCLUÍDA")
    print("=" * 70)
    print(f"  Novos macros inseridos : {ok_total:,}")
    print(f"  Skipped                : {skip_total}")
    print(f"  Total macros (23/03)   : {final_macros:,}")
    print("=" * 70)

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
