"""
02_importar_historico.py  —  PASSO 2
=====================================
Importa o CSV normalizado gerado pelo PASSO 1 para o banco de dados.

Fluxo (importação histórica com resultados já processados):
  1. Cria registro em staging_imports   (filename='23/03/300k.csv')
  2. Insere TODAS as linhas em staging_import_rows
  3. INSERT IGNORE em clientes          (por CPF único)
  4. INSERT IGNORE em cliente_uc        (por cliente_id + UC)
  5. INSERT em tabela_macros            (com status final já definido)

Como é importação histórica, os registros entram com o status final
(consolidado, excluido, reprocessar ou pendente) e extraido=1.

Uso:
    python etl/migration/fornecedor2/periodo_pos_20260312/02_importar_historico.py
    python etl/migration/fornecedor2/periodo_pos_20260312/02_importar_historico.py --dry-run
    python etl/migration/fornecedor2/periodo_pos_20260312/02_importar_historico.py --reset
"""

import sys
import json
import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd
import pymysql

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))
from config import db_destino  # noqa: E402

# ---------------------------------------------------------------------------
# Configurações
# ---------------------------------------------------------------------------
BASE_DIR   = Path(__file__).resolve().parents[4]
PERIODO    = "migration_periodo_pos_20260312"
CSV_PADRAO = BASE_DIR / "dados" / "fornecedor2" / PERIODO / "processed" / "historico_normalizado_para_importar.csv"
STATE_DIR  = BASE_DIR / "dados" / "fornecedor2" / PERIODO / "state"
PROGRESSO  = STATE_DIR / "importacao_progresso.json"

STAGING_FILENAME = "23/03/300k.csv"
IMPORTED_BY      = "migration_historica_pos_20260312"
DATA_IMPORTACAO  = "2026-03-23"

BATCH_SIZE = 10
VALID_RESPOSTA_IDS = set(range(12))  # 0..11


# ---------------------------------------------------------------------------
# Progresso
# ---------------------------------------------------------------------------

def carregar_progresso(csv_name: str) -> dict:
    if not PROGRESSO.exists():
        return {}
    try:
        with open(PROGRESSO, "r", encoding="utf-8") as f:
            dados = json.load(f)
        if dados.get("csv_file") != csv_name:
            return {}
        return dados
    except Exception:
        return {}


def salvar_progresso(csv_name: str, ultimo_idx: int, ok: int,
                     skipped: int, erros: int, total: int,
                     staging_id: int, iniciado_em: str,
                     erros_detalhe: list, status: str = ""):
    if not status:
        status = "em_andamento" if (ok + skipped + erros) < total else "concluido"
    dados = {
        "csv_file":      csv_name,
        "total":         total,
        "ultimo_indice": ultimo_idx,
        "ok":            ok,
        "skipped":       skipped,
        "erros":         erros,
        "staging_id":    staging_id,
        "status":        status,
        "iniciado_em":   iniciado_em,
        "atualizado_em": datetime.now().isoformat(timespec="seconds"),
        "erros_detalhe": erros_detalhe[-200:],
    }
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    with open(PROGRESSO, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Bulk helpers
# ---------------------------------------------------------------------------

def bulk_upsert_clientes(cursor, registros: list) -> dict:
    """
    Insere clientes em lote. Retorna {cpf: id}.
    registros: [(cpf, nome, data_criacao), ...]
    """
    if not registros:
        return {}

    ph = ", ".join(["(%s, %s, %s, %s)"] * len(registros))
    vals = []
    for cpf, nome, data_criacao in registros:
        vals.extend([cpf, nome, data_criacao, data_criacao])

    cursor.execute(
        f"""INSERT IGNORE INTO clientes (cpf, nome, data_criacao, data_update)
            VALUES {ph}""",
        vals,
    )

    cpfs = [r[0] for r in registros]
    fmt  = ", ".join(["%s"] * len(cpfs))
    cursor.execute(f"SELECT id, cpf FROM clientes WHERE cpf IN ({fmt})", cpfs)
    return {row[1]: row[0] for row in cursor.fetchall()}


def bulk_upsert_cliente_uc(cursor, registros: list) -> dict:
    """
    Insere cliente_uc em lote. Retorna {(cliente_id, uc): id}.
    registros: [(cliente_id, uc, distribuidora_id, data_criacao), ...]
    """
    if not registros:
        return {}

    ph = ", ".join(["(%s, %s, %s, %s)"] * len(registros))
    vals = []
    for cid, uc, did, dc in registros:
        vals.extend([cid, uc, did, dc])

    cursor.execute(
        f"""INSERT IGNORE INTO cliente_uc (cliente_id, uc, distribuidora_id, data_criacao)
            VALUES {ph}""",
        vals,
    )

    conds = " OR ".join(["(cliente_id = %s AND uc = %s)"] * len(registros))
    params = []
    for cid, uc, _, _ in registros:
        params.extend([cid, uc])
    cursor.execute(
        f"SELECT id, cliente_id, uc FROM cliente_uc WHERE {conds}",
        params,
    )
    return {(row[1], row[2]): row[0] for row in cursor.fetchall()}


def bulk_inserir_macros(cursor, registros: list):
    """
    Insere tabela_macros em lote.
    registros: [(cliente_id, distribuidora_id, cliente_uc_id,
                 resposta_id, qtd_faturas, valor_debito, valor_credito,
                 data_update, data_criacao, status, extraido,
                 data_inic_parc, qtd_parcelas, valor_parcelas), ...]
    """
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


def bulk_inserir_staging_rows(cursor, staging_id: int, registros: list):
    """
    Insere staging_import_rows em lote.
    registros: [(row_idx, raw_cpf, raw_nome, normalized_cpf, normalized_uc,
                 validation_status, validation_message), ...]
    """
    if not registros:
        return

    ph = ", ".join(["(%s,%s,%s,%s,%s,%s,%s,%s,%s)"] * len(registros))
    vals = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for r in registros:
        vals.extend([staging_id, *r, now])

    cursor.execute(
        f"""INSERT INTO staging_import_rows
            (staging_id, row_idx, raw_cpf, raw_nome,
             normalized_cpf, normalized_uc,
             validation_status, validation_message, processed_at)
            VALUES {ph}""",
        vals,
    )


# ---------------------------------------------------------------------------
# Processar lote
# ---------------------------------------------------------------------------

def processar_lote(cursor, lote: pd.DataFrame, cpf_cache: dict,
                   uc_cache: dict) -> tuple:
    """
    Processa um lote do CSV normalizado.
    Retorna (ok, skipped, erros_detalhe).
    """
    ok = 0
    skipped = 0
    erros = []

    # --- 1. Clientes (INSERT IGNORE) ---
    clientes_novos = []
    for _, row in lote.iterrows():
        cpf = str(row["cpf"]).strip()
        if not cpf or cpf in cpf_cache:
            continue
        nome = str(row["nome"]).strip() if row.get("nome") else None
        clientes_novos.append((cpf, nome, row["data_importacao"]))

    if clientes_novos:
        # Deduplica dentro do lote
        seen = set()
        dedup = []
        for c in clientes_novos:
            if c[0] not in seen:
                seen.add(c[0])
                dedup.append(c)
        result = bulk_upsert_clientes(cursor, dedup)
        cpf_cache.update(result)

    # --- 2. Cliente UC (INSERT IGNORE) ---
    ucs_novos = []
    for _, row in lote.iterrows():
        cpf = str(row["cpf"]).strip()
        uc  = str(row["uc"]).strip()
        did = row.get("distribuidora_id")
        if not cpf or not uc or not did or cpf not in cpf_cache:
            continue
        cid = cpf_cache[cpf]
        try:
            did = int(float(did))
        except (ValueError, TypeError):
            continue
        key = (cid, uc)
        if key in uc_cache:
            continue
        ucs_novos.append((cid, uc, did, row["data_importacao"]))

    if ucs_novos:
        seen = set()
        dedup = []
        for u in ucs_novos:
            key = (u[0], u[1])
            if key not in seen:
                seen.add(key)
                dedup.append(u)
        result = bulk_upsert_cliente_uc(cursor, dedup)
        uc_cache.update(result)

    # --- 3. Tabela macros ---
    macros_batch = []
    seen_macro = set()   # (cpf, uc, distrib) → evita duplicata dentro do lote

    for _, row in lote.iterrows():
        cpf = str(row["cpf"]).strip()
        if not cpf or cpf not in cpf_cache:
            skipped += 1
            continue

        cid = cpf_cache[cpf]
        uc  = str(row["uc"]).strip()

        try:
            did = int(float(row["distribuidora_id"]))
        except (ValueError, TypeError):
            erros.append(f"row {row.get('row_idx','?')}: distribuidora_id inválido")
            skipped += 1
            continue

        # resposta_id
        try:
            rid = int(float(row["resposta_id"]))
            if rid not in VALID_RESPOSTA_IDS:
                rid = 6
        except (ValueError, TypeError):
            rid = 6

        status = str(row.get("status", "pendente")).strip()
        if status not in ("pendente", "processando", "reprocessar", "consolidado", "excluido"):
            status = "pendente"

        # cliente_uc_id
        uc_id = None
        if uc:
            uc_id = uc_cache.get((cid, uc))

        # Valores numéricos
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

        # Datas
        data_proc = row.get("data_processamento", "")
        if pd.isna(data_proc) or str(data_proc).strip() == "":
            data_update = row["data_importacao"]
        else:
            data_update = str(data_proc).strip()

        data_criacao = str(row["data_importacao"]).strip()

        # Parcelamento
        d_inic = None
        v_parc = row.get("data_inic_parc", "")
        if not pd.isna(v_parc) and str(v_parc).strip():
            d_inic = str(v_parc).strip()

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

        # Extraido = 1 para histórico (já foi processado)
        extraido = 1

        # Dedup dentro do lote: para mesmo CPF+UC+distrib, mantém só 1 macro
        dedup_key = (cpf, uc, did)
        if dedup_key in seen_macro:
            skipped += 1
            continue
        seen_macro.add(dedup_key)

        macros_batch.append((
            cid, did, uc_id,
            rid, qtd_fat, val_deb, val_cred,
            data_update, data_criacao, status, extraido,
            d_inic, qtd_parc, val_parc,
        ))
        ok += 1

    if macros_batch:
        bulk_inserir_macros(cursor, macros_batch)

    return ok, skipped, erros


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="PASSO 2: Importa CSV normalizado para o banco (histórico)."
    )
    parser.add_argument("--file", type=str, default=str(CSV_PADRAO))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--reset", action="store_true")
    args = parser.parse_args()

    csv_path = Path(args.file)
    if not csv_path.exists():
        print(f"[ERRO] Arquivo '{csv_path}' nao encontrado.")
        print("       Execute primeiro: 01_normalizar_historico.py")
        sys.exit(1)

    print("=" * 70)
    print("PASSO 2 — Importar histórico para o banco")
    print(f"  CSV: {csv_path.name}")
    print(f"  Staging filename: {STAGING_FILENAME}")
    print(f"  Dry-run: {args.dry_run}")
    print("=" * 70)

    # Ler CSV
    df = pd.read_csv(csv_path, sep=";", dtype=str, encoding="utf-8-sig")
    total = len(df)
    print(f"\n[INFO] {total:,} linhas no CSV")

    # Progresso
    prog = {} if args.reset else carregar_progresso(csv_path.name)
    start_idx = prog.get("ultimo_indice", -1) + 1 if prog else 0
    ok_total = prog.get("ok", 0)
    skip_total = prog.get("skipped", 0)
    err_total = prog.get("erros", 0)
    erros_detalhe = prog.get("erros_detalhe", [])
    staging_id = prog.get("staging_id")
    iniciado_em = prog.get("iniciado_em", datetime.now().isoformat(timespec="seconds"))

    if start_idx > 0:
        print(f"[INFO] Retomando do indice {start_idx}  (ok={ok_total}, skip={skip_total}, err={err_total})")

    if args.dry_run:
        print("\n[DRY-RUN] Nenhuma alteracao sera feita no banco.")
        # Mostrar preview
        status_counts = df["status"].value_counts()
        print("\nDistribuicao de status:")
        for st, cnt in status_counts.items():
            print(f"  {st}: {cnt:,}")
        print(f"\nTotal: {total:,}")
        return

    # Conectar ao banco
    conn = pymysql.connect(**db_destino(autocommit=False))
    cursor = conn.cursor()

    try:
        # --- Criar staging_imports (se ainda não existe) ---
        if staging_id is None:
            cursor.execute(
                """INSERT INTO staging_imports
                   (filename, distribuidora_nome, target_macro_table,
                    total_rows, status, imported_by, created_at, started_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())""",
                (STAGING_FILENAME, "1,2,3",  # todas distribuidoras
                 "tabela_macros", total, "processing",
                 IMPORTED_BY, DATA_IMPORTACAO),
            )
            staging_id = cursor.lastrowid
            conn.commit()
            print(f"\n[INFO] staging_imports criado → id={staging_id}")
        else:
            print(f"\n[INFO] Usando staging_imports existente → id={staging_id}")

        # --- Inserir staging_import_rows (todas as linhas) ---
        if start_idx == 0:
            print(f"\n[STAGING ROWS] Inserindo {total:,} linhas em staging_import_rows ...")
            stg_batch = []
            for i, (_, row) in enumerate(df.iterrows()):
                cpf = str(row["cpf"]).strip()
                nome = str(row["nome"]).strip() if not pd.isna(row.get("nome")) else None
                uc = str(row["uc"]).strip()

                v_status = "valid" if cpf and len(cpf) == 11 else "invalid"
                v_msg = None if v_status == "valid" else "CPF inválido"

                stg_batch.append((
                    i,                  # row_idx
                    row.get("cpf", ""), # raw_cpf (original from CSV)
                    nome,               # raw_nome
                    cpf if v_status == "valid" else None,   # normalized_cpf
                    uc if uc else None,  # normalized_uc
                    v_status,
                    v_msg,
                ))

                if len(stg_batch) >= BATCH_SIZE:
                    bulk_inserir_staging_rows(cursor, staging_id, stg_batch)
                    conn.commit()
                    stg_batch = []
                    if (i + 1) % 10000 == 0:
                        print(f"  ... {i + 1:,}/{total:,} staging rows")

            if stg_batch:
                bulk_inserir_staging_rows(cursor, staging_id, stg_batch)
                conn.commit()
            print(f"  [OK] {total:,} staging rows inseridos")

        # --- Processar lotes: clientes + cliente_uc + tabela_macros ---
        print(f"\n[IMPORTAÇÃO] Processando lotes de {BATCH_SIZE} ...")
        cpf_cache = {}
        uc_cache  = {}

        for batch_start in range(start_idx, total, BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, total)
            lote = df.iloc[batch_start:batch_end]

            # Retry em caso de deadlock
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    ok, skipped, errs = processar_lote(cursor, lote, cpf_cache, uc_cache)
                    conn.commit()
                    break
                except pymysql.err.OperationalError as e:
                    if "Deadlock found" in str(e) and attempt < max_retries - 1:
                        print(f"  [DEADLOCK] Tentativa {attempt + 1}/{max_retries} falhou, tentando novamente...")
                        conn.rollback()
                        import time
                        time.sleep(1)  # Pequena pausa
                        continue
                    else:
                        raise

            ok_total += ok
            skip_total += skipped
            err_total += len(errs)
            erros_detalhe.extend(errs)

            salvar_progresso(
                csv_path.name, batch_end - 1, ok_total,
                skip_total, err_total, total, staging_id,
                iniciado_em, erros_detalhe,
            )

            if batch_end % 10000 < BATCH_SIZE:
                pct = batch_end / total * 100
                print(f"  ... {batch_end:,}/{total:,} ({pct:.1f}%)  "
                      f"ok={ok_total:,} skip={skip_total:,} err={err_total}")

        # --- Finalizar staging_imports ---
        valid_count = df[df["cpf"].str.len() == 11].shape[0] if "cpf" in df.columns else total
        cursor.execute(
            """UPDATE staging_imports
               SET status='completed', rows_success=%s, rows_failed=%s,
                   finished_at=NOW()
               WHERE id=%s""",
            (valid_count, total - valid_count, staging_id),
        )
        conn.commit()

        salvar_progresso(
            csv_path.name, total - 1, ok_total, skip_total, err_total,
            total, staging_id, iniciado_em, erros_detalhe, "concluido",
        )

        # --- Resumo ---
        print("\n" + "=" * 70)
        print("IMPORTAÇÃO CONCLUÍDA")
        print("=" * 70)
        print(f"  Staging ID       : {staging_id}")
        print(f"  Total linhas     : {total:,}")
        print(f"  Macros inseridos : {ok_total:,}")
        print(f"  Skipped          : {skip_total:,}")
        print(f"  Erros            : {err_total}")
        print("=" * 70)

    except Exception as e:
        conn.rollback()
        print(f"\n[ERRO] {e}")
        if staging_id:
            salvar_progresso(
                csv_path.name, start_idx, ok_total, skip_total, err_total,
                total, staging_id, iniciado_em, erros_detalhe, "erro",
            )
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
