"""
Migration: 20260415_backfill_normalized_uc_dia6
===============================================
Preenche retroativamente staging_import_rows.normalized_uc para os arquivos
do dia 06-04-2026 (staging_id 1, 2, 3, 4).

Contexto:
    A coluna normalized_uc foi adicionada em staging_import_rows em 10/04/2026
    (migration 20260410_cliente_uc_id_tabela_macros). Os arquivos do dia 06-04-2026
    foram importados ANTES dessa coluna existir e por isso ficaram com
    normalized_uc = NULL, mesmo que os arquivos originais contenham a UC.

O que este script faz:
    1. Relê cada arquivo original (CSV/XLSX) do dia 06-04-2026.
    2. Para cada linha, extrai normalized_cpf e normalized_uc.
    3. Faz UPDATE em staging_import_rows por (staging_id, row_idx),
       preenchendo normalized_uc onde está NULL.

Staging IDs afetados:
    staging_id=1 → 06-04-2026/35K_20260402_CELP.csv    (distribuidora_id=3)
    staging_id=2 → 06-04-2026/35K_20260402_COELBA.csv  (distribuidora_id=1)
    staging_id=3 → 06-04-2026/35K_20260402_COSERN.csv  (distribuidora_id=2)
    staging_id=4 → 06-04-2026/celpe_final_3103.xlsx    (distribuidora_id=3)

Uso:
    python db/improvements/20260415_backfill_normalized_uc_dia6/migration.py
    python db/improvements/20260415_backfill_normalized_uc_dia6/migration.py --dry-run
"""

import argparse
import re
import sys
from pathlib import Path

import pandas as pd
import pymysql

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from config import db_destino  # noqa: E402

SEP = "=" * 70

# Mapeamento: staging_id -> (caminho_relativo_ao_root, col_cpf, col_uc, separador_csv_ou_None_se_xlsx)
STAGING_FILES = {
    1: ("dados/fornecedor2/operacional/06-04-2026/35K_20260402_CELP.csv",   "cpf",           "uc", ";"),
    2: ("dados/fornecedor2/operacional/06-04-2026/35K_20260402_COELBA.csv", "cpf",           "uc", ";"),
    3: ("dados/fornecedor2/operacional/06-04-2026/35K_20260402_COSERN.csv", "cpf",           "uc", ";"),
    4: ("dados/fornecedor2/operacional/06-04-2026/celpe_final_3103.xlsx",   "cpf_consultado","uc", None),
}

BATCH_SIZE = 1000


def normalizar_cpf(val) -> str | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = re.sub(r"\D", "", str(val).split(".")[0].strip())
    s = s.zfill(11)
    return s if len(s) == 11 else None


def normalizar_uc(val) -> str | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = re.sub(r"\D", "", str(val).split(".")[0].strip())
    return s.zfill(10) if s else None


def carregar_arquivo(path: Path, col_cpf: str, col_uc: str, sep: str | None) -> pd.DataFrame:
    """Carrega o arquivo e retorna DataFrame com colunas [row_idx, normalized_cpf, normalized_uc]."""
    if sep is not None:
        df = pd.read_csv(path, sep=sep, dtype=str, encoding="utf-8-sig")
    else:
        df = pd.read_excel(path, dtype=str)

    df.columns = [c.strip().lower() for c in df.columns]
    col_cpf = col_cpf.lower()
    col_uc  = col_uc.lower()

    if col_cpf not in df.columns:
        raise ValueError(f"Coluna '{col_cpf}' não encontrada. Colunas: {df.columns.tolist()}")
    if col_uc not in df.columns:
        raise ValueError(f"Coluna '{col_uc}' não encontrada. Colunas: {df.columns.tolist()}")

    result = []
    for idx, row in df.iterrows():
        norm_cpf = normalizar_cpf(row[col_cpf])
        norm_uc  = normalizar_uc(row[col_uc])
        result.append((int(idx), norm_cpf, norm_uc))

    return result


def run(dry_run: bool = False):
    print(SEP)
    print("MIGRATION 20260415 — backfill normalized_uc em staging_import_rows (dia 06-04-2026)")
    print("Modo: DRY-RUN (sem alterações)" if dry_run else "Modo: EXECUÇÃO REAL")
    print(SEP)

    conn = pymysql.connect(**db_destino(), connect_timeout=30)
    cur  = conn.cursor()

    total_atualizados = 0

    for staging_id, (rel_path, col_cpf, col_uc, sep) in STAGING_FILES.items():
        full_path = ROOT / rel_path
        print(f"\n[staging_id={staging_id}] {rel_path}")

        if not full_path.exists():
            print(f"  AVISO: arquivo não encontrado em {full_path} — pulando.")
            continue

        # Verificar quantas linhas ainda estão com NULL
        cur.execute(
            "SELECT COUNT(*) FROM staging_import_rows "
            "WHERE staging_id = %s AND normalized_uc IS NULL AND validation_status = 'valid'",
            (staging_id,),
        )
        qtd_null = cur.fetchone()[0]
        if qtd_null == 0:
            print("  Todas as UCs já estão preenchidas — pulando.")
            continue

        print(f"  Linhas válidas com normalized_uc=NULL: {qtd_null:,}")
        print(f"  Carregando arquivo...")

        rows = carregar_arquivo(full_path, col_cpf, col_uc, sep)
        total_lidos = len(rows)
        com_uc      = sum(1 for _, _, uc in rows if uc)
        sem_uc      = total_lidos - com_uc
        print(f"  Arquivo lido: {total_lidos:,} linhas | Com UC: {com_uc:,} | Sem UC: {sem_uc:,}")

        # Preparar updates: apenas onde há UC válida
        updates = [
            (norm_uc, staging_id, row_idx)
            for row_idx, norm_cpf, norm_uc in rows
            if norm_uc
        ]

        print(f"  Updates a executar: {len(updates):,}")

        if dry_run:
            # Mostrar amostra
            for u in updates[:5]:
                print(f"    [DRY] SET normalized_uc='{u[0]}' WHERE staging_id={u[1]} AND row_idx={u[2]}")
            if len(updates) > 5:
                print(f"    [DRY] ... e mais {len(updates) - 5:,} updates")
        else:
            # Estratégia eficiente: tabela temporária + UPDATE JOIN (uma única query)
            print(f"  Criando tabela temporária e carregando {len(updates):,} UCs...")
            cur.execute("DROP TEMPORARY TABLE IF EXISTS tmp_backfill_uc")
            cur.execute("""
                CREATE TEMPORARY TABLE tmp_backfill_uc (
                    row_idx   INT NOT NULL,
                    norm_uc   CHAR(10) NOT NULL,
                    PRIMARY KEY (row_idx)
                )
            """)

            # INSERT em lotes para a temp table (bulk values)
            for i in range(0, len(updates), BATCH_SIZE):
                batch = updates[i:i + BATCH_SIZE]
                placeholders = ",".join(["(%s,%s)"] * len(batch))
                flat = [v for row_idx, _, norm_uc in [(u[2], u[1], u[0]) for u in batch] for v in (row_idx, norm_uc)]
                cur.execute(
                    f"INSERT INTO tmp_backfill_uc (row_idx, norm_uc) VALUES {placeholders}",
                    flat,
                )
            conn.commit()
            print(f"  Temp table populada. Executando UPDATE JOIN...")

            cur.execute(f"""
                UPDATE staging_import_rows sir
                JOIN tmp_backfill_uc t ON t.row_idx = sir.row_idx
                SET sir.normalized_uc = t.norm_uc
                WHERE sir.staging_id = {staging_id}
                  AND sir.normalized_uc IS NULL
            """)
            conn.commit()
            atualizados = cur.rowcount
            cur.execute("DROP TEMPORARY TABLE IF EXISTS tmp_backfill_uc")

            print(f"  Linhas atualizadas: {atualizados:,}")
            total_atualizados += atualizados

            # Verificar resultado
            cur.execute(
                "SELECT COUNT(*) FROM staging_import_rows "
                "WHERE staging_id = %s AND normalized_uc IS NOT NULL AND validation_status = 'valid'",
                (staging_id,),
            )
            preenchidas = cur.fetchone()[0]
            print(f"  Verificação pós-update: {preenchidas:,} linhas válidas com UC preenchida.")

    print()
    print(SEP)
    if dry_run:
        print("DRY-RUN concluído — nenhuma alteração foi feita.")
    else:
        print(f"Migration concluída. Total de linhas atualizadas: {total_atualizados:,}")
    print(SEP)

    cur.close()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Não executa alterações, apenas simula.")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
