"""
01_staging_import.py
====================
Passo 1 do pipeline operacional.

Lê todos os arquivos CSV/Excel da pasta do dia em
  dados/fornecedor2/operacional/<DD-MM-YYYY>/

e carrega nas tabelas de staging:
  staging_imports      → um registro por arquivo
  staging_import_rows  → uma linha por linha do arquivo (validada/normalizada)

NÃO toca nas tabelas de produção.

Uso:
    python etl/load/01_staging_import.py                    # pasta de hoje
    python etl/load/01_staging_import.py --data 06-04-2026  # data específica
    python etl/load/01_staging_import.py --dry-run          # sem gravar
"""

import argparse
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pymysql

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from config import db_destino  # noqa: E402

DB_CONFIG = db_destino(autocommit=False)

DISTRIBUIDORA_MAP = {
    "celp":               3,
    "celpe":              3,
    "neoenergia celpe":   3,
    "coelba":             1,
    "neoenergia coelba":  1,
    "cosern":             2,
    "neoenergia cosern":  2,
    "brasilia":           4,
    "neoenergia brasilia":4,
}

SEP = "=" * 70


# ---------------------------------------------------------------------------
# Normalização
# ---------------------------------------------------------------------------

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


def detectar_distribuidora_id(nome_arquivo: str, df: pd.DataFrame) -> int | None:
    """Detecta distribuidora_id pelo conteúdo da coluna ou pelo nome do arquivo."""
    for col in ("companhia", "operadora_energia", "lote_nome"):
        if col in df.columns:
            v = df[col].dropna()
            if not v.empty:
                chave = str(v.iloc[0]).strip().lower()
                if chave in DISTRIBUIDORA_MAP:
                    return DISTRIBUIDORA_MAP[chave]
    # Fallback: nome do arquivo
    nome = nome_arquivo.lower()
    for chave, did in DISTRIBUIDORA_MAP.items():
        if chave in nome:
            return did
    return None


# ---------------------------------------------------------------------------
# Leitura normalizada (CSV e Excel)
# ---------------------------------------------------------------------------

def ler_arquivo(filepath: Path) -> pd.DataFrame:
    """Lê o arquivo e retorna DataFrame com colunas em lowercase padronizadas."""
    ext = filepath.suffix.lower()
    if ext == ".csv":
        df = pd.read_csv(filepath, dtype=str, encoding="utf-8",
                         sep=None, engine="python")
    else:
        df = pd.read_excel(filepath, dtype=str)

    df.columns = [c.strip().lower() for c in df.columns]

    # Aliases de colunas entre formatos
    if "cpf_consultado" in df.columns and "cpf" not in df.columns:
        df = df.rename(columns={"cpf_consultado": "cpf"})
    if "estado" in df.columns and "uf" not in df.columns:
        df = df.rename(columns={"estado": "uf"})

    return df


def primeiro_telefone(row: pd.Series, df_cols: list[str]) -> str | None:
    """Retorna o primeiro telefone não-vazio da linha (para staging)."""
    tel_cols = [c for c in df_cols if re.match(r"telefone\D*\d+$", c)]
    for col in tel_cols:
        v = row.get(col)
        if v and str(v).strip() not in ("", "nan", "None"):
            return str(v).strip()[:255]
    return None


# ---------------------------------------------------------------------------
# Processamento por arquivo
# ---------------------------------------------------------------------------

def processar_arquivo(conn, filepath: Path, dry_run: bool) -> dict:
    """Carrega um arquivo no staging. Retorna stats."""
    cur = conn.cursor()
    df = ler_arquivo(filepath)
    distrib_id = detectar_distribuidora_id(filepath.name, df)
    n_total = len(df)
    df_cols = list(df.columns)

    # Checa se este arquivo já foi importado
    # filename_curto = <pasta_data>/<arquivo>  (ex: 16-04-2026/COELBA_35K.csv)
    pasta_pai = filepath.parent.name
    if not re.match(r"^\d{2}-\d{2}-\d{4}$", pasta_pai):
        print(f"  [ERRO] Pasta pai '{pasta_pai}' não é uma data DD-MM-YYYY. "
              f"Arquivo ignorado: {filepath.name}")
        cur.close()
        return {"staging_id": 0, "total": 0, "valid": 0,
                "invalid": 0, "skipped": True}
    filename_curto = f"{pasta_pai}/{filepath.name}"
    cur.execute(
        "SELECT id, status FROM staging_imports WHERE filename=%s LIMIT 1",
        (filename_curto,),
    )
    existente = cur.fetchone()
    if existente:
        sid, st = existente
        if st == "processing":
            # Crash recovery: limpa importação parcial e reimporta
            print(f"  [RECOVERY] {filepath.name} está preso em 'processing' "
                  f"(id={sid}). Limpando para reimportar...")
            if not dry_run:
                cur.execute(
                    "DELETE FROM staging_import_rows WHERE staging_id=%s", (sid,)
                )
                cur.execute(
                    "DELETE FROM staging_imports WHERE id=%s", (sid,)
                )
                conn.commit()
        else:
            print(f"  [SKIP] {filepath.name} já existe no staging "
                  f"(id={sid}, status={st})")
            cur.close()
            return {"staging_id": sid, "total": 0, "valid": 0,
                    "invalid": 0, "skipped": True}

    print(f"\n  Arquivo : {filepath.name}")
    print(f"  Linhas  : {n_total:,}")
    print(f"  Distrib : id={distrib_id}")

    if not dry_run:
        cur.execute(
            """INSERT INTO staging_imports
               (filename, distribuidora_nome, target_macro_table,
                total_rows, status, imported_by, started_at)
               VALUES (%s, %s, %s, %s, 'processing', 'pipeline_operacional', NOW())""",
            (
                filename_curto,
                str(distrib_id) if distrib_id else None,
                "tabela_macros",
                n_total,
            ),
        )
        staging_id = cur.lastrowid
        conn.commit()
    else:
        staging_id = 0

    n_valid = n_invalid = 0
    buf = []

    for idx, row in df.iterrows():
        cpf_raw = row.get("cpf")
        norm_cpf_val = normalizar_cpf(cpf_raw)
        nome = str(row.get("nome", "") or "").strip()[:255] or None
        tel_raw = primeiro_telefone(row, df_cols)
        endereco_raw = str(row.get("endereco", "") or "").strip()[:255] or None

        norm_uc_val = normalizar_uc(row.get("uc"))

        # Validação
        if norm_cpf_val is None:
            vstatus, vmsg = "invalid", "CPF invalido ou ausente"
        elif norm_uc_val is None:
            vstatus, vmsg = "invalid", "UC ausente ou invalida"
        elif distrib_id is None:
            vstatus, vmsg = "invalid", "Distribuidora nao identificada"
        else:
            vstatus, vmsg = "valid", None
            n_valid += 1

        if vstatus == "invalid":
            n_invalid += 1

        buf.append((
            staging_id, int(idx),
            str(cpf_raw)[:50] if cpf_raw else None,
            nome, tel_raw, endereco_raw,
            norm_cpf_val, norm_uc_val, vstatus, vmsg,
        ))

        if len(buf) >= 500 and not dry_run:
            cur.executemany(
                """INSERT INTO staging_import_rows
                   (staging_id, row_idx, raw_cpf, raw_nome, raw_telefone,
                    raw_endereco, normalized_cpf, normalized_uc,
                    validation_status, validation_message)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                buf,
            )
            conn.commit()
            buf.clear()

    if buf and not dry_run:
        cur.executemany(
            """INSERT INTO staging_import_rows
               (staging_id, row_idx, raw_cpf, raw_nome, raw_telefone,
                raw_endereco, normalized_cpf, normalized_uc,
                validation_status, validation_message)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            buf,
        )
        conn.commit()

    if not dry_run:
        cur.execute(
            """UPDATE staging_imports
               SET status='completed', rows_success=%s, rows_failed=%s, finished_at=NOW()
               WHERE id=%s""",
            (n_valid, n_invalid, staging_id),
        )
        conn.commit()

    cur.close()
    print(f"  Válidas : {n_valid:,}  |  Inválidas: {n_invalid:,}")
    return {
        "staging_id": staging_id,
        "total": n_total,
        "valid": n_valid,
        "invalid": n_invalid,
        "skipped": False,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data", default=None,
        help="Data da pasta DD-MM-YYYY (padrão: hoje)",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    data_str = args.data or datetime.today().strftime("%d-%m-%Y")
    pasta = ROOT / "dados" / "fornecedor2" / "operacional" / data_str

    if not pasta.exists():
        print(f"[ERRO] Pasta não encontrada: {pasta}")
        sys.exit(1)

    arquivos = sorted(
        f for f in pasta.iterdir()
        if f.suffix.lower() in (".csv", ".xlsx", ".xls")
    )

    if not arquivos:
        print(f"[AVISO] Nenhum arquivo CSV/Excel em {pasta}")
        sys.exit(0)

    print(SEP)
    print(f"STAGING IMPORT  —  {data_str}  ({len(arquivos)} arquivo(s))")
    print(SEP)

    if args.dry_run:
        print("[INFO] DRY-RUN — nada será gravado.\n")

    conn = pymysql.connect(**DB_CONFIG)
    resultados = []
    try:
        for fp in arquivos:
            stats = processar_arquivo(conn, fp, args.dry_run)
            resultados.append((fp.name, stats))
    finally:
        conn.close()

    print(f"\n{SEP}")
    print("RESUMO STAGING")
    print(SEP)
    total_v = total_i = 0
    for nome, s in resultados:
        skip = " [JÁ EXISTIA]" if s.get("skipped") else ""
        print(f"  {nome:<45} valid={s['valid']:>6,}  invalid={s['invalid']:>4,}"
              f"  staging_id={s['staging_id']}{skip}")
        total_v += s["valid"]
        total_i += s["invalid"]
    print(f"\n  TOTAL  valid={total_v:,}  invalid={total_i:,}")
    print(SEP)


if __name__ == "__main__":
    main()
