"""
04_processar_retorno_macro.py
=============================
ETAPA AUTOMÁTICA — Passo 3 do ciclo da macro.

Responsabilidade:
  1. Lê o arquivo de resultado gerado pela macro (macro/dados/resultado_lote.csv).
  2. Cruza com lote_meta.json para obter o macro_id de cada linha.
  3. Chama interpretar_resposta() para mapear a resposta bruta em
     (resposta_id, novo_status).
  4. Atualiza tabela_macros com o resultado.
  5. Registros que ficaram em 'processando' mas não aparecem no resultado
     (macro parou no meio) são devolvidos para 'reprocessar' automaticamente.
  6. Arquiva os arquivos de lote com timestamp para auditoria.

Fluxo do status por registro:
  pendente     → processando  (feito pelo passo 03)
  processando  → consolidado  | reprocessar | excluido  (feito aqui)
  processando  → reprocessar  (registros sem resultado — recuperação)

Chamado por:
  macro/macro/executar_automatico.py  (após consulta_contrato.py terminar)

Uso manual:
  python etl/load/macro/04_processar_retorno_macro.py
  python etl/load/macro/04_processar_retorno_macro.py --dry-run
"""

import argparse
import json
import shutil
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pymysql

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from config import db_destino  # noqa: E402

# Importa a lógica de interpretação da camada transformation
sys.path.insert(0, str(ROOT / "etl" / "transformation" / "macro"))
from interpretar_resposta import interpretar  # noqa: E402

DB_CONFIG = db_destino(autocommit=False)

LOTE_META     = ROOT / "macro" / "dados" / "lote_meta.json"
RESULTADO_CSV = ROOT / "macro" / "dados" / "resultado_lote.csv"
ARQUIVO_DIR   = ROOT / "macro" / "dados" / "arquivo"

BATCH = 500
SEP = "=" * 70

SQL_UPDATE_MACRO = """
UPDATE tabela_macros
SET status       = %s,
    resposta_id  = %s,
    data_extracao = NOW(),
    data_update  = NOW()
WHERE id = %s
  AND status = 'processando'
"""

# Devolve para 'reprocessar' registros que ficaram presos em 'processando'
# (macro abortou antes de processar todos os registros do lote)
SQL_RECUPERAR_PROCESSANDO = """
UPDATE tabela_macros
SET status      = 'reprocessar',
    data_update = NOW()
WHERE id IN ({placeholders})
  AND status = 'processando'
"""


def carregar_meta() -> dict:
    if not LOTE_META.exists():
        print(f"  [ERRO] Arquivo de meta não encontrado: {LOTE_META}")
        sys.exit(1)
    with open(LOTE_META, encoding="utf-8") as f:
        return json.load(f)


def carregar_resultado() -> pd.DataFrame:
    if not RESULTADO_CSV.exists():
        print(f"  [ERRO] Arquivo de resultado não encontrado: {RESULTADO_CSV}")
        print("         A macro não gerou saída ou falhou antes de concluir.")
        sys.exit(1)
    df = pd.read_csv(RESULTADO_CSV, dtype=str)
    df.columns = [c.strip().lower() for c in df.columns]
    print(f"  Resultado carregado: {len(df):,} linhas  |  colunas: {list(df.columns)}")
    return df


def construir_indice_meta(meta: dict) -> dict[tuple, int]:
    """
    Monta um índice (cpf, uc_normalizada) → macro_id a partir do lote_meta.json.
    Permite correlacionar cada linha do resultado com o registro original do banco.
    """
    idx: dict[tuple, int] = {}
    for reg in meta.get("registros", []):
        cpf = str(reg.get("cpf", "")).strip().zfill(11)
        uc  = str(reg.get("codigo cliente", "")).strip().zfill(10)
        idx[(cpf, uc)] = int(reg["macro_id"])
    return idx


def normalizar_cpf(val) -> str:
    digits = "".join(c for c in str(val) if c.isdigit())
    return digits.zfill(11) if digits else ""


def normalizar_uc(val) -> str:
    digits = "".join(c for c in str(val) if c.isdigit())
    return digits.zfill(10) if digits else ""


def processar(conn, df_resultado: pd.DataFrame, meta: dict, dry_run: bool) -> dict:
    cur = conn.cursor()
    idx = construir_indice_meta(meta)
    ids_no_lote = {int(r["macro_id"]) for r in meta.get("registros", [])}

    stats = {
        "consolidado": 0,
        "reprocessar": 0,
        "excluido":    0,
        "pendente":    0,   # ERRO sem resposta — volta pra fila
        "sem_match":   0,   # linha do resultado sem macro_id correspondente
        "recuperados": 0,   # registros processando sem resultado
    }

    ids_processados: set[int] = set()
    pendentes_update: list[tuple] = []   # (novo_status, resposta_id, macro_id)

    # Detecta coluna de resposta (a macro salva como 'resposta')
    col_resposta = next(
        (c for c in df_resultado.columns if "resposta" in c.lower()), None
    )
    col_cpf = next(
        (c for c in df_resultado.columns if c in ("cpf", "cpf_cnpj")), None
    )
    col_uc = next(
        (c for c in df_resultado.columns
         if c in ("codigo cliente", "codigo_cliente", "contrato")), None
    )

    if not col_resposta:
        print("  [ERRO] Coluna 'resposta' não encontrada no arquivo de resultado.")
        sys.exit(1)

    for _, row in df_resultado.iterrows():
        cpf_norm = normalizar_cpf(row.get(col_cpf, "")) if col_cpf else ""
        uc_norm  = normalizar_uc(row.get(col_uc, "")) if col_uc else ""
        resposta_bruta = row.get(col_resposta)

        macro_id = idx.get((cpf_norm, uc_norm))
        if macro_id is None:
            stats["sem_match"] += 1
            continue

        resposta_id, novo_status = interpretar(resposta_bruta)
        stats[novo_status] = stats.get(novo_status, 0) + 1

        pendentes_update.append((novo_status, resposta_id, macro_id))
        ids_processados.add(macro_id)

        if len(pendentes_update) >= BATCH and not dry_run:
            for args in pendentes_update:
                cur.execute(SQL_UPDATE_MACRO, args)
            conn.commit()
            pendentes_update.clear()

    # Flush final
    if not dry_run and pendentes_update:
        for args in pendentes_update:
            cur.execute(SQL_UPDATE_MACRO, args)
        conn.commit()

    # Recuperação: registros do lote sem resultado (macro parou no meio)
    ids_sem_resultado = ids_no_lote - ids_processados
    if ids_sem_resultado:
        stats["recuperados"] = len(ids_sem_resultado)
        if not dry_run:
            ph = ",".join(["%s"] * len(ids_sem_resultado))
            cur.execute(
                SQL_RECUPERAR_PROCESSANDO.format(placeholders=ph),
                list(ids_sem_resultado),
            )
            conn.commit()
            print(f"  [OK] {cur.rowcount:,} registros devolvidos para 'reprocessar' (macro interrompida)")

    cur.close()
    return stats


def arquivar(dry_run: bool):
    """Move os arquivos de lote para pasta de arquivo com timestamp."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    ARQUIVO_DIR.mkdir(parents=True, exist_ok=True)

    for src in (LOTE_META, RESULTADO_CSV):
        if src.exists():
            dst = ARQUIVO_DIR / f"{src.stem}_{ts}{src.suffix}"
            if not dry_run:
                shutil.move(str(src), dst)
                print(f"  [OK] Arquivado: {dst.name}")
            else:
                print(f"  [DRY-RUN] Seria arquivado: {dst.name}")

    # Arquiva também o lote_pendente.csv
    lote_csv = ROOT / "macro" / "dados" / "lote_pendente.csv"
    if lote_csv.exists():
        dst = ARQUIVO_DIR / f"lote_pendente_{ts}.csv"
        if not dry_run:
            shutil.move(str(lote_csv), dst)
            print(f"  [OK] Arquivado: {dst.name}")


def main():
    parser = argparse.ArgumentParser(
        description="Passo 3 da macro: processa retorno e atualiza banco"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Simula sem gravar nada no banco")
    args = parser.parse_args()

    print(SEP)
    print("PASSO 04  —  Processar retorno macro → banco")
    if args.dry_run:
        print("  [DRY-RUN] nenhuma alteração será gravada")
    print(SEP)

    meta = carregar_meta()
    total_lote = len(meta.get("registros", []))
    print(f"\n  Lote original: {total_lote:,} registros | gerado em: {meta.get('gerado_em')}")

    df_resultado = carregar_resultado()

    conn = pymysql.connect(**DB_CONFIG)
    try:
        stats = processar(conn, df_resultado, meta, args.dry_run)

        print(f"\n{SEP}")
        print("RESULTADO DO PASSO 04")
        print(SEP)
        labels = {
            "consolidado": "Consolidados      ",
            "reprocessar": "Reprocessar       ",
            "excluido":    "Excluídos         ",
            "pendente":    "Devolvidos (erro) ",
            "sem_match":   "Sem match no lote ",
            "recuperados": "Recuperados*      ",
        }
        for k, label in labels.items():
            print(f"  {label}: {stats.get(k, 0):>8,}")
        print("  (*) registros em 'processando' sem resultado — devolvidos p/ reprocessar")

        arquivar(args.dry_run)

        print(f"\n{SEP}")
        print("PASSO 04 CONCLUÍDO")
        print(SEP)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
