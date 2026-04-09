"""
migration.py
============
Melhoria 20260409 — Truncar staging_imports.filename para data/arquivo

Problema:
    O campo `filename` em `staging_imports` armazenava o caminho completo
    do arquivo no sistema de arquivos local (ex:
    C:\\Users\\...\\dados\\fornecedor2\\operacional\\06-04-2026\\35K_CELP.csv).

O que este script faz:
    Atualiza `filename` para manter apenas os 2 últimos segmentos do caminho,
    que identificam exclusivamente o lote e o arquivo sem expor paths locais.
    Exemplo: '06-04-2026/35K_20260402_CELP.csv'

    Registros cujo filename já esteja no formato curto (sem barra ou apenas
    1 segmento) não são tocados.

Rollback:
    Não há rollback automático — os paths completos não são reconstructíveis.
    Faça backup antes se necessário.

Uso:
    python db/improvements/20260409_truncar_filename_staging/migration.py
    python db/improvements/20260409_truncar_filename_staging/migration.py --dry-run
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


SQL_COUNT = """
    SELECT COUNT(*)
    FROM staging_imports
    WHERE CHAR_LENGTH(filename) - CHAR_LENGTH(REPLACE(filename, '/', '')) +
          CHAR_LENGTH(filename) - CHAR_LENGTH(REPLACE(filename, '\\\\', '')) > 1
"""

SQL_SAMPLE = """
    SELECT id, filename,
           SUBSTRING_INDEX(REPLACE(filename, '\\\\', '/'), '/', -2) AS novo
    FROM staging_imports
    WHERE CHAR_LENGTH(filename) - CHAR_LENGTH(REPLACE(filename, '/', '')) +
          CHAR_LENGTH(filename) - CHAR_LENGTH(REPLACE(filename, '\\\\', '')) > 1
    LIMIT 10
"""

SQL_UPDATE = """
    UPDATE staging_imports
    SET filename = SUBSTRING_INDEX(REPLACE(filename, '\\\\', '/'), '/', -2)
    WHERE CHAR_LENGTH(filename) - CHAR_LENGTH(REPLACE(filename, '/', '')) +
          CHAR_LENGTH(filename) - CHAR_LENGTH(REPLACE(filename, '\\\\', '')) > 1
"""


def run(dry_run: bool):
    log(SEP)
    log("MIGRATION 20260409 -- Truncar filename -> data/arquivo")
    log("Modo: DRY-RUN (sem alteracoes)" if dry_run else "Modo: EXECUCAO REAL")
    log(SEP)

    conn = pymysql.connect(**db_destino(), connect_timeout=30)
    cur  = conn.cursor()

    log("\n[1/2] Contando registros a atualizar...")
    cur.execute(SQL_COUNT)
    total = cur.fetchone()[0]
    log(f"      Total: {total:,} registros")

    if total == 0:
        log("\nNenhum registro para atualizar. Tudo ja esta no formato curto.")
        cur.close(); conn.close()
        log(SEP)
        return

    log("\n[2/2] Amostra (primeiros 10):")
    cur.execute(SQL_SAMPLE)
    for sid, atual, novo in cur.fetchall():
        log(f"    id={sid}")
        log(f"      antes: {atual}")
        log(f"      depois: {novo}")

    if dry_run:
        log(f"\nDry-run concluido. {total:,} registros seriam atualizados.")
        log("Execute sem --dry-run para aplicar.")
        cur.close(); conn.close()
        log(SEP)
        return

    log(f"\n[UPDATE] Executando ({total:,} registros)...")
    cur.execute(SQL_UPDATE)
    conn.commit()
    log(f"  OK  {cur.rowcount:,} linhas atualizadas.")

    cur.close()
    conn.close()
    log(SEP)
    log("Migration concluida com sucesso.")
    log(SEP)


if __name__ == "__main__":
    import traceback
    parser = argparse.ArgumentParser(description="Truncar filename staging_imports")
    parser.add_argument("--dry-run", action="store_true", help="Simula sem alterar o banco")
    args = parser.parse_args()
    try:
        run(dry_run=args.dry_run)
    except Exception:
        traceback.print_exc()
        sys.exit(1)
