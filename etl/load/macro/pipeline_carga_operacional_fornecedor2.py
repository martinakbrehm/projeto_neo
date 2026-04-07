"""
pipeline_carga_operacional_fornecedor2.py
=========================================
Pipeline operacional completo para carga de dados novos do dia — fornecedor2.

Responsabilidade: importar os arquivos CSV recebidos diariamente do fornecedor2
para o banco de dados (staging → produção), populando clientes, cliente_uc e
tabela_macros com status='pendente' para processamento pela macro.

Executa em sequência:
  Passo 1 → 01_staging_import.py    (arquivos CSV do dia → staging)
  Passo 2 → 02_processar_staging.py (staging → produção)

Uso:
    python etl/load/macro/pipeline_carga_operacional_fornecedor2.py                    # data de hoje
    python etl/load/macro/pipeline_carga_operacional_fornecedor2.py --data 06-04-2026  # data específica
    python etl/load/macro/pipeline_carga_operacional_fornecedor2.py --dry-run           # simula sem gravar
    python etl/load/macro/pipeline_carga_operacional_fornecedor2.py --so-staging        # apenas passo 1
    python etl/load/macro/pipeline_carga_operacional_fornecedor2.py --so-processar      # apenas passo 2
"""

import argparse
import importlib.util
import sys
import time
from datetime import datetime
from pathlib import Path

SEP = "=" * 70
ETL_DIR = Path(__file__).resolve().parent


def carregar_modulo(nome: str, caminho: Path):
    """Importa um arquivo .py como módulo pelo caminho absoluto."""
    spec = importlib.util.spec_from_file_location(nome, caminho)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def main():
    parser = argparse.ArgumentParser(
        description="Pipeline de carga operacional — fornecedor2"
    )
    parser.add_argument(
        "--data", default=None,
        help="Data da pasta DD-MM-YYYY (padrão: hoje)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Simula sem gravar nada",
    )
    parser.add_argument(
        "--so-staging", action="store_true",
        help="Executa apenas o passo 1 (staging import)",
    )
    parser.add_argument(
        "--so-processar", action="store_true",
        help="Executa apenas o passo 2 (produção)",
    )
    args = parser.parse_args()

    data_str = args.data or datetime.today().strftime("%d-%m-%Y")
    t0 = time.time()

    print(SEP)
    print(f"PIPELINE CARGA OPERACIONAL  |  fornecedor2  |  {data_str}")
    if args.dry_run:
        print("  [DRY-RUN] nenhuma alteração será gravada")
    print(SEP)

    # -----------------------------------------------------------------------
    # PASSO 1 — Staging Import
    # -----------------------------------------------------------------------
    if not args.so_processar:
        print(f"\n{'-' * 70}")
        print(f"PASSO 1  --  Staging import  ({data_str})")
        print(f"{'-' * 70}")

        argv_staging = ["01_staging_import.py"]
        if args.data:
            argv_staging += ["--data", args.data]
        if args.dry_run:
            argv_staging.append("--dry-run")

        sys.argv = argv_staging
        mod1 = carregar_modulo("staging_import",
                               ETL_DIR / "01_staging_import.py")
        mod1.main()

    # -----------------------------------------------------------------------
    # PASSO 2 — Processar Staging → Produção
    # -----------------------------------------------------------------------
    if not args.so_staging:
        print(f"\n{'-' * 70}")
        print("PASSO 2  --  Processar staging -> producao")
        print(f"{'-' * 70}")

        argv_proc = ["02_processar_staging.py"]
        if args.dry_run:
            argv_proc.append("--dry-run")

        sys.argv = argv_proc
        mod2 = carregar_modulo("processar_staging",
                               ETL_DIR / "02_processar_staging.py")
        mod2.main()

    # -----------------------------------------------------------------------
    # Resumo final
    # -----------------------------------------------------------------------
    elapsed = time.time() - t0
    print(f"\n{SEP}")
    print(f"Pipeline concluído em {elapsed:.1f}s"
          + (" [DRY-RUN]" if args.dry_run else ""))
    print(SEP)


if __name__ == "__main__":
    main()
