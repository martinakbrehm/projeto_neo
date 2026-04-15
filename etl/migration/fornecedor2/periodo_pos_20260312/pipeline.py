"""
etl/migration/fornecedor2/periodo_pos_20260312/pipeline.py
==========================================================
Pipeline de migracao historica — Periodo pos 12/03/2026.

Dados de origem:
    dados/fornecedor2/migration_periodo_pos_20260312/raw/
        clientes_300k_25_03.csv              (300k registros de entrada)
        saida_unica_dados_filtrados_sem_erros.xlsx  (293k resultados das macros)

Steps:
    1. 01_normalizar_historico.py   — Cruza CSV de entrada com Excel de saída,
       mapeia status, gera CSV normalizado pronto para importação.
    2. 02_importar_historico.py     — Importa para o banco: staging_imports,
       staging_import_rows, clientes, cliente_uc, tabela_macros.

Uso:
    python etl/migration/fornecedor2/periodo_pos_20260312/pipeline.py
    python etl/migration/fornecedor2/periodo_pos_20260312/pipeline.py --step 1
    python etl/migration/fornecedor2/periodo_pos_20260312/pipeline.py --step 2
    python etl/migration/fornecedor2/periodo_pos_20260312/pipeline.py --dry-run
"""

import subprocess
import sys
import argparse
from pathlib import Path

SCRIPTS = [
    ("01_normalizar_historico.py", "Normalizar CSV + Excel → CSV normalizado"),
    ("02_importar_historico.py",   "Importar CSV normalizado → banco de dados"),
]

HERE = Path(__file__).resolve().parent


def run_step(idx: int, dry_run: bool = False):
    script, desc = SCRIPTS[idx]
    path = HERE / script
    print(f"\n{'='*60}")
    print(f"STEP {idx+1}: {desc}")
    print(f"  Script: {script}")
    print(f"{'='*60}\n")

    cmd = [sys.executable, str(path)]
    if dry_run and idx == 1:
        cmd.append("--dry-run")

    result = subprocess.run(cmd, cwd=str(HERE.parents[3]))
    if result.returncode != 0:
        print(f"\n[ERRO] Step {idx+1} falhou (exit code {result.returncode})")
        sys.exit(result.returncode)


def main():
    parser = argparse.ArgumentParser(description="Pipeline migracao periodo pos 20260312")
    parser.add_argument("--step", type=int, default=0, help="Executar step especifico (1 ou 2)")
    parser.add_argument("--dry-run", action="store_true", help="Modo simulacao (step 2)")
    parser.add_argument("--list", action="store_true", help="Listar steps")
    args = parser.parse_args()

    if args.list:
        for i, (script, desc) in enumerate(SCRIPTS):
            print(f"  Step {i+1}: {desc}  ({script})")
        return

    if args.step:
        run_step(args.step - 1, args.dry_run)
    else:
        for i in range(len(SCRIPTS)):
            run_step(i, args.dry_run)

    print(f"\n{'='*60}")
    print("Pipeline concluido.")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
