"""
etl/migration/pipeline.py
=========================
Orquestrador da pipeline de migracao historica.

Executa os 4 steps em sequencia, com log estruturado, tracking de status
por step, relatorio final e suporte a execucao parcial.

Uso:
    python etl/migration/pipeline.py                  # todos os steps
    python etl/migration/pipeline.py --steps 1 2      # steps especificos
    python etl/migration/pipeline.py --from-step 3    # a partir do step 3
    python etl/migration/pipeline.py --dry-run        # repassa --dry-run aos scripts
    python etl/migration/pipeline.py --list           # lista steps sem executar

Steps:
    1  normalizar_historico    Normaliza Excel -> CSV intermediario
    2  importar_historico_csv  Importa CSV normalizado -> banco (bulk + checkpoint)
    3  enriquecer_clientes     Enriquece nome/endereco/tel via controle_bases.neo
"""

import argparse
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Definicao dos steps
# ---------------------------------------------------------------------------
MIGRATION_DIR = Path(__file__).resolve().parent
LOG_FILE      = MIGRATION_DIR / "migration_run.log"


@dataclass
class Step:
    id:          int
    name:        str
    script:      str           # caminho relativo a MIGRATION_DIR
    description: str
    args:        list[str] = field(default_factory=list)   # args fixos adicionais
    dry_run_arg: str = "--dry-run"                         # flag de dry-run do script
    supports_dry_run: bool = True


STEPS: list[Step] = [
    Step(
        id=1,
        name="normalizar_historico",
        script="01_normalizar_historico.py",
        description="Normaliza arquivos Excel de dados/ -> CSV intermediario",
        supports_dry_run=False,
    ),
    Step(
        id=2,
        name="importar_historico_csv",
        script="02_importar_historico_csv.py",
        description="Importa CSV normalizado -> banco em bulk com checkpoint/resume",
    ),
    Step(
        id=3,
        name="enriquecer_clientes",
        script="03_enriquecer_clientes.py",
        description="Enriquece clientes com nome, endereco e telefone via controle_bases.neo",
        args=["--batch", "2000", "--chunk", "50000"],
    ),
]


# ---------------------------------------------------------------------------
# Formatacao de output
# ---------------------------------------------------------------------------
WIDTH = 72

def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _log(msg: str, file=None):
    line = f"[{_ts()}] {msg}"
    print(line)
    if file:
        file.write(line + "\n")
        file.flush()

def _sep(char="=", file=None):
    line = char * WIDTH
    print(line)
    if file:
        file.write(line + "\n")
        file.flush()

def _header(title: str, file=None):
    _sep(file=file)
    _log(f"  {title}", file=file)
    _sep(file=file)


# ---------------------------------------------------------------------------
# Execucao de um step
# ---------------------------------------------------------------------------

def run_step(step: Step, dry_run: bool, log_file) -> tuple[bool, float]:
    """
    Executa um step como subprocesso.
    Retorna (sucesso, duracao_segundos).
    Output do script e passado ao vivo para o terminal E gravado no log.
    """
    script_path = MIGRATION_DIR / step.script
    cmd = [sys.executable, "-u", str(script_path)] + step.args

    if dry_run and step.supports_dry_run:
        cmd.append(step.dry_run_arg)

    _log(f"  Comando: {' '.join(cmd)}", file=log_file)
    _sep("-", file=log_file)

    t0 = time.time()
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        for line in proc.stdout:
            line = line.rstrip("\n")
            print(f"  | {line}")
            log_file.write(f"  | {line}\n")
            log_file.flush()

        proc.wait()
        duration = time.time() - t0
        success  = proc.returncode == 0
    except Exception as exc:
        duration = time.time() - t0
        success  = False
        msg = f"[EXCEPTION] {exc}"
        print(msg)
        log_file.write(msg + "\n")

    return success, duration


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def run_pipeline(selected_steps: list[int], dry_run: bool):
    start_time = time.time()

    steps_to_run = [s for s in STEPS if s.id in selected_steps]

    with open(LOG_FILE, "a", encoding="utf-8") as lf:
        _header(f"MIGRATION PIPELINE  |  {_ts()}", file=lf)
        mode_label = "DRY-RUN" if dry_run else "PRODUCAO"
        _log(f"  Modo: {mode_label}  |  Steps: {[s.id for s in steps_to_run]}", file=lf)
        _sep(file=lf)

        results: list[dict] = []

        for step in steps_to_run:
            _log(f"  STEP {step.id}/{len(STEPS)}  [{step.name}]", file=lf)
            _log(f"  {step.description}", file=lf)
            _sep("-", file=lf)

            success, duration = run_step(step, dry_run, lf)
            status = "OK" if success else "FALHOU"

            results.append({
                "id":       step.id,
                "name":     step.name,
                "status":   status,
                "duration": duration,
            })

            marker = "v" if success else "X"
            _log(f"  [{marker}] Step {step.id} {status}  ({duration:.1f}s)", file=lf)
            _sep(file=lf)

            if not success:
                _log("  Pipeline interrompida: step falhou. Use --from-step para retomar.", file=lf)
                _sep(file=lf)
                _print_summary(results, time.time() - start_time, lf)
                return False

        _print_summary(results, time.time() - start_time, lf)
        return all(r["status"] == "OK" for r in results)


def _print_summary(results: list[dict], elapsed: float, log_file):
    _header("RELATORIO FINAL", file=log_file)
    for r in results:
        marker = "v" if r["status"] == "OK" else "X"
        _log(
            f"  [{marker}] Step {r['id']:>2}  {r['name']:<30}  "
            f"{r['status']:<8}  {r['duration']:>7.1f}s",
            file=log_file,
        )
    _sep("-", file=log_file)
    total_ok = sum(1 for r in results if r["status"] == "OK")
    _log(f"  Steps concluidos: {total_ok}/{len(results)}  |  Tempo total: {elapsed:.1f}s", file=log_file)
    _sep(file=log_file)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Pipeline de migracao historica de dados.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--steps", nargs="+", type=int, metavar="N",
        help="Executa apenas os steps especificados (ex: --steps 1 2)",
    )
    parser.add_argument(
        "--from-step", type=int, metavar="N",
        help="Executa a partir do step N ate o final",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Repassa --dry-run para os scripts que suportam (sem gravar no banco)",
    )
    parser.add_argument(
        "--list", action="store_true",
        help="Lista os steps disponíveis sem executar",
    )
    args = parser.parse_args()

    if args.list:
        print("\nSteps disponíveis:\n")
        for s in STEPS:
            dry_label = "(suporta --dry-run)" if s.supports_dry_run else "(sem --dry-run)"
            print(f"  {s.id}  {s.name:<30}  {dry_label}")
            print(f"     {s.description}")
        print()
        return

    # Determina quais steps rodar
    if args.steps:
        selected = sorted(set(args.steps))
    elif args.from_step:
        selected = [s.id for s in STEPS if s.id >= args.from_step]
    else:
        selected = [s.id for s in STEPS]

    invalid = [n for n in selected if n not in {s.id for s in STEPS}]
    if invalid:
        print(f"[ERRO] Steps invalidos: {invalid}. Escolha entre 1-{len(STEPS)}.")
        sys.exit(1)

    print(f"\nLog gravado em: {LOG_FILE}\n")
    success = run_pipeline(selected, dry_run=args.dry_run)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
