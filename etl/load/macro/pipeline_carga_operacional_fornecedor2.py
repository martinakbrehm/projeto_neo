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

try:
    import pymysql
    _MYSQL_ERROS_RETRIAVEL = (2013, 1205, 2006, 1213)  # lost conn, lock timeout, gone away, deadlock
except ImportError:
    pymysql = None
    _MYSQL_ERROS_RETRIAVEL = ()

SEP = "=" * 70
ETL_DIR = Path(__file__).resolve().parent

MAX_TENTATIVAS = 5          # quantas vezes tenta antes de desistir
ESPERA_BASE_S  = 30         # segundos de espera antes de cada reintento
FATOR_BACKOFF  = 2          # dobra a espera a cada falha consecutiva


def _e_erro_retriavel(exc: Exception) -> bool:
    """Retorna True se o erro é transitório e vale a pena tentar novamente."""
    if pymysql and isinstance(exc, pymysql.err.OperationalError):
        codigo = exc.args[0] if exc.args else 0
        return codigo in _MYSQL_ERROS_RETRIAVEL
    # timeout genérico de socket
    if isinstance(exc, TimeoutError):
        return True
    nome = type(exc).__name__
    return "Timeout" in nome or "Lost" in nome or "Deadlock" in nome.capitalize()


def carregar_modulo(nome: str, caminho: Path):
    """Importa um arquivo .py como módulo pelo caminho absoluto."""
    spec = importlib.util.spec_from_file_location(nome, caminho)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def executar_com_retry(nome_passo: str, fn):
    """
    Executa fn() e reintenta automaticamente em caso de erros transitórios.
    Cada nova tentativa recarrega o módulo do zero para garantir conexão limpa.
    """
    espera = ESPERA_BASE_S
    for tentativa in range(1, MAX_TENTATIVAS + 1):
        try:
            fn()
            return  # sucesso
        except Exception as exc:
            if tentativa == MAX_TENTATIVAS or not _e_erro_retriavel(exc):
                print(f"\n[ERRO] {nome_passo} falhou na tentativa {tentativa}/{MAX_TENTATIVAS}: {exc}")
                raise
            print(f"\n[AVISO] {nome_passo} tentativa {tentativa}/{MAX_TENTATIVAS} falhou: "
                  f"{type(exc).__name__}({exc.args[0] if exc.args else ''})")
            print(f"[AVISO] Aguardando {espera}s antes de reiniciar...")
            time.sleep(espera)
            espera = min(espera * FATOR_BACKOFF, 300)  # máximo 5 min
            print(f"[INFO] Reiniciando {nome_passo} (tentativa {tentativa + 1}/{MAX_TENTATIVAS})...")


def main():
    parser = argparse.ArgumentParser(
        description="Pipeline de carga operacional - fornecedor2"
    )
    parser.add_argument(
        "--data", default=None,
        help="Data da pasta DD-MM-YYYY (padrao: hoje)",
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
        help="Executa apenas o passo 2 (producao)",
    )
    args = parser.parse_args()

    data_str = args.data or datetime.today().strftime("%d-%m-%Y")
    t0 = time.time()

    print(SEP)
    print(f"PIPELINE CARGA OPERACIONAL  |  fornecedor2  |  {data_str}")
    if args.dry_run:
        print("  [DRY-RUN] nenhuma alteracao sera gravada")
    print(f"  [RETRY] max {MAX_TENTATIVAS} tentativas por passo, backoff {ESPERA_BASE_S}s base")
    print(SEP)

    # -----------------------------------------------------------------------
    # PASSO 1 -- Staging Import
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

        def _rodar_staging():
            sys.argv = argv_staging[:]
            mod1 = carregar_modulo("staging_import",
                                   ETL_DIR / "01_staging_import.py")
            mod1.main()

        executar_com_retry("PASSO 1 (staging import)", _rodar_staging)

    # -----------------------------------------------------------------------
    # PASSO 2 -- Processar Staging -> Producao
    # -----------------------------------------------------------------------
    if not args.so_staging:
        print(f"\n{'-' * 70}")
        print("PASSO 2  --  Processar staging -> producao")
        print(f"{'-' * 70}")

        argv_proc = ["02_processar_staging.py"]
        if args.dry_run:
            argv_proc.append("--dry-run")

        def _rodar_processar():
            sys.argv = argv_proc[:]
            mod2 = carregar_modulo("processar_staging",
                                   ETL_DIR / "02_processar_staging.py")
            mod2.main()

        executar_com_retry("PASSO 2 (processar staging)", _rodar_processar)

    # -----------------------------------------------------------------------
    # Resumo final
    # -----------------------------------------------------------------------
    elapsed = time.time() - t0
    print(f"\n{SEP}")
    print(f"Pipeline concluido em {elapsed:.1f}s"
          + (" [DRY-RUN]" if args.dry_run else ""))
    print(SEP)


if __name__ == "__main__":
    main()
