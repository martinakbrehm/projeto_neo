"""
Scheduler seguro para atualização das tabelas materializadas do dashboard.

Executa refresh de dashboard_macros_agg e dashboard_arquivos_agg em intervalo
configurável (padrão: 1 hora), com proteções:

  1. Lock file impede múltiplas instâncias rodando simultaneamente
  2. Antes de cada refresh, verifica e mata queries órfãs no RDS
  3. Timeouts explícitos em todas as conexões
  4. Logging com timestamp para auditoria

Uso:
    python -m dashboard_macros.refresh_scheduler          # roda em loop (1h)
    python -m dashboard_macros.refresh_scheduler --once   # roda uma vez e sai
    python -m dashboard_macros.refresh_scheduler --interval 1800  # a cada 30min
"""

import sys
import os
import time
import argparse
import logging
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import db_destino  # noqa: E402

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
LOCK_FILE = Path(__file__).parent / ".refresh_scheduler.lock"
DEFAULT_INTERVAL = 1200  # 20 minutos em segundos
DB_CONFIG = db_destino()

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("refresh_scheduler")

# Tabelas que serão protegidas contra queries órfãs
TABELAS_PROTEGIDAS = ("dashboard_macros_agg", "dashboard_arquivos_agg")


# ---------------------------------------------------------------------------
# Lock file — impede execução simultânea
# ---------------------------------------------------------------------------
def adquirir_lock() -> bool:
    """Tenta criar lock file. Retorna False se outra instância está rodando."""
    if LOCK_FILE.exists():
        # Verifica se o PID gravado ainda está vivo
        try:
            pid = int(LOCK_FILE.read_text().strip())
            # No Windows: verificar se processo existe
            import ctypes
            kernel32 = ctypes.windll.kernel32
            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid)
            if handle:
                kernel32.CloseHandle(handle)
                log.warning(f"Outra instância já rodando (PID {pid}). Abortando.")
                return False
            # Processo morreu — lock stale
            log.info(f"Lock file stale (PID {pid} não existe). Removendo.")
        except (ValueError, OSError, AttributeError):
            log.info("Lock file inválido. Removendo.")

    LOCK_FILE.write_text(str(os.getpid()))
    return True


def liberar_lock():
    """Remove lock file."""
    try:
        LOCK_FILE.unlink(missing_ok=True)
    except OSError:
        pass


# ---------------------------------------------------------------------------
# Proteção contra queries órfãs
# ---------------------------------------------------------------------------
def limpar_queries_orfas() -> int:
    """Verifica SHOW PROCESSLIST e mata queries que referenciam tabelas protegidas.

    Retorna quantas queries foram mortas.
    """
    import pymysql

    mortas = 0
    try:
        conn = pymysql.connect(**DB_CONFIG, connect_timeout=10, read_timeout=10)
        cur = conn.cursor()
        cur.execute("SHOW PROCESSLIST")
        processos = cur.fetchall()

        meu_pid = None
        for p in processos:
            # p = (Id, User, Host, db, Command, Time, State, Info)
            pid, user, host, db, cmd, tempo, state, info = p[:8]
            if info and "SHOW PROCESSLIST" in str(info):
                meu_pid = pid
                continue

            info_str = str(info).lower() if info else ""
            for tabela in TABELAS_PROTEGIDAS:
                if tabela in info_str and int(tempo) > 30:
                    log.warning(
                        f"Matando query órfã pid={pid} time={tempo}s "
                        f"cmd={cmd} info={str(info)[:80]}"
                    )
                    try:
                        cur.execute(f"KILL {pid}")
                        mortas += 1
                    except Exception:
                        pass
                    break

            # Mata também TRUNCATE/INSERT travados há muito tempo
            if cmd == "Query" and int(tempo) > 120:
                for keyword in ("truncate", "insert into dashboard_"):
                    if keyword in info_str:
                        log.warning(
                            f"Matando operação bloqueada pid={pid} time={tempo}s"
                        )
                        try:
                            cur.execute(f"KILL {pid}")
                            mortas += 1
                        except Exception:
                            pass
                        break

        conn.close()
    except Exception as e:
        log.error(f"Erro ao verificar queries órfãs: {e}")

    if mortas:
        log.info(f"Total queries órfãs eliminadas: {mortas}")
        time.sleep(2)  # Aguarda limpeza dos locks
    else:
        log.debug("Nenhuma query órfã encontrada.")

    return mortas


# ---------------------------------------------------------------------------
# Funções de refresh — chamam stored procedures otimizadas
# ---------------------------------------------------------------------------
def _call_sp(sp_name: str, table_name: str) -> bool:
    """Executa uma stored procedure e retorna True se bem-sucedido."""
    import pymysql

    log.info(f"Executando CALL {sp_name}()...")
    t0 = time.time()
    try:
        conn = pymysql.connect(**DB_CONFIG, connect_timeout=10,
                               read_timeout=600, write_timeout=600)
        cur = conn.cursor()
        cur.execute(f"CALL {sp_name}()")
        conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        n = cur.fetchone()[0]
        conn.close()
        elapsed = time.time() - t0
        log.info(f"{table_name} OK: {n} linhas em {elapsed:.1f}s")
        return True
    except Exception as e:
        elapsed = time.time() - t0
        log.error(f"{table_name} FALHOU em {elapsed:.1f}s: {e}")
        return False


def refresh_macros() -> bool:
    """Atualiza dashboard_macros_agg via stored procedure."""
    return _call_sp("sp_refresh_dashboard_macros_agg", "dashboard_macros_agg")


def refresh_arquivos() -> bool:
    """Atualiza dashboard_arquivos_agg via stored procedure."""
    return _call_sp("sp_refresh_dashboard_arquivos_agg", "dashboard_arquivos_agg")


def refresh_cobertura() -> bool:
    """Atualiza dashboard_cobertura_agg via stored procedure."""
    return _call_sp("sp_refresh_dashboard_cobertura_agg", "dashboard_cobertura_agg")


# ---------------------------------------------------------------------------
# Loop principal
# ---------------------------------------------------------------------------
def executar_refresh():
    """Executa um ciclo completo de refresh com proteções."""
    log.info("=" * 60)
    log.info("Iniciando ciclo de refresh das tabelas materializadas")
    log.info("=" * 60)

    # 1. Limpar queries órfãs antes de começar
    limpar_queries_orfas()

    # 2. Refresh macros (rápido ~1s)
    ok_macros = refresh_macros()

    # 3. Refresh cobertura (rápido — só staging_import_rows)
    ok_cobertura = refresh_cobertura()

    # 4. Refresh arquivos (mais pesado ~10-20s)
    ok_arquivos = refresh_arquivos()

    status = "OK" if (ok_macros and ok_arquivos and ok_cobertura) else "PARCIAL"
    log.info(
        f"Ciclo concluído ({status}): "
        f"macros={'OK' if ok_macros else 'FALHA'}, "
        f"cobertura={'OK' if ok_cobertura else 'FALHA'}, "
        f"arquivos={'OK' if ok_arquivos else 'FALHA'}"
    )
    return ok_macros and ok_arquivos


def main():
    parser = argparse.ArgumentParser(
        description="Scheduler para refresh das tabelas materializadas do dashboard"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Executa uma vez e sai (útil para cron/Task Scheduler)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_INTERVAL,
        help=f"Intervalo em segundos entre cada refresh (padrão: {DEFAULT_INTERVAL})",
    )
    args = parser.parse_args()

    # Lock — impede execução simultânea
    if not adquirir_lock():
        sys.exit(1)

    try:
        if args.once:
            log.info("Modo --once: executando refresh único")
            ok = executar_refresh()
            sys.exit(0 if ok else 1)

        log.info(
            f"Scheduler iniciado — intervalo de {args.interval}s "
            f"({args.interval // 60} min)"
        )
        while True:
            executar_refresh()
            log.info(
                f"Próximo refresh em {args.interval // 60} min. "
                f"Aguardando..."
            )
            time.sleep(args.interval)

    except KeyboardInterrupt:
        log.info("Scheduler encerrado pelo usuário (Ctrl+C).")
    finally:
        liberar_lock()


if __name__ == "__main__":
    main()
