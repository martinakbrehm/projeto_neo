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
# Funções de refresh (isoladas com tratamento de erro completo)
# ---------------------------------------------------------------------------
def refresh_macros() -> bool:
    """Atualiza dashboard_macros_agg via stored procedure."""
    import pymysql

    log.info("Iniciando refresh de dashboard_macros_agg...")
    t0 = time.time()
    try:
        conn = pymysql.connect(**DB_CONFIG, connect_timeout=10, read_timeout=120)
        with conn.cursor() as cur:
            cur.execute("CALL sp_refresh_dashboard_macros_agg()")
            conn.commit()
            cur.execute("SELECT COUNT(*) FROM dashboard_macros_agg")
            n = cur.fetchone()[0]
        conn.close()
        elapsed = time.time() - t0
        log.info(f"dashboard_macros_agg OK: {n} linhas em {elapsed:.1f}s")
        return True
    except Exception as e:
        elapsed = time.time() - t0
        log.error(f"dashboard_macros_agg FALHOU em {elapsed:.1f}s: {e}")
        return False


def refresh_arquivos() -> bool:
    """Atualiza dashboard_arquivos_agg com estratégia de temp table.

    Estratégia segura:
      1. Cria temp table sem índice → bulk insert rápido
      2. Adiciona índice após bulk insert (passagem única)
      3. TRUNCATE + INSERT na tabela final
    """
    import pymysql

    log.info("Iniciando refresh de dashboard_arquivos_agg...")
    t0 = time.time()
    try:
        conn = pymysql.connect(**DB_CONFIG, connect_timeout=10, read_timeout=180)
        with conn.cursor() as cur:
            # Passo 1: temp table sem índice + bulk insert
            cur.execute("DROP TEMPORARY TABLE IF EXISTS tmp_cpf_status")
            cur.execute("""
                CREATE TEMPORARY TABLE tmp_cpf_status (
                    cpf              VARCHAR(20)  NOT NULL,
                    distribuidora_id INT UNSIGNED NOT NULL,
                    status           VARCHAR(30)  NOT NULL
                )
            """)
            cur.execute("""
                INSERT INTO tmp_cpf_status (cpf, distribuidora_id, status)
                SELECT cl.cpf, tm.distribuidora_id, tm.status
                FROM tabela_macros tm
                INNER JOIN (
                    SELECT MAX(id) AS max_id
                    FROM tabela_macros
                    WHERE status != 'pendente'
                      AND resposta_id IS NOT NULL
                    GROUP BY cliente_id, distribuidora_id
                ) latest ON tm.id = latest.max_id
                JOIN clientes cl ON cl.id = tm.cliente_id
            """)
            n_tmp = cur.rowcount
            log.info(f"  tmp_cpf_status preenchida: {n_tmp:,} registros")

            # Passo 2: índice após bulk insert
            cur.execute(
                "ALTER TABLE tmp_cpf_status ADD INDEX idx_cpf_dist (cpf, distribuidora_id)"
            )
            conn.commit()

            # Passo 2b: temp tables para CPFs/UCs inéditos (primeiro staging por CPF/UC)
            cur.execute("DROP TEMPORARY TABLE IF EXISTS tmp_cpf_first")
            cur.execute("""
                CREATE TEMPORARY TABLE tmp_cpf_first (
                    normalized_cpf   CHAR(11)     NOT NULL,
                    first_staging_id INT UNSIGNED NOT NULL,
                    INDEX (first_staging_id),
                    INDEX (normalized_cpf)
                )
                SELECT normalized_cpf, MIN(staging_id) AS first_staging_id
                FROM staging_import_rows
                WHERE validation_status = 'valid'
                GROUP BY normalized_cpf
            """)
            cur.execute("DROP TEMPORARY TABLE IF EXISTS tmp_uc_first")
            cur.execute("""
                CREATE TEMPORARY TABLE tmp_uc_first (
                    normalized_cpf   CHAR(11)     NOT NULL,
                    normalized_uc    CHAR(10)     NOT NULL,
                    first_staging_id INT UNSIGNED NOT NULL,
                    INDEX (first_staging_id)
                )
                SELECT normalized_cpf, normalized_uc, MIN(staging_id) AS first_staging_id
                FROM staging_import_rows
                WHERE validation_status = 'valid'
                  AND normalized_uc IS NOT NULL
                  AND normalized_uc != ''
                GROUP BY normalized_cpf, normalized_uc
            """)
            conn.commit()

            # Passo 3: popular tabela final
            cur.execute("TRUNCATE TABLE dashboard_arquivos_agg")
            cur.execute("""
                INSERT INTO dashboard_arquivos_agg
                    (arquivo, data_carga, cpfs_no_arquivo, cpfs_processados, ativos, inativos,
                     cpfs_ineditos, ucs_ineditas, ineditos_processados, ineditos_ativos, ineditos_inativos)
                SELECT
                    si.filename                                                                   AS arquivo,
                    DATE(si.created_at)                                                           AS data_carga,
                    COUNT(DISTINCT sir.normalized_cpf)                                            AS cpfs_no_arquivo,
                    COUNT(DISTINCT CASE WHEN cs.status IS NOT NULL THEN sir.normalized_cpf END)   AS cpfs_processados,
                    COUNT(DISTINCT CASE WHEN cs.status = 'consolidado' THEN sir.normalized_cpf END) AS ativos,
                    COUNT(DISTINCT CASE
                        WHEN cs.status IN ('excluido', 'reprocessar') THEN sir.normalized_cpf
                    END)                                                                          AS inativos,
                    COUNT(DISTINCT CASE
                        WHEN cf.first_staging_id = si.id THEN sir.normalized_cpf
                    END)                                                                          AS cpfs_ineditos,
                    COUNT(DISTINCT CASE
                        WHEN uf.first_staging_id = si.id
                        THEN CONCAT(sir.normalized_cpf, '|', sir.normalized_uc)
                    END)                                                                          AS ucs_ineditas,
                    COUNT(DISTINCT CASE
                        WHEN cf.first_staging_id = si.id AND cs.status IS NOT NULL
                        THEN sir.normalized_cpf
                    END)                                                                          AS ineditos_processados,
                    COUNT(DISTINCT CASE
                        WHEN cf.first_staging_id = si.id AND cs.status = 'consolidado'
                        THEN sir.normalized_cpf
                    END)                                                                          AS ineditos_ativos,
                    COUNT(DISTINCT CASE
                        WHEN cf.first_staging_id = si.id AND cs.status IN ('excluido', 'reprocessar')
                        THEN sir.normalized_cpf
                    END)                                                                          AS ineditos_inativos
                FROM staging_imports si
                JOIN staging_import_rows sir
                    ON  sir.staging_id        = si.id
                    AND sir.validation_status = 'valid'
                LEFT JOIN tmp_cpf_status cs
                    ON  cs.cpf              = sir.normalized_cpf
                    AND cs.distribuidora_id = CAST(si.distribuidora_nome AS UNSIGNED)
                LEFT JOIN tmp_cpf_first cf
                    ON cf.normalized_cpf = sir.normalized_cpf
                LEFT JOIN tmp_uc_first uf
                    ON  uf.normalized_cpf = sir.normalized_cpf
                    AND uf.normalized_uc  = sir.normalized_uc
                GROUP BY si.id, si.filename, DATE(si.created_at)
                ORDER BY si.id DESC
            """)
            n_final = cur.rowcount
            conn.commit()

            cur.execute("DROP TEMPORARY TABLE IF EXISTS tmp_cpf_status")
            cur.execute("DROP TEMPORARY TABLE IF EXISTS tmp_cpf_first")
            cur.execute("DROP TEMPORARY TABLE IF EXISTS tmp_uc_first")

        conn.close()
        elapsed = time.time() - t0
        log.info(f"dashboard_arquivos_agg OK: {n_final} arquivos em {elapsed:.1f}s")
        return True
    except Exception as e:
        elapsed = time.time() - t0
        log.error(f"dashboard_arquivos_agg FALHOU em {elapsed:.1f}s: {e}")
        # Tentar limpar temp table
        try:
            conn2 = pymysql.connect(**DB_CONFIG, connect_timeout=5, read_timeout=5)
            with conn2.cursor() as c:
                c.execute("DROP TEMPORARY TABLE IF EXISTS tmp_cpf_status")
            conn2.close()
        except Exception:
            pass
        return False


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

    # 3. Refresh arquivos (mais pesado ~10-20s)
    ok_arquivos = refresh_arquivos()

    status = "OK" if (ok_macros and ok_arquivos) else "PARCIAL"
    log.info(
        f"Ciclo concluído ({status}): "
        f"macros={'OK' if ok_macros else 'FALHA'}, "
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
