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
    """Atualiza dashboard_arquivos_agg calculando as métricas em Python (pandas).

    Para contornar o net_write_timeout do RDS ao carregar tabela_macros (831k linhas),
    usa estratégia de 2 queries pequenas:
      1. SELECT cliente_id, distribuidora_id, MAX(id) GROUP BY  → só ints, 363k × 12 bytes = 4MB
      2. SELECT id, status WHERE id IN (lista de max_ids)        → 363k × 2 colunas = ~3MB
    Ambas completam bem abaixo dos 60s de net_write_timeout.
    """
    import pymysql
    import pandas as pd

    log.info("Iniciando refresh de dashboard_arquivos_agg (modo Python/2-step)...")
    t0 = time.time()
    try:
        conn = pymysql.connect(**DB_CONFIG, connect_timeout=10, read_timeout=300)
        cur = conn.cursor()

        # Aumentar timeouts de sessão para queries analíticas pesadas (GROUP BY 831k linhas)
        cur.execute("SET SESSION net_write_timeout = 300")
        cur.execute("SET SESSION net_read_timeout  = 300")

        # --- 1. staging_imports ---
        cur.execute("""
            SELECT id, filename, DATE(created_at) AS data_carga, distribuidora_nome
            FROM staging_imports
        """)
        cols = [d[0] for d in cur.description]
        imports_df = pd.DataFrame(cur.fetchall(), columns=cols)
        imports_df["id"] = imports_df["id"].astype(int)
        imports_df["dist_id"] = pd.to_numeric(imports_df["distribuidora_nome"], errors="coerce")
        log.info(f"  staging_imports: {len(imports_df)} arquivos")

        # --- 2. staging_import_rows (somente valid) ---
        cur.execute("""
            SELECT staging_id, normalized_cpf, normalized_uc
            FROM staging_import_rows
            WHERE validation_status = 'valid'
              AND normalized_cpf IS NOT NULL
        """)
        cols = [d[0] for d in cur.description]
        rows_df = pd.DataFrame(cur.fetchall(), columns=cols)
        rows_df["staging_id"] = rows_df["staging_id"].astype(int)
        rows_df["normalized_uc"] = rows_df["normalized_uc"].fillna("").str.strip()
        log.info(f"  staging_import_rows (valid): {len(rows_df):,} linhas")

        # --- 3. tabela_macros: 2 queries pequenas para evitar transmitir 831k linhas completas ---

        # 3a. Somente (cliente_id, distribuidora_id, max_id) — tipos inteiros, ~4MB
        cur.execute("""
            SELECT cliente_id, distribuidora_id, MAX(id) AS max_id
            FROM tabela_macros
            WHERE status != 'pendente'
              AND resposta_id IS NOT NULL
            GROUP BY cliente_id, distribuidora_id
        """)
        cols = [d[0] for d in cur.description]
        grp_df = pd.DataFrame(cur.fetchall(), columns=cols)
        grp_df = grp_df.astype(int)
        log.info(f"  tabela_macros GROUP BY: {len(grp_df):,} combinações cliente×dist")

        # 3b. Status somente para os max_ids — usa índice PRIMARY KEY, leve
        max_ids = grp_df["max_id"].tolist()
        # Carrega em batches de 20k ids para evitar IN() gigante
        batch_size = 20000
        status_parts = []
        for i in range(0, len(max_ids), batch_size):
            batch = max_ids[i:i + batch_size]
            ids_str = ",".join(str(x) for x in batch)
            cur.execute(f"SELECT id, status FROM tabela_macros WHERE id IN ({ids_str})")
            status_parts.extend(cur.fetchall())
        status_map = {r[0]: r[1] for r in status_parts}  # id → status

        # Juntar: (cliente_id, distribuidora_id) → status
        grp_df["status"] = grp_df["max_id"].map(status_map)
        log.info(f"  status mapeado: {grp_df['status'].notna().sum():,} registros")

        # --- 4. clientes: cpf → cliente_id ---
        cur.execute("SELECT id, cpf FROM clientes")
        cols = [d[0] for d in cur.description]
        clientes_df = pd.DataFrame(cur.fetchall(), columns=cols)
        clientes_df.columns = ["cliente_id", "cpf"]
        log.info(f"  clientes: {len(clientes_df):,} linhas")

        conn.close()

        # Montar status_df: (cpf, distribuidora_id, status)
        status_df = grp_df.merge(clientes_df, on="cliente_id", how="left")
        status_df = status_df[["cpf", "distribuidora_id", "status"]].dropna(subset=["cpf"])
        log.info(f"  status_df final: {len(status_df):,} linhas")

        # ----------------------------------------------------------------
        # Cálculos em pandas
        # ----------------------------------------------------------------

        cpf_first = (
            rows_df.groupby("normalized_cpf")["staging_id"]
            .min().reset_index()
            .rename(columns={"staging_id": "first_staging_id"})
        )
        uc_rows = rows_df[rows_df["normalized_uc"] != ""].copy()
        uc_first = (
            uc_rows.groupby(["normalized_cpf", "normalized_uc"])["staging_id"]
            .min().reset_index()
            .rename(columns={"staging_id": "first_staging_id"})
        )

        rows_df = rows_df.merge(cpf_first, on="normalized_cpf", how="left")
        rows_df["cpf_inedito"] = rows_df["staging_id"] == rows_df["first_staging_id"]
        rows_df = rows_df.drop(columns=["first_staging_id"])

        uc_rows = rows_df[rows_df["normalized_uc"] != ""].copy()
        uc_rows = uc_rows.merge(uc_first, on=["normalized_cpf", "normalized_uc"], how="left")
        uc_rows["uc_inedita"] = uc_rows["staging_id"] == uc_rows["first_staging_id"]

        resultados = []
        for _, arq in imports_df.iterrows():
            sid = int(arq["id"])
            dist_id = arq["dist_id"]

            sub = rows_df[rows_df["staging_id"] == sid]
            if sub.empty:
                continue

            cpfs_no_arquivo = sub["normalized_cpf"].nunique()
            cpfs_ineditos   = int(sub[sub["cpf_inedito"]]["normalized_cpf"].nunique())

            sub_uc = uc_rows[uc_rows["staging_id"] == sid]
            ucs_ineditas = int(sub_uc["uc_inedita"].sum()) if not sub_uc.empty else 0

            cpfs_processados = ativos = inativos = 0
            ineditos_proc = ineditos_at = ineditos_inat = 0

            if pd.notna(dist_id):
                status_arq = status_df[status_df["distribuidora_id"] == int(dist_id)]
                sub_status = sub.merge(
                    status_arq[["cpf", "status"]],
                    left_on="normalized_cpf", right_on="cpf", how="left"
                )
                cpfs_processados = int(sub_status[sub_status["status"].notna()]["normalized_cpf"].nunique())
                ativos           = int(sub_status[sub_status["status"] == "consolidado"]["normalized_cpf"].nunique())
                inativos         = int(sub_status[sub_status["status"].isin(["excluido", "reprocessar"])]["normalized_cpf"].nunique())
                sub_in = sub_status[sub_status["cpf_inedito"]]
                ineditos_proc = int(sub_in[sub_in["status"].notna()]["normalized_cpf"].nunique())
                ineditos_at   = int(sub_in[sub_in["status"] == "consolidado"]["normalized_cpf"].nunique())
                ineditos_inat = int(sub_in[sub_in["status"].isin(["excluido", "reprocessar"])]["normalized_cpf"].nunique())

            resultados.append((
                arq["filename"], str(arq["data_carga"]),
                cpfs_no_arquivo, cpfs_processados, ativos, inativos,
                cpfs_ineditos, ucs_ineditas,
                ineditos_proc, ineditos_at, ineditos_inat,
            ))
            log.info(f"  arquivo id={sid}: {cpfs_no_arquivo:,} CPFs, {cpfs_processados:,} proc, {ativos:,} ativos")

        # --- 5. Gravar na tabela ---
        limpar_queries_orfas()
        conn_out = pymysql.connect(**DB_CONFIG, connect_timeout=10, read_timeout=120)
        cur_out = conn_out.cursor()
        cur_out.execute("SET SESSION lock_wait_timeout = 60")
        cur_out.execute("TRUNCATE TABLE dashboard_arquivos_agg")
        cur_out.executemany("""
            INSERT INTO dashboard_arquivos_agg
                (arquivo, data_carga, cpfs_no_arquivo, cpfs_processados, ativos, inativos,
                 cpfs_ineditos, ucs_ineditas, ineditos_processados, ineditos_ativos, ineditos_inativos)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, resultados)
        conn_out.commit()
        conn_out.close()

        elapsed = time.time() - t0
        log.info(f"dashboard_arquivos_agg OK: {len(resultados)} arquivos em {elapsed:.1f}s")
        return True
    except Exception as e:
        elapsed = time.time() - t0
        log.error(f"dashboard_arquivos_agg FALHOU em {elapsed:.1f}s: {e}")
        return False



    import pymysql
    import pandas as pd

    log.info("Iniciando refresh de dashboard_arquivos_agg (modo Python)...")
    t0 = time.time()
    try:
        conn = pymysql.connect(**DB_CONFIG, connect_timeout=10, read_timeout=60)
        cur = conn.cursor()

        # --- 1. staging_imports ---
        cur.execute("""
            SELECT id, filename, DATE(created_at) AS data_carga, distribuidora_nome
            FROM staging_imports
        """)
        cols = [d[0] for d in cur.description]
        imports_df = pd.DataFrame(cur.fetchall(), columns=cols)
        imports_df["id"] = imports_df["id"].astype(int)
        imports_df["dist_id"] = pd.to_numeric(imports_df["distribuidora_nome"], errors="coerce")
        log.info(f"  staging_imports: {len(imports_df)} arquivos")

        # --- 2. staging_import_rows (somente valid) ---
        cur.execute("""
            SELECT staging_id, normalized_cpf, normalized_uc
            FROM staging_import_rows
            WHERE validation_status = 'valid'
              AND normalized_cpf IS NOT NULL
        """)
        cols = [d[0] for d in cur.description]
        rows_df = pd.DataFrame(cur.fetchall(), columns=cols)
        rows_df["staging_id"] = rows_df["staging_id"].astype(int)
        rows_df["normalized_uc"] = rows_df["normalized_uc"].fillna("").str.strip()
        log.info(f"  staging_import_rows (valid): {len(rows_df):,} linhas")

        # --- 3. clientes: cpf → id mapping (necessário para lookup em tabela_macros) ---
        cur.execute("SELECT id, cpf FROM clientes")
        cols = [d[0] for d in cur.description]
        clientes_df = pd.DataFrame(cur.fetchall(), columns=cols)
        clientes_df.columns = ["cliente_id", "cpf"]
        log.info(f"  clientes: {len(clientes_df):,} linhas")

        conn.close()

        # ----------------------------------------------------------------
        # Cálculos em pandas
        # ----------------------------------------------------------------

        # Primeira ocorrência de cada CPF e CPF+UC por staging_id
        cpf_first = (
            rows_df.groupby("normalized_cpf")["staging_id"]
            .min()
            .reset_index()
            .rename(columns={"staging_id": "first_staging_id"})
        )
        uc_rows = rows_df[rows_df["normalized_uc"] != ""].copy()
        uc_first = (
            uc_rows.groupby(["normalized_cpf", "normalized_uc"])["staging_id"]
            .min()
            .reset_index()
            .rename(columns={"staging_id": "first_staging_id"})
        )

        # Marcar inéditos no rows_df
        rows_df = rows_df.merge(cpf_first, on="normalized_cpf", how="left")
        rows_df["cpf_inedito"] = rows_df["staging_id"] == rows_df["first_staging_id"]
        rows_df = rows_df.drop(columns=["first_staging_id"])

        uc_rows = rows_df[rows_df["normalized_uc"] != ""].copy()
        uc_rows = uc_rows.merge(uc_first, on=["normalized_cpf", "normalized_uc"], how="left")
        uc_rows["uc_inedita"] = uc_rows["staging_id"] == uc_rows["first_staging_id"]

        # Mapear CPF → cliente_id para lookup pontual em tabela_macros
        cpf_to_cliente = clientes_df.set_index("cpf")["cliente_id"].to_dict()

        # Agregar por arquivo — status via lookup pontual por cliente_id
        resultados = []
        for _, arq in imports_df.iterrows():
            sid = int(arq["id"])
            dist_id = arq["dist_id"]

            sub = rows_df[rows_df["staging_id"] == sid]
            if sub.empty:
                continue

            cpfs_unicos = sub["normalized_cpf"].unique().tolist()
            cpfs_no_arquivo = len(cpfs_unicos)
            cpfs_ineditos   = int(sub[sub["cpf_inedito"]]["normalized_cpf"].nunique())

            # UCs inéditas
            sub_uc = uc_rows[uc_rows["staging_id"] == sid]
            ucs_ineditas = int(sub_uc["uc_inedita"].sum()) if not sub_uc.empty else 0

            # Lookup status em tabela_macros para apenas os CPFs deste arquivo
            cpfs_processados = ativos = inativos = ineditos_proc = ineditos_at = ineditos_inat = 0
            if pd.notna(dist_id) and cpfs_unicos:
                cliente_ids = [
                    cpf_to_cliente[c] for c in cpfs_unicos if c in cpf_to_cliente
                ]
                if cliente_ids:
                    try:
                        conn2 = pymysql.connect(**DB_CONFIG, connect_timeout=10, read_timeout=60)
                        cur2 = conn2.cursor()
                        # Buscar status mais recente por cliente_id nesta distribuidora
                        ids_str = ",".join(str(i) for i in cliente_ids)
                        cur2.execute(f"""
                            SELECT tm.cliente_id, tm.status
                            FROM tabela_macros tm
                            INNER JOIN (
                                SELECT MAX(id) AS max_id
                                FROM tabela_macros
                                WHERE cliente_id IN ({ids_str})
                                  AND distribuidora_id = {int(dist_id)}
                                  AND status != 'pendente'
                                  AND resposta_id IS NOT NULL
                                GROUP BY cliente_id
                            ) latest ON tm.id = latest.max_id
                        """)
                        status_rows = cur2.fetchall()
                        conn2.close()

                        # Mapear cliente_id → status
                        cli_status = {r[0]: r[1] for r in status_rows}
                        # Mapear CPF → status via cliente_id
                        cpf_status = {}
                        for cpf in cpfs_unicos:
                            cid = cpf_to_cliente.get(cpf)
                            if cid and cid in cli_status:
                                cpf_status[cpf] = cli_status[cid]

                        ineditos_cpfs = set(
                            sub[sub["cpf_inedito"]]["normalized_cpf"].unique()
                        )
                        cpfs_processados = len(cpf_status)
                        ativos    = sum(1 for s in cpf_status.values() if s == "consolidado")
                        inativos  = sum(1 for s in cpf_status.values() if s in ("excluido", "reprocessar"))
                        ineditos_proc = sum(1 for c, s in cpf_status.items() if c in ineditos_cpfs)
                        ineditos_at   = sum(1 for c, s in cpf_status.items() if c in ineditos_cpfs and s == "consolidado")
                        ineditos_inat = sum(1 for c, s in cpf_status.items() if c in ineditos_cpfs and s in ("excluido", "reprocessar"))
                    except Exception as e_status:
                        log.warning(f"  Status lookup falhou para arquivo {sid}: {e_status}")

            resultados.append((
                arq["filename"], str(arq["data_carga"]),
                int(cpfs_no_arquivo), int(cpfs_processados), int(ativos), int(inativos),
                int(cpfs_ineditos), int(ucs_ineditas),
                int(ineditos_proc), int(ineditos_at), int(ineditos_inat),
            ))
            log.info(f"  arquivo id={sid}: {cpfs_no_arquivo:,} CPFs, {cpfs_processados:,} proc, {ativos:,} ativos")

        # --- 4. Gravar na tabela ---
        conn_out = pymysql.connect(**DB_CONFIG, connect_timeout=10, read_timeout=30)
        cur_out = conn_out.cursor()
        cur_out.execute("TRUNCATE TABLE dashboard_arquivos_agg")
        cur_out.executemany("""
            INSERT INTO dashboard_arquivos_agg
                (arquivo, data_carga, cpfs_no_arquivo, cpfs_processados, ativos, inativos,
                 cpfs_ineditos, ucs_ineditas, ineditos_processados, ineditos_ativos, ineditos_inativos)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, resultados)
        conn_out.commit()
        conn_out.close()

        elapsed = time.time() - t0
        log.info(f"dashboard_arquivos_agg OK: {len(resultados)} arquivos em {elapsed:.1f}s")
        return True
    except Exception as e:
        elapsed = time.time() - t0
        log.error(f"dashboard_arquivos_agg FALHOU em {elapsed:.1f}s: {e}")
        return False


# ---------------------------------------------------------------------------
# Refresh: cobertura (novos vs existentes por arquivo) — query rápida
# ---------------------------------------------------------------------------
def refresh_cobertura() -> bool:
    """Atualiza dashboard_cobertura_agg: CPFs/UCs novos vs existentes por arquivo.

    Query leve — só usa staging_import_rows (sem JOIN em tabela_macros).
    """
    import pymysql

    log.info("Iniciando refresh de dashboard_cobertura_agg...")
    t0 = time.time()
    try:
        conn = pymysql.connect(**DB_CONFIG, connect_timeout=10, read_timeout=180)
        cur = conn.cursor()

        # Temp table: primeiro staging_id por combinação CPF+UC (unidade de contagem)
        cur.execute("DROP TEMPORARY TABLE IF EXISTS tmp_cob_combo_first")
        cur.execute("""
            CREATE TEMPORARY TABLE tmp_cob_combo_first (
                normalized_cpf   CHAR(11)     NOT NULL,
                normalized_uc    CHAR(10)     NOT NULL,
                first_staging_id INT UNSIGNED NOT NULL,
                INDEX (normalized_cpf, normalized_uc)
            )
            SELECT normalized_cpf, normalized_uc, MIN(staging_id) AS first_staging_id
            FROM staging_import_rows
            WHERE validation_status = 'valid'
              AND normalized_uc IS NOT NULL AND normalized_uc != ''
            GROUP BY normalized_cpf, normalized_uc
        """)
        conn.commit()

        # Popula cobertura — contagem exclusiva por combinação CPF+UC
        cur.execute("DELETE FROM dashboard_cobertura_agg")
        cur.execute("""
            INSERT INTO dashboard_cobertura_agg
                (arquivo, data_carga, total_combos, combos_novas, combos_existentes)
            SELECT
                si.filename                                                           AS arquivo,
                DATE(si.created_at)                                                   AS data_carga,
                COUNT(DISTINCT CONCAT(sir.normalized_cpf, '|', sir.normalized_uc))   AS total_combos,
                COUNT(DISTINCT CASE
                    WHEN cf.first_staging_id = si.id
                    THEN CONCAT(sir.normalized_cpf, '|', sir.normalized_uc)
                END)                                                                  AS combos_novas,
                COUNT(DISTINCT CONCAT(sir.normalized_cpf, '|', sir.normalized_uc))
                  - COUNT(DISTINCT CASE
                    WHEN cf.first_staging_id = si.id
                    THEN CONCAT(sir.normalized_cpf, '|', sir.normalized_uc)
                  END)                                                                AS combos_existentes
            FROM staging_imports si
            JOIN staging_import_rows sir
                ON  sir.staging_id        = si.id
                AND sir.validation_status = 'valid'
                AND sir.normalized_uc IS NOT NULL AND sir.normalized_uc != ''
            LEFT JOIN tmp_cob_combo_first cf
                ON  cf.normalized_cpf = sir.normalized_cpf
                AND cf.normalized_uc  = sir.normalized_uc
            GROUP BY si.id, si.filename, DATE(si.created_at)
            ORDER BY si.id DESC
        """)
        n_final = cur.rowcount
        conn.commit()

        cur.execute("DROP TEMPORARY TABLE IF EXISTS tmp_cob_combo_first")
        conn.close()

        elapsed = time.time() - t0
        log.info(f"dashboard_cobertura_agg OK: {n_final} arquivos em {elapsed:.1f}s")
        return True
    except Exception as e:
        elapsed = time.time() - t0
        log.error(f"dashboard_cobertura_agg FALHOU em {elapsed:.1f}s: {e}")
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
