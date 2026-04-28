"""
Migration: Corrigir sp_refresh_dashboard_macros_agg para usar CPF+UC como chave
de atribuição de arquivo_origem, em vez de só CPF com MAX(staging_id).

PROBLEMA ANTERIOR:
  A SP usava MAX(staging_id) por CPF para definir arquivo_origem.
  Resultado: ao importar arquivo novo com CPF já existente, TODO o histórico
  antigo desse CPF era re-atribuído ao arquivo novo — aparecia como se tivesse
  sido processado naquele arquivo.

CORREÇÃO:
  Usa MIN(staging_id) por par (cpf, uc) — o primeiro arquivo que trouxe
  exatamente essa combinação CPF+UC. Assim cada macro fica no arquivo
  onde o par apareceu pela primeira vez, sem re-atribuição pelo arquivo mais recente.

  Join path:
    tabela_macros.cliente_id  → clientes.cpf
    tabela_macros.cliente_uc_id → cliente_uc.uc
    ↕
    staging_import_rows.normalized_cpf + normalized_uc → MIN(staging_id)
    → staging_imports.filename
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
import config
import pymysql
import time

NEW_SP = """
CREATE PROCEDURE `sp_refresh_dashboard_macros_agg`()
BEGIN
    SET SESSION innodb_lock_wait_timeout = 300;
    SET SESSION lock_wait_timeout = 300;

    -- -----------------------------------------------------------------------
    -- Mapeia cada par (cpf, uc) ao PRIMEIRO arquivo em que apareceu (MIN staging_id).
    -- Isso garante que macros processadas em datas anteriores continuem
    -- atribuídas ao arquivo original, mesmo que o mesmo CPF apareça em
    -- arquivos mais recentes.
    -- -----------------------------------------------------------------------
    DROP TEMPORARY TABLE IF EXISTS tmp_combo_arquivo;
    CREATE TEMPORARY TABLE tmp_combo_arquivo (
        cpf      CHAR(11)     NOT NULL,
        uc       CHAR(10)     NOT NULL,
        filename VARCHAR(255) NOT NULL,
        PRIMARY KEY (cpf, uc)
    ) ENGINE=InnoDB;

    INSERT INTO tmp_combo_arquivo (cpf, uc, filename)
    SELECT sub.normalized_cpf, sub.normalized_uc, si.filename
    FROM (
        SELECT normalized_cpf, normalized_uc,
               MIN(staging_id) AS first_staging_id
        FROM staging_import_rows
        WHERE validation_status = 'valid'
          AND normalized_uc IS NOT NULL
          AND normalized_uc != ''
        GROUP BY normalized_cpf, normalized_uc
    ) sub
    JOIN staging_imports si ON si.id = sub.first_staging_id;

    TRUNCATE TABLE dashboard_macros_agg;

    INSERT INTO dashboard_macros_agg
        (dia, status, mensagem, resposta_status, empresa, fornecedor, arquivo_origem, qtd)
    SELECT
        DATE(COALESCE(m.data_extracao, m.data_update)) AS dia,
        m.status,
        r.mensagem,
        r.status                                        AS resposta_status,
        d.nome                                          AS empresa,
        COALESCE(co.fornecedor, 'fornecedor2')          AS fornecedor,
        COALESCE(ta.filename, 'Dados historicos')       AS arquivo_origem,
        COUNT(*)                                        AS qtd
    FROM tabela_macros m
    LEFT JOIN respostas      r  ON r.id  = m.resposta_id
    LEFT JOIN distribuidoras d  ON d.id  = m.distribuidora_id
    LEFT JOIN cliente_origem co ON co.cliente_id = m.cliente_id
    LEFT JOIN clientes       cl ON cl.id = m.cliente_id
    LEFT JOIN cliente_uc     cu ON cu.id = m.cliente_uc_id
    LEFT JOIN tmp_combo_arquivo ta ON ta.cpf = cl.cpf
                                  AND ta.uc  = cu.uc
    WHERE m.status != 'pendente'
      AND m.resposta_id IS NOT NULL
    GROUP BY
        DATE(COALESCE(m.data_extracao, m.data_update)),
        m.status, r.mensagem, r.status, d.nome,
        COALESCE(co.fornecedor, 'fornecedor2'),
        COALESCE(ta.filename, 'Dados historicos');

    DROP TEMPORARY TABLE IF EXISTS tmp_combo_arquivo;
END
"""


def run():
    conn = pymysql.connect(**config.db_destino(), read_timeout=300, write_timeout=300)
    conn.autocommit(True)
    cur = conn.cursor()

    print("Dropping old procedure...")
    cur.execute("DROP PROCEDURE IF EXISTS sp_refresh_dashboard_macros_agg")

    print("Creating new procedure...")
    cur.execute(NEW_SP)
    print("Procedure created OK.")

    print("\nExecuting refresh to validate...")
    t0 = time.time()
    cur.execute("CALL sp_refresh_dashboard_macros_agg()")
    while cur.nextset():
        pass
    print(f"Refresh concluído em {time.time() - t0:.1f}s")

    cur.execute("SELECT COUNT(*), COUNT(DISTINCT arquivo_origem) FROM dashboard_macros_agg")
    rows, arqs = cur.fetchone()
    print(f"dashboard_macros_agg: {rows} linhas, {arqs} arquivos distintos")

    # Mostrar distribuição por arquivo (top 15)
    cur.execute("""
        SELECT arquivo_origem, SUM(qtd) as total
        FROM dashboard_macros_agg
        GROUP BY arquivo_origem
        ORDER BY MIN(dia), arquivo_origem
        LIMIT 20
    """)
    print("\nDistribuição por arquivo_origem (após fix):")
    for r in cur.fetchall():
        print(f"  {str(r[0]):55s}  {r[1]:>6}")

    conn.close()
    print("\nMigration concluída com sucesso.")


if __name__ == "__main__":
    run()
