"""
Fix e populate dashboard_macros_agg — executando as queries diretamente
(sem SP, sem MEMORY temp table que estoura com 769k rows).
"""
import sys
sys.path.insert(0, '.')
from config import db_destino
import pymysql

c = pymysql.connect(**db_destino(), read_timeout=600, write_timeout=600)
cur = c.cursor()

cur.execute("SET SESSION innodb_lock_wait_timeout = 300")
cur.execute("SET SESSION lock_wait_timeout = 300")

# 1. Criar temp table com InnoDB (não MEMORY)
print("[1/4] Criando tmp_cpf_arquivo...")
cur.execute("DROP TEMPORARY TABLE IF EXISTS tmp_cpf_arquivo")
cur.execute("""
    CREATE TEMPORARY TABLE tmp_cpf_arquivo (
        cpf      CHAR(11)     NOT NULL,
        filename VARCHAR(255) NOT NULL,
        PRIMARY KEY (cpf)
    ) ENGINE=InnoDB
""")
c.commit()

# 2. Popular temp table
print("[2/4] Populando tmp_cpf_arquivo (pode demorar)...")
cur.execute("""
    INSERT INTO tmp_cpf_arquivo (cpf, filename)
    SELECT sub.normalized_cpf, si.filename
    FROM (
        SELECT normalized_cpf, MAX(staging_id) AS latest_staging_id
        FROM staging_import_rows
        WHERE validation_status = 'valid'
        GROUP BY normalized_cpf
    ) sub
    JOIN staging_imports si ON si.id = sub.latest_staging_id
""")
c.commit()
cur.execute("SELECT COUNT(*) FROM tmp_cpf_arquivo")
print(f"  {cur.fetchone()[0]:,} CPFs mapeados")

# 3. Truncate e popular dashboard_macros_agg
print("[3/4] Populando dashboard_macros_agg...")
cur.execute("TRUNCATE TABLE dashboard_macros_agg")
c.commit()

cur.execute("""
    INSERT INTO dashboard_macros_agg
        (dia, status, mensagem, resposta_status, empresa, fornecedor, arquivo_origem, qtd)
    SELECT
        DATE(COALESCE(m.data_extracao, m.data_update)) AS dia,
        m.status,
        r.mensagem,
        r.status AS resposta_status,
        d.nome AS empresa,
        COALESCE(co.fornecedor, 'fornecedor2') AS fornecedor,
        COALESCE(ta.filename, 'Dados históricos') AS arquivo_origem,
        COUNT(*) AS qtd
    FROM tabela_macros m
    LEFT JOIN respostas r ON r.id = m.resposta_id
    LEFT JOIN distribuidoras d ON d.id = m.distribuidora_id
    LEFT JOIN cliente_origem co ON co.cliente_id = m.cliente_id
    LEFT JOIN clientes cl ON cl.id = m.cliente_id
    LEFT JOIN tmp_cpf_arquivo ta ON ta.cpf = cl.cpf
    WHERE m.status != 'pendente'
      AND m.resposta_id IS NOT NULL
    GROUP BY
        DATE(COALESCE(m.data_extracao, m.data_update)),
        m.status, r.mensagem, r.status, d.nome,
        COALESCE(co.fornecedor, 'fornecedor2'),
        COALESCE(ta.filename, 'Dados históricos')
""")
c.commit()

# 4. Resultado
cur.execute("SELECT COUNT(*) FROM dashboard_macros_agg")
total = cur.fetchone()[0]
print(f"\n[4/4] dashboard_macros_agg: {total:,} rows")

cur.execute("SELECT arquivo_origem, SUM(qtd) t FROM dashboard_macros_agg GROUP BY arquivo_origem ORDER BY t DESC")
for r in cur.fetchall():
    print(f"  {r[0]}: {int(r[1]):,}")

cur.execute("DROP TEMPORARY TABLE IF EXISTS tmp_cpf_arquivo")
c.close()
print("\nPronto!")
