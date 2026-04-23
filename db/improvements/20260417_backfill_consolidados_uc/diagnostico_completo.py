"""
Diagnóstico completo do estado do banco após múltiplas execuções do backfill.
Apenas leitura — não altera nada.
"""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
import config
import pymysql

c = pymysql.connect(**config.db_destino(), connect_timeout=30)
cur = c.cursor()

print("=" * 70)
print("DIAGNÓSTICO COMPLETO DO BANCO")
print("=" * 70)

# ── 1. tabela_macros: status geral ─────────────────────────────
print("\n[1] tabela_macros — status geral")
print("-" * 50)
cur.execute("""
    SELECT status, COUNT(*),
           SUM(cliente_uc_id IS NOT NULL), SUM(cliente_uc_id IS NULL)
    FROM tabela_macros GROUP BY status
    ORDER BY FIELD(status,'pendente','processando','reprocessar','consolidado','excluido')
""")
total_all = 0
for r in cur.fetchall():
    print(f"  {r[0]:<15} total={r[1]:>10,}  com_uc={r[2]:>10,}  sem_uc={r[3]:>10,}")
    total_all += r[1]
print(f"  {'TOTAL':<15} {total_all:>10,}")

# ── 2. Duplicatas em tabela_macros ──────────────────────────────
print("\n[2] tabela_macros — duplicatas")
print("-" * 50)
cur.execute("""
    SELECT COUNT(*), COALESCE(SUM(qtd - 1), 0) FROM (
        SELECT cliente_id, distribuidora_id, cliente_uc_id, COUNT(*) as qtd
        FROM tabela_macros WHERE cliente_uc_id IS NOT NULL
        GROUP BY cliente_id, distribuidora_id, cliente_uc_id
        HAVING COUNT(*) > 1
    ) t
""")
n_combos, n_excess = cur.fetchone()
print(f"  Combos (cpf+uc+distrib) com duplicatas: {n_combos:,}")
print(f"  Registros em excesso (a remover):       {n_excess:,}")

# Distribuição dos duplicados por status
if n_combos > 0:
    cur.execute("""
        SELECT tm.status, COUNT(*) as qtd
        FROM tabela_macros tm
        INNER JOIN (
            SELECT cliente_id, distribuidora_id, cliente_uc_id
            FROM tabela_macros WHERE cliente_uc_id IS NOT NULL
            GROUP BY cliente_id, distribuidora_id, cliente_uc_id
            HAVING COUNT(*) > 1
        ) dup ON tm.cliente_id = dup.cliente_id
            AND tm.distribuidora_id = dup.distribuidora_id
            AND tm.cliente_uc_id = dup.cliente_uc_id
        GROUP BY tm.status ORDER BY qtd DESC
    """)
    print("  Status dos registros envolvidos em duplicatas:")
    for r in cur.fetchall():
        print(f"    {r[0]}: {r[1]:,}")

    # Quantos duplicados seriam removidos vs mantidos por status
    cur.execute("""
        SELECT status, COUNT(*) FROM (
            SELECT t.id, t.status,
                   ROW_NUMBER() OVER (
                       PARTITION BY t.cliente_id, t.distribuidora_id, t.cliente_uc_id
                       ORDER BY
                           FIELD(t.status,'consolidado','reprocessar','processando','pendente','excluido'),
                           t.id ASC
                   ) as rn
            FROM tabela_macros t
            WHERE t.cliente_uc_id IS NOT NULL
            AND (t.cliente_id, t.distribuidora_id, t.cliente_uc_id) IN (
                SELECT cliente_id, distribuidora_id, cliente_uc_id
                FROM tabela_macros WHERE cliente_uc_id IS NOT NULL
                GROUP BY cliente_id, distribuidora_id, cliente_uc_id
                HAVING COUNT(*) > 1
            )
        ) ranked WHERE rn > 1
        GROUP BY status ORDER BY COUNT(*) DESC
    """)
    print("  Status dos que SERIAM removidos (rn > 1):")
    for r in cur.fetchall():
        print(f"    {r[0]}: {r[1]:,}")

# ── 3. staging_imports ──────────────────────────────────────────
print("\n[3] staging_imports — duplicatas de filename")
print("-" * 50)
cur.execute("""
    SELECT filename, COUNT(*) as qtd, GROUP_CONCAT(id ORDER BY id) as ids
    FROM staging_imports GROUP BY filename HAVING COUNT(*) > 1
    ORDER BY qtd DESC
""")
rows = cur.fetchall()
print(f"  Filenames duplicados: {len(rows)}")
for r in rows:
    print(f"    qtd={r[1]} ids=[{r[2]}] arquivo={r[0]}")

cur.execute("SELECT COUNT(*) FROM staging_imports")
print(f"  Total staging_imports: {cur.fetchone()[0]:,}")
cur.execute("SELECT COUNT(*) FROM staging_import_rows")
print(f"  Total staging_import_rows: {cur.fetchone()[0]:,}")

# ── 4. Clientes — enriquecimento ───────────────────────────────
print("\n[4] Clientes — enriquecimento")
print("-" * 50)
cur.execute("SELECT COUNT(*) FROM clientes")
print(f"  Total clientes: {cur.fetchone()[0]:,}")
cur.execute("""SELECT COUNT(*) FROM clientes
    WHERE nome IN ('COELBA','COSERN','CELPE','CELP','BRASILIA',
                   'coelba','cosern','celpe','celp','brasilia',
                   'Neoenergia Coelba','Neoenergia Cosern','Neoenergia Celpe',
                   'Neoenergia Brasilia')""")
print(f"  Com nome de distribuidora (bad): {cur.fetchone()[0]:,}")
cur.execute("SELECT COUNT(*) FROM clientes WHERE data_nascimento IS NULL")
print(f"  Sem data_nascimento: {cur.fetchone()[0]:,}")
cur.execute("SELECT COUNT(*) FROM clientes WHERE data_nascimento IS NOT NULL")
print(f"  Com data_nascimento: {cur.fetchone()[0]:,}")

# ── 5. Telefones ───────────────────────────────────────────────
print("\n[5] Telefones")
print("-" * 50)
cur.execute("SELECT COUNT(*) FROM telefones")
print(f"  Total registros telefones: {cur.fetchone()[0]:,}")
cur.execute("SELECT COUNT(DISTINCT cliente_id) FROM telefones")
print(f"  Clientes com telefone: {cur.fetchone()[0]:,}")
cur.execute("""
    SELECT COUNT(*) FROM clientes c
    LEFT JOIN telefones t ON t.cliente_id = c.id
    WHERE t.id IS NULL
""")
print(f"  Clientes SEM telefone: {cur.fetchone()[0]:,}")

# ── 6. Endereços ──────────────────────────────────────────────
print("\n[6] Endereços")
print("-" * 50)
cur.execute("SELECT COUNT(*) FROM enderecos")
print(f"  Total enderecos: {cur.fetchone()[0]:,}")
cur.execute("SELECT COUNT(*) FROM enderecos WHERE bairro = 'NULL'")
print(f"  Bairro = 'NULL' literal: {cur.fetchone()[0]:,}")
cur.execute("SELECT COUNT(*) FROM enderecos WHERE cep LIKE '%%-%%'")
print(f"  CEP com hífen: {cur.fetchone()[0]:,}")

# ── 7. cliente_uc ─────────────────────────────────────────────
print("\n[7] cliente_uc")
print("-" * 50)
cur.execute("SELECT COUNT(*) FROM cliente_uc")
print(f"  Total registros: {cur.fetchone()[0]:,}")
cur.execute("SELECT distribuidora_id, COUNT(*) FROM cliente_uc GROUP BY distribuidora_id ORDER BY distribuidora_id")
for r in cur.fetchall():
    print(f"    distribuidora_id={r[0]}: {r[1]:,}")

# ── 8. Dados de migration fornecedor2 ─────────────────────────
print("\n[8] Verificar se migration fornecedor2 está incluído")
print("-" * 50)
# Verificar staging_imports com nomes de migration
cur.execute("SELECT id, filename, total_rows FROM staging_imports WHERE filename LIKE '%%migration%%' OR filename LIKE '%%historico%%'")
rows = cur.fetchall()
print(f"  staging_imports com 'migration' ou 'historico' no nome: {len(rows)}")
for r in rows:
    print(f"    sid={r[0]} rows={r[2]:,} {r[1]}")

print("\n" + "=" * 70)
print("FIM DO DIAGNÓSTICO")
print("=" * 70)

c.close()
