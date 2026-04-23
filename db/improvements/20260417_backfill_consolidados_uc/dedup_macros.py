"""
Dedup tabela_macros: remove registros duplicados por (cliente_id, distribuidora_id, cliente_uc_id).
Mantém o registro com melhor status (consolidado > reprocessar > pendente > excluido > processando)
e, em caso de empate, o mais antigo (menor id).
"""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
import config
import pymysql

DRY_RUN = "--dry-run" in sys.argv
BATCH = 5000

c = pymysql.connect(**config.db_destino(), connect_timeout=30)
c.autocommit(False)
cur = c.cursor()

# 1. Contar duplicatas
cur.execute("""
    SELECT COUNT(*), SUM(qtd - 1) FROM (
        SELECT cliente_id, distribuidora_id, cliente_uc_id, COUNT(*) as qtd
        FROM tabela_macros
        WHERE cliente_uc_id IS NOT NULL
        GROUP BY cliente_id, distribuidora_id, cliente_uc_id
        HAVING COUNT(*) > 1
    ) t
""")
n_combos, n_excess = cur.fetchone()
n_excess = n_excess or 0
print(f"Combos duplicadas: {n_combos:,}")
print(f"Registros em excesso: {n_excess:,}")

if n_combos == 0:
    print("Nenhuma duplicata. Nada a fazer.")
    c.close()
    sys.exit(0)

# 2. Encontrar IDs a manter (melhor status + menor id)
# Status priority: consolidado=1, reprocessar=2, processando=3, pendente=4, excluido=5
print("Identificando registros a manter...")
cur.execute("""
    SELECT MIN(keep_id) FROM (
        SELECT id as keep_id,
               cliente_id, distribuidora_id, cliente_uc_id,
               ROW_NUMBER() OVER (
                   PARTITION BY cliente_id, distribuidora_id, cliente_uc_id
                   ORDER BY
                       FIELD(status, 'consolidado','reprocessar','processando','pendente','excluido'),
                       id ASC
               ) as rn
        FROM tabela_macros
        WHERE cliente_uc_id IS NOT NULL
    ) ranked
    WHERE rn = 1
    GROUP BY keep_id
""")
# That's too complex. Simpler approach: mark the ones to DELETE.
# Delete all except the best one per group.

print("Buscando IDs duplicados a excluir...")
cur.execute("""
    SELECT t.id
    FROM tabela_macros t
    INNER JOIN (
        SELECT cliente_id, distribuidora_id, cliente_uc_id,
               MIN(CASE
                   WHEN status = 'consolidado' THEN CONCAT('1_', LPAD(id, 10, '0'))
                   WHEN status = 'reprocessar' THEN CONCAT('2_', LPAD(id, 10, '0'))
                   WHEN status = 'processando' THEN CONCAT('3_', LPAD(id, 10, '0'))
                   WHEN status = 'pendente'    THEN CONCAT('4_', LPAD(id, 10, '0'))
                   ELSE CONCAT('5_', LPAD(id, 10, '0'))
               END) as best_key
        FROM tabela_macros
        WHERE cliente_uc_id IS NOT NULL
        GROUP BY cliente_id, distribuidora_id, cliente_uc_id
        HAVING COUNT(*) > 1
    ) keep_info ON t.cliente_id = keep_info.cliente_id
              AND t.distribuidora_id = keep_info.distribuidora_id
              AND t.cliente_uc_id = keep_info.cliente_uc_id
    WHERE CONCAT(
        CASE
            WHEN t.status = 'consolidado' THEN '1_'
            WHEN t.status = 'reprocessar' THEN '2_'
            WHEN t.status = 'processando' THEN '3_'
            WHEN t.status = 'pendente'    THEN '4_'
            ELSE '5_'
        END, LPAD(t.id, 10, '0')
    ) != keep_info.best_key
""")
ids_to_delete = [r[0] for r in cur.fetchall()]
print(f"IDs a excluir: {len(ids_to_delete):,}")

if DRY_RUN:
    print("[DRY-RUN] Nenhum registro excluído.")
    # Mostrar amostra por status
    from collections import Counter
    cur.execute(f"""
        SELECT status FROM tabela_macros WHERE id IN ({','.join(str(i) for i in ids_to_delete[:10000])})
    """)
    statuses = Counter(r[0] for r in cur.fetchall())
    print(f"Amostra status dos duplicados: {dict(statuses)}")
    c.close()
    sys.exit(0)

# 3. Deletar em batches
deleted = 0
for i in range(0, len(ids_to_delete), BATCH):
    chunk = ids_to_delete[i:i + BATCH]
    ph = ",".join(["%s"] * len(chunk))
    cur.execute(f"DELETE FROM tabela_macros WHERE id IN ({ph})", chunk)
    deleted += cur.rowcount
    c.commit()
    if (i // BATCH) % 10 == 0:
        print(f"  Excluídos: {deleted:,}/{len(ids_to_delete):,}", flush=True)

c.commit()
print(f"\nTotal excluídos: {deleted:,}")

# 4. Verificação final
cur.execute("""
    SELECT COUNT(*) FROM (
        SELECT cliente_id, distribuidora_id, cliente_uc_id, COUNT(*) as qtd
        FROM tabela_macros WHERE cliente_uc_id IS NOT NULL
        GROUP BY cliente_id, distribuidora_id, cliente_uc_id
        HAVING COUNT(*) > 1
    ) t
""")
remaining = cur.fetchone()[0]
print(f"Duplicatas restantes: {remaining}")

cur.execute("SELECT status, COUNT(*) FROM tabela_macros GROUP BY status ORDER BY COUNT(*) DESC")
print("\nStatus final:")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]:,}")

c.close()
print("CONCLUÍDO")
