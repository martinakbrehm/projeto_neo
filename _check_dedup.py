import config, pymysql
c = pymysql.connect(**config.db_destino())
cur = c.cursor()

# Duplicatas em staging_imports (mesmo filename)
cur.execute("""
SELECT filename, COUNT(*) as qtd, GROUP_CONCAT(id ORDER BY id) as ids
FROM staging_imports
GROUP BY filename
HAVING COUNT(*) > 1
ORDER BY qtd DESC
""")
rows = cur.fetchall()
print(f"=== staging_imports com filename duplicado: {len(rows)} ===")
for r in rows:
    print(f"  qtd={r[1]} ids=[{r[2]}] arquivo={r[0]}")

print()

# Duplicatas em tabela_macros (mesmo cpf+distribuidora+cliente_uc_id)
cur.execute("""
SELECT COUNT(*) FROM (
    SELECT cliente_id, distribuidora_id, cliente_uc_id, COUNT(*) as qtd
    FROM tabela_macros
    WHERE cliente_uc_id IS NOT NULL
    GROUP BY cliente_id, distribuidora_id, cliente_uc_id
    HAVING COUNT(*) > 1
) t
""")
dup_macros = cur.fetchone()[0]
print(f"=== tabela_macros - combos duplicadas (cpf+dist+uc): {dup_macros} ===")

# Total por status
cur.execute("""
SELECT status, COUNT(*) as qtd
FROM tabela_macros
GROUP BY status
ORDER BY qtd DESC
""")
print("\n=== tabela_macros por status ===")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]:,}")

# Sem UC
cur.execute("SELECT COUNT(*) FROM tabela_macros WHERE cliente_uc_id IS NULL")
print(f"\nSem UC (deveriam ter sido apagados): {cur.fetchone()[0]:,}")

# Total geral
cur.execute("SELECT COUNT(*) FROM tabela_macros")
print(f"Total registros: {cur.fetchone()[0]:,}")

# Excesso duplicado
cur.execute("""
SELECT SUM(qtd - 1) FROM (
    SELECT cliente_id, distribuidora_id, cliente_uc_id, COUNT(*) as qtd
    FROM tabela_macros
    WHERE cliente_uc_id IS NOT NULL
    GROUP BY cliente_id, distribuidora_id, cliente_uc_id
    HAVING COUNT(*) > 1
) t
""")
row = cur.fetchone()
excess = row[0] if row[0] else 0
print(f"Registros em excesso (duplicatas): {excess:,}")

# Amostra
if dup_macros > 0:
    cur.execute("""
    SELECT tm.id, c.cpf, cu.uc, tm.distribuidora_id, tm.status, tm.resposta_id, tm.data_criacao
    FROM tabela_macros tm
    JOIN clientes c ON c.id = tm.cliente_id
    JOIN cliente_uc cu ON cu.id = tm.cliente_uc_id
    WHERE (tm.cliente_id, tm.distribuidora_id, tm.cliente_uc_id) IN (
        SELECT cliente_id, distribuidora_id, cliente_uc_id
        FROM tabela_macros WHERE cliente_uc_id IS NOT NULL
        GROUP BY cliente_id, distribuidora_id, cliente_uc_id
        HAVING COUNT(*) > 1 LIMIT 5
    )
    ORDER BY tm.cliente_uc_id, tm.id LIMIT 20
    """)
    print("\nAmostra duplicatas:")
    for r in cur.fetchall():
        print(f"  id={r[0]} cpf={r[1]} uc={r[2]} did={r[3]} status={r[4]} resp={r[5]} {r[6]}")

c.close()
