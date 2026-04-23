"""Verificação de qualidade dos nomes na tabela clientes."""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
import config, pymysql

c = pymysql.connect(**config.db_destino())
cur = c.cursor()

print("=" * 70)
print("VERIFICAÇÃO DE NOMES NA TABELA clientes")
print("=" * 70)

cur.execute("SELECT COUNT(*) FROM clientes")
total = cur.fetchone()[0]
print(f"\nTotal de clientes: {total:,}\n")

# 1. Nomes que parecem distribuidoras
print("--- Nomes que parecem distribuidoras ---")
cur.execute("""
    SELECT nome, COUNT(*) as qtd FROM clientes
    WHERE LOWER(nome) REGEXP 'coelba|cosern|celpe|brasilia|neoenergia|distribuidora|energisa|enel|cpfl|cemig|light|equatorial'
    GROUP BY nome ORDER BY qtd DESC LIMIT 30
""")
rows = cur.fetchall()
if rows:
    for r in rows:
        print(f"  {r[0]!r:55s} ({r[1]:,}x)")
else:
    print("  Nenhum encontrado.")

# 2. Nomes que são apenas números
print("\n--- Nomes que são apenas números ---")
cur.execute("""
    SELECT nome, COUNT(*) as qtd FROM clientes
    WHERE nome REGEXP '^[0-9 ./-]+$'
    GROUP BY nome ORDER BY qtd DESC LIMIT 30
""")
rows = cur.fetchall()
if rows:
    for r in rows:
        print(f"  {r[0]!r:55s} ({r[1]:,}x)")
else:
    print("  Nenhum encontrado.")

# 3. Nomes muito curtos (1-2 chars)
print("\n--- Nomes muito curtos (1-2 chars) ---")
cur.execute("""
    SELECT nome, COUNT(*) as qtd FROM clientes
    WHERE CHAR_LENGTH(TRIM(nome)) <= 2 AND nome IS NOT NULL
    GROUP BY nome ORDER BY qtd DESC LIMIT 20
""")
rows = cur.fetchall()
if rows:
    for r in rows:
        print(f"  {r[0]!r:55s} ({r[1]:,}x)")
else:
    print("  Nenhum encontrado.")

# 4. Nomes NULL ou vazios
print("\n--- Nomes NULL ou vazios ---")
cur.execute("""
    SELECT
        SUM(nome IS NULL) as nulos,
        SUM(TRIM(nome) = '') as vazios
    FROM clientes
""")
r = cur.fetchone()
print(f"  NULL: {r[0]:,}   Vazios: {r[1]:,}")

# 5. Nomes com padrão de UC ou CPF (sequências longas de dígitos)
print("\n--- Nomes que parecem UC ou CPF (6+ dígitos seguidos) ---")
cur.execute("""
    SELECT nome, COUNT(*) as qtd FROM clientes
    WHERE nome REGEXP '[0-9]{6,}'
    GROUP BY nome ORDER BY qtd DESC LIMIT 20
""")
rows = cur.fetchall()
if rows:
    for r in rows:
        print(f"  {r[0]!r:55s} ({r[1]:,}x)")
else:
    print("  Nenhum encontrado.")

# 6. Nomes com caracteres especiais
print("\n--- Nomes com caracteres especiais (@#$%&*) ---")
cur.execute(r"""
    SELECT nome, COUNT(*) as qtd FROM clientes
    WHERE nome REGEXP '[@#$%*!?=+<>{}|~^]'
    GROUP BY nome ORDER BY qtd DESC LIMIT 20
""")
rows = cur.fetchall()
if rows:
    for r in rows:
        print(f"  {r[0]!r:55s} ({r[1]:,}x)")
else:
    print("  Nenhum encontrado.")

# 7. Nomes com apenas uma palavra (sem sobrenome)
print("\n--- Nomes com apenas 1 palavra (sem sobrenome) ---")
cur.execute("""
    SELECT COUNT(*) FROM clientes
    WHERE nome IS NOT NULL
      AND TRIM(nome) != ''
      AND TRIM(nome) NOT LIKE '%% %%'
""")
sem_sobrenome = cur.fetchone()[0]
print(f"  Total sem sobrenome: {sem_sobrenome:,}")
cur.execute("""
    SELECT nome, COUNT(*) as qtd FROM clientes
    WHERE nome IS NOT NULL
      AND TRIM(nome) != ''
      AND TRIM(nome) NOT LIKE '%% %%'
    GROUP BY nome ORDER BY qtd DESC LIMIT 15
""")
for r in cur.fetchall():
    print(f"  {r[0]!r:55s} ({r[1]:,}x)")

# 8. Resumo geral
print("\n" + "=" * 70)
print("RESUMO")
print("=" * 70)
cur.execute("""
    SELECT COUNT(*) FROM clientes
    WHERE nome REGEXP '^[0-9 ./-]+$'
       OR LOWER(nome) REGEXP 'coelba|cosern|celpe|brasilia|neoenergia|distribuidora'
       OR CHAR_LENGTH(TRIM(COALESCE(nome, ''))) <= 2
       OR nome IS NULL
       OR TRIM(nome) = ''
       OR nome REGEXP '[0-9]{6,}'
""")
prob = cur.fetchone()[0]
print(f"Total potencialmente problemáticos: {prob:,} de {total:,} ({100*prob/total:.2f}%)")

c.close()
