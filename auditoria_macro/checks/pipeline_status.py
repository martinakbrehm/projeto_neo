import sys
sys.path.insert(0, '.')
from config import db_destino
import pymysql
conn = pymysql.connect(**db_destino())
cur = conn.cursor()

# Status dos staging_imports do dia 16-04
print('=== staging_imports id=9 e id=10 ===')
cur.execute('SELECT id, filename, rows_success, rows_failed, status, finished_at FROM staging_imports WHERE id IN (9,10) ORDER BY id')
for r in cur.fetchall():
    print(f'  id={r[0]} {r[1]} ok={r[2]:,} fail={r[3]} status={r[4]} finished={r[5]}')

# tabela_macros inseridos pelo pipeline de hoje
print()
cur.execute('SELECT COUNT(*) FROM tabela_macros WHERE data_criacao >= "2026-04-17 00:00:00"')
r = cur.fetchone()
print(f'Novos registros tabela_macros hoje (17-04): {r[0]:,}')

cur.execute('SELECT COUNT(*) FROM tabela_macros WHERE status = "pendente" AND data_criacao >= "2026-04-17 00:00:00"')
r = cur.fetchone()
print(f'  pendentes (aguardando macro): {r[0]:,}')

cur.execute('SELECT COUNT(*) FROM tabela_macros WHERE status != "pendente" AND data_criacao >= "2026-04-17 00:00:00"')
r = cur.fetchone()
print(f'  processados: {r[0]:,}')

# Breakdown por distribuidora
print()
cur.execute('''
    SELECT d.nome, tm.status, COUNT(*) as qtd
    FROM tabela_macros tm
    JOIN distribuidoras d ON d.id = tm.distribuidora_id
    WHERE tm.data_criacao >= "2026-04-17 00:00:00"
    GROUP BY d.nome, tm.status
    ORDER BY d.nome, tm.status
''')
print('Breakdown por distribuidora/status:')
for r in cur.fetchall():
    print(f'  {r[0]:<20} {r[1]:<15} {r[2]:>8,}')

conn.close()