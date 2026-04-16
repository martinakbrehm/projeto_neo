"""Verifica status do dashboard após backfill do staging_import_rows."""
import sys; sys.path.insert(0, '.')
from config import db_destino
import pymysql

conn = pymysql.connect(**db_destino())
cur = conn.cursor()

# 1. Confirmar staging_import_rows para id=8
cur.execute('SELECT COUNT(*) FROM staging_import_rows WHERE staging_id=8')
print(f'staging_import_rows (id=8): {cur.fetchone()[0]:,}')

cur.execute('SELECT COUNT(*) FROM staging_import_rows WHERE staging_id=8 AND validation_status=%s', ('valid',))
print(f'  valid: {cur.fetchone()[0]:,}')

# 2. Refresh stored procedures
print('\nRefreshando dashboard_macros_agg...')
cur.execute('CALL sp_refresh_dashboard_macros_agg()')
conn.commit()
print('  OK')

print('Refreshando dashboard_arquivos_agg...')
cur.execute('CALL sp_refresh_dashboard_arquivos_agg()')
conn.commit()
print('  OK')

# 3. Verificar resultado
print('\n=== dashboard_arquivos_agg ===')
cur.execute('SELECT arquivo, data_carga, cpfs_no_arquivo, cpfs_processados, ativos, inativos FROM dashboard_arquivos_agg ORDER BY data_carga DESC')
for r in cur.fetchall():
    print(f'  {r[0]}: carga={r[1]} cpfs={r[2]:,} proc={r[3]:,} ativos={r[4]:,} inativos={r[5]:,}')

print('\n=== dashboard_macros_agg by arquivo_origem ===')
cur.execute('SELECT arquivo_origem, SUM(qtd) total FROM dashboard_macros_agg GROUP BY arquivo_origem ORDER BY total DESC')
for r in cur.fetchall():
    print(f'  {r[0]}: {int(r[1]):,}')

conn.close()
