import sys; sys.path.insert(0, '.')
from config import db_destino
import pymysql
conn = pymysql.connect(**db_destino())
cur = conn.cursor()

cur.execute("SELECT id, filename, rows_success, rows_failed, status FROM staging_imports WHERE id IN (5,6,7)")
print("=== staging_imports 5-7 ===")
for r in cur.fetchall():
    print(f"  id={r[0]} file={r[1]} success={r[2]} failed={r[3]} status={r[4]}")

cur.execute("SELECT staging_id, COUNT(*) as total, SUM(CASE WHEN processed_at IS NOT NULL THEN 1 ELSE 0 END) as done FROM staging_import_rows WHERE staging_id IN (5,6,7) GROUP BY staging_id")
print("=== staging_import_rows processadas ===")
for r in cur.fetchall():
    print(f"  staging_id={r[0]} total={r[1]:,} done={r[2]:,} restantes={r[1]-r[2]:,}")

cur.execute("SELECT COUNT(*) FROM tabela_macros WHERE DATE(created_at) = CURDATE()")
r = cur.fetchone()
print(f"=== macros inseridas hoje: {r[0]:,} ===")
conn.close()
