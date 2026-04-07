import sys; sys.path.insert(0, ".")
from config import db_destino
import pymysql
conn = pymysql.connect(**db_destino())
cur = conn.cursor()
cur.execute("SELECT status, COUNT(*) FROM tabela_macros GROUP BY status ORDER BY FIELD(status,'pendente','processando','reprocessar','consolidado','excluido')")
total = 0
for r in cur.fetchall():
    print(f"  {r[0]:<15} {r[1]:>9,}")
    total += r[1]
print(f"  {'TOTAL':<15} {total:>9,}")
conn.close()
