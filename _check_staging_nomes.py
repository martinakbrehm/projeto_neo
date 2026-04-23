"""Listar nomes corrompidos unicos."""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
import config, pymysql

c = pymysql.connect(**config.db_destino())
cur = c.cursor()

cur.execute("SELECT DISTINCT nome FROM clientes WHERE nome REGEXP '[+?¦]' ORDER BY nome")
rows = cur.fetchall()
print(f"Nomes corrompidos unicos ({len(rows)}):")
for i, r in enumerate(rows, 1):
    print(f"  {i:2d}. {r[0]!r}")

c.close()
