import config, pymysql

c = pymysql.connect(**config.db_destino())
cur = c.cursor()

cur.execute("SELECT COUNT(*) FROM clientes WHERE nome IN ('COELBA','COSERN','CELPE','CELP','BRASILIA','coelba','cosern','celpe','celp','brasilia')")
print(f"Clientes com nome distribuidora: {cur.fetchone()[0]}")

cur.execute("SELECT COUNT(*) FROM clientes WHERE data_nascimento IS NULL")
print(f"Clientes sem dt_nascimento: {cur.fetchone()[0]}")

cur.execute("SELECT COUNT(*) FROM clientes c LEFT JOIN telefones t ON t.cliente_id=c.id WHERE t.id IS NULL")
print(f"Clientes sem telefone: {cur.fetchone()[0]}")

c.close()
