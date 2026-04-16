import pymysql
from config import db_destino

# Conectar ao banco
conn = pymysql.connect(**db_destino())
cursor = conn.cursor()

# Query para contar clientes sem nome
query1 = """
SELECT COUNT(*) AS total_clientes_sem_nome
FROM clientes
WHERE nome IS NULL;
"""

# Query para contar registros sem distribuidora
query2 = """
SELECT COUNT(*) AS total_registros_sem_distribuidora
FROM tabela_macro_api
WHERE distribuidora_id IS NULL;
"""

cursor.execute(query1)
results1 = cursor.fetchall()

cursor.execute(query2)
results2 = cursor.fetchall()

print("Verificação de dados sem nome ou distribuidora:")
print(f"Total de clientes sem nome: {results1[0][0] if results1 else 0}")
print(f"Total de registros macro_api sem distribuidora: {results2[0][0] if results2 else 0}")

cursor.close()
conn.close()