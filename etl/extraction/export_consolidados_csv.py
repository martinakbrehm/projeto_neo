import pymysql
import csv
from config import db_destino

# Conectar ao banco
conn = pymysql.connect(**db_destino())
cursor = conn.cursor()

# Query para filtrar dados da macro com retorno "Titularidade confirmada com contrato ativo"
query = """
SELECT
    cu.uc AS uc_ativa,
    c.cpf,
    c.nome,
    GROUP_CONCAT(DISTINCT t.telefone SEPARATOR '; ') AS telefones,
    CONCAT(e.endereco, ', ', COALESCE(e.numero, ''), ' ', COALESCE(e.complemento, ''), ', ', COALESCE(e.bairro, ''), ', ', COALESCE(e.cidade, ''), ' - ', COALESCE(e.uf, ''), ' CEP: ', COALESCE(e.cep, '')) AS endereco_completo,
    d.nome AS distribuidora,
    r.mensagem AS retorno
FROM tabela_macros tma
JOIN clientes c ON tma.cliente_id = c.id
LEFT JOIN cliente_uc cu ON tma.cliente_uc_id = cu.id
LEFT JOIN telefones t ON t.cliente_id = c.id
LEFT JOIN enderecos e ON e.cliente_id = c.id AND e.id = (SELECT MIN(id) FROM enderecos WHERE cliente_id = c.id)
LEFT JOIN distribuidoras d ON tma.distribuidora_id = d.id
JOIN respostas r ON tma.resposta_id = r.id
WHERE r.mensagem = 'Titularidade confirmada com contrato ativo'
  AND tma.status = 'consolidado'
GROUP BY cu.uc, c.cpf, c.nome, e.endereco, e.numero, e.complemento, e.bairro, e.cidade, e.uf, e.cep, d.nome, r.mensagem
ORDER BY c.cpf
"""

cursor.execute(query)
results = cursor.fetchall()

# Escrever para CSV
with open('macro_filtrado.csv', 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile, delimiter=';')
    writer.writerow(['UC Ativa', 'CPF', 'Nome', 'Telefones', 'Endereco Completo', 'Distribuidora', 'Retorno'])
    for row in results:
        writer.writerow(row)

print(f"Arquivo 'macro_filtrado.csv' criado com {len(results)} registros.")

cursor.close()
conn.close()