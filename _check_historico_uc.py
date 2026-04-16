import pandas as pd
import sys
sys.path.insert(0, '.')
from config import db_destino
import pymysql, csv

# Carregar historico com resposta_id=3
df = pd.read_csv(
    'dados/fornecedor2/migration_periodo_ate_20260312/processed/historico_normalizado_para_importar.csv',
    sep=';', dtype=str, encoding='utf-8-sig'
)
hist_ativos = df[df['resposta_id'] == '3'][['cpf','uc','distribuidora_id']].drop_duplicates()
print(f'Historico ativos (resposta_id=3): {len(hist_ativos):,} combos CPF+UC')
print(f'CPFs unicos no historico: {hist_ativos["cpf"].nunique():,}')

# Construir lookup: cpf -> list of (uc, distribuidora_id)
hist_map = {}
for _, row in hist_ativos.iterrows():
    hist_map.setdefault(row['cpf'], []).append((row['uc'], row['distribuidora_id']))

conn = pymysql.connect(**db_destino())
cur = conn.cursor()

# Buscar consolidados sem cliente_uc_id com dados completos
cur.execute('''
    SELECT
        cl.cpf,
        cl.nome,
        d.nome AS distribuidora,
        tm.distribuidora_id,
        tm.id AS macro_id,
        e.endereco,
        e.numero,
        e.bairro,
        e.cidade,
        e.uf,
        e.cep,
        GROUP_CONCAT(DISTINCT t.telefone ORDER BY t.id SEPARATOR ';') AS telefones
    FROM tabela_macros tm
    JOIN clientes cl ON cl.id = tm.cliente_id
    JOIN distribuidoras d ON d.id = tm.distribuidora_id
    LEFT JOIN enderecos e ON e.cliente_id = cl.id AND e.distribuidora_id = tm.distribuidora_id
    LEFT JOIN telefones t ON t.cliente_id = cl.id
    WHERE tm.resposta_id = 3 AND tm.cliente_uc_id IS NULL
    GROUP BY tm.id, cl.cpf, cl.nome, d.nome, tm.distribuidora_id,
             e.endereco, e.numero, e.bairro, e.cidade, e.uf, e.cep
''')
sem_uc = cur.fetchall()
print(f'\nConsolidados sem cliente_uc_id: {len(sem_uc):,}')

# Buscar consolidados COM cliente_uc_id (esses ja temos certeza)
cur.execute('''
    SELECT
        cl.cpf,
        cl.nome,
        cu.uc,
        d.nome AS distribuidora,
        e.endereco,
        e.numero,
        e.bairro,
        e.cidade,
        e.uf,
        e.cep,
        GROUP_CONCAT(DISTINCT t.telefone ORDER BY t.id SEPARATOR ';') AS telefones
    FROM tabela_macros tm
    JOIN clientes cl ON cl.id = tm.cliente_id
    JOIN distribuidoras d ON d.id = tm.distribuidora_id
    JOIN cliente_uc cu ON cu.id = tm.cliente_uc_id
    LEFT JOIN enderecos e ON e.cliente_id = cl.id AND e.distribuidora_id = tm.distribuidora_id
    LEFT JOIN telefones t ON t.cliente_id = cl.id
    WHERE tm.resposta_id = 3 AND tm.cliente_uc_id IS NOT NULL
    GROUP BY tm.id, cl.cpf, cl.nome, cu.uc, d.nome,
             e.endereco, e.numero, e.bairro, e.cidade, e.uf, e.cep
''')
com_uc_certo = cur.fetchall()
print(f'Consolidados COM cliente_uc_id (certeza): {len(com_uc_certo):,}')
conn.close()

# Resolver UC dos sem_uc via historico
resolvidos = []
nao_resolvidos = []

for row in sem_uc:
    cpf, nome, distribuidora, distrib_id, macro_id, ender, num, bairro, cidade, uf, cep, tels = row
    matches = hist_map.get(cpf, [])
    # Filtrar pelo distribuidora_id
    matches_distrib = [uc for uc, did in matches if did == str(distrib_id)]

    if len(matches_distrib) == 1:
        # UC unica no historico para esse CPF+distribuidora -> certeza
        resolvidos.append((cpf, nome, matches_distrib[0], distribuidora, ender, num, bairro, cidade, uf, cep, tels, 'historico_unico'))
    elif len(matches_distrib) > 1:
        # Multiplas UCs no historico -> inclui todas (cliente tinha multiplas UCs ativas)
        for uc in matches_distrib:
            resolvidos.append((cpf, nome, uc, distribuidora, ender, num, bairro, cidade, uf, cep, tels, 'historico_multiplo'))
    else:
        nao_resolvidos.append((cpf, nome, None, distribuidora, ender, num, bairro, cidade, uf, cep, tels, 'sem_historico'))

print(f'\nResolvidos via historico: {len(resolvidos):,}')
print(f'Nao resolvidos (sem match no historico): {len(nao_resolvidos):,}')

# Gerar CSV final
with open('titularidade_confirmada_contrato_ativo.csv', 'w', newline='', encoding='utf-8-sig') as f:
    w = csv.writer(f, delimiter=';')
    w.writerow(['cpf','nome','uc','distribuidora','endereco','numero','bairro','cidade','uf','cep','telefones','origem_uc'])

    # Com certeza (cliente_uc_id direto)
    for row in com_uc_certo:
        w.writerow(list(row) + ['cliente_uc_id'])

    # Resolvidos via historico
    for row in resolvidos:
        w.writerow(list(row))

    # Nao resolvidos
    for row in nao_resolvidos:
        w.writerow(list(row))

total = len(com_uc_certo) + len(resolvidos) + len(nao_resolvidos)
com_uc_final = len(com_uc_certo) + len(resolvidos)
print(f'\nTotal linhas no CSV: {total:,}')
print(f'Com UC preenchida: {com_uc_final:,}')
print(f'Sem UC (sem historico): {len(nao_resolvidos):,}')
print('Arquivo gerado: titularidade_confirmada_contrato_ativo.csv')
