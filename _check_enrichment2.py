"""Check enrichment recovery potential and rows_success issue."""
import sys, os, json
sys.path.insert(0, os.path.dirname(__file__))
import pandas as pd
import config, pymysql

c = pymysql.connect(**config.db_destino())
cur = c.cursor()

# 1. Load raw1 names
df_raw1 = pd.read_csv(
    'dados/fornecedor2/migration_periodo_pos_20260312/raw/clientes_300k_25_03.csv',
    dtype=str, encoding='utf-8-sig', sep=None, engine='python'
)
nomes = {}
for _, row in df_raw1.iterrows():
    cpf = str(row['cpf']).replace('.', '').replace('-', '').zfill(11)
    nome = str(row.get('nome', '')).strip()
    if nome and nome.lower() != 'nan':
        nomes[cpf] = nome

# 2. Clientes sem nome
cur.execute("SELECT cpf FROM clientes WHERE nome IS NULL OR TRIM(nome) = ''")
cpfs_sem_nome = [r[0] for r in cur.fetchall()]
print(f"Clientes sem nome: {len(cpfs_sem_nome):,}")

recuperaveis = [(cpf, nomes[cpf]) for cpf in cpfs_sem_nome if cpf in nomes]
print(f"Recuperáveis do clientes_300k: {len(recuperaveis):,}")

# 3. State file
with open('dados/fornecedor2/migration_periodo_pos_20260312/state/importacao_progresso.json') as f:
    state = json.load(f)
print(f"\nState periodo_pos:")
print(f"  ultimo_indice={state['ultimo_indice']}, ok={state['ok']}, total={state['total']}")
print(f"  Status: {state['status']}")

# 4. Entender o 371265
# Original migration fez 263110 ok (estado parcial)
# Reimport fez o restante (300000 total no arquivo)
# 371265 = 263110 + 108155? NÃO, vamos ver...
# O reimport registrou rows_success baseado no retroativo que completou tudo
# Mas o valor 371265 não bate com 300000
print(f"\n  rows_success no banco (id=8): 371,265")
print(f"  total_rows no banco (id=8):   300,000")
print(f"  Diferença:                      71,265")
print(f"  state.ok (parcial original):  {state['ok']:,}")

# 5. Verificar tabela_macros entries sem data_extracao
# que seriam os dados históricos
cur.execute("SELECT COUNT(*) FROM tabela_macros WHERE data_extracao IS NULL")
hist = cur.fetchone()[0]
print(f"\n  tabela_macros historico (sem data_extracao): {hist:,}")

c.close()
