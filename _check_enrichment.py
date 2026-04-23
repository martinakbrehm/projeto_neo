"""Diagnóstico: migration_periodo_pos enrichment + rows_success anomaly."""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
import pandas as pd
import config, pymysql

SEP = "=" * 70

print(SEP)
print("DIAGNÓSTICO: migration_periodo_pos + enrichment")
print(SEP)

# 1. raw files
df_raw1 = pd.read_csv(
    'dados/fornecedor2/migration_periodo_pos_20260312/raw/clientes_300k_25_03.csv',
    dtype=str, encoding='utf-8-sig', sep=None, engine='python'
)
cpfs_raw1 = set(df_raw1['cpf'].str.replace(r'[.-]', '', regex=True).str.zfill(11))
print(f"\nclientes_300k_25_03.csv: {len(df_raw1):,} linhas, {len(cpfs_raw1):,} CPFs únicos")
print(f"  Colunas: {list(df_raw1.columns)}")

# 2. processed
df_proc = pd.read_csv(
    'dados/fornecedor2/migration_periodo_pos_20260312/processed/historico_normalizado_para_importar.csv',
    dtype=str, encoding='utf-8-sig', sep=None, engine='python'
)
cpfs_proc = set(df_proc['cpf'].str.zfill(11))
print(f"\nhistorico_normalizado_para_importar.csv: {len(df_proc):,} linhas, {len(cpfs_proc):,} CPFs únicos")

# 3. Overlap
print(f"\nraw1 & proc: {len(cpfs_raw1 & cpfs_proc):,}")
print(f"raw1 - proc (em raw mas não no processed): {len(cpfs_raw1 - cpfs_proc):,}")
print(f"proc - raw1 (em proc mas não no raw): {len(cpfs_proc - cpfs_raw1):,}")

# 4. Check banco
c = pymysql.connect(**config.db_destino())
cur = c.cursor()

# Quantos CPFs do raw1 estão no banco?
cur.execute("SELECT COUNT(*) FROM clientes")
total_clientes = cur.fetchone()[0]
print(f"\nTotal clientes no banco: {total_clientes:,}")

cur.execute("SELECT COUNT(*) FROM clientes WHERE nome IS NULL OR TRIM(nome) = ''")
sem_nome = cur.fetchone()[0]
print(f"Clientes sem nome: {sem_nome:,}")

# 5. Verificar o rows_success=371265 do ID 8
# O processed tem 300k linhas, mas rows_success=371265
# Verificar quantos registros existem na tabela_macros que vieram do 300k
cur.execute("""
    SELECT COUNT(*) FROM tabela_macros
    WHERE data_extracao IS NOT NULL
""")
print(f"\ntabela_macros com data_extracao (operacional): {cur.fetchone()[0]:,}")

cur.execute("""
    SELECT COUNT(*) FROM tabela_macros
    WHERE data_extracao IS NULL
""")
print(f"tabela_macros sem data_extracao (historico): {cur.fetchone()[0]:,}")

cur.execute("SELECT COUNT(*) FROM tabela_macros")
print(f"tabela_macros total: {cur.fetchone()[0]:,}")

# 6. Verificar se saida_unica já foi lida para enriquecer
# saida_unica tem: data_contrato, parcelamento, etc
# Verificar tabela_macro_api
cur.execute("SELECT COUNT(*) FROM tabela_macro_api")
print(f"\ntabela_macro_api total: {cur.fetchone()[0]:,}")

cur.execute("""
    SELECT COUNT(*) FROM tabela_macro_api
    WHERE data_contrato IS NOT NULL
""")
print(f"tabela_macro_api com data_contrato: {cur.fetchone()[0]:,}")

# 7. Verificar o que NÃO tem no banco
# Pegar CPFs do raw1 que não tem nome no banco
cpfs_lista = list(cpfs_raw1)[:500]
fmt = ','.join(['%s'] * len(cpfs_lista))
cur.execute(
    f"SELECT cpf, nome FROM clientes WHERE cpf IN ({fmt}) AND (nome IS NULL OR TRIM(nome) = '')",
    cpfs_lista
)
sem_nome_raw = cur.fetchall()
print(f"\nAmostra 500 CPFs raw1 → sem nome no banco: {len(sem_nome_raw)}")

# Nomes do raw1 que poderiam enriquecer
nomes_raw1 = df_raw1.set_index(
    df_raw1['cpf'].str.replace(r'[.-]', '', regex=True).str.zfill(11)
)['nome'].to_dict()

# Quantos CPFs no banco inteiro estão sem nome mas têm nome no raw1?
cur.execute("SELECT cpf FROM clientes WHERE nome IS NULL OR TRIM(nome) = ''")
cpfs_sem_nome = [r[0] for r in cur.fetchall()]
recuperaveis = sum(1 for cpf in cpfs_sem_nome if cpf in nomes_raw1 and pd.notna(nomes_raw1.get(cpf)))
print(f"CPFs sem nome no banco que têm nome no raw1: {recuperaveis}")

# 8. Check the state file
import json
state_file = 'dados/fornecedor2/migration_periodo_pos_20260312/state/importacao_progresso.json'
if os.path.exists(state_file):
    with open(state_file, encoding='utf-8') as f:
        state = json.load(f)
    print(f"\nState file:")
    print(json.dumps(state, indent=2, ensure_ascii=False, default=str)[:2000])

c.close()
print(f"\n{SEP}")
print("FIM DIAGNÓSTICO")
print(SEP)
