"""
inspecionar_controle_bases.py
=============================
Inspeciona o banco controle_bases, tabela neo.

Exibe:
  1. Colunas e tipos da tabela
  2. Contagem total de linhas
  3. Colunas com NULL / vazias e % de preenchimento
  4. Amostra das primeiras 10 linhas
  5. Colunas que parecem CPF (tenta detectar automaticamente)
  6. Contagem de CPFs distintos encontrados no banco de destino
     (bd_Automacoes_time_dadosV2.clientes) para estimar o aproveitamento

Uso:
    python scripts/inspecionar_controle_bases.py
"""

import sys
from pathlib import Path

import pymysql
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import db_destino, db_origem  # noqa: E402

# ---------------------------------------------------------------------------
# Configs
# ---------------------------------------------------------------------------
DB_CONFIG_ORIGEM  = db_origem()
DB_CONFIG_DESTINO = db_destino()

TABELA = "neo"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
SEP = "=" * 70


def conectar(cfg: dict, label: str):
    try:
        conn = pymysql.connect(**cfg)
        print(f"[OK] Conectado ao banco: {label}")
        return conn
    except Exception as e:
        print(f"[ERRO] Falha ao conectar em {label}: {e}")
        raise


def resumo_colunas(df: pd.DataFrame):
    total = len(df)
    print(f"\n{'Coluna':<40} {'Tipo':<20} {'Preenchidas':>12} {'%':>7}")
    print("-" * 82)
    for col in df.columns:
        nao_nulo = df[col].notna().sum()
        # considera string vazia também como ausente
        if df[col].dtype == object:
            nao_nulo = df[col].replace("", pd.NA).notna().sum()
        pct = nao_nulo / total * 100 if total else 0
        tipo = str(df[col].dtype)
        print(f"{col:<40} {tipo:<20} {nao_nulo:>12,} {pct:>6.1f}%")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    # ── 1. Conectar à origem ────────────────────────────────────────────────
    conn_orig = conectar(DB_CONFIG_ORIGEM, f"controle_bases")
    cur = conn_orig.cursor()

    # ── 2. Estrutura da tabela ──────────────────────────────────────────────
    print(f"\n{SEP}")
    print(f"ESTRUTURA DA TABELA  `{TABELA}`")
    print(SEP)
    cur.execute(f"DESCRIBE `{TABELA}`")
    colunas_info = cur.fetchall()
    print(f"\n{'Campo':<40} {'Tipo':<30} {'Null':>6} {'Key':>6} {'Default'}")
    print("-" * 95)
    for row in colunas_info:
        campo, tipo, nulo, key, default, extra = row
        print(f"{campo:<40} {tipo:<30} {nulo:>6} {key:>6}  {str(default)}")

    nomes_colunas = [r[0] for r in colunas_info]

    # ── 3. Contagem total ───────────────────────────────────────────────────
    cur.execute(f"SELECT COUNT(*) FROM `{TABELA}`")
    total_linhas = cur.fetchone()[0]
    print(f"\n[INFO] Total de linhas: {total_linhas:,}")

    # ── 4. Amostra (primeiras 10 linhas) ───────────────────────────────────
    print(f"\n{SEP}")
    print("AMOSTRA  (primeiras 10 linhas)")
    print(SEP)
    cur.execute(f"SELECT * FROM `{TABELA}` LIMIT 10")
    rows_sample = cur.fetchall()
    df_sample = pd.DataFrame(rows_sample, columns=nomes_colunas)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)
    pd.set_option("display.max_colwidth", 40)
    print(df_sample.to_string(index=False))

    # ── 5. Preenchimento de todas as colunas ────────────────────────────────
    print(f"\n{SEP}")
    print("PREENCHIMENTO POR COLUNA")
    print(SEP)
    # Carrega amostra maior para estatísticas (máx 5 000 linhas)
    cur.execute(f"SELECT * FROM `{TABELA}` LIMIT 5000")
    rows_stat = cur.fetchall()
    df_stat = pd.DataFrame(rows_stat, columns=nomes_colunas)
    resumo_colunas(df_stat)

    # ── 6. Detectar coluna CPF ──────────────────────────────────────────────
    print(f"\n{SEP}")
    print("DETECÇÃO DE COLUNA CPF")
    print(SEP)
    candidatos_cpf = []
    for col in nomes_colunas:
        nome_lower = col.lower()
        if any(k in nome_lower for k in ("cpf", "documento", "doc_", "cpfcnpj")):
            candidatos_cpf.append(col)
    if candidatos_cpf:
        print(f"Candidatos encontrados pelo nome: {candidatos_cpf}")
        for col in candidatos_cpf:
            sample_vals = df_stat[col].dropna().astype(str).head(5).tolist()
            print(f"  {col}: exemplos → {sample_vals}")
    else:
        print("Nenhuma coluna com nome óbvio de CPF. Verifique a amostra acima manualmente.")

    # ── 7. Cruzamento com destino (quantos CPFs já existem) ─────────────────
    if candidatos_cpf:
        col_cpf_origem = candidatos_cpf[0]
        print(f"\n{SEP}")
        print(f"CRUZAMENTO COM DESTINO  (coluna origem: `{col_cpf_origem}`)")
        print(SEP)
        try:
            conn_dest = conectar(DB_CONFIG_DESTINO, "bd_Automacoes_time_dadosV2")
            cur_dest = conn_dest.cursor()

            # Conta CPFs distintos na origem
            cur.execute(f"SELECT COUNT(DISTINCT `{col_cpf_origem}`) FROM `{TABELA}`")
            cpfs_distintos_orig = cur.fetchone()[0]
            print(f"CPFs distintos na origem : {cpfs_distintos_orig:,}")

            # Conta CPFs do destino que existem na origem (via JOIN no servidor)
            cur_dest.execute(
                f"""
                SELECT COUNT(DISTINCT c.cpf)
                FROM bd_Automacoes_time_dadosV2.clientes c
                """
            )
            cpfs_destino = cur_dest.fetchone()[0]
            print(f"CPFs no destino (clientes): {cpfs_destino:,}")

            cur_dest.close()
            conn_dest.close()
        except Exception as e:
            print(f"[AVISO] Não foi possível cruzar com o destino: {e}")

    # ── 8. Valores distintos para colunas de baixa cardinalidade ────────────
    print(f"\n{SEP}")
    print("VALORES DISTINTOS (colunas com < 50 valores únicos na amostra)")
    print(SEP)
    for col in nomes_colunas:
        distintos = df_stat[col].nunique()
        if 0 < distintos < 50:
            vals = df_stat[col].value_counts().head(10).to_dict()
            print(f"\n  {col}  ({distintos} distintos):")
            for v, cnt in vals.items():
                print(f"    {str(v):<40} → {cnt:,}")

    cur.close()
    conn_orig.close()
    print(f"\n{SEP}")
    print("Inspeção concluída.")
    print(SEP)


if __name__ == "__main__":
    main()
