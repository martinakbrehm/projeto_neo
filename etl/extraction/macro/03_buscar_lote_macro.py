"""
03_buscar_lote_macro.py
=======================
ETAPA AUTOMÁTICA — Passo 1 do ciclo da macro.

Responsabilidade:
  1. Consulta o banco com a lógica de prioridade do pipeline:
       a) pendente antes de reprocessar
       b) fornecedor2 antes de contatus (dentro do mesmo status)
       c) mais antigo primeiro dentro de mesmo status+fornecedor
  2. Marca os registros selecionados como 'processando' para evitar
     dupla captura em execuções concorrentes.
  3. Exporta o lote como CSV no caminho esperado pela macro
     (macro/dados/lote_pendente.csv).
  4. Salva metadados do lote (macro/dados/lote_meta.json) para
     que 04_processar_retorno_macro.py possa correlacionar resultados.

Dependências:
  - Migração 001 aplicada (tabela cliente_origem)
    Sem ela, todos os registros são tratados como fornecedor2 (LEFT JOIN).
  - tabela_macros com status IN ('pendente', 'reprocessar')
  - cliente_uc populado (uc é a ContaContrato enviada à API)

Chamado por:
  macro/macro/executar_automatico.py  (antes do túnel SSH)

Uso manual:
  python etl/extraction/macro/03_buscar_lote_macro.py
  python etl/extraction/macro/03_buscar_lote_macro.py --tamanho 1000
  python etl/extraction/macro/03_buscar_lote_macro.py --dry-run
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pymysql

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from config import db_destino  # noqa: E402

DB_CONFIG = db_destino(autocommit=False)

# Caminho de saída esperado pela macro
LOTE_CSV  = ROOT / "macro" / "dados" / "lote_pendente.csv"
LOTE_META = ROOT / "macro" / "dados" / "lote_meta.json"

TAMANHO_PADRAO = 2000
SEP = "=" * 70

# ---------------------------------------------------------------------------
# Query de prioridade
# ---------------------------------------------------------------------------
# Ordem de prioridade:
#   1. status='pendente' > status='reprocessar'
#   2. fornecedor='fornecedor2' > fornecedor='contatus' (ou NULL)
#   3. data_update ASC (mais antigo primeiro — garante que nenhum registro
#      fique esperando indefinidamente)
#   4. id ASC (desempate determinístico)
#
# Dois flavours de SQL: com e sem cliente_origem.
# Se a melhoria 20260406 ainda não foi aplicada, a tabela não existe →
# detectamos em runtime e usamos o SQL simples (todos tratados como fornecedor2).
# ---------------------------------------------------------------------------
SQL_BUSCAR_LOTE_COM_ORIGEM = """
SELECT
    tm.id                          AS macro_id,
    c.cpf                          AS cpf,
    cu.uc                          AS `codigo cliente`,
    d.nome                         AS empresa,
    tm.status                      AS status_atual,
    COALESCE(co.fornecedor, 'fornecedor2') AS fornecedor
FROM tabela_macros tm
JOIN clientes       c  ON c.id  = tm.cliente_id
JOIN cliente_uc     cu ON cu.cliente_id      = tm.cliente_id
                       AND cu.distribuidora_id = tm.distribuidora_id
JOIN distribuidoras d  ON d.id  = tm.distribuidora_id
LEFT JOIN cliente_origem co ON co.cliente_id = tm.cliente_id
WHERE tm.status IN ('pendente', 'reprocessar')
ORDER BY
    (tm.status = 'pendente')                              DESC,
    (COALESCE(co.fornecedor, 'fornecedor2') = 'fornecedor2') DESC,
    tm.data_update                                         ASC,
    tm.id                                                  ASC
LIMIT %s
"""

# Fallback: sem JOIN em cliente_origem (melhoria 20260406 ainda não aplicada)
SQL_BUSCAR_LOTE_SEM_ORIGEM = """
SELECT
    tm.id                          AS macro_id,
    c.cpf                          AS cpf,
    cu.uc                          AS `codigo cliente`,
    d.nome                         AS empresa,
    tm.status                      AS status_atual,
    'fornecedor2'                  AS fornecedor
FROM tabela_macros tm
JOIN clientes       c  ON c.id  = tm.cliente_id
JOIN cliente_uc     cu ON cu.cliente_id      = tm.cliente_id
                       AND cu.distribuidora_id = tm.distribuidora_id
JOIN distribuidoras d  ON d.id  = tm.distribuidora_id
WHERE tm.status IN ('pendente', 'reprocessar')
ORDER BY
    (tm.status = 'pendente') DESC,
    tm.data_update           ASC,
    tm.id                    ASC
LIMIT %s
"""


def _tabela_existe(conn, tabela: str) -> bool:
    """Verifica se uma tabela existe no banco atual."""
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = DATABASE() AND table_name = %s",
        (tabela,)
    )
    existe = cur.fetchone()[0] > 0
    cur.close()
    return existe

SQL_MARCAR_PROCESSANDO = """
UPDATE tabela_macros
SET status = 'processando',
    data_update = NOW()
WHERE id IN ({placeholders})
  AND status IN ('pendente', 'reprocessar')
"""


def buscar_lote(conn, tamanho: int, dry_run: bool) -> pd.DataFrame:
    cur = conn.cursor(pymysql.cursors.DictCursor)

    # Detecta se a melhoria 20260406 já foi aplicada
    if _tabela_existe(conn, "cliente_origem"):
        sql = SQL_BUSCAR_LOTE_COM_ORIGEM
    else:
        print("  [AVISO] Tabela cliente_origem não encontrada — usando fallback (tudo como fornecedor2).")
        print("          Execute db/improvements/20260406_cliente_origem_views_fornecedor/migration.py para ativar priorização por fornecedor.")
        sql = SQL_BUSCAR_LOTE_SEM_ORIGEM

    print(f"  Consultando lote de até {tamanho:,} registros...")
    cur.execute(sql, (tamanho,))
    rows = cur.fetchall()

    if not rows:
        print("  [INFO] Nenhum registro pendente ou a reprocessar.")
        cur.close()
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # Estatísticas do lote
    resumo = df.groupby(["status_atual", "fornecedor"]).size().reset_index(name="qtd")
    print(f"\n  Lote obtido: {len(df):,} registros")
    for _, r in resumo.iterrows():
        print(f"    {r['status_atual']:<12} | {r['fornecedor']:<15} | {r['qtd']:>6,}")

    if not dry_run:
        ids = df["macro_id"].tolist()
        placeholders = ",".join(["%s"] * len(ids))
        cur.execute(SQL_MARCAR_PROCESSANDO.format(placeholders=placeholders), ids)
        conn.commit()
        print(f"\n  [OK] {cur.rowcount:,} registros marcados como 'processando'")

    cur.close()
    return df


def exportar_csv(df: pd.DataFrame, dry_run: bool):
    """Exporta o lote no formato esperado pela macro (consulta_contrato.py)."""
    LOTE_CSV.parent.mkdir(parents=True, exist_ok=True)

    # A macro espera: cpf, codigo cliente, empresa
    # Mantemos macro_id e fornecedor apenas nos metadados (lote_meta.json)
    df_macro = df[["cpf", "codigo cliente", "empresa"]].copy()

    if dry_run:
        print(f"\n  [DRY-RUN] CSV seria exportado para: {LOTE_CSV}")
        print(f"  [DRY-RUN] {len(df_macro):,} linhas")
        print(df_macro.head(3).to_string(index=False))
        return

    df_macro.to_csv(LOTE_CSV, index=False, encoding="utf-8")
    print(f"\n  [OK] CSV exportado → {LOTE_CSV}")
    print(f"       {len(df_macro):,} linhas")


def salvar_meta(df: pd.DataFrame, dry_run: bool):
    """Salva metadados do lote para correlação no passo 4."""
    meta = {
        "gerado_em": datetime.now().isoformat(),
        "total": len(df),
        "dry_run": dry_run,
        # Lista de IDs para que 04_processar_retorno possa mapear cpf+uc → macro_id
        "registros": df[["macro_id", "cpf", "codigo cliente", "empresa", "fornecedor"]].to_dict(orient="records"),
    }

    if dry_run:
        print(f"  [DRY-RUN] META seria salvo em: {LOTE_META}")
        return

    LOTE_META.parent.mkdir(parents=True, exist_ok=True)
    with open(LOTE_META, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    print(f"  [OK] Meta salvo → {LOTE_META}")


def main():
    parser = argparse.ArgumentParser(
        description="Passo 1 da macro: busca lote priorizado do banco"
    )
    parser.add_argument("--tamanho", type=int, default=TAMANHO_PADRAO,
                        help=f"Tamanho máximo do lote (padrão: {TAMANHO_PADRAO})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Consulta sem marcar como 'processando' e sem exportar")
    args = parser.parse_args()

    print(SEP)
    print(f"PASSO 03  —  Buscar lote macro  |  tamanho={args.tamanho:,}")
    if args.dry_run:
        print("  [DRY-RUN] nenhuma alteração será gravada")
    print(SEP)

    conn = pymysql.connect(**DB_CONFIG)
    try:
        df = buscar_lote(conn, args.tamanho, args.dry_run)
        if df.empty:
            print("\n[INFO] Lote vazio — macro não será executada.")
            sys.exit(0)

        exportar_csv(df, args.dry_run)
        salvar_meta(df, args.dry_run)

        print(f"\n{SEP}")
        print("PASSO 03 CONCLUÍDO — lote pronto para a macro")
        print(SEP)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
