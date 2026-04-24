"""
Migration: Backfill status do 300k a partir de resultados rodados manualmente
Data: 2026-04-24

Origem: dados/fornecedor2/migration_periodo_pos_20260312/raw/saida_unica_dados_filtrados_sem_erros.xlsx
  - 293.380 linhas com resultado da macro rodada manualmente
  - CodigoRetorno mapeado diretamente para resposta_id (0-5)
  - empresa → distribuidora_id (celpe=3, coelba=1, cosern=2)
  - data_hora como data_update (spread real de 31/03 a 13/04)

Ação:
  Para cada linha no xlsx, encontra o registro pendente em tabela_macros
  com (cpf, distribuidora) e atualiza:
    status → respostas.status correspondente
    resposta_id → CodigoRetorno
    data_update → data_hora do xlsx

  Só atualiza registros com status='pendente' para não sobrescrever
  resultados já processados pela macro automatizada.
"""
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
import config
import pymysql

XLSX_PATH = Path(__file__).resolve().parents[3] / \
    "dados" / "fornecedor2" / "migration_periodo_pos_20260312" / "raw" / \
    "saida_unica_dados_filtrados_sem_erros.xlsx"

DIST_MAP = {"celpe": 3, "coelba": 1, "cosern": 2}

# CodigoRetorno → resposta_id (direto) → status
RESPOSTA_STATUS = {
    0: "excluido",       # Conta Contrato não existe
    1: "excluido",       # Doc. fiscal não existe
    2: "excluido",       # Titularidade não confirmada
    3: "consolidado",    # Titularidade confirmada com contrato ativo
    4: "reprocessar",    # Titularidade confirmada com contrato inativo
    5: "reprocessar",    # Titularidade confirmada com inst. suspensa
}

BATCH_SIZE = 5000


def main():
    print("=" * 70)
    print("Backfill: status do 300k a partir de resultados manuais")
    print("=" * 70)

    # 1. Carregar xlsx
    print(f"\nCarregando {XLSX_PATH.name}...")
    df = pd.read_excel(str(XLSX_PATH))
    print(f"  {len(df)} linhas carregadas")

    # Normalizar
    df["_cpf_norm"] = df["_cpf_norm"].astype(str).str.zfill(11)
    df["dist_id"] = df["empresa"].map(DIST_MAP)
    df["resp_id"] = df["CodigoRetorno"].fillna(-1).astype(int)
    df["new_status"] = df["resp_id"].map(RESPOSTA_STATUS)
    df["data_hora_parsed"] = pd.to_datetime(df["data_hora"], errors="coerce")

    # Remover linhas sem mapeamento válido
    valid = df[df["new_status"].notna() & df["dist_id"].notna()].copy()
    invalid = len(df) - len(valid)
    if invalid > 0:
        print(f"  {invalid} linhas sem mapeamento válido (ignoradas)")
    print(f"  {len(valid)} linhas válidas para atualização")

    # 2. Conectar ao banco
    conn = pymysql.connect(**config.db_destino(), read_timeout=600, write_timeout=600)
    cur = conn.cursor()

    # 3. Contar pendentes atuais do 300k
    cur.execute("""
        SELECT COUNT(*)
        FROM tabela_macros tm
        JOIN clientes cl ON cl.id = tm.cliente_id
        JOIN staging_import_rows sir ON sir.normalized_cpf = cl.cpf
            AND sir.staging_id = 8 AND sir.validation_status = 'valid'
        WHERE tm.status = 'pendente'
    """)
    pendentes_antes = cur.fetchone()[0]
    print(f"\n  Pendentes do 300k ANTES: {pendentes_antes:,}")

    # 4. Criar tabela temporária com os resultados
    print("\nCriando tabela temporária com resultados...")
    cur.execute("DROP TABLE IF EXISTS _tmp_backfill_300k")
    conn.commit()

    cur.execute("""
        CREATE TABLE _tmp_backfill_300k (
            cpf              CHAR(11)     NOT NULL,
            distribuidora_id INT UNSIGNED NOT NULL,
            resposta_id      INT          NOT NULL,
            new_status       VARCHAR(30)  NOT NULL,
            data_update      DATETIME     NULL,
            INDEX idx_cpf_dist (cpf, distribuidora_id)
        ) ENGINE=InnoDB
    """)
    conn.commit()

    # Inserir em batches
    insert_sql = """
        INSERT INTO _tmp_backfill_300k (cpf, distribuidora_id, resposta_id, new_status, data_update)
        VALUES (%s, %s, %s, %s, %s)
    """
    rows_to_insert = []
    for _, row in valid.iterrows():
        dt = row["data_hora_parsed"]
        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(dt) else None
        rows_to_insert.append((
            row["_cpf_norm"],
            int(row["dist_id"]),
            int(row["resp_id"]),
            row["new_status"],
            dt_str,
        ))

    total_inserted = 0
    for i in range(0, len(rows_to_insert), BATCH_SIZE):
        batch = rows_to_insert[i:i + BATCH_SIZE]
        cur.executemany(insert_sql, batch)
        conn.commit()
        total_inserted += len(batch)
        if total_inserted % 50000 == 0 or total_inserted == len(rows_to_insert):
            print(f"  Inseridos {total_inserted:,} / {len(rows_to_insert):,}")

    print(f"  Total na tabela temporária: {total_inserted:,}")

    # 5. UPDATE: apenas registros pendentes
    print("\nAtualizando tabela_macros (somente pendentes)...")
    t0 = time.time()
    cur.execute("""
        UPDATE tabela_macros tm
        JOIN clientes cl ON cl.id = tm.cliente_id
        JOIN _tmp_backfill_300k tmp
            ON tmp.cpf = cl.cpf
            AND tmp.distribuidora_id = tm.distribuidora_id
        SET tm.status      = tmp.new_status,
            tm.resposta_id = tmp.resposta_id,
            tm.data_update = COALESCE(tmp.data_update, NOW())
        WHERE tm.status = 'pendente'
    """)
    updated = cur.rowcount
    conn.commit()
    elapsed = time.time() - t0
    print(f"  Atualizados: {updated:,} registros ({elapsed:.1f}s)")

    # 6. Contar pendentes depois
    cur.execute("""
        SELECT COUNT(*)
        FROM tabela_macros tm
        JOIN clientes cl ON cl.id = tm.cliente_id
        JOIN staging_import_rows sir ON sir.normalized_cpf = cl.cpf
            AND sir.staging_id = 8 AND sir.validation_status = 'valid'
        WHERE tm.status = 'pendente'
    """)
    pendentes_depois = cur.fetchone()[0]
    print(f"\n  Pendentes do 300k ANTES:  {pendentes_antes:,}")
    print(f"  Pendentes do 300k DEPOIS: {pendentes_depois:,}")
    print(f"  Redução: {pendentes_antes - pendentes_depois:,}")

    # 7. Verificar distribuição de status após update
    cur.execute("""
        SELECT tm.status, COUNT(*) as cnt
        FROM staging_import_rows sir
        JOIN clientes cl ON cl.cpf = sir.normalized_cpf
        JOIN tabela_macros tm ON tm.cliente_id = cl.id
        WHERE sir.staging_id = 8 AND sir.validation_status = 'valid'
        GROUP BY tm.status
        ORDER BY cnt DESC
    """)
    print("\n  Status do 300k após backfill:")
    for r in cur.fetchall():
        print(f"    {r[0]:20s} {r[1]:>8,}")

    # 8. Cleanup
    cur.execute("DROP TABLE IF EXISTS _tmp_backfill_300k")
    conn.commit()

    cur.close()
    conn.close()
    print("\nConcluído.")


if __name__ == "__main__":
    main()
