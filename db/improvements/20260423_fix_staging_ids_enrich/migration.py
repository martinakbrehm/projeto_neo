"""
Migration: 20260423_fix_staging_ids_enrich
Últimas correções:
  1. Move id=20 → id=12 (sem gap no autoincrement)
  2. Corrige rows_success do id=8 de 371265 → 300000
  3. Enriquece ~1408 clientes sem nome usando clientes_300k_25_03.csv
  4. Corrige filename do id=8: '23-03/300k.csv' → '300k.csv'
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
import config
import pymysql
import pandas as pd

SEP = "=" * 70
ROOT = os.path.join(os.path.dirname(__file__), '..', '..', '..')


def run():
    conn = pymysql.connect(**config.db_destino())
    cur = conn.cursor()

    print(SEP)
    print("CORREÇÃO FINAL: staging IDs + enriquecimento nomes")
    print(SEP)

    # ------------------------------------------------------------------
    # PASSO 1: Corrigir rows_success do id=8
    # ------------------------------------------------------------------
    print("\n[1/4] Corrigindo rows_success do id=8 (300k.csv)...")
    cur.execute(
        "UPDATE staging_imports SET rows_success = %s, filename = %s WHERE id = %s",
        (300000, '300k.csv', 8)
    )
    conn.commit()
    print("  rows_success: 371265 → 300000")
    print("  filename: '23-03/300k.csv' → '300k.csv'")

    # ------------------------------------------------------------------
    # PASSO 2: Mover id=20 → id=12
    # ------------------------------------------------------------------
    print("\n[2/4] Movendo id=20 → id=12...")

    # 2a. Verificar que id=12 não existe
    cur.execute("SELECT COUNT(*) FROM staging_imports WHERE id = 12")
    if cur.fetchone()[0] > 0:
        print("  ERRO: id=12 já existe! Pulando.")
    else:
        # Desabilitar FK check temporariamente
        cur.execute("SET FOREIGN_KEY_CHECKS = 0")

        # Inserir cópia com id=12
        cur.execute("""
            INSERT INTO staging_imports (id, filename, distribuidora_nome, target_macro_table,
                total_rows, rows_success, rows_failed, status, imported_by,
                created_at, started_at, finished_at)
            SELECT 12, filename, distribuidora_nome, target_macro_table,
                total_rows, rows_success, rows_failed, status, imported_by,
                created_at, started_at, finished_at
            FROM staging_imports WHERE id = 20
        """)
        conn.commit()
        print("  Criado id=12 com dados do id=20")

        # Atualizar staging_import_rows
        cur.execute("UPDATE staging_import_rows SET staging_id = 12 WHERE staging_id = 20")
        n_rows = cur.rowcount
        conn.commit()
        print(f"  staging_import_rows: {n_rows:,} rows atualizadas (staging_id 20→12)")

        # Deletar id=20
        cur.execute("DELETE FROM staging_imports WHERE id = 20")
        conn.commit()
        print("  Deletado id=20")

        # Reativar FK check
        cur.execute("SET FOREIGN_KEY_CHECKS = 1")

        # Resetar AUTO_INCREMENT para próximo valor correto (13)
        cur.execute("ALTER TABLE staging_imports AUTO_INCREMENT = 13")
        conn.commit()
        print("  AUTO_INCREMENT resetado para 13")

    # ------------------------------------------------------------------
    # PASSO 3: Enriquecer clientes sem nome usando clientes_300k_25_03.csv
    # ------------------------------------------------------------------
    print("\n[3/4] Enriquecendo clientes sem nome...")

    raw_path = os.path.join(ROOT, 'dados', 'fornecedor2',
                            'migration_periodo_pos_20260312', 'raw',
                            'clientes_300k_25_03.csv')
    df_raw = pd.read_csv(raw_path, dtype=str, encoding='utf-8-sig',
                         sep=None, engine='python')

    # Build CPF → nome dict
    nomes = {}
    for _, row in df_raw.iterrows():
        cpf = str(row['cpf']).replace('.', '').replace('-', '').zfill(11)
        nome = str(row.get('nome', '')).strip()
        if nome and nome.lower() != 'nan':
            nomes[cpf] = nome

    # Get clients without a name
    cur.execute("SELECT id, cpf FROM clientes WHERE nome IS NULL OR TRIM(nome) = ''")
    sem_nome = cur.fetchall()
    print(f"  Clientes sem nome no banco: {len(sem_nome):,}")

    # Build batch of updates
    batch = [(cid, nomes[cpf]) for cid, cpf in sem_nome if cpf in nomes]
    print(f"  Preparados para atualizar: {len(batch):,}")

    if batch:
        # Usar temp table + JOIN UPDATE (muito mais rápido que executemany para UPDATEs)
        cur.execute("DROP TEMPORARY TABLE IF EXISTS _tmp_nomes")
        cur.execute("""
            CREATE TEMPORARY TABLE _tmp_nomes (
                cliente_id INT PRIMARY KEY,
                nome VARCHAR(255)
            )
        """)
        cur.executemany(
            "INSERT INTO _tmp_nomes (cliente_id, nome) VALUES (%s, %s)",
            batch
        )
        cur.execute("""
            UPDATE clientes c
            INNER JOIN _tmp_nomes t ON c.id = t.cliente_id
            SET c.nome = t.nome
        """)
        updated = cur.rowcount
        cur.execute("DROP TEMPORARY TABLE IF EXISTS _tmp_nomes")
        conn.commit()
    else:
        updated = 0
    print(f"  Nomes recuperados do clientes_300k: {updated:,}")

    # Verificar restantes
    cur.execute("SELECT COUNT(*) FROM clientes WHERE nome IS NULL OR TRIM(nome) = ''")
    restantes = cur.fetchone()[0]
    print(f"  Ainda sem nome: {restantes:,}")

    # ------------------------------------------------------------------
    # PASSO 4: Verificação final
    # ------------------------------------------------------------------
    print(f"\n[4/4] Verificação final...")

    cur.execute("""
        SELECT id, filename, total_rows, rows_success, rows_failed, imported_by
        FROM staging_imports ORDER BY id
    """)
    print("\n  staging_imports:")
    for r in cur.fetchall():
        flag = ' ***' if r[3] and r[2] and r[3] > r[2] else ''
        print(f"    id={r[0]:>3d}  tot={r[2]:>8,}  ok={r[3]:>8,}  "
              f"fail={r[4]:>8,}  by={r[5]:30s}  {r[1]}{flag}")

    cur.execute("SELECT COUNT(*) FROM staging_import_rows")
    print(f"\n  staging_import_rows total: {cur.fetchone()[0]:,}")

    cur.execute("SELECT MAX(id) FROM staging_imports")
    max_id = cur.fetchone()[0]
    print(f"  Max ID: {max_id}")

    cur.close()
    conn.close()
    print(f"\n{SEP}")
    print("CONCLUIDO")
    print(SEP)


if __name__ == "__main__":
    run()
