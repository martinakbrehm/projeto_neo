"""
Migration: 20260423_corrigir_staging_imports
Corrige a tabela staging_imports após reimportação retroativa:

  1. Remove duplicatas retroativas (IDs 12-19, 21-31)
     - staging_import_rows dessas entradas são deletadas
  2. Atualiza rows_success nos originais (1-11) com valores corretos
  3. Corrige imported_by conforme fonte real
  4. Corrige filename da entrada de historico_ate (ID 20)
  5. Ajusta created_at da entrada retroativa mantida (ID 20)
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
import config
import pymysql

SEP = "=" * 70


def run():
    conn = pymysql.connect(**config.db_destino())
    cur = conn.cursor()

    print(SEP)
    print("CORREÇÃO: staging_imports + staging_import_rows")
    print(SEP)

    # ------------------------------------------------------------------
    # Estado inicial
    # ------------------------------------------------------------------
    cur.execute("SELECT COUNT(*) FROM staging_imports")
    print(f"\nstaging_imports antes: {cur.fetchone()[0]}")
    cur.execute("SELECT COUNT(*) FROM staging_import_rows")
    print(f"staging_import_rows antes: {cur.fetchone()[0]:,}")

    # ------------------------------------------------------------------
    # PASSO 1: Atualizar rows_success nos originais usando valores retroativos
    # ------------------------------------------------------------------
    print(f"\n[1/5] Atualizando rows_success nos originais...")

    # Mapeamento retroativo_id → original_id
    retro_to_orig = {
        21: 1,   # 35K_20260402_CELP
        22: 2,   # 35K_20260402_COELBA
        23: 3,   # 35K_20260402_COSERN
        24: 4,   # celpe_final_3103
        25: 5,   # 20260409_CELP_35K
        26: 6,   # 20260409_COELBA_35K
        27: 7,   # 20260409_COSERN_35K
        28: 9,   # 20260414_CELP_35K
        29: 10,  # 20260414_COELBA_35K
        # 30 é duplicata pura de 29 → ignorar
        31: 11,  # lote_97_resultado (failed=161377, já igual)
    }

    updated = 0
    for retro_id, orig_id in retro_to_orig.items():
        cur.execute(
            "SELECT rows_success, rows_failed FROM staging_imports WHERE id = %s",
            (retro_id,)
        )
        row = cur.fetchone()
        if not row:
            continue
        cur.execute(
            "UPDATE staging_imports SET rows_success = %s, rows_failed = %s WHERE id = %s",
            (row[0], row[1], orig_id)
        )
        if cur.rowcount:
            updated += 1
            print(f"  id={orig_id:>3d} ← rows_success={row[0]:,}, rows_failed={row[1]:,} (de id={retro_id})")
    conn.commit()
    print(f"  Atualizados: {updated}")

    # ------------------------------------------------------------------
    # PASSO 2: Deletar staging_import_rows dos retroativos/duplicatas
    # ------------------------------------------------------------------
    print(f"\n[2/5] Deletando staging_import_rows duplicadas...")

    ids_to_delete = [12, 13, 14, 15, 16, 17, 18, 19,
                     21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]

    total_deleted_rows = 0
    for sid in ids_to_delete:
        cur.execute("DELETE FROM staging_import_rows WHERE staging_id = %s", (sid,))
        n = cur.rowcount
        if n > 0:
            total_deleted_rows += n
            print(f"  staging_id={sid:>3d}: {n:>8,} rows deletadas")
        conn.commit()
    print(f"  Total rows deletadas: {total_deleted_rows:,}")

    # ------------------------------------------------------------------
    # PASSO 3: Deletar staging_imports duplicadas
    # ------------------------------------------------------------------
    print(f"\n[3/5] Deletando staging_imports duplicadas...")

    for sid in ids_to_delete:
        cur.execute("DELETE FROM staging_imports WHERE id = %s", (sid,))
        if cur.rowcount:
            print(f"  Deletado id={sid}")
    conn.commit()
    print(f"  Total: {len(ids_to_delete)} entradas removidas")

    # ------------------------------------------------------------------
    # PASSO 4: Corrigir imported_by
    # ------------------------------------------------------------------
    print(f"\n[4/5] Corrigindo imported_by...")

    # ID 8: era 'migration_historica_pos_20260312', corrigir para padrão da pasta
    cur.execute(
        "UPDATE staging_imports SET imported_by = %s WHERE id = %s",
        ('migration_periodo_pos_20260312', 8)
    )
    print(f"  id=8: imported_by → 'migration_periodo_pos_20260312'")

    # ID 20: era 'reimport_retroativo', corrigir para fonte real
    cur.execute(
        "UPDATE staging_imports SET imported_by = %s WHERE id = %s",
        ('migration_periodo_ate_20260312', 20)
    )
    print(f"  id=20: imported_by → 'migration_periodo_ate_20260312'")
    conn.commit()

    # ------------------------------------------------------------------
    # PASSO 5: Corrigir filenames e created_at
    # ------------------------------------------------------------------
    print(f"\n[5/5] Corrigindo filenames e datas...")

    # ID 20: filename era 'retroativo/processed/historico_normalizado_para_importar.csv'
    cur.execute(
        "UPDATE staging_imports SET filename = %s, created_at = %s WHERE id = %s",
        ('historico_normalizado_para_importar.csv', '2026-03-23 00:00:00', 20)
    )
    print(f"  id=20: filename → 'historico_normalizado_para_importar.csv'")
    print(f"  id=20: created_at → 2026-03-23")
    conn.commit()

    # ------------------------------------------------------------------
    # Resultado final
    # ------------------------------------------------------------------
    print(f"\n{SEP}")
    print("RESULTADO FINAL:")
    print(SEP)

    cur.execute("SELECT COUNT(*) FROM staging_imports")
    print(f"\nstaging_imports: {cur.fetchone()[0]}")
    cur.execute("SELECT COUNT(*) FROM staging_import_rows")
    print(f"staging_import_rows: {cur.fetchone()[0]:,}")

    print("\nEntradas restantes:")
    cur.execute("""
        SELECT si.id, si.filename, si.imported_by, si.created_at,
               si.total_rows, si.rows_success, si.rows_failed,
               COUNT(sir.id) AS ref_rows
        FROM staging_imports si
        LEFT JOIN staging_import_rows sir ON sir.staging_id = si.id
        GROUP BY si.id
        ORDER BY si.id
    """)
    for r in cur.fetchall():
        print(f"  id={r[0]:>3d}  {r[1]:55s}  by={r[2]:35s}  "
              f"at={str(r[3])[:10]}  tot={r[4]:>7,}  ok={r[5]:>7,}  "
              f"fail={r[6]:>7,}  sir={r[7]:>8,}")

    cur.close()
    conn.close()
    print(f"\n{SEP}")
    print("CONCLUIDO")
    print(SEP)


if __name__ == "__main__":
    run()
