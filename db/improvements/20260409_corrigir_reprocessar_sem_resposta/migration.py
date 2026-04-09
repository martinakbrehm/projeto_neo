"""
Migration: 20260409_corrigir_reprocessar_sem_resposta

Problema:
    Existem 11.173 registros em tabela_macros com status='reprocessar' e resposta_id=NULL.
    Esses registros ainda não têm resposta definitiva e deveriam estar como 'pendente',
    aguardando nova consulta na macro.

Correção:
    UPDATE tabela_macros SET status='pendente' WHERE status='reprocessar' AND resposta_id IS NULL
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from config import db_destino  # noqa: E402
import pymysql

DRY_RUN = "--dry-run" in sys.argv


def main():
    conn = pymysql.connect(**db_destino())
    cur = conn.cursor()

    # Diagnóstico
    cur.execute("""
        SELECT COUNT(*)
        FROM tabela_macros
        WHERE status = 'reprocessar' AND resposta_id IS NULL
    """)
    qtd = cur.fetchone()[0]
    print(f"Registros reprocessar sem resposta_id: {qtd:,}")

    if qtd == 0:
        print("Nada a corrigir.")
        conn.close()
        return

    if DRY_RUN:
        print("[DRY-RUN] Nenhuma alteração aplicada.")
        conn.close()
        return

    # Corrigir: reprocessar sem resposta → pendente
    cur.execute("""
        UPDATE tabela_macros
        SET status = 'pendente'
        WHERE status = 'reprocessar' AND resposta_id IS NULL
    """)
    print(f"Corrigidos: {cur.rowcount:,} registros → status='pendente'")

    conn.commit()

    # Verificação final
    cur.execute("""
        SELECT status, COUNT(*) FROM tabela_macros
        WHERE status IN ('pendente', 'reprocessar')
        GROUP BY status ORDER BY status
    """)
    print("\nDistribuição após correção:")
    for r in cur.fetchall():
        print(f"  {r[0]}: {r[1]:,}")

    conn.close()
    print("\nMigration concluída.")


if __name__ == "__main__":
    main()
