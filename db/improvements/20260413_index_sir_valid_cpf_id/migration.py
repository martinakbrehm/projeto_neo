"""
Migration: 20260413_index_sir_valid_cpf_id
Adiciona índice cobrindo (validation_status, normalized_cpf, id) em staging_import_rows.

Este índice cobre a subquery de lookup de arquivo_origem:
    SELECT normalized_cpf, MAX(id) AS max_id
    FROM staging_import_rows
    WHERE validation_status = 'valid'
    GROUP BY normalized_cpf

Com o índice (validation_status, normalized_cpf, id):
- Filtra validation_status='valid' diretamente no índice (sem table scan)
- GROUP BY normalized_cpf segue no índice
- MAX(id) obtido do último valor no grupo — leitura somente de índice
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from config import db_destino
import pymysql


def run():
    conn = pymysql.connect(**db_destino())
    try:
        with conn.cursor() as cur:
            # Verificar se o índice já existe
            cur.execute("""
                SELECT COUNT(*) FROM information_schema.STATISTICS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = 'staging_import_rows'
                  AND INDEX_NAME = 'idx_sir_valid_cpf_id'
            """)
            if cur.fetchone()[0] > 0:
                print("Índice idx_sir_valid_cpf_id já existe, pulando.")
                return

            print("Criando índice cobrindo (validation_status, normalized_cpf, id) em staging_import_rows...")
            cur.execute("""
                ALTER TABLE staging_import_rows
                ADD INDEX idx_sir_valid_cpf_id (validation_status, normalized_cpf, id)
            """)
            conn.commit()
            print("Índice criado com sucesso.")

            # Testar a subquery que usará o índice
            print("Testando subquery com novo índice...")
            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT normalized_cpf, MAX(id) AS max_id
                    FROM staging_import_rows
                    WHERE validation_status = 'valid'
                    GROUP BY normalized_cpf
                ) t
            """)
            count = cur.fetchone()[0]
            print(f"CPFs únicos válidos em staging_import_rows: {count}")
    finally:
        conn.close()


if __name__ == "__main__":
    run()
