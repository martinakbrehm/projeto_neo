"""
Migration: 20260416_ucs_no_arquivo_dashboard
============================================
Adiciona coluna ucs_no_arquivo em dashboard_arquivos_agg.

Contexto:
    A tabela dashboard_arquivos_agg já tinha cpfs_no_arquivo (CPFs únicos por arquivo)
    e ucs_ineditas (CPF+UC combos inéditas), mas não o total de CPF+UC combos no arquivo.
    Sem o total, não era possível calcular quantas UCs já existiam no banco antes do arquivo.

O que faz:
    ALTER TABLE dashboard_arquivos_agg ADD COLUMN ucs_no_arquivo INT UNSIGNED NOT NULL DEFAULT 0
    AFTER cpfs_no_arquivo

Após a migration, executar refresh_scheduler --once para repopular a tabela.

Uso:
    python db/improvements/20260416_ucs_no_arquivo_dashboard/migration.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from config import db_destino
import pymysql


def run():
    conn = pymysql.connect(**db_destino())
    cur  = conn.cursor()

    # Verificar se a coluna já existe
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME   = 'dashboard_arquivos_agg'
          AND COLUMN_NAME  = 'ucs_no_arquivo'
    """)
    if cur.fetchone()[0] > 0:
        print("Coluna ucs_no_arquivo já existe — pulando.")
        conn.close()
        return

    print("Adicionando coluna ucs_no_arquivo em dashboard_arquivos_agg...")
    cur.execute("""
        ALTER TABLE dashboard_arquivos_agg
            ADD COLUMN ucs_no_arquivo INT UNSIGNED NOT NULL DEFAULT 0
            AFTER cpfs_no_arquivo
    """)
    conn.commit()
    print("Coluna adicionada com sucesso.")
    print("Execute refresh_scheduler --once para repopular os dados.")

    cur.close()
    conn.close()


if __name__ == "__main__":
    run()
