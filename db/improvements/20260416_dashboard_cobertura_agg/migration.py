"""
Migration: 20260416_dashboard_cobertura_agg
===========================================
Cria tabela materializada dashboard_cobertura_agg.

Unidade de contagem: combinações únicas de CPF+UC por arquivo.
Um mesmo CPF com UCs diferentes gera combinações distintas.
Combinações já vistas em arquivos anteriores são classificadas
como existentes; as que aparecem pela primeira vez, como novas.

Colunas:
  - total_combos      : pares CPF+UC distintos no arquivo
  - combos_novas      : pares CPF+UC que aparecem pela 1ª vez no sistema
  - combos_existentes : pares CPF+UC já vistos em arquivos anteriores

Uso:
    python db/improvements/20260416_dashboard_cobertura_agg/migration.py
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from config import db_destino
import pymysql


def run():
    conn = pymysql.connect(**db_destino(), connect_timeout=30)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS dashboard_cobertura_agg (
            id                INT UNSIGNED NOT NULL AUTO_INCREMENT,
            arquivo           VARCHAR(255) NOT NULL,
            data_carga        DATE         NOT NULL,
            total_combos      INT UNSIGNED NOT NULL DEFAULT 0,
            combos_novas      INT UNSIGNED NOT NULL DEFAULT 0,
            combos_existentes INT UNSIGNED NOT NULL DEFAULT 0,
            atualizado_em     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            UNIQUE KEY ux_cobertura_arquivo (arquivo)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    conn.commit()
    print("Tabela dashboard_cobertura_agg criada (ou já existia).")
    cur.close()
    conn.close()


if __name__ == "__main__":
    run()
