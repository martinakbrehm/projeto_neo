"""
Migration: 20260414_materialized_dashboard_macros_agg
Implementa Tabela Materializada para o dashboard de macros.

Estratégia:
  - Tabela física `dashboard_macros_agg` (não é view): SELECT é O(1) — lê dados já prontos
  - Stored procedure `sp_refresh_dashboard_macros_agg`: TRUNCATE + INSERT ... SELECT
    → chamada ao final do ETL (passo 04) sempre que novos dados chegarem
  - Índices cobrindo todos os filtros do dashboard (dia, status, empresa, fornecedor)

Vantagens sobre VIEW:
  - Nenhum JOIN executado no momento da consulta do dashboard
  - Consulta é simples SELECT numa tabela pequena (~100-200 linhas)
  - Procedure pode ser chamada a qualquer momento sem travar o dashboard
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from config import db_destino
import pymysql

# ---------------------------------------------------------------------------
# DDL – tabela materializada
# ---------------------------------------------------------------------------
SQL_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS dashboard_macros_agg (
    id              INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    dia             DATE            NOT NULL,
    status          VARCHAR(30)     NOT NULL,
    mensagem        VARCHAR(255)        NULL,
    resposta_status VARCHAR(30)         NULL,
    empresa         VARCHAR(100)        NULL,
    fornecedor      VARCHAR(50)     NOT NULL,
    arquivo_origem  VARCHAR(20)     NOT NULL,
    qtd             INT UNSIGNED    NOT NULL DEFAULT 0,
    atualizado_em   DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),

    -- Índice principal para leitura do dashboard (cobre ORDER BY dia DESC)
    INDEX idx_dag_dia_status       (dia, status),

    -- Filtros isolados usados nos dropdowns
    INDEX idx_dag_status           (status),
    INDEX idx_dag_empresa          (empresa),
    INDEX idx_dag_fornecedor       (fornecedor),
    INDEX idx_dag_arquivo_origem   (arquivo_origem),

    -- Índice composto para GROUP BY no lado Python (orchestrator)
    INDEX idx_dag_dia_empresa_forn (dia, empresa, fornecedor)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

# ---------------------------------------------------------------------------
# Stored procedure — TRUNCATE + INSERT atômico (dentro de transação implícita)
# Chamada pelo ETL ao final de cada ciclo de processamento.
# ---------------------------------------------------------------------------
SQL_DROP_PROCEDURE = "DROP PROCEDURE IF EXISTS sp_refresh_dashboard_macros_agg"

SQL_CREATE_PROCEDURE = """
CREATE PROCEDURE sp_refresh_dashboard_macros_agg()
BEGIN
    -- Remove dados antigos e reinsere os agregados atuais
    -- TRUNCATE é mais rápido que DELETE e reseta o AUTO_INCREMENT
    TRUNCATE TABLE dashboard_macros_agg;

    INSERT INTO dashboard_macros_agg
        (dia, status, mensagem, resposta_status, empresa, fornecedor, arquivo_origem, qtd)
    SELECT
        DATE(COALESCE(m.data_extracao, m.data_update))    AS dia,
        m.status,
        r.mensagem,
        r.status                                          AS resposta_status,
        d.nome                                            AS empresa,
        COALESCE(co.fornecedor, 'fornecedor2')            AS fornecedor,
        CASE
            WHEN m.data_extracao IS NULL THEN 'Dados históricos'
            ELSE 'Operacional'
        END                                               AS arquivo_origem,
        COUNT(*)                                          AS qtd
    FROM tabela_macros m
    LEFT JOIN respostas      r   ON r.id  = m.resposta_id
    LEFT JOIN distribuidoras d   ON d.id  = m.distribuidora_id
    LEFT JOIN cliente_origem co  ON co.cliente_id = m.cliente_id
    WHERE m.status != 'pendente'
      AND m.resposta_id IS NOT NULL
    GROUP BY
        DATE(COALESCE(m.data_extracao, m.data_update)),
        m.status, r.mensagem, r.status, d.nome,
        COALESCE(co.fornecedor, 'fornecedor2'),
        CASE
            WHEN m.data_extracao IS NULL THEN 'Dados históricos'
            ELSE 'Operacional'
        END;
END
"""


def run():
    conn = pymysql.connect(**db_destino())
    try:
        with conn.cursor() as cur:
            print("Criando tabela dashboard_macros_agg...")
            cur.execute(SQL_CREATE_TABLE)

            print("Criando stored procedure sp_refresh_dashboard_macros_agg...")
            cur.execute(SQL_DROP_PROCEDURE)
            cur.execute(SQL_CREATE_PROCEDURE)

            conn.commit()
            print("Tabela e procedure criadas.")

            print("Populando dados iniciais (CALL sp_refresh_dashboard_macros_agg)...")
            cur.execute("CALL sp_refresh_dashboard_macros_agg()")
            conn.commit()

            cur.execute("SELECT COUNT(*) FROM dashboard_macros_agg")
            count = cur.fetchone()[0]
            print(f"Linhas na tabela materializada: {count}")

            cur.execute(
                "SELECT MIN(dia), MAX(dia) FROM dashboard_macros_agg"
            )
            r = cur.fetchone()
            print(f"Período: {r[0]} → {r[1]}")

    finally:
        conn.close()


if __name__ == "__main__":
    run()
