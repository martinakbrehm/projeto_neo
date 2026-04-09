"""
Migração 20260409 — Limpar associações de registros históricos com arquivos de staging

Problema:
- Registros históricos foram inseridos/processados antes de certos arquivos serem importados
- Sistema associa esses registros a arquivos posteriores via JOIN com staging_imports
- Resultado: registros históricos aparecem associados a arquivos incorretos

Solução:
- Identificar registros históricos (criados/processados antes de 2026-01-01)
- Remover associações com staging_imports para que apareçam como "histórico"
- Registros históricos devem aparecer como "histórico", não associados a arquivos específicos

Esta migração garante que dados históricos não sejam incorretamente associados
a arquivos de importação posteriores.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from config import db_destino
import pymysql

# Data limite: registros criados/processados antes desta data são considerados históricos
DATA_LIMITE_HISTORICO = "2026-01-01"

def main():
    print("=" * 70)
    print("MIGRAÇÃO 20260409 — Limpar associações históricas")
    print("=" * 70)

    conn = pymysql.connect(**db_destino(autocommit=False))
    cur = conn.cursor()

    try:
        # 1. Identificar registros históricos que têm associação com staging
        print("\n[INFO] Identificando registros históricos associados a arquivos...")

        cur.execute("""
            SELECT COUNT(*)
            FROM tabela_macros tm
            JOIN clientes cl ON cl.id = tm.cliente_id
            JOIN cliente_uc cu ON cu.cliente_id = tm.cliente_id AND cu.distribuidora_id = tm.distribuidora_id
            JOIN staging_import_rows sir ON sir.normalized_cpf = cl.cpf
            JOIN staging_imports si ON si.id = sir.staging_id
            WHERE tm.data_criacao < %s
               OR (tm.data_extracao IS NOT NULL AND tm.data_extracao < %s)
        """, (DATA_LIMITE_HISTORICO, DATA_LIMITE_HISTORICO))

        count_problema = cur.fetchone()[0]
        print(f"[INFO] Encontrados {count_problema:,} registros históricos associados a arquivos")

        if count_problema == 0:
            print("[INFO] Nenhum registro histórico associado a arquivos encontrado.")
            print("[INFO] Migração concluída - nada a fazer.")
            return

        # 2. Mostrar exemplo dos registros afetados
        print("\n[INFO] Exemplos de registros históricos associados a arquivos:")
        cur.execute("""
            SELECT
                tm.id,
                cl.cpf,
                cu.uc,
                tm.data_criacao,
                tm.data_extracao,
                si.filename,
                si.created_at as arquivo_data
            FROM tabela_macros tm
            JOIN clientes cl ON cl.id = tm.cliente_id
            JOIN cliente_uc cu ON cu.cliente_id = tm.cliente_id AND cu.distribuidora_id = tm.distribuidora_id
            JOIN staging_import_rows sir ON sir.normalized_cpf = cl.cpf
            JOIN staging_imports si ON si.id = sir.staging_id
            WHERE tm.data_criacao < %s
               OR (tm.data_extracao IS NOT NULL AND tm.data_extracao < %s)
            ORDER BY tm.data_criacao ASC
            LIMIT 10
        """, (DATA_LIMITE_HISTORICO, DATA_LIMITE_HISTORICO))

        print("<10")
        print("-" * 120)
        for row in cur.fetchall():
            print("<10")

        # 3. Estratégia: marcar registros históricos para aparecerem como "histórico"
        # Como o sistema usa data_extracao IS NULL para identificar históricos,
        # vamos limpar data_extracao de registros históricos que foram processados
        print("
[INFO] Estratégia: limpar data_extracao de registros históricos processados"        print(f"[INFO] Registros criados/processados antes de {DATA_LIMITE_HISTORICO} serão marcados como históricos")

        # 4. Executar limpeza
        print("
[EXEC] Limpando associações históricas..."        cur.execute("""
            UPDATE tabela_macros tm
            SET data_extracao = NULL,
                data_update = NOW()
            WHERE (tm.data_criacao < %s
                   OR (tm.data_extracao IS NOT NULL AND tm.data_extracao < %s))
              AND tm.data_extracao IS NOT NULL
        """, (DATA_LIMITE_HISTORICO, DATA_LIMITE_HISTORICO))

        registros_afetados = cur.rowcount
        print(f"[OK] {registros_afetados:,} registros históricos tiveram data_extracao limpa")

        # 5. Verificar resultado
        print("
[VERIFICAÇÃO] Verificando resultado..."        cur.execute("""
            SELECT
                CASE
                    WHEN tm.data_extracao IS NULL THEN 'historico'
                    ELSE 'com_arquivo'
                END as categoria,
                COUNT(*) as qtd
            FROM tabela_macros tm
            WHERE tm.status != 'pendente'
              AND tm.resposta_id IS NOT NULL
              AND (tm.data_criacao < %s OR (tm.data_extracao IS NOT NULL AND tm.data_extracao < %s))
            GROUP BY categoria
        """, (DATA_LIMITE_HISTORICO, DATA_LIMITE_HISTORICO))

        print("\n[RESULTADO] Distribuição de registros históricos:")
        for categoria, qtd in cur.fetchall():
            print("<15")

        conn.commit()
        print("
[SUCCESS] Migração concluída com sucesso!"        print(f"[STATS] {registros_afetados:,} registros históricos ajustados")

    except Exception as e:
        print(f"\n[ERRO] Falha na migração: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()