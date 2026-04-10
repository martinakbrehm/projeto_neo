#!/usr/bin/env python
"""
Migration: 20260410_backfill_pendente_historico

Como a macro só rodou uma vez, cada registro operacional processado
(consolidado/excluido/reprocessar com data_extracao) era pendente no dia
em que o arquivo foi carregado - mas esse estado foi perdido porque o ETL
fazia UPDATE no mesmo registro.

Esta migration cria a linha de estado 'pendente' com data retroativa para
cada registro que foi processado, permitindo rastrear:
  - Dia do upload: registro pendente
  - Dia do processamento: registro com resultado (consolidado/excluido/...)

Uso:
    python migration.py --dry-run
    python migration.py
"""
import sys
import os
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))
from config import db_destino
import pymysql


def run(dry_run: bool = True):
    conn = pymysql.connect(**db_destino())
    cur = conn.cursor()

    try:
        # Buscar todos os registros operacionais processados (nao-pendente, com UC)
        cur.execute("""
            SELECT id, cliente_id, distribuidora_id, cliente_uc_id, data_criacao
            FROM tabela_macros
            WHERE data_extracao IS NOT NULL
              AND status != 'pendente'
              AND cliente_uc_id IS NOT NULL
        """)
        registros = cur.fetchall()
        print(f"Registros operacionais processados: {len(registros):,}")

        # Verificar quais ja tem uma linha pendente com a mesma data/cliente/uc
        cur.execute("""
            SELECT cliente_id, distribuidora_id, cliente_uc_id,
                   DATE(data_criacao) as dia
            FROM tabela_macros
            WHERE status = 'pendente'
              AND data_extracao IS NULL
              AND cliente_uc_id IS NOT NULL
        """)
        ja_pendentes: set = set()
        for r in cur.fetchall():
            ja_pendentes.add((r[0], r[1], r[2], str(r[3])))
        print(f"Linhas pendente ja existentes: {len(ja_pendentes):,}")

        to_insert = []
        ja_existe = 0
        for tm_id, cl_id, dist_id, cu_id, data_criacao in registros:
            dia_str = str(data_criacao.date()) if hasattr(data_criacao, 'date') else str(data_criacao)[:10]
            chave = (cl_id, int(dist_id), cu_id, dia_str)
            if chave in ja_pendentes:
                ja_existe += 1
                continue
            to_insert.append((cl_id, int(dist_id), cu_id, str(data_criacao)))
            ja_pendentes.add(chave)

        print(f"Ja existiam como pendente:   {ja_existe:,}")
        print(f"Novos registros pendente:    {len(to_insert):,}")

        if dry_run:
            if to_insert:
                print("\nSamples (primeiros 5):")
                for r in to_insert[:5]:
                    print(f"  cliente_id={r[0]}  dist={r[1]}  uc_id={r[2]}  data_criacao={r[3]}")
            print("\n[DRY-RUN] Nenhuma alteracao feita.")
            return

        if not to_insert:
            print("Nada a inserir.")
            return

        BATCH = 500
        total = 0
        for i in range(0, len(to_insert), BATCH):
            batch = to_insert[i:i + BATCH]
            placeholders = ", ".join(
                ["(%s, %s, %s, 'pendente', 6, 0, %s, %s)"] * len(batch)
            )
            sql = f"""
                INSERT INTO tabela_macros
                    (cliente_id, distribuidora_id, cliente_uc_id,
                     status, resposta_id, extraido,
                     data_criacao, data_update)
                VALUES {placeholders}
            """
            flat = []
            for cl, dist, cu, dt in batch:
                flat.extend([cl, dist, cu, dt, dt])
            cur.execute(sql, flat)
            total += len(batch)
            conn.commit()
            if total % 5000 == 0 or total >= len(to_insert):
                print(f"  Inseridos {total}/{len(to_insert)}...")

        print(f"\nSUCESSO: {total:,} linhas pendente retroativas inseridas")

    except Exception as e:
        conn.rollback()
        print(f"ERRO: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
