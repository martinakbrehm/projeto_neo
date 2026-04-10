#!/usr/bin/env python
"""
Migration: 20260410_reinserir_combinacoes_uc_pendente

Para os 7.581 registros operacionais em tabela_macros com cliente_uc_id=NULL
(todos oriundos dos CSVs de 06-04-2026), re-insere TODAS as combinacoes
(cliente_id, distribuidora_id, cliente_uc_id) como novos registros pendentes
com data retroativa 2026-04-06.

Motivacao: o ETL antigo colapsava N UCs por CPF em 1 registro sem cliente_uc_id.
Nao ha como saber qual UC era a correta sem rodar a automacao. A solucao e inserir
todas as combinacoes como pendente para o pipeline descobrir quais estao ativas.

Uso:
    python migration.py --dry-run   # mostra o que seria feito
    python migration.py             # executa
"""
import sys
import os
import argparse
import csv
import re
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))
from config import db_destino
import pymysql

DATA_RETROATIVA = "2026-04-06 00:00:00"

# CSVs que originaram os 7.581 registros NULL operacionais
# staging_id -> (csv_path_relativo_ao_root, distribuidora_id)
STAGING_FILES = {
    1: ("dados/fornecedor2/operacional/06-04-2026/35K_20260402_CELP.csv",   3),
    2: ("dados/fornecedor2/operacional/06-04-2026/35K_20260402_COELBA.csv", 1),
    3: ("dados/fornecedor2/operacional/06-04-2026/35K_20260402_COSERN.csv", 2),
}


def normalizar_cpf(val) -> str | None:
    if not val:
        return None
    s = re.sub(r"\D", "", str(val).split(".")[0].strip())
    s = s.zfill(11)
    return s if len(s) == 11 else None


def normalizar_uc(val) -> str | None:
    if not val:
        return None
    s = re.sub(r"\D", "", str(val).split(".")[0].strip())
    return s.zfill(10) if s else None


def carregar_csv_cpf_ucs(full_path: str) -> dict:
    """Retorna dict: normalized_cpf -> set of normalized_uc."""
    cpf_ucs: dict = defaultdict(set)
    with open(full_path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            cpf = normalizar_cpf(row.get("cpf"))
            uc = normalizar_uc(row.get("uc"))
            if cpf and uc:
                cpf_ucs[cpf].add(uc)
    return dict(cpf_ucs)


def run(dry_run: bool = True):
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))

    print(f"{'[DRY-RUN] ' if dry_run else ''}Carregando CSVs...")

    # distrib_id -> { normalized_cpf -> set(normalized_uc) }
    distrib_cpf_ucs: dict = {}
    for staging_id, (csv_rel, distrib_id) in STAGING_FILES.items():
        full_csv = os.path.join(root, csv_rel)
        if not os.path.exists(full_csv):
            print(f"  AVISO: CSV nao encontrado: {full_csv}")
            continue
        cpf_ucs = carregar_csv_cpf_ucs(full_csv)
        distrib_cpf_ucs[distrib_id] = cpf_ucs
        total_ucs = sum(len(v) for v in cpf_ucs.values())
        print(f"  staging_id={staging_id} dist_id={distrib_id}: {len(cpf_ucs)} CPFs, {total_ucs} combinacoes CPF+UC")

    conn = pymysql.connect(**db_destino())
    cur = conn.cursor()

    try:
        # 1. Buscar os registros NULL operacionais
        cur.execute("""
            SELECT tm.id, tm.cliente_id, tm.distribuidora_id, cl.cpf
            FROM tabela_macros tm
            JOIN clientes cl ON cl.id = tm.cliente_id
            WHERE tm.cliente_uc_id IS NULL
              AND tm.data_extracao IS NOT NULL
              AND tm.distribuidora_id IN (1, 2, 3)
        """)
        null_records = cur.fetchall()
        print(f"\nRegistros NULL operacionais encontrados: {len(null_records)}")

        # 2. Mapa (cliente_id, uc, distribuidora_id) -> cliente_uc_id
        cur.execute("SELECT id, cliente_id, uc, distribuidora_id FROM cliente_uc")
        uc_map: dict = {}
        for cu_id, cl_id, uc_val, dist_id in cur.fetchall():
            uc_map[(cl_id, uc_val, int(dist_id))] = cu_id
        print(f"Entradas cliente_uc carregadas: {len(uc_map)}")

        # 3. Combinacoes ja existentes como pendente/processando para evitar bloat
        cur.execute("""
            SELECT cliente_id, distribuidora_id, cliente_uc_id
            FROM tabela_macros
            WHERE status IN ('pendente', 'processando')
              AND cliente_uc_id IS NOT NULL
        """)
        ja_pendente: set = set()
        for cl_id, dist_id, cu_id in cur.fetchall():
            ja_pendente.add((cl_id, int(dist_id), cu_id))
        print(f"Combinacoes ja pendente/processando: {len(ja_pendente)}")

        # 4. Gerar registros a inserir
        to_insert: list = []
        sem_csv = 0
        sem_uc_no_csv = 0
        sem_cliente_uc = 0
        ja_existe = 0

        for _tm_id, cliente_id, distrib_id, cpf_raw in null_records:
            distrib_id_int = int(distrib_id)
            cpf = normalizar_cpf(str(cpf_raw)) or str(cpf_raw)
            cpf_ucs_map = distrib_cpf_ucs.get(distrib_id_int, {})

            if not cpf_ucs_map:
                sem_csv += 1
                continue

            ucs = cpf_ucs_map.get(cpf, set())
            if not ucs:
                sem_uc_no_csv += 1
                continue

            for uc in sorted(ucs):
                cu_id = uc_map.get((cliente_id, uc, distrib_id_int))
                if cu_id is None:
                    sem_cliente_uc += 1
                    continue
                chave = (cliente_id, distrib_id_int, cu_id)
                if chave in ja_pendente:
                    ja_existe += 1
                    continue
                to_insert.append((cliente_id, distrib_id_int, cu_id, DATA_RETROATIVA))
                ja_pendente.add(chave)  # evitar duplicacao dentro do mesmo batch

        print(f"\n=== Resumo do planejamento ===")
        print(f"  Sem CSV p/ distribuidora:      {sem_csv}")
        print(f"  Sem UC no CSV p/ o CPF:        {sem_uc_no_csv}")
        print(f"  Sem cliente_uc correspondente: {sem_cliente_uc}")
        print(f"  Ja existem como pendente:      {ja_existe}")
        print(f"  NOVOS registros a inserir:     {len(to_insert)}")

        if dry_run:
            if to_insert:
                print(f"\nSamples (primeiros 10):")
                for r in to_insert[:10]:
                    print(f"  cliente_id={r[0]:>6}  dist={r[1]}  cliente_uc_id={r[2]:>6}  data={r[3]}")
            print(f"\n[DRY-RUN] Nenhuma alteracao feita.")
            return

        if not to_insert:
            print("Nada a inserir.")
            return

        # 5. Inserir em lotes usando INSERT multi-row (muito mais eficiente que executemany)
        BATCH = 500
        total_inserted = 0
        for i in range(0, len(to_insert), BATCH):
            batch = to_insert[i : i + BATCH]
            placeholders = ", ".join(
                ["(%s, %s, %s, 'pendente', 0, 6, %s, %s)"] * len(batch)
            )
            sql = f"""
                INSERT INTO tabela_macros
                    (cliente_id, distribuidora_id, cliente_uc_id,
                     status, extraido, resposta_id, data_criacao, data_update)
                VALUES {placeholders}
            """
            flat_params = []
            for cl, dist, cu, dt in batch:
                flat_params.extend([cl, dist, cu, dt, dt])
            cur.execute(sql, flat_params)
            total_inserted += len(batch)
            conn.commit()  # commit por lote para evitar timeout de conexao
            if total_inserted % 2000 == 0 or total_inserted >= len(to_insert):
                print(f"  Inseridos {total_inserted}/{len(to_insert)}...")
        print(f"\nSUCESSO: {total_inserted} novos registros pendentes inseridos")
        print(f"  data_criacao retroativa: {DATA_RETROATIVA}")
        print(f"  Status: pendente | resposta_id: 6 | extraido: 0")

    except Exception as e:
        conn.rollback()
        print(f"ERRO: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reinserir todas combinacoes UC como pendente")
    parser.add_argument("--dry-run", action="store_true", help="Apenas simula, nao altera o banco")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
