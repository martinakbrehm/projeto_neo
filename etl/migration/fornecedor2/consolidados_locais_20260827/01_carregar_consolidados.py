"""
carregar_consolidados_simples.py
================================
Carrega consolidados como se fossem resultados da macro.
Apenas insere em tabela_macros replicando a execução da macro.
"""

import pandas as pd
import json
import pymysql
import sys
from datetime import datetime

sys.path.insert(0, '.')
from config import db_destino

DB_CONFIG = db_destino(autocommit=False)

RESPOSTA_MAP = {
    "ATIVO": 6,
    "INATIVO": 7,
}

DISTRIBUIDORA_MAP = {
    "celpe": 3,
    "coelba": 1,
    "cosern": 2,
}

def main():
    print("=" * 100)
    print("CARREGAR CONSOLIDADOS (Simples)")
    print("=" * 100)

    # Tentar ler o arquivo
    arquivo = "RESULTADO_COMPLETO_CONSOLIDADO.csv"

    # Extrair data do nome (ex: RESULTADO_COMPLETO_CONSOLIDADO_180626.csv = 18/06/2026)
    data_retroativa = datetime.now()  # padrão = hoje

    if "_" in arquivo:
        partes = arquivo.split("_")
        for parte in partes:
            if parte.replace(".csv", "").isdigit() and len(parte.replace(".csv", "")) == 6:
                try:
                    data_str = parte.replace(".csv", "")
                    dia = int(data_str[0:2])
                    mes = int(data_str[2:4])
                    ano = int("20" + data_str[4:6])
                    data_retroativa = datetime(ano, mes, dia)
                    print(f"Data extraida do arquivo: {data_retroativa.strftime('%d/%m/%Y')}")
                    break
                except:
                    pass

    for encoding in ['utf-8-sig', 'latin-1', 'iso-8859-1', 'cp1252']:
        try:
            print(f"\nTentando ler com {encoding}...")
            df = pd.read_csv(arquivo, dtype=str, encoding=encoding)
            print(f"OK! Carregadas {len(df):,} linhas")
            break
        except Exception as e:
            continue
    else:
        print("Erro: nao conseguiu ler o arquivo")
        return

    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Processar
    total_ok = 0
    total_erro = 0
    batch = []
    BATCH_SIZE = 1000

    print(f"\nInserindo em tabela_macros...")

    for idx, row in df.iterrows():
        try:
            cpf_raw = row.get("cpf", "").strip()
            empresa = row.get("empresa", "").strip().lower()
            resposta_json = row.get("resposta", "{}")

            # Validar
            if not cpf_raw or len(cpf_raw) != 11:
                total_erro += 1
                continue

            # Extrair status do JSON
            try:
                resp = json.loads(resposta_json)
                status = resp.get("Status", "ATIVO")
            except:
                status = "ATIVO"

            distribuidora_id = DISTRIBUIDORA_MAP.get(empresa)
            if not distribuidora_id:
                total_erro += 1
                continue

            resposta_id = RESPOSTA_MAP.get(status, 6)

            # Procurar cliente pelo CPF
            cursor.execute("""
                SELECT id FROM clientes WHERE cpf = %s LIMIT 1
            """, (cpf_raw,))

            resultado_cliente = cursor.fetchone()
            if not resultado_cliente:
                total_erro += 1
                continue

            cliente_id = resultado_cliente[0]

            # Procurar cliente_uc
            cursor.execute("""
                SELECT id FROM cliente_uc
                WHERE cliente_id = %s AND distribuidora_id = %s
                LIMIT 1
            """, (cliente_id, distribuidora_id))

            resultado_uc = cursor.fetchone()
            if not resultado_uc:
                total_erro += 1
                continue

            cliente_uc_id = resultado_uc[0]

            # Adicionar ao batch
            batch.append((
                cliente_id,
                cliente_uc_id,
                distribuidora_id,
                resposta_id,
                "consolidado",  # status
                data_retroativa,  # data original do consolidado
                resposta_json,
            ))

            total_ok += 1

            # Executar batch
            if len(batch) >= BATCH_SIZE:
                cursor.executemany("""
                    INSERT INTO tabela_macros
                    (cliente_id, cliente_uc_id, distribuidora_id, resposta_id, status, data_criacao, resposta)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, batch)
                conn.commit()
                batch = []

                if total_ok % 10000 == 0:
                    print(f"  {total_ok:,} inseridos...")

        except Exception as e:
            total_erro += 1

    # Inserir restante
    if batch:
        cursor.executemany("""
            INSERT INTO tabela_macros
            (cliente_id, cliente_uc_id, distribuidora_id, resposta_id, status, data_criacao, resposta)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, batch)
        conn.commit()

    cursor.close()
    conn.close()

    print("\n" + "=" * 100)
    print(f"RESULTADO:")
    print(f"  Inseridos: {total_ok:,}")
    print(f"  Erros:     {total_erro:,}")
    print("=" * 100)

if __name__ == "__main__":
    main()
