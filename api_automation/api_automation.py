"""
api_automation.py
=================
Automação de consulta financeira via API Neo Energia para CPFs sem UC.

Prioriza processamento por fornecedor: contatus > fornecedor1 > outros.

Seleciona automaticamente do banco de dados registros sem UC.
"""

import requests
import pandas as pd
import json
import csv
import os
import time
from datetime import datetime, timezone
from collections import deque, defaultdict
import pymysql
import sys

# Add path for config
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config import db_destino
from api_credentials import BASE_URL, API_KEY

HEADERS = {
    "apikey": API_KEY,
    "Content-Type": "application/json"
}

# Dados de exemplo para o corpo da requisição
BODY = {
    "documento": "81623275504",
    "usuario": "UCSCOMM",
    "canalSolicitante": "PAP"
}

def consulta_financeiro(body):
    """Faz a consulta financeiro."""
    url = f"{BASE_URL}/consulta/1.0.0/pre-venda/financeiro"
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.post(url, headers=HEADERS, json=body)
            if response.status_code == 429:
                try:
                    data = response.json()
                    next_time_str = data.get('nextAccessTime')
                    if next_time_str:
                        next_time = datetime.strptime(next_time_str, "%Y-%b-%d %H:%M:%S+0000 UTC").replace(tzinfo=timezone.utc)
                        now = datetime.now(timezone.utc)
                        wait_seconds = (next_time - now).total_seconds()
                        if wait_seconds > 0:
                            print(f"Throttled. Aguardando {int(wait_seconds)} segundos até {next_time_str}")
                            time.sleep(wait_seconds)
                            # Retry
                            response = requests.post(url, headers=HEADERS, json=body)
                except Exception as e:
                    print(f"Erro ao processar throttling: {e}")
            if response.status_code == 200:
                return {'status': response.status_code, 'response': response.json()}
            else:
                return {'status': response.status_code, 'response': response.text}
        except requests.exceptions.RequestException as e:
            print(f"Erro de conexão na tentativa {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                return {'status': 'error', 'response': str(e)}

if __name__ == "__main__":
    # Conectar ao banco de dados
    db_config = db_destino()
    conn = pymysql.connect(**db_config)
    cur = conn.cursor()

    # Query para selecionar CPFs sem UC, priorizando contatus > fornecedor1 > outros
    query = """
    SELECT c.cpf, COALESCE(co.fornecedor, 'desconhecido') as fornecedor
    FROM tabela_macros tm
    JOIN clientes c ON c.id = tm.cliente_id
    LEFT JOIN cliente_uc cu ON cu.cliente_id = tm.cliente_id AND cu.distribuidora_id = tm.distribuidora_id
    LEFT JOIN cliente_origem co ON co.cliente_id = tm.cliente_id
    WHERE cu.uc IS NULL
      AND tm.status IN ('pendente', 'reprocessar')
    ORDER BY
        CASE COALESCE(co.fornecedor, 'desconhecido')
            WHEN 'contatus' THEN 1
            WHEN 'fornecedor1' THEN 2
            ELSE 3
        END ASC,
        tm.data_update ASC
    LIMIT 1000
    """

    cur.execute(query)
    rows = cur.fetchall()
    conn.close()

    if not rows:
        print("Nenhum CPF sem UC encontrado para processar.")
        exit()

    # Criar DataFrame
    df = pd.DataFrame(rows, columns=['cpf', 'fornecedor'])
    df['cpf'] = df['cpf'].astype(str).str.zfill(11)

    print(f"Encontrados {len(df)} CPFs sem UC para processar.")
    print(df['fornecedor'].value_counts())

    # Verificar CPFs já processados
    OUTPUT_FILE = 'outputs.csv'
    processed = set()
    if os.path.exists(OUTPUT_FILE):
        try:
            processed_df = pd.read_csv(OUTPUT_FILE, sep=';')
            if 'cpf' in processed_df.columns:
                processed = set(processed_df['cpf'].astype(str))
            print(f"Encontrados {len(processed)} registros já processados. Retomando...")
        except Exception as e:
            print(f"Erro ao ler {OUTPUT_FILE}: {e}. Iniciando do zero.")

    # Lista para armazenar resultados
    results = []
    active = 0
    inactive = 0
    total = len(df)
    processed_count = 0
    start_time = time.time()

    # Verificar se arquivo já existe para não duplicar header
    file_exists = os.path.exists(OUTPUT_FILE)

    # Preparar fila de CPFs a processar (apenas cpfs não processados)
    queue = deque()
    for index, row in df.iterrows():
        documento = row['cpf']
        if documento in processed:
            continue
        queue.append({'cpf': documento, 'fornecedor': row['fornecedor'], 'attempts': 0, 'index': index+1})

    # Configurações de tentativa
    MAX_ATTEMPTS = 6
    failed = []

    # Processar fila: reenqueue até sucesso
    while queue:
        job = queue.popleft()
        documento = job['cpf']
        job['attempts'] += 1

        body = {
            "documento": documento,
            "usuario": "UCSCOMM",
            "canalSolicitante": "PAP"
        }

        print(f"Processando fila (tentativa {job['attempts']}): CPF {documento}")
        fin_response = consulta_financeiro(body)

        status = fin_response.get('status')
        resp = fin_response.get('response')

        if status == 200 and resp:
            # Determinar ativo/inativo
            if isinstance(fin_response['response'], list) and fin_response['response']:
                item = fin_response['response'][0]
                if 'statusInstalacao' in item and item['statusInstalacao'] == 'LIGADA':
                    active += 1
                else:
                    inactive += 1
            else:
                inactive += 1

            # Adicionar aos resultados
            result_row = {
                'cpf': documento,
                'fornecedor': job['fornecedor'],
                'financeiro_status': fin_response['status'],
                'financeiro_resposta': json.dumps(fin_response['response'], ensure_ascii=False)
            }
            results.append(result_row)

            # Salvar linha no CSV imediatamente
            with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, delimiter=';')
                if not file_exists:
                    writer.writerow(result_row.keys())
                    file_exists = True
                writer.writerow(result_row.values())

            processed.add(documento)
            processed_count += 1

            # Monitoramento periódico
            if processed_count % 10 == 0:
                time_elapsed = time.time() - start_time
                rate_per_second = processed_count / time_elapsed if time_elapsed > 0 else 0
                total_24h = rate_per_second * 86400
                active_rate = active / processed_count if processed_count > 0 else 0
                projected_active = total_24h * active_rate
                print(f"--- Monitoramento (linha {processed_count}) ---")
                print(f"Ativos: {active} ({active_rate*100:.2f}%), Taxa: {rate_per_second:.2f}/s, Proj 24h: {total_24h:.0f} regs, {projected_active:.0f} ativos")

            # Pequena pausa para evitar rate limit
            time.sleep(0.1)
            print("\n" + "="*50 + "\n")
        else:
            # Decidir se re-enfileirar ou marcar como falha permanente
            # Se status for 'error' (exceção de conexão) ou status >=500 ou 429, re-enfileirar
            requeue = False
            if status == 'error' or (isinstance(status, int) and (status >= 500 or status == 429)):
                requeue = True
            elif isinstance(status, int) and status == 200 and not resp:
                # 200 mas sem resposta: re-enfileirar indefinidamente (tentar até obter resposta)
                requeue = True
            else:
                # 4xx (exceto 429) -> falha permanente
                requeue = False

            if requeue and job['attempts'] < MAX_ATTEMPTS:
                print(f"Tentativa {job['attempts']} para CPF {documento} falhou (status={status}). Reenfileirando com backoff.")
                sleep_seconds = min(60, 2 ** job['attempts'])
                time.sleep(sleep_seconds)
                queue.append(job)
            else:
                print(f"Falha permanente CPF {documento}: status={status}. Registrando como failed.")
                result_row = {
                    'cpf': documento,
                    'financeiro_status': status,
                    'financeiro_resposta': json.dumps(resp, ensure_ascii=False) if resp is not None else ''
                }
                results.append(result_row)
                failed.append(documento)
                # salvar falha no CSV também
                with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f, delimiter=';')
                    if not file_exists:
                        writer.writerow(result_row.keys())
                        file_exists = True
                    writer.writerow(result_row.values())

    # Salvar resultados em CSV (já salvo progressivamente)
    print("Resultados salvos progressivamente em output.csv")
    
    # Cálculos de projeção
    time_elapsed = time.time() - start_time
    rate_per_second = processed_count / time_elapsed if time_elapsed > 0 else 0
    total_24h = rate_per_second * 86400  # 24 horas em segundos
    active_rate = active / processed_count if processed_count > 0 else 0
    projected_active = total_24h * active_rate
    pct_active = active_rate * 100
    
    # Resumo
    print(f"\nResumo:")
    print(f"Total processado: {processed_count}")
    print(f"Ativos: {active} ({pct_active:.2f}%)")
    print(f"Inativos: {inactive}")
    print(f"Tempo decorrido: {time_elapsed:.2f} segundos")
    print(f"Taxa: {rate_per_second:.2f} registros/segundo")
    print(f"Projeção 24h: {total_24h:.0f} registros")
    print(f"Ativos projetados 24h: {projected_active:.0f} ({pct_active:.2f}%)")

    # Lista de todos os CPFs processados (inclui processados em execuções anteriores)
    processed_cpfs = sorted(processed)
    print('\nLista de CPFs processados:')
    print(processed_cpfs)

    # Reprocessar CPFs que tiveram resposta 200 mas lista vazia ([]) até obter resposta
    if os.path.exists('output.csv'):
        try:
            out_df = pd.read_csv(OUTPUT_FILE, sep=';', dtype=str)
            out_df['financeiro_status'] = out_df['financeiro_status'].astype(object)

            # Identificar CPFs com resposta vazia
            empties = []
            for idx, r in out_df.iterrows():
                resp_text = str(r.get('financeiro_resposta', '')).strip()
                parsed = None
                try:
                    parsed = json.loads(resp_text) if resp_text else None
                except Exception:
                    parsed = None
                status_val = r.get('financeiro_status')
                try:
                    status_int = int(status_val)
                except Exception:
                    status_int = None
                if status_int == 200 and (parsed == [] or resp_text == '[]' or resp_text == '"[]"'):
                    empties.append((idx, r['cpf']))

            if empties:
                print(f"Reprocessando {len(empties)} CPFs com resposta vazia...")
                for idx, cpf in empties:
                    print(f"Reprocessando CPF {cpf} até obter resposta...")
                    attempt = 0
                    while True:
                        attempt += 1
                        body = {"documento": cpf, "usuario": "UCSCOMM", "canalSolicitante": "PAP"}
                        res = consulta_financeiro(body)
                        status = res.get('status')
                        resp = res.get('response')
                        # Se obteve dados, atualiza o DataFrame e salva o arquivo (sobrescreve)
                        if status == 200 and resp:
                            out_df.at[idx, 'financeiro_status'] = str(status)
                            out_df.at[idx, 'financeiro_resposta'] = json.dumps(resp, ensure_ascii=False)
                            out_df.to_csv(OUTPUT_FILE, sep=';', index=False)
                            print(f"CPF {cpf} reprocessado com sucesso na tentativa {attempt}.")
                            break
                        else:
                            # Aguarda antes de tentar novamente (backoff limitado)
                            sleep_seconds = min(300, 2 ** min(attempt, 8))
                            print(f"Tentativa {attempt} CPF {cpf} falhou (status={status}). Aguardando {sleep_seconds}s antes de nova tentativa.")
                            time.sleep(sleep_seconds)
        except Exception as e:
            print(f"Erro ao reprocessar empties: {e}")