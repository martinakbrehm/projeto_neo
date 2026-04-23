"""
enriquecimento_nome_bases_locais/pipeline.py
===========================================
Enriquece clientes sem nome buscando dados em arquivos locais
nas pastas de bases dos fornecedores.

Estratégia:
  - Busca CPFs de clientes sem nome
  - Varre arquivos CSV/XLSX em dados/fornecedor*/migration_periodo_*/raw/bases/
  - Identifica colunas de CPF (padrão 11 dígitos) e nome (padrões como 'nome', 'name')
  - Atualiza clientes com nomes encontrados

Fonte local:
  Arquivos CSV/XLSX com dados de clientes

Destino:
  - clientes (UPDATE nome WHERE id = ...)

Uso:
    python etl/migration/enriquecimento_nome_bases_locais/pipeline.py
    python etl/migration/enriquecimento_nome_bases_locais/pipeline.py --dry-run
"""

import sys
import os
import re
import json
import argparse
import csv
import time
from datetime import datetime
from pathlib import Path
import pandas as pd
import pymysql

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from config import db_destino  # noqa: E402

# ---------------------------------------------------------------------------
# Configurações
# ---------------------------------------------------------------------------
STATE_DIR = Path(__file__).resolve().parent / "state"
PROGRESSO = STATE_DIR / "enriquecimento_nome_bases_progresso.json"
BASES_DIR = Path(__file__).resolve().parents[3] / "dados" / "fornecedor2" / "migration_periodo_ate_20260312" / "raw" / "bases"

# ---------------------------------------------------------------------------
# Progresso
# ---------------------------------------------------------------------------

def carregar_progresso() -> dict:
    if not PROGRESSO.exists():
        return {}
    try:
        with open(PROGRESSO, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def salvar_progresso(data: dict):
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    data["atualizado_em"] = datetime.now().isoformat(timespec="seconds")
    with open(PROGRESSO, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def limpar_cpf(val) -> str | None:
    """Extrai CPF para 11 dígitos."""
    if val is None:
        return None
    s = re.sub(r"\D", "", str(val).strip())
    if len(s) == 11:
        return s
    return None

def identificar_colunas(df) -> tuple:
    """Identifica colunas de CPF e nome."""
    headers = [str(h).lower().strip() for h in df.columns]
    cpf_col = None
    nome_col = None

    # Procurar por padrões
    for i, h in enumerate(headers):
        if any(word in h for word in ['cpf', 'documento', 'api_cpf']):
            cpf_col = df.columns[i]
        if any(word in h for word in ['nome', 'name', 'api_nome', 'clientes_nome']):
            nome_col = df.columns[i]

    # Se não encontrou, tentar detectar por dados
    if cpf_col is None:
        for col in df.columns:
            sample = df[col].dropna().head(10)
            if any(limpar_cpf(v) for v in sample):
                cpf_col = col
                break

    if nome_col is None:
        for col in df.columns:
            sample = df[col].dropna().head(10)
            if any(isinstance(v, str) and len(v.strip()) > 3 and not v.isdigit() and not limpar_cpf(v) for v in sample):
                nome_col = col
                break

    return cpf_col, nome_col

def extrair_dados_arquivo(filepath) -> dict:
    """Extrai {cpf: nome} do arquivo."""
    try:
        dados = {}
        if filepath.suffix.lower() == '.csv':
            # Try pandas first
            try:
                df = pd.read_csv(filepath, dtype=str, sep=None, engine='python', on_bad_lines='skip')
            except Exception:
                # Fallback to csv module
                with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                    reader = csv.reader(f, delimiter=';')  # Assume ; separator
                    headers = next(reader)
                    headers = [h.lower().strip() for h in headers]
                    cpf_idx = None
                    nome_idx = None
                    for i, h in enumerate(headers):
                        if any(word in h for word in ['cpf', 'documento', 'api_cpf']):
                            cpf_idx = i
                        if any(word in h for word in ['nome', 'name', 'api_nome', 'clientes_nome']):
                            nome_idx = i
                    if cpf_idx is not None and nome_idx is not None:
                        for row in reader:
                            if len(row) > max(cpf_idx, nome_idx):
                                cpf = limpar_cpf(row[cpf_idx])
                                nome = row[nome_idx].strip() if row[nome_idx] else None
                                if cpf and nome and len(nome) > 2:
                                    dados[cpf] = nome
                return dados
        elif filepath.suffix.lower() in ['.xlsx', '.xls']:
            df = pd.read_excel(filepath, dtype=str)
        else:
            return {}

        cpf_col, nome_col = identificar_colunas(df)
        if not cpf_col or not nome_col:
            print(f"Colunas não encontradas em {filepath.name}: CPF={cpf_col}, Nome={nome_col}")
            return {}

        for _, row in df.iterrows():
            cpf = limpar_cpf(row[cpf_col])
            nome = str(row[nome_col]).strip() if pd.notna(row[nome_col]) else None
            if cpf and nome and len(nome) > 2:
                dados[cpf] = nome

        print(f"{filepath.name}: {len(dados)} nomes extraídos")
        return dados
    except Exception as e:
        print(f"Erro ao processar {filepath}: {e}")
        return {}

# ---------------------------------------------------------------------------
# Buscar clientes sem nome
# ---------------------------------------------------------------------------

def buscar_clientes_sem_nome(conn_local) -> dict:
    """Retorna {cpf: cliente_id} para clientes sem nome."""
    cur = conn_local.cursor()
    cur.execute("""
        SELECT id, cpf
        FROM clientes
        WHERE nome IS NULL
    """)
    return {str(row[1]).zfill(11): row[0] for row in cur.fetchall()}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Enriquecimento de nomes via bases locais")
    parser.add_argument("--dry-run", action="store_true", help="Simula sem alterar banco")
    args = parser.parse_args()

    while True:
        progresso = carregar_progresso()
        dry_run = args.dry_run

        try:
            conn_local = pymysql.connect(**db_destino())

            # Buscar clientes sem nome
            clientes = buscar_clientes_sem_nome(conn_local)
            print(f"Clientes sem nome encontrados: {len(clientes)}")

            if not clientes:
                print("Nenhum cliente precisa de enriquecimento.")
                return

            # Varrer arquivos
            arquivos = list(BASES_DIR.rglob("*"))
            arquivos = [f for f in arquivos if f.is_file() and f.suffix.lower() in ['.csv', '.xlsx', '.xls']]

            print(f"Arquivos encontrados: {len(arquivos)}")

            atualizados = progresso.get("atualizados", 0)
            arquivos_processados = progresso.get("arquivos_processados", 0)

            for i, filepath in enumerate(arquivos[arquivos_processados:], start=arquivos_processados):
                print(f"Processando {filepath.name}...")
                dados = extrair_dados_arquivo(filepath)
                registros = []
                for cpf, nome in dados.items():
                    if cpf in clientes:
                        registros.append((clientes[cpf], nome))

                if registros:
                    if not dry_run:
                        cur = conn_local.cursor()
                        batch_size = 10
                        for j in range(0, len(registros), batch_size):
                            batch = registros[j:j+batch_size]
                            for cliente_id, nome in batch:
                                retries = 3
                                while retries > 0:
                                    try:
                                        cur.execute("""
                                            UPDATE clientes
                                            SET nome = %s
                                            WHERE id = %s AND nome IS NULL
                                        """, (nome, cliente_id))
                                        break
                                    except pymysql.err.OperationalError as e:
                                        if "Deadlock" in str(e):
                                            retries -= 1
                                            time.sleep(0.1)
                                            continue
                                        else:
                                            raise
                                if retries == 0:
                                    print(f"Falhou ao atualizar cliente {cliente_id}")
                            conn_local.commit()  # Commit after each batch
                        atualizados += len(registros)
                    else:
                        atualizados += len(registros)
                    print(f"  -> {len(registros)} nomes atualizados")

                # Salvar progresso após cada arquivo
                progresso["arquivos_processados"] = i + 1
                progresso["atualizados"] = atualizados
                salvar_progresso(progresso)

            print(f"Enriquecimento concluído. Total atualizados: {atualizados}")
            break  # Exit loop on success

        except KeyboardInterrupt:
            print("Interrompido pelo usuário. Salvando progresso...")
            salvar_progresso(progresso)
            break
        except Exception as e:
            print(f"Erro: {e}. Tentando novamente em 5 segundos...")
            time.sleep(5)
            continue
        finally:
            if 'conn_local' in locals():
                conn_local.close()

if __name__ == "__main__":
    main()