"""
import_historico_macros.py
=========================
Importa dados históricos de planilhas Excel para tabela_macros.

Fluxo por linha:
  1. Normaliza CPF (remove máscara, pad com zeros à esquerda → 11 dígitos)
  2. Normaliza UC  (remove espaços, pad com zeros à esquerda → 10 dígitos)
  3. Normaliza distribuidora (nome → id via DISTRIBUIDORA_MAP)
  4. Normaliza Msg → resposta_id (carregado dinamicamente da tabela `respostas`)
  5. Upsert em `clientes`   (apenas CPF; sem nome/endereço/telefone)
  6. Upsert em `cliente_uc` (cliente_id + uc + distribuidora_id)
  7. Insert em `tabela_macros` com:
       status       = 'consolidado'
       data_criacao = DATA_CRIACAO_FIXA (2026-02-25)
       data_update  = distribuída nos 3 dias antes da data do arquivo
       extraido     = 1
       resposta_id  = normalizado da coluna Msg

Uso:
    python scripts/import_historico_macros.py                   # processa todos os Excel em dados/
    python scripts/import_historico_macros.py --dry-run         # sem inserir no banco
    python scripts/import_historico_macros.py --file dados/meuarquivo.xlsx  # arquivo específico
"""

import os
import re
import sys
import argparse
import math
from datetime import date, timedelta, datetime
from pathlib import Path

import pandas as pd
import pymysql

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from config import db_destino  # noqa: E402

# ---------------------------------------------------------------------------
# Configurações
# ---------------------------------------------------------------------------
DB_CONFIG = db_destino(autocommit=False)  # usamos transação por arquivo

# Pasta padrão de dados (relativa ao diretório do script pai)
BASE_DIR   = Path(__file__).resolve().parent.parent
DADOS_DIR  = BASE_DIR / "dados"

# Data de criação fixa para todos os registros históricos
DATA_CRIACAO_FIXA = date(2026, 2, 25)

# Mapeamento nome distribuidora (lowercase) → id
DISTRIBUIDORA_MAP: dict[str, int] = {
    "coelba":   1,
    "cosern":   2,
    "celpe":    3,
    "brasilia": 4,
    "neoenergia brasilia": 4,
    "neoenergia celpe":    3,
    "neoenergia cosern":   2,
    "neoenergia coelba":   1,
}

# ---------------------------------------------------------------------------
# Helpers de normalização
# ---------------------------------------------------------------------------

def normalizar_cpf(valor) -> str | None:
    """Remove tudo que não é dígito e faz pad de zeros à esquerda até 11."""
    if pd.isna(valor):
        return None
    s = re.sub(r"\D", "", str(valor).strip())
    if not s:
        return None
    return s.zfill(11)


def normalizar_uc(valor) -> str | None:
    """Remove espaços e faz pad de zeros à esquerda até 10."""
    if pd.isna(valor):
        return None
    s = str(valor).strip().replace(" ", "")
    # Remove caracteres não alfanuméricos que não fazem sentido em UCs
    s = re.sub(r"[^\w]", "", s)
    if not s:
        return None
    return s.zfill(10)


def normalizar_distribuidora(valor) -> int | None:
    """Retorna o id da distribuidora a partir do nome (case-insensitive)."""
    if pd.isna(valor):
        return None
    nome = str(valor).strip().lower()
    return DISTRIBUIDORA_MAP.get(nome)


def extrair_data_arquivo(caminho: Path) -> date | None:
    """
    Tenta extrair uma data do nome do arquivo.
    Aceita: DD-MM-YYYY, DD_MM_YYYY, YYYY-MM-DD, YYYY_MM_DD, DDMMYYYY
    Ex: base_06-03-2026.xlsx  →  2026-03-06
    """
    nome = caminho.stem  # nome sem extensão
    padroes = [
        (r"(\d{2})[-_](\d{2})[-_](\d{4})", lambda m: date(int(m[3]), int(m[2]), int(m[1]))),  # DD-MM-YYYY
        (r"(\d{4})[-_](\d{2})[-_](\d{2})", lambda m: date(int(m[1]), int(m[2]), int(m[3]))),  # YYYY-MM-DD
        (r"(\d{2})(\d{2})(\d{4})",          lambda m: date(int(m[3]), int(m[2]), int(m[1]))),  # DDMMYYYY
    ]
    for padrao, construir in padroes:
        m = re.search(padrao, nome)
        if m:
            try:
                return construir(m.groups())
            except ValueError:
                continue
    return None


def distribuir_datas(n_total: int, data_arquivo: date) -> list[date]:
    """
    Retorna uma lista de n_total datas distribuídas igualmente pelos
    3 dias anteriores à data_arquivo (D-3, D-2, D-1).
    Ex: 10 linhas → [D-3]*4, [D-2]*3, [D-1]*3
    """
    dias = [data_arquivo - timedelta(days=3),
            data_arquivo - timedelta(days=2),
            data_arquivo - timedelta(days=1)]
    tamanho_chunk = math.ceil(n_total / 3)
    datas: list[date] = []
    for i, linha in enumerate(range(n_total)):
        datas.append(dias[min(i // tamanho_chunk, 2)])
    return datas


# ---------------------------------------------------------------------------
# Cache e lookups no banco
# ---------------------------------------------------------------------------

def carregar_respostas(cursor) -> dict[str, int]:
    """
    Carrega todas as respostas do banco e retorna um dicionário
    mensagem_normalizada → id.
    A normalização usa lowercase + strip para comparação fuzzy.
    """
    cursor.execute("SELECT id, mensagem FROM respostas")
    rows = cursor.fetchall()
    return {str(msg).strip().lower(): rid for rid, msg in rows if msg}


def match_resposta(msg_raw, respostas_map: dict[str, int]) -> int | None:
    """
    Tenta encontrar o resposta_id buscando a mensagem que melhor combina.
    Primeiro tenta match exato, depois verifica se a mensagem do banco
    está contida no texto recebido ou vice-versa.
    """
    if pd.isna(msg_raw):
        return None
    msg = str(msg_raw).strip().lower()
    # 1. Exato
    if msg in respostas_map:
        return respostas_map[msg]
    # 2. Parcial: chave do mapa está contida na mensagem recebida
    for chave, rid in respostas_map.items():
        if chave in msg or msg in chave:
            return rid
    return None


def upsert_cliente(cursor, cpf: str) -> int:
    """
    Insere o cliente se não existir (apenas CPF).
    Retorna o id do cliente.
    """
    cursor.execute(
        "INSERT INTO clientes (cpf, data_criacao, data_update) "
        "VALUES (%s, %s, %s) "
        "ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)",
        (cpf, DATA_CRIACAO_FIXA, DATA_CRIACAO_FIXA),
    )
    cursor.execute("SELECT id FROM clientes WHERE cpf = %s", (cpf,))
    row = cursor.fetchone()
    return row[0]


def upsert_cliente_uc(cursor, cliente_id: int, uc: str, distribuidora_id: int) -> int:
    """
    Insere o cliente_uc se não existir.
    Retorna o id do cliente_uc.
    """
    cursor.execute(
        "INSERT INTO cliente_uc (cliente_id, uc, distribuidora_id, data_criacao) "
        "VALUES (%s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)",
        (cliente_id, uc, distribuidora_id, DATA_CRIACAO_FIXA),
    )
    cursor.execute(
        "SELECT id FROM cliente_uc WHERE cliente_id = %s AND uc = %s",
        (cliente_id, uc),
    )
    row = cursor.fetchone()
    return row[0]


def inserir_macro(
    cursor,
    cliente_id: int,
    distribuidora_id: int,
    resposta_id: int | None,
    data_update: date,
) -> int:
    """Insere um registro consolidado em tabela_macros."""
    cursor.execute(
        """
        INSERT INTO tabela_macros
            (cliente_id, distribuidora_id, resposta_id,
             data_update, data_criacao, status, extraido)
        VALUES (%s, %s, %s, %s, %s, 'consolidado', 1)
        """,
        (
            cliente_id,
            distribuidora_id,
            resposta_id,
            datetime.combine(data_update, datetime.min.time()),
            datetime.combine(DATA_CRIACAO_FIXA, datetime.min.time()),
        ),
    )
    return cursor.lastrowid


# ---------------------------------------------------------------------------
# Processamento de um arquivo
# ---------------------------------------------------------------------------

def processar_arquivo(
    caminho: Path,
    cursor,
    respostas_map: dict[str, int],
    dry_run: bool,
) -> tuple[int, int, int]:
    """
    Processa um Excel e insere os dados.
    Retorna (ok, skipped, erros).
    """
    print(f"\n[ARQUIVO] {caminho.name}")

    # Extrair data do nome do arquivo
    data_arquivo = extrair_data_arquivo(caminho)
    if data_arquivo is None:
        print(f"  [AVISO] Nao foi possivel extrair data do nome '{caminho.name}'.")
        print(f"          Usando data de hoje como referencia.")
        data_arquivo = date.today()
    else:
        print(f"  [INFO] Data do arquivo: {data_arquivo}  →  data_update distribuída em "
              f"{data_arquivo - timedelta(days=3)}, "
              f"{data_arquivo - timedelta(days=2)}, "
              f"{data_arquivo - timedelta(days=1)}")

    # Ler Excel — tenta a primeira aba; lê todas as colunas como string para evitar perda de zeros
    try:
        df = pd.read_excel(caminho, dtype=str)
    except Exception as e:
        print(f"  [ERRO] Falha ao ler arquivo: {e}")
        return 0, 0, 1

    # Normaliza nomes de colunas: lowercase + strip
    df.columns = [str(c).strip().lower() for c in df.columns]

    # Mapeamento flexível de colunas
    col_map = {}
    for col in df.columns:
        col_clean = col.replace(" ", "_")
        if col_clean in ("cpf",):
            col_map["cpf"] = col
        elif col_clean in ("codigo_cliente", "uc", "codigo_uc", "cod_cliente", "codigocliente"):
            col_map["uc"] = col
        elif col_clean in ("empresa", "distribuidora", "empresa_distribuidora"):
            col_map["distribuidora"] = col
        elif col_clean in ("msg", "mensagem", "retorno", "resposta", "retorno_mensagem"):
            col_map["msg"] = col

    # Verifica colunas obrigatórias
    ausentes = [k for k in ("cpf", "distribuidora") if k not in col_map]
    if ausentes:
        print(f"  [ERRO] Colunas obrigatorias ausentes: {ausentes}")
        print(f"         Colunas encontradas: {list(df.columns)}")
        return 0, 0, len(df)

    print(f"  [INFO] {len(df)} linhas | Colunas mapeadas: {col_map}")

    # Distribuir datas de update
    datas_update = distribuir_datas(len(df), data_arquivo)

    ok = skipped = erros = 0

    for i, (idx, row) in enumerate(df.iterrows()):
        linha_num = i + 2  # linha no Excel (1=header, então dados a partir de 2)

        # Normalizar campos
        cpf = normalizar_cpf(row.get(col_map.get("cpf", ""), None))
        uc  = normalizar_uc(row.get(col_map.get("uc", ""), None)) if "uc" in col_map else None
        distribuidora_id = normalizar_distribuidora(row.get(col_map.get("distribuidora", ""), None))
        resposta_id = match_resposta(row.get(col_map.get("msg", ""), None), respostas_map) if "msg" in col_map else None
        data_update = datas_update[i]

        # Validações
        if not cpf:
            print(f"  [SKIP  L{linha_num}] CPF vazio ou invalido")
            skipped += 1
            continue
        if len(cpf) != 11:
            print(f"  [SKIP  L{linha_num}] CPF fora do tamanho esperado: '{cpf}'")
            skipped += 1
            continue
        if distribuidora_id is None:
            val = row.get(col_map.get("distribuidora", ""), "?")
            print(f"  [SKIP  L{linha_num}] Distribuidora nao reconhecida: '{val}'")
            skipped += 1
            continue

        if dry_run:
            print(f"  [DRY   L{linha_num}] CPF={cpf} UC={uc} distrib={distribuidora_id} "
                  f"resposta={resposta_id} data_update={data_update}")
            ok += 1
            continue

        try:
            cliente_id = upsert_cliente(cursor, cpf)

            if uc:
                upsert_cliente_uc(cursor, cliente_id, uc, distribuidora_id)

            inserir_macro(cursor, cliente_id, distribuidora_id, resposta_id, data_update)
            ok += 1

        except Exception as e:
            print(f"  [ERRO  L{linha_num}] CPF={cpf}: {e}")
            erros += 1

    return ok, skipped, erros


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Importa historico de macros de arquivos Excel para tabela_macros."
    )
    parser.add_argument("--dry-run", action="store_true", help="Exibe o que seria inserido sem gravar")
    parser.add_argument("--file", type=str, default=None, help="Caminho para um Excel especifico")
    parser.add_argument("--data-dir", type=str, default=str(DADOS_DIR),
                        help=f"Pasta com os arquivos Excel (default: {DADOS_DIR})")
    args = parser.parse_args()

    # Listar arquivos a processar
    if args.file:
        arquivos = [Path(args.file)]
    else:
        pasta = Path(args.data_dir)
        if not pasta.exists():
            print(f"[ERRO] Pasta '{pasta}' nao encontrada.")
            sys.exit(1)
        arquivos = sorted(pasta.glob("*.xls*"))
        if not arquivos:
            print(f"[AVISO] Nenhum arquivo .xlsx/.xls encontrado em '{pasta}'")
            sys.exit(0)

    print(f"[INFO] {len(arquivos)} arquivo(s) encontrado(s)")
    if args.dry_run:
        print("[INFO] Modo DRY-RUN — nada sera gravado no banco.\n")

    # Conectar ao banco
    if not args.dry_run:
        try:
            conn = pymysql.connect(**DB_CONFIG)
            cursor = conn.cursor()
            print("[INFO] Conectado ao banco.\n")
        except Exception as e:
            print(f"[ERRO] Falha ao conectar: {e}")
            sys.exit(1)
    else:
        conn = cursor = None

    # Carregar respostas do banco (ou usar mapa vazio em dry-run)
    if not args.dry_run:
        respostas_map = carregar_respostas(cursor)
        print(f"[INFO] {len(respostas_map)} respostas carregadas do banco.")
    else:
        # Mapa estático para dry-run (baseado nos seeds conhecidos)
        respostas_map = {
            "conta contrato não existe": 0,
            "doc. fiscal não existe": 1,
            "titularidade não confirmada": 2,
            "titularidade confirmada com contrato ativo": 3,
            "titularidade confirmada com contrato inativo": 4,
            "titularidade confirmada com inst. suspensa": 5,
            "aguardando processamento": 6,
            "doc. fiscal nao cadastrado no sap": 7,
            "parceiro informado não possui conta contrato": 8,
            "status instalacao: desligado": 9,
            "status instalacao: ligado": 10,
        }

    # Processar cada arquivo
    total_ok = total_skip = total_err = 0
    for arquivo in arquivos:
        try:
            ok, skip, err = processar_arquivo(arquivo, cursor, respostas_map, args.dry_run)
            total_ok   += ok
            total_skip += skip
            total_err  += err

            # Commit por arquivo (exceto dry-run)
            if not args.dry_run and conn:
                conn.commit()
                print(f"  [COMMIT] {ok} inseridos, {skip} ignorados, {err} erros.")

        except Exception as e:
            print(f"  [ERRO GERAL] {arquivo.name}: {e}")
            if not args.dry_run and conn:
                conn.rollback()
                print("  [ROLLBACK] Alteracoes do arquivo revertidas.")
            total_err += 1

    # Fechar conexão
    if cursor:
        cursor.close()
    if conn:
        conn.close()

    # Relatório final
    print("\n" + "=" * 60)
    print(f"TOTAL  →  OK: {total_ok}  |  Ignorados: {total_skip}  |  Erros: {total_err}")
    if args.dry_run:
        print("DRY-RUN concluido — nenhuma alteracao foi feita.")
    print("=" * 60)


if __name__ == "__main__":
    main()
