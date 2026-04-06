"""
normalizar_historico.py  —  PASSO 1 de 2
=========================================
Lê todos os arquivos Excel da pasta `dados/`, normaliza os campos e
gera um CSV separado por ponto-e-vírgula para revisão manual.

Só depois de revisar o CSV execute o PASSO 2 (importar_historico.py).

Uso:
    python scripts/normalizar_historico.py                         # processa dados/
    python scripts/normalizar_historico.py --file dados/arq.xlsx   # arquivo único
    python scripts/normalizar_historico.py --out saida.csv         # CSV de saída custom

Colunas geradas no CSV:
    arquivo_origem   | nome do arquivo Excel de origem
    linha_excel      | número da linha no Excel (para rastreabilidade)
    cpf              | CPF normalizado (11 dígitos, zeros à esquerda)
    uc               | UC normalizada (10 dígitos, zeros à esquerda) ou vazio
    distribuidora_id | id numérico da distribuidora
    distribuidora_original | valor bruto do Excel (para conferência)
    resposta_id      | id da resposta (baseado na Msg) ou vazio
    msg_original     | valor bruto da coluna Msg (para conferência)
    data_update      | data distribuída nos 3 dias antes da data do arquivo
    data_criacao     | fixo: 2026-02-25
    status           | fixo: consolidado
    observacao       | motivo de erro/skip (vazio se ok)
"""

import re
import sys
import math
import argparse
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Configurações
# ---------------------------------------------------------------------------
BASE_DIR   = Path(__file__).resolve().parent.parent.parent  # raiz do projeto
PERIODO    = "migration_periodo_ate_20260312"
DADOS_DIR  = BASE_DIR / "dados" / PERIODO / "raw"
CSV_SAIDA  = BASE_DIR / "dados" / PERIODO / "processed" / "historico_normalizado.csv"

DATA_CRIACAO_FIXA = date(2026, 2, 25)

DISTRIBUIDORA_MAP: dict[str, int] = {
    "coelba": 1,
    "cosern": 2,
    "celpe":  3,
    "brasilia": 4,
    "neoenergia brasilia": 4,
    "neoenergia celpe":    3,
    "neoenergia cosern":   2,
    "neoenergia coelba":   1,
}

# Mapa resposta_id → status (espelha exatamente a coluna `status` da tabela `respostas`)
RESPOSTA_STATUS_MAP: dict[int, str] = {
    0:  "excluido",
    1:  "excluido",
    2:  "excluido",
    3:  "consolidado",
    4:  "reprocessar",
    5:  "reprocessar",
    6:  "pendente",
    7:  "excluido",
    8:  "excluido",
    9:  "reprocessar",
    10: "consolidado",
    11: "pendente",
}

# Mapa de respostas (baseado nos seeds do banco)
# Chave: fragmento da mensagem em lowercase — valor: resposta_id
RESPOSTAS_MAP: dict[str, int] = {
    "conta contrato não existe":                       0,
    "conta contrato nao existe":                       0,
    "doc. fiscal não existe":                          1,
    "doc. fiscal nao existe":                          1,
    "titularidade não confirmada":                     2,
    "titularidade nao confirmada":                     2,
    "titularidade confirmada com contrato ativo":      3,
    "titularidade confirmada com contrato inativo":    4,
    "titularidade confirmada com inst. suspensa":      5,
    "aguardando processamento":                        6,
    "doc. fiscal nao cadastrado no sap":               7,
    "doc. fiscal não cadastrado no sap":               7,
    "parceiro informado não possui conta contrato":    8,
    "parceiro informado nao possui conta contrato":    8,
    "status instalacao: desligado":                    9,
    "status instalação: desligado":                    9,
    "status instalacao: ligado":                       10,
    "status instalação: ligado":                       10,
    "erro":                                            11,  # qualquer msg que comece com ERRO
}

# ---------------------------------------------------------------------------
# Normalizações
# ---------------------------------------------------------------------------

def norm_cpf(valor) -> tuple[str | None, str | None]:
    """Retorna (cpf_normalizado, observacao)."""
    if pd.isna(valor) or str(valor).strip() == "":
        return None, "CPF vazio"
    s = re.sub(r"\D", "", str(valor).strip())
    if not s:
        return None, f"CPF sem digitos: '{valor}'"
    if len(s) > 11:
        return None, f"CPF com mais de 11 digitos apos limpeza: '{s}'"
    return s.zfill(11), None


def norm_uc(valor) -> str | None:
    if pd.isna(valor) or str(valor).strip() == "":
        return None
    s = re.sub(r"[^\w]", "", str(valor).strip().replace(" ", ""))
    if not s:
        return None
    return s.zfill(10)


def norm_distribuidora(valor) -> tuple[int | None, str | None]:
    if pd.isna(valor) or str(valor).strip() == "":
        return None, "Distribuidora vazia"
    nome = str(valor).strip().lower()
    rid = DISTRIBUIDORA_MAP.get(nome)
    if rid is None:
        # Tenta match parcial
        for chave, did in DISTRIBUIDORA_MAP.items():
            if chave in nome or nome in chave:
                return did, None
        return None, f"Distribuidora nao reconhecida: '{valor}'"
    return rid, None


def norm_resposta(valor) -> int | None:
    if pd.isna(valor) or str(valor).strip() == "":
        return None
    msg = str(valor).strip().lower()
    # Mensagens que começam com "erro" → resposta_id 11
    if msg.startswith("erro"):
        return 11
    # Exato
    if msg in RESPOSTAS_MAP:
        return RESPOSTAS_MAP[msg]
    # Parcial
    for chave, rid in RESPOSTAS_MAP.items():
        if chave in msg or msg in chave:
            return rid
    return None


def extrair_data_arquivo(caminho: Path) -> date | None:
    nome = caminho.stem
    padroes = [
        (r"(\d{2})[-_](\d{2})[-_](\d{4})", lambda g: date(int(g[2]), int(g[1]), int(g[0]))),
        (r"(\d{4})[-_](\d{2})[-_](\d{2})", lambda g: date(int(g[0]), int(g[1]), int(g[2]))),
        (r"(\d{2})(\d{2})(\d{4})",          lambda g: date(int(g[2]), int(g[1]), int(g[0]))),
    ]
    for padrao, construir in padroes:
        m = re.search(padrao, nome)
        if m:
            try:
                return construir(m.groups())
            except ValueError:
                continue
    return None


def distribuir_datas(n: int, data_arquivo: date) -> list[date]:
    """Distribui n datas entre D-3, D-2 e D-1."""
    dias = [
        data_arquivo - timedelta(days=3),
        data_arquivo - timedelta(days=2),
        data_arquivo - timedelta(days=1),
    ]
    chunk = math.ceil(n / 3)
    return [dias[min(i // chunk, 2)] for i in range(n)]


def mapear_colunas(cols: list[str]) -> dict[str, str]:
    """Mapeia nomes flexíveis de colunas para chaves padronizadas."""
    mapa = {}
    for col in cols:
        c = col.strip().lower().replace(" ", "_")
        if c == "cpf":
            mapa["cpf"] = col
        elif c in ("codigo_cliente", "uc", "codigo_uc", "cod_cliente",
                   "codigocliente", "cod_uc", "unidade_consumidora"):
            mapa["uc"] = col
        elif c in ("empresa", "distribuidora", "empresa_distribuidora", "concessionaria"):
            mapa["distribuidora"] = col
        elif c in ("msg", "mensagem", "retorno", "resposta",
                   "retorno_mensagem", "mensagem_retorno"):
            mapa["msg"] = col
    return mapa


# ---------------------------------------------------------------------------
# Processar um arquivo
# ---------------------------------------------------------------------------

def processar_arquivo(caminho: Path) -> list[dict]:
    """Retorna lista de dicts com os dados normalizados (1 por linha do Excel)."""
    print(f"\n[ARQUIVO] {caminho.name}")

    data_arq = extrair_data_arquivo(caminho)
    if data_arq is None:
        print(f"  [AVISO] Data nao encontrada no nome — usando hoje como referencia.")
        data_arq = date.today()
    else:
        print(f"  [INFO] Data do arquivo: {data_arq}  →  updates em "
              f"{data_arq - timedelta(days=3)} / "
              f"{data_arq - timedelta(days=2)} / "
              f"{data_arq - timedelta(days=1)}")

    # --- Leitura de abas ---
    try:
        xl = pd.ExcelFile(caminho)
        nomes_abas = xl.sheet_names
    except Exception as e:
        print(f"  [ERRO] Nao foi possivel abrir: {e}")
        return []

    _ABAS_PRIORITARIAS = {"todos", "total"}
    aba_prioritaria = next(
        (a for a in nomes_abas if str(a).strip().lower() in _ABAS_PRIORITARIAS), None
    )
    if aba_prioritaria:
        abas_usar = [aba_prioritaria]
        print(f"  [INFO] Aba '{aba_prioritaria}' encontrada — lendo apenas ela.")
    else:
        abas_usar = nomes_abas
        print(f"  [INFO] Nenhuma aba prioritaria ('Todos'/'Total') — lendo {len(abas_usar)} aba(s): {abas_usar}")

    frames = []
    for aba in abas_usar:
        try:
            df_aba = pd.read_excel(caminho, sheet_name=aba, dtype=str)
            df_aba["__aba__"] = str(aba)
            frames.append(df_aba)
        except Exception as e:
            print(f"  [AVISO] Erro ao ler aba '{aba}': {e} — pulando.")

    if not frames:
        print(f"  [ERRO] Nenhuma aba legivel no arquivo.")
        return []

    df = pd.concat(frames, ignore_index=True)
    df.columns = [str(c).strip() for c in df.columns]
    col_map = mapear_colunas([c for c in df.columns if c != "__aba__"])

    ausentes = [k for k in ("cpf", "distribuidora") if k not in col_map]
    if ausentes:
        print(f"  [ERRO] Colunas obrigatorias ausentes: {ausentes}")
        print(f"         Colunas encontradas: {[c for c in df.columns if c != '__aba__']}")
        return []

    print(f"  [INFO] {len(df)} linhas totais | Mapeamento: {col_map}")

    datas = distribuir_datas(len(df), data_arq)
    linhas = []

    for i, (_, row) in enumerate(df.iterrows()):
        cpf_raw     = row.get(col_map.get("cpf", "__"), None)
        uc_raw      = row.get(col_map.get("uc", "__"), None)
        distrib_raw = row.get(col_map.get("distribuidora", "__"), None)
        msg_raw     = row.get(col_map.get("msg", "__"), None)
        aba_nome    = row.get("__aba__", "")

        cpf,              obs_cpf = norm_cpf(cpf_raw)
        distribuidora_id, obs_d   = norm_distribuidora(distrib_raw)
        uc                        = norm_uc(uc_raw)
        resposta_id               = norm_resposta(msg_raw)

        obs = "; ".join(filter(None, [obs_cpf, obs_d]))

        # Deriva status a partir do resposta_id (segue a regra da tabela respostas)
        if resposta_id is not None:
            status = RESPOSTA_STATUS_MAP.get(resposta_id, "pendente")
        else:
            status = "pendente"
            if not obs:
                obs = "resposta_id nao identificado — status definido como pendente"

        # ---- nomes de coluna espelham exatamente as colunas do banco ----
        linhas.append({
            # rastreabilidade (não vão para o banco)
            "arquivo_origem":         caminho.name,
            "aba_excel":              aba_nome,
            "linha_excel":            i + 2,
            # campos que batem 1-1 com tabela_macros / clientes / cliente_uc
            "cpf":                    cpf or "",           # clientes.cpf         CHAR(11)
            "uc":                     uc or "",            # cliente_uc.uc        CHAR(10)
            "distribuidora_id":       distribuidora_id if distribuidora_id is not None else "",  # TINYINT
            "resposta_id":            resposta_id if resposta_id is not None else "",            # TINYINT
            "status":                 status,              # derivado de RESPOSTA_STATUS_MAP
            "extraido":               1,                   # tabela_macros.extraido
            "data_criacao":           str(DATA_CRIACAO_FIXA),  # DATE → 2026-02-25
            "data_update":            str(datas[i]),           # DATE distribuída
            # valores originais para conferência
            "distribuidora_original": str(distrib_raw).strip() if not pd.isna(distrib_raw) else "",
            "msg_original":           str(msg_raw).strip() if not pd.isna(msg_raw) else "",
            "observacao":             obs,
        })

    validos   = sum(1 for l in linhas if not l["observacao"])
    invalidos = sum(1 for l in linhas if l["observacao"])
    print(f"  [OK] {validos} validos  |  {invalidos} com observacao")
    return linhas


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="PASSO 1: Normaliza Excels → CSV para revisao.")
    parser.add_argument("--file", type=str, default=None,
                        help="Arquivo Excel especifico (default: todos em dados/)")
    parser.add_argument("--out", type=str, default=str(CSV_SAIDA),
                        help=f"Caminho do CSV de saida (default: {CSV_SAIDA})")
    args = parser.parse_args()

    if args.file:
        arquivos = [Path(args.file)]
    else:
        if not DADOS_DIR.exists():
            print(f"[ERRO] Pasta '{DADOS_DIR}' nao encontrada.")
            sys.exit(1)
        arquivos = sorted(DADOS_DIR.glob("*.xls*"))
        # Exclui o proprio CSV de saida se existir como xlsx
        if not arquivos:
            print(f"[AVISO] Nenhum arquivo .xlsx/.xls encontrado em '{DADOS_DIR}'")
            sys.exit(0)

    print(f"[INFO] {len(arquivos)} arquivo(s) para processar\n")

    todas_linhas: list[dict] = []
    for arq in arquivos:
        todas_linhas.extend(processar_arquivo(arq))

    if not todas_linhas:
        print("\n[AVISO] Nenhuma linha gerada.")
        sys.exit(0)

    # ------------------------------------------------------------------
    # Deduplicação por CPF
    # Regra: se houver mais de uma linha com o mesmo CPF, prevalece a
    # que tem resposta_id=3. Se não houver resposta_id=3, prevalece a
    # primeira ocorrência. As demais são marcadas como descartadas.
    # ------------------------------------------------------------------
    cpf_escolhido: dict[str, int] = {}   # cpf → índice escolhido
    for idx, linha in enumerate(todas_linhas):
        cpf = linha["cpf"]
        if not cpf:
            continue
        resp = str(linha.get("resposta_id", "")).strip()
        if cpf not in cpf_escolhido:
            cpf_escolhido[cpf] = idx
        else:
            atual_idx  = cpf_escolhido[cpf]
            atual_resp = str(todas_linhas[atual_idx].get("resposta_id", "")).strip()
            # Troca somente se o novo tem resposta_id=3 e o atual não tem
            if resp == "3" and atual_resp != "3":
                # Descarta o atual
                todas_linhas[atual_idx]["observacao"] = (
                    "duplicata descartada — CPF repetido, prevalece resposta_id=3"
                )
                cpf_escolhido[cpf] = idx
            else:
                # Descarta o novo
                todas_linhas[idx]["observacao"] = (
                    "duplicata descartada — CPF repetido, prevalece resposta_id=3"
                    if atual_resp == "3"
                    else "duplicata descartada — CPF repetido, mantida primeira ocorrencia"
                )

    descartadas = sum(1 for l in todas_linhas if "duplicata descartada" in l.get("observacao", ""))
    print(f"\n[DEDUP] {descartadas} linha(s) descartadas por CPF duplicado.")

    # Salvar CSV
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    df_out = pd.DataFrame(todas_linhas)
    df_out.to_csv(out_path, sep=";", index=False, encoding="utf-8-sig")

    total     = len(df_out)
    validos   = df_out["observacao"].eq("").sum()
    invalidos = total - validos

    # CSV limpo — apenas linhas prontas para importar (sem observacao)
    clean_path = out_path.parent / (out_path.stem + "_para_importar.csv")
    df_clean = df_out[df_out["observacao"].eq("")]
    df_clean.to_csv(clean_path, sep=";", index=False, encoding="utf-8-sig")

    print("\n" + "=" * 60)
    print(f"CSV completo  : {out_path}")
    print(f"CSV p/ importar: {clean_path}")
    print(f"Total de linhas       : {total}")
    print(f"  Prontas p/ importar : {validos}")
    print(f"  Com observacao      : {invalidos}  (duplicatas + erros)")
    print("=" * 60)
    print("\nPROXIMO PASSO:")
    print("  1. Abra o CSV e revise as linhas com 'observacao' preenchida.")
    print("  2. Corrija ou remova as linhas problematicas.")
    print("  3. Execute: python scripts/importar_historico.py")


if __name__ == "__main__":
    main()
