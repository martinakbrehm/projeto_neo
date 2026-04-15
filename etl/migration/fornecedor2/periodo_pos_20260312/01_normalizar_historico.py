"""
01_normalizar_historico.py  —  PASSO 1
=======================================
Lê o CSV de entrada (300k registros) e o Excel de saída das macros,
cruza por CPF + operadora/empresa, e gera um CSV normalizado com
os status finais corretos, pronto para importação histórica.

Todos os 300k registros do CSV são mantidos (inclusive duplicatas),
pois representam o que realmente foi enviado no arquivo de origem.

Dados fixos:
    - data_importacao  :  2026-03-23  (importação retroativa)
    - arquivo_origem   :  23/03/300k.csv
    - data_processamento: extraída do Excel (data_hora) quando houver

Uso:
    python etl/migration/fornecedor2/periodo_pos_20260312/01_normalizar_historico.py
"""

import re
import sys
from pathlib import Path

import pandas as pd
import numpy as np

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR  = Path(__file__).resolve().parents[4]               # raiz do projeto
RAW_DIR   = BASE_DIR / "dados" / "fornecedor2" / "migration_periodo_pos_20260312" / "raw"
PROC_DIR  = BASE_DIR / "dados" / "fornecedor2" / "migration_periodo_pos_20260312" / "processed"

CSV_INPUT  = RAW_DIR / "clientes_300k_25_03.csv"
XL_INPUT   = RAW_DIR / "saida_unica_dados_filtrados_sem_erros.xlsx"

CSV_OUTPUT_COMPLETO    = PROC_DIR / "historico_normalizado.csv"
CSV_OUTPUT_IMPORTAR    = PROC_DIR / "historico_normalizado_para_importar.csv"

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
DATA_IMPORTACAO   = "2026-03-23"
ARQUIVO_ORIGEM    = "23/03/300k.csv"

DISTRIBUIDORA_MAP = {
    "celpe":  3,
    "coelba": 1,
    "cosern": 2,
}

# resposta_id  →  status na tabela_macros   (espelha tabela `respostas`)
RESPOSTA_STATUS = {
    0:  "excluido",       # Conta Contrato não existe
    1:  "excluido",       # Doc. fiscal não existe
    2:  "excluido",       # Titularidade não confirmada
    3:  "consolidado",    # Titularidade confirmada com contrato ativo
    4:  "reprocessar",    # Titularidade confirmada com contrato inativo
    5:  "reprocessar",    # Titularidade confirmada com inst. suspensa
    6:  "pendente",       # Aguardando processamento
    7:  "excluido",       # Doc. Fiscal nao cadastrado no SAP
    8:  "excluido",       # Parceiro informado não possui conta contrato
    9:  "reprocessar",    # Status instalacao: desligado
    10: "consolidado",    # Status instalacao: ligado
    11: "reprocessar",    # ERRO / fallback
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def norm_cpf(val) -> str:
    """Retorna CPF com 11 dígitos (zero-padded). '' se inválido."""
    if pd.isna(val) or str(val).strip() == "":
        return ""
    s = re.sub(r"\D", "", str(val).strip())
    if not s or len(s) > 11:
        return ""
    return s.zfill(11)


def norm_uc(val) -> str:
    """Retorna UC com 10 dígitos (zero-padded). '' se vazio."""
    if pd.isna(val) or str(val).strip() == "":
        return ""
    s = re.sub(r"\D", "", str(val).strip())
    if not s:
        return ""
    return s.zfill(10)


def parse_valor_br(val) -> str:
    """Converte '1.234,5600' → '1234.56'. Retorna '' se inválido."""
    if pd.isna(val) or str(val).strip() == "":
        return ""
    s = str(val).strip().replace(".", "").replace(",", ".")
    try:
        return str(round(float(s), 2))
    except ValueError:
        return ""


def parse_parcelamento(detalhe) -> dict:
    """
    Extrai DATA_INIC_PARC, QTD_PARCELAS, VALOR_PARCELAS do campo
    DetalheParcelamento (formato [{key:val,...}]).
    """
    result = {"data_inic_parc": "", "qtd_parcelas": "", "valor_parcelas": ""}
    if pd.isna(detalhe) or str(detalhe).strip() == "":
        return result
    s = str(detalhe)
    m_data = re.search(r"DATA_INIC_PARC[:\s]*(\d{4}-\d{2}-\d{2})", s)
    m_qtd  = re.search(r"QTD_PARCELAS[:\s]*(\d+)", s)
    m_val  = re.search(r"VALOR_PARCELAS[:\s]*([\d.,]+)", s)
    if m_data:
        result["data_inic_parc"] = m_data.group(1)
    if m_qtd:
        result["qtd_parcelas"] = str(int(m_qtd.group(1)))
    if m_val:
        result["valor_parcelas"] = parse_valor_br(m_val.group(1))
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 70)
    print("PASSO 1 — Normalizar histórico  (periodo_pos_20260312)")
    print("=" * 70)

    # ---- 1. Ler CSV de entrada (todos os 300k) ---------------------------
    print(f"\n[1/5] Lendo CSV de entrada: {CSV_INPUT.name}")
    df_csv = pd.read_csv(CSV_INPUT, encoding="latin1", sep=None, engine="python",
                         dtype=str)
    df_csv.columns = [c.strip() for c in df_csv.columns]
    print(f"       {len(df_csv):,} linhas  |  Colunas: {list(df_csv.columns)}")

    # Normalizar
    df_csv["cpf_norm"]  = df_csv["cpf"].apply(norm_cpf)
    df_csv["uc_norm"]   = df_csv["contract_account"].apply(norm_uc)
    df_csv["oper_norm"] = df_csv["operadora"].str.strip().str.lower()

    invalidos_cpf = (df_csv["cpf_norm"] == "").sum()
    invalidos_uc  = (df_csv["uc_norm"]  == "").sum()
    print(f"       CPFs inválidos: {invalidos_cpf}  |  UCs vazias: {invalidos_uc}")

    # ---- 2. Ler Excel de saídas (macros processadas) ---------------------
    print(f"\n[2/5] Lendo Excel de resultados: {XL_INPUT.name}")
    df_xl = pd.read_excel(XL_INPUT, dtype={"_cpf_norm": str, "cpf": str})
    print(f"       {len(df_xl):,} linhas")

    df_xl["cpf_norm"]  = df_xl["_cpf_norm"].astype(str).str.strip().str.zfill(11)
    df_xl["oper_norm"] = df_xl["empresa"].str.strip().str.lower()

    # Garantir que não há duplicatas por CPF+empresa no Excel
    dups_xl = df_xl.duplicated(subset=["cpf_norm", "oper_norm"], keep="first")
    if dups_xl.sum() > 0:
        print(f"       [AVISO] {dups_xl.sum()} duplicatas CPF+empresa no Excel — mantendo primeira")
        df_xl = df_xl[~dups_xl].copy()

    # Preparar colunas do Excel para merge
    xl_cols = [
        "cpf_norm", "oper_norm",
        "codigo_cliente",                           # UC processada
        "CodigoRetorno", "Msg", "Status",           # resultado macro
        "QtdFaturas", "VlrDebito", "VlrCredito",
        "DtAtivacaoContrato", "ParcelamentoAtivo",
        "DetalheParcelamento",
        "data_hora", "erro",
    ]
    df_xl_merge = df_xl[xl_cols].copy()

    # ---- 3. Merge CSV ← Excel (left join por CPF + operadora) -----------
    print(f"\n[3/5] Cruzando CSV com Excel por CPF + operadora ...")
    df = df_csv.merge(df_xl_merge, on=["cpf_norm", "oper_norm"], how="left",
                      suffixes=("", "_xl"))
    assert len(df) == len(df_csv), "Merge alterou número de linhas!"

    matched    = df["CodigoRetorno"].notna() | df["erro"].notna()
    n_matched  = matched.sum()
    n_pending  = (~matched).sum()
    print(f"       Matched: {n_matched:,}  |  Sem resultado (pendente): {n_pending:,}")

    # ---- 4. Mapear resposta_id e status ----------------------------------
    print(f"\n[4/5] Mapeando resposta_id e status ...")

    def resolver_resposta(row):
        """Retorna (resposta_id, status, observacao)."""
        codigo = row.get("CodigoRetorno")
        erro   = row.get("erro")

        # Sem match no Excel → pendente
        if pd.isna(codigo) and pd.isna(erro):
            return 6, "pendente", ""

        # Erro de API (LIMIT_EXCEEDED etc) → reprocessar
        if pd.isna(codigo) and not pd.isna(erro):
            return 11, "reprocessar", f"erro API: {erro}"

        # Código de retorno válido
        rid = int(float(codigo))
        status = RESPOSTA_STATUS.get(rid, "pendente")
        return rid, status, ""

    resultados = df.apply(resolver_resposta, axis=1, result_type="expand")
    resultados.columns = ["resposta_id", "status_final", "observacao_resultado"]
    df = pd.concat([df, resultados], axis=1)

    # Validação de CPF
    df["observacao"] = ""
    mask_cpf_inv = df["cpf_norm"] == ""
    df.loc[mask_cpf_inv, "observacao"] = "CPF inválido"
    df.loc[mask_cpf_inv, "status_final"] = "excluido"

    # Combinar observações
    df["observacao"] = df.apply(
        lambda r: "; ".join(filter(None, [r["observacao"], r["observacao_resultado"]])),
        axis=1
    )

    # ---- 5. Montar CSV normalizado ---------------------------------------
    print(f"\n[5/5] Gerando CSV normalizado ...")

    # Parse valores monetários
    df["valor_debito"]  = df["VlrDebito"].apply(parse_valor_br)
    df["valor_credito"] = df["VlrCredito"].apply(parse_valor_br)

    # Parse qtd_faturas
    df["qtd_faturas_n"] = pd.to_numeric(df["QtdFaturas"], errors="coerce")
    df["qtd_faturas_n"] = df["qtd_faturas_n"].apply(
        lambda v: str(int(v)) if not pd.isna(v) else ""
    )

    # Parse parcelamento
    parc_data = df["DetalheParcelamento"].apply(parse_parcelamento).apply(pd.Series)
    df = pd.concat([df, parc_data], axis=1)

    # Construir dataframe final
    out = pd.DataFrame({
        "row_idx":              range(len(df)),
        "cpf":                  df["cpf_norm"],
        "nome":                 df["nome"].fillna(""),
        "uc":                   df["uc_norm"],
        "operadora":            df["operadora"].str.strip(),
        "distribuidora_id":     df["oper_norm"].map(DISTRIBUIDORA_MAP).fillna("").apply(
                                    lambda v: str(int(v)) if v != "" else ""
                                ),
        "resposta_id":          df["resposta_id"].astype(int),
        "status":               df["status_final"],
        "msg":                  df["Msg"].fillna(""),
        "qtd_faturas":          df["qtd_faturas_n"],
        "valor_debito":         df["valor_debito"],
        "valor_credito":        df["valor_credito"],
        "data_processamento":   df["data_hora"].fillna(""),
        "data_importacao":      DATA_IMPORTACAO,
        "data_contrato":        df["DtAtivacaoContrato"].fillna(""),
        "status_contrato":      df["Status"].fillna(""),
        "parcelamento_ativo":   df["ParcelamentoAtivo"].fillna(""),
        "data_inic_parc":       df["data_inic_parc"],
        "qtd_parcelas":         df["qtd_parcelas"],
        "valor_parcelas":       df["valor_parcelas"],
        "arquivo_origem":       ARQUIVO_ORIGEM,
        "observacao":           df["observacao"],
    })

    # ---- Salvar -----------------------------------------------------------
    PROC_DIR.mkdir(parents=True, exist_ok=True)

    out.to_csv(CSV_OUTPUT_COMPLETO, sep=";", index=False, encoding="utf-8-sig")

    # CSV limpo (sem CPFs inválidos)
    out_clean = out[out["cpf"] != ""].copy()
    out_clean.to_csv(CSV_OUTPUT_IMPORTAR, sep=";", index=False, encoding="utf-8-sig")

    # ---- Resumo -----------------------------------------------------------
    print("\n" + "=" * 70)
    print("RESUMO")
    print("=" * 70)
    print(f"  Total de linhas          : {len(out):>10,}")
    print(f"  CPFs válidos (p/ import) : {len(out_clean):>10,}")
    print(f"  CPFs inválidos           : {(out['cpf'] == '').sum():>10,}")
    print()

    status_counts = out_clean["status"].value_counts()
    print("  Distribuição de status (apenas válidos):")
    for st, cnt in status_counts.items():
        print(f"    {st:<15}: {cnt:>10,}")
    print()

    dist_counts = out_clean["operadora"].value_counts()
    print("  Por operadora:")
    for op, cnt in dist_counts.items():
        print(f"    {op:<10}: {cnt:>10,}")
    print()

    print(f"  CSV completo    : {CSV_OUTPUT_COMPLETO}")
    print(f"  CSV p/ importar : {CSV_OUTPUT_IMPORTAR}")
    print("=" * 70)
    print("\nPROXIMO PASSO:")
    print("  Revise o CSV e execute:")
    print("  python etl/migration/fornecedor2/periodo_pos_20260312/02_importar_historico.py")


if __name__ == "__main__":
    main()
