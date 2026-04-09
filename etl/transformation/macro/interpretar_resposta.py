"""
interpretar_resposta.py
=======================
ETAPA AUTOMÁTICA — Transformation: interpreta a resposta bruta da API Neo Energia.

A API retorna JSON com o campo CodigoRetorno que mapeia diretamente para o id
da tabela `respostas` do banco:

  CodigoRetorno → id em `respostas` → status em tabela_macros
  ─────────────────────────────────────────────────────────────
  000  Conta Contrato nao existe                    → excluido
  001  Doc. fiscal não existe                       → excluido
  002  Titularidade não confirmada                  → excluido
  003  Titularidade confirmada com contrato ativo   → consolidado
  004  Titularidade confirmada com contrato inativo → reprocessar
  005  Titularidade confirmada com inst. suspensa   → reprocessar
  006  Aguardando processamento                     → pendente
  007  Doc. Fiscal nao cadastrado no SAP            → excluido
  008  Parceiro informado nao possui conta contrato → excluido
  009  Status instalacao: desligado                 → reprocessar
  010  Status instalacao: ligado                    → consolidado
  011  ERRO                                         → reprocessar

Erros de comunicação (timeout, LIMIT_EXCEEDED, ERRO_RETRY) → reprocessar, id=11
Resposta desconhecida → reprocessar, id=11

A fonte de verdade do mapeamento é a tabela `respostas` do banco.
Se um mapa carregado do banco for passado, ele tem precedência sobre o fallback.

Chamado por:
  etl/load/macro/04_processar_retorno_macro.py
"""

from __future__ import annotations

import json

# ---------------------------------------------------------------------------
# Tradução do campo `status` da tabela `respostas` → ENUM de tabela_macros
# (a tabela usa 'excluir', o ENUM usa 'excluido')
# ---------------------------------------------------------------------------
_STATUS_RESPOSTAS_PARA_ENUM: dict[str, str] = {
    "excluir":    "excluido",
    "excluido":   "excluido",
    "consolidado":"consolidado",
    "reprocessar":"reprocessar",
    "pendente":   "pendente",
}

# ---------------------------------------------------------------------------
# Fallback hardcoded — espelho da tabela `respostas` do banco.
# Usado quando mapa_respostas não é fornecido.
# Formato: codigo_int → (resposta_id, novo_status_enum)
# ---------------------------------------------------------------------------
_CODIGO_PARA_STATUS: dict[int, tuple[int, str]] = {
    0:  (0,  "excluido"),
    1:  (1,  "excluido"),
    2:  (2,  "excluido"),
    3:  (3,  "consolidado"),
    4:  (4,  "reprocessar"),
    5:  (5,  "reprocessar"),
    6:  (6,  "pendente"),
    7:  (7,  "excluido"),
    8:  (8,  "excluido"),
    9:  (9,  "reprocessar"),
    10: (10, "consolidado"),
    11: (11, "reprocessar"),
}

# Usado quando a resposta é vazia/None (dado não chegou — API não respondeu)
# Código 6 = "Aguardando processamento" → status pendente (volta para a fila)
_PADRAO_VAZIO       = (6, "pendente")
# Usado para strings de erro de comunicação ou código desconhecido
_PADRAO_DESCONHECIDO = (11, "reprocessar")

# ---------------------------------------------------------------------------
# Regras de fallback para respostas que NÃO são JSON válido
# (erros de comunicação: timeout, ERRO_RETRY, LIMIT_EXCEEDED…)
# Ordem importa: mais específicas ANTES das genéricas.
# ---------------------------------------------------------------------------
_REGRAS_TEXTO: list[tuple[str, int, str]] = [
    ("peak connections limit",  11, "reprocessar"),
    ("limit_exceeded",          11, "reprocessar"),
    ("erro_retry",              11, "reprocessar"),
    ("timeout",                 11, "reprocessar"),
    ("erro:",                   11, "reprocessar"),
]


def interpretar(
    resposta_bruta: str | None,
    mapa_respostas: dict[int, dict] | None = None,
) -> tuple[int, str]:
    """
    Interpreta a resposta bruta da API e retorna (resposta_id, novo_status).

    Parâmetros
    ----------
    resposta_bruta : str | None
        Texto retornado pela API Neo Energia, ou None/vazio se a consulta falhou.
    mapa_respostas : dict | None
        Mapa carregado do banco via carregar_mapa_respostas(). Se fornecido,
        usado para traduzir CodigoRetorno → status dinamicamente.
        Formato: {id_int: {'mensagem': str, 'status': str}}

    Retorna
    -------
    (resposta_id: int, novo_status: str)
        Valores prontos para UPDATE em tabela_macros.
    """
    if not resposta_bruta or not str(resposta_bruta).strip():
        return _PADRAO_VAZIO

    texto = str(resposta_bruta).strip()

    # ── 1. Tenta parsear JSON e usar CodigoRetorno ──────────────────────────
    try:
        data = json.loads(texto)
        codigo_str = str(data.get("CodigoRetorno", "")).strip()
        if codigo_str.isdigit():
            codigo = int(codigo_str)
            if mapa_respostas and codigo in mapa_respostas:
                # Fonte de verdade: tabela respostas do banco
                status_db = mapa_respostas[codigo].get("status", "reprocessar")
                status_enum = _STATUS_RESPOSTAS_PARA_ENUM.get(status_db, "reprocessar")
                return codigo, status_enum
            # Fallback hardcoded
            if codigo in _CODIGO_PARA_STATUS:
                return _CODIGO_PARA_STATUS[codigo]
    except (json.JSONDecodeError, ValueError, TypeError):
        pass

    # ── 2. Fallback por palavras-chave (erros de comunicação) ───────────────
    texto_lower = texto.lower()
    for substring, rid, status in _REGRAS_TEXTO:
        if substring in texto_lower:
            return rid, status

    return _PADRAO_DESCONHECIDO


# ---------------------------------------------------------------------------
# Tabela de referência: permite que outros módulos carreguem do banco
# (evita hard-code de IDs caso a tabela respostas seja estendida)
# ---------------------------------------------------------------------------

def carregar_mapa_respostas(cur) -> dict[int, dict]:
    """
    Carrega a tabela `respostas` do banco e retorna um dicionário
    {id: {'mensagem': ..., 'status': ...}} para uso em logs/relatórios.
    """
    cur.execute("SELECT id, mensagem, status FROM respostas")
    return {r[0]: {"mensagem": r[1], "status": r[2]} for r in cur.fetchall()}


# ---------------------------------------------------------------------------
# Teste rápido (python -m etl.transformation.macro.interpretar_resposta)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    casos = [
        ("Status instalacao: ligado",                    (10, "consolidado")),
        ("Status instalacao: desligado",                 (9,  "reprocessar")),
        ("Doc. Fiscal nao cadastrado no SAP",            (7,  "excluido")),
        ("Parceiro informado nao possui conta contrato", (8,  "excluido")),
        ("peak connections limit exceeded",              (11, "reprocessar")),
        ("LIMIT_EXCEEDED",                               (11, "reprocessar")),
        ("ERRO_RETRY: ReadTimeout",                      (11, "reprocessar")),
        ("",                                             (6,  "pendente")),   # _PADRAO_VAZIO: sem resposta da API → pendente (volta para fila)
        (None,                                           (6,  "pendente")),   # idem
        ("alguma resposta desconhecida",                 (11, "reprocessar")),
    ]

    print(f"{'Entrada':<50} {'Esperado':<25} {'Obtido':<25} OK?")
    print("-" * 110)
    for entrada, esperado in casos:
        obtido = interpretar(entrada)
        ok = "✓" if obtido == esperado else "✗"
        entrada_str = str(entrada)[:48] if entrada else "(vazio)"
        print(f"{entrada_str:<50} {str(esperado):<25} {str(obtido):<25} {ok}")
