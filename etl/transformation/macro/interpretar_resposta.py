"""
interpretar_resposta.py
=======================
ETAPA AUTOMÁTICA — Transformation: interpreta a resposta bruta da API Neo Energia.

A API retorna texto livre (XML/JSON/string). Este módulo mapeia o conteúdo
da resposta para os valores estruturados do banco:
  - resposta_id  (FK → tabela respostas)
  - novo_status  (ENUM de tabela_macros)

Mapeamento baseado na tabela `respostas` do schema:
  id=6   'Aguardando processamento'         → status='pendente'      (não deveria aparecer aqui)
  id=7   'Doc. Fiscal nao cadastrado no SAP' → status='excluido'
  id=8   'Parceiro informado não possui...'  → status='excluido'
  id=9   'Status instalacao: desligado'      → status='reprocessar'
  id=10  'Status instalacao: ligado'         → status='consolidado'
  id=11  'ERRO'                              → status='pendente'  (recoloca na fila)

Erros de comunicação (timeout, LIMIT_EXCEEDED, ERRO_RETRY) → status='reprocessar'
Resposta desconhecida → status='reprocessar' com resposta_id=11 (ERRO)

Chamado por:
  etl/load/macro/04_processar_retorno_macro.py
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Mapeamento de palavras-chave → (resposta_id, novo_status)
# Ordem importa: verificações mais específicas ANTES das genéricas.
# ---------------------------------------------------------------------------
# Cada entrada: (substring_lower, resposta_id, novo_status)
_REGRAS: list[tuple[str, int, str]] = [
    # Instalação ligada → consolidado
    ("instalacao: ligado",              10, "consolidado"),
    ("instalação: ligado",              10, "consolidado"),
    ("ligado",                          10, "consolidado"),   # fallback

    # Instalação desligada → reprocessar (pode ligar depois)
    ("instalacao: desligado",           9,  "reprocessar"),
    ("instalação: desligado",           9,  "reprocessar"),
    ("desligado",                       9,  "reprocessar"),   # fallback

    # SAP — parceiro sem conta contrato → excluir
    ("parceiro informado n",            8,  "excluido"),
    ("nao possui conta contrato",       8,  "excluido"),
    ("não possui conta contrato",       8,  "excluido"),

    # SAP — documento fiscal não cadastrado → excluir
    ("doc. fiscal nao cadastrado",      7,  "excluido"),
    ("doc. fiscal não cadastrado",      7,  "excluido"),
    ("nao cadastrado no sap",           7,  "excluido"),
    ("não cadastrado no sap",           7,  "excluido"),

    # Limite de conexões SAP — recoloca na fila com reprocessar
    ("peak connections limit",          11, "reprocessar"),
    ("limit_exceeded",                  11, "reprocessar"),

    # Erros de comunicação → reprocessar
    ("erro_retry",                      11, "reprocessar"),
    ("timeout",                         11, "reprocessar"),
]

# Resposta padrão para qualquer coisa desconhecida
_PADRAO_DESCONHECIDO = (11, "reprocessar")

# resposta_id para erros sem texto (linha vazia / None)
_PADRAO_VAZIO = (11, "pendente")   # recoloca como pendente: dado não chegou


def interpretar(resposta_bruta: str | None) -> tuple[int, str]:
    """
    Interpreta a resposta bruta da API e retorna (resposta_id, novo_status).

    Parâmetros
    ----------
    resposta_bruta : str | None
        Texto retornado pela API Neo Energia, ou None/vazio se a consulta falhou.

    Retorna
    -------
    (resposta_id: int, novo_status: str)
        Valores prontos para UPDATE em tabela_macros.
    """
    if not resposta_bruta or not str(resposta_bruta).strip():
        return _PADRAO_VAZIO

    texto = str(resposta_bruta).strip().lower()

    for substring, rid, status in _REGRAS:
        if substring in texto:
            return rid, status

    # Nenhuma regra bateu
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
        ("",                                             (11, "pendente")),
        (None,                                           (11, "pendente")),
        ("alguma resposta desconhecida",                 (11, "reprocessar")),
    ]

    print(f"{'Entrada':<50} {'Esperado':<25} {'Obtido':<25} OK?")
    print("-" * 110)
    for entrada, esperado in casos:
        obtido = interpretar(entrada)
        ok = "✓" if obtido == esperado else "✗"
        entrada_str = str(entrada)[:48] if entrada else "(vazio)"
        print(f"{entrada_str:<50} {str(esperado):<25} {str(obtido):<25} {ok}")
