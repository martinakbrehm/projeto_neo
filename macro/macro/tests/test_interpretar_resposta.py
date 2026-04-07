"""
test_interpretar_resposta.py
============================
Testa a função `interpretar()` do módulo transformation/macro/interpretar_resposta.py.

Cobre:
  - Todos os CodigoRetorno documentados (000-011)
  - Respostas em JSON válido
  - Erros de comunicação (texto livre: ERRO_RETRY, LIMIT_EXCEEDED, timeout)
  - Respostas vazias / None
  - JSON malformado
  - Comportamento com e sem mapa carregado do banco
  - Mapeamento correto excluir→excluido (tradução de ENUM)

Execução:
    pytest tests/test_interpretar_resposta.py -v
"""
import json
import pytest
from interpretar_resposta import interpretar, _STATUS_RESPOSTAS_PARA_ENUM


# ── Helpers ────────────────────────────────────────────────────────────────

def resposta(codigo: str, msg: str = "", status_api: str = "INATIVO") -> str:
    return json.dumps({
        "Error": "false",
        "CodigoRetorno": codigo,
        "Msg": msg or f"Mensagem codigo {codigo}",
        "Status": status_api,
    })


# ── Mapeamento esperado por código ─────────────────────────────────────────
# (resposta_id, status_enum_tabela_macros)
ESPERADO_POR_CODIGO = {
    "000": (0,  "excluido"),
    "001": (1,  "excluido"),
    "002": (2,  "excluido"),
    "003": (3,  "consolidado"),
    "004": (4,  "reprocessar"),
    "005": (5,  "reprocessar"),
    "006": (6,  "pendente"),
    "007": (7,  "excluido"),
    "008": (8,  "excluido"),
    "009": (9,  "reprocessar"),
    "010": (10, "consolidado"),
    "011": (11, "reprocessar"),
}


class TestInterpretarComMapa:
    """Tests com mapa carregado do banco (comportamento de produção)."""

    def test_todos_codigos_sem_mapa(self):
        """Fallback hardcoded deve cobrir todos os códigos."""
        for cod, esperado in ESPERADO_POR_CODIGO.items():
            resultado = interpretar(resposta(cod))
            assert resultado == esperado, (
                f"Código {cod}: esperado {esperado}, obtido {resultado}"
            )

    def test_todos_codigos_com_mapa(self, mapa_respostas_fixo):
        """
        Com mapa do banco, resultado deve seguir a tabela `respostas`.
        Exceção: código 011 (ERRO) — banco define status='pendente',
        enquanto o fallback hardcoded usa 'reprocessar' para qualquer erro.
        """
        # Mapa do banco: código 11 (ERRO) → pendente (recoloca na fila)
        ESPERADO_COM_MAPA = {**ESPERADO_POR_CODIGO, "011": (11, "pendente")}
        for cod, esperado in ESPERADO_COM_MAPA.items():
            resultado = interpretar(resposta(cod), mapa_respostas_fixo)
            assert resultado == esperado, (
                f"Código {cod}: esperado {esperado}, obtido {resultado}"
            )

    def test_codigo_003_e_consolidado(self, mapa_respostas_fixo):
        """003 (titularidade confirmada ativo) → consolidado."""
        r = resposta("003", "Titularidade confirmada com contrato ativo", "ATIVO")
        assert interpretar(r, mapa_respostas_fixo) == (3, "consolidado")

    def test_codigo_000_e_excluido(self, mapa_respostas_fixo):
        """000 (conta contrato não existe) → excluido."""
        r = resposta("000", "Conta Contrato nao existe")
        assert interpretar(r, mapa_respostas_fixo) == (0, "excluido")

    def test_codigo_004_e_reprocessar(self, mapa_respostas_fixo):
        """004 (contrato inativo) → reprocessar (pode ativar no futuro)."""
        r = resposta("004", "Titularidade confirmada com contrato inativo")
        assert interpretar(r, mapa_respostas_fixo) == (4, "reprocessar")

    def test_status_excluir_do_banco_vira_excluido_enum(self, mapa_respostas_fixo):
        """Tabela respostas usa 'excluir', ENUM de tabela_macros usa 'excluido'."""
        assert mapa_respostas_fixo[0]["status"] == "excluir"
        _, status_enum = interpretar(resposta("000"), mapa_respostas_fixo)
        assert status_enum == "excluido"  # tradução aplicada

    def test_traducao_enum_completa(self):
        """Todos os valores da tabela respostas devem ter tradução definida."""
        valores_banco = {"excluir", "consolidado", "reprocessar", "pendente"}
        for v in valores_banco:
            assert v in _STATUS_RESPOSTAS_PARA_ENUM, f"'{v}' sem tradução em _STATUS_RESPOSTAS_PARA_ENUM"


class TestRespostasDeErro:
    """Tests para erros de comunicação (não-JSON)."""

    def test_erro_retry(self):
        assert interpretar("ERRO_RETRY: ReadTimeout") == (11, "reprocessar")

    def test_erro_retry_case_insensitive(self):
        assert interpretar("erro_retry: connection refused") == (11, "reprocessar")

    def test_limit_exceeded(self):
        assert interpretar("LIMIT_EXCEEDED") == (11, "reprocessar")

    def test_peak_connections(self):
        assert interpretar("peak connections limit exceeded (100)") == (11, "reprocessar")

    def test_timeout(self):
        assert interpretar("timeout occurred after 4s") == (11, "reprocessar")

    def test_erro_prefixado(self):
        assert interpretar("ERRO: OSError connection refused") == (11, "reprocessar")

    def test_string_desconhecida(self):
        """Qualquer string desconhecida → reprocessar (seguro por padrão)."""
        assert interpretar("algo completamente inesperado xyz123") == (11, "reprocessar")


class TestRespostasVazias:
    """Tests para respostas nulas ou vazias."""

    def test_none(self):
        assert interpretar(None) == (11, "reprocessar")

    def test_string_vazia(self):
        assert interpretar("") == (11, "reprocessar")

    def test_string_espacos(self):
        assert interpretar("   ") == (11, "reprocessar")

    def test_none_com_mapa(self, mapa_respostas_fixo):
        assert interpretar(None, mapa_respostas_fixo) == (11, "reprocessar")


class TestJsonMalformado:
    """Tests para JSON inválido que devem cair no fallback de texto."""

    def test_json_incompleto(self):
        assert interpretar('{"Error":"false"') == (11, "reprocessar")

    def test_json_sem_codigo_retorno(self):
        r = json.dumps({"Error": "false", "Status": "INATIVO"})
        assert interpretar(r) == (11, "reprocessar")

    def test_json_codigo_nao_numerico(self):
        r = json.dumps({"CodigoRetorno": "ABC"})
        assert interpretar(r) == (11, "reprocessar")

    def test_json_codigo_como_int(self):
        """CodigoRetorno como inteiro (não string) ainda deve funcionar."""
        r = json.dumps({"Error": "false", "CodigoRetorno": 3})
        # "3" → código 3 → consolidado
        resultado = interpretar(r)
        assert resultado == (3, "consolidado")

    def test_texto_puro_xml(self):
        """Resposta XML (formato antigo) → reprocessar por segurança."""
        xml = "<response><status>INATIVO</status></response>"
        assert interpretar(xml) == (11, "reprocessar")


class TestCodigosLimite:
    """Tests de valores de borda no CodigoRetorno."""

    def test_codigo_com_zero_a_esquerda_006(self):
        """Código '006' → pendente."""
        assert interpretar(resposta("006")) == (6, "pendente")

    def test_codigo_010_consolidado(self):
        """Código '010' (instalação ligada, formato antigo) → consolidado."""
        assert interpretar(resposta("010")) == (10, "consolidado")

    def test_codigo_011_sem_mapa_reprocessar(self):
        """Código '011' sem mapa (fallback hardcoded) → reprocessar."""
        assert interpretar(resposta("011")) == (11, "reprocessar")

    def test_codigo_011_com_mapa_pendente(self, mapa_respostas_fixo):
        """Código '011' com mapa do banco → pendente (banco define ERRO = recoloca na fila)."""
        assert interpretar(resposta("011"), mapa_respostas_fixo) == (11, "pendente")

    def test_codigo_999_desconhecido(self):
        """Código não mapeado → reprocessar."""
        assert interpretar(resposta("999")) == (11, "reprocessar")
