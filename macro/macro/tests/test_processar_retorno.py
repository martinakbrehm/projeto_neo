"""
test_processar_retorno.py
=========================
Testa a lógica de processamento do módulo etl/load/macro/04_processar_retorno_macro.py.

Cobre:
  - normalizar_cpf / normalizar_uc
  - construir_indice_meta
  - Agregação por macro_id (múltiplas UCs → melhor status)
  - Prioridade de status: consolidado > reprocessar > excluido > pendente
  - Registros sem match no índice (sem_match)
  - Registros do lote sem resultado (recuperados)
  - Coleta de colunas por nome flexível

Todos unitários — sem banco de dados.

Execução:
    pytest tests/test_processar_retorno.py -v
"""
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# sys.path já configurado pelo conftest.py em tempo de coleta
from importlib import import_module

_mod = import_module("04_processar_retorno_macro")
normalizar_cpf        = _mod.normalizar_cpf
normalizar_uc         = _mod.normalizar_uc
construir_indice_meta = _mod.construir_indice_meta
processar             = _mod.processar

PROJETO_DIR = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJETO_DIR / "etl" / "transformation" / "macro"))
from interpretar_resposta import interpretar


# ── Helpers ────────────────────────────────────────────────────────────────

def _resp(codigo: str) -> str:
    return json.dumps({"Error": "false", "CodigoRetorno": codigo, "Msg": "", "Status": "INATIVO"})


def _df(linhas: list[dict]) -> pd.DataFrame:
    """Cria DataFrame simulando a saída do consulta_contrato.py."""
    return pd.DataFrame(linhas)


def _mock_conn(mapa_respostas: dict):
    """Simula conexão + cursor do banco com mapa de respostas injetado."""
    cur = MagicMock()
    cur.fetchall.return_value = [
        (k, v["mensagem"], v["status"]) for k, v in mapa_respostas.items()
    ]
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn, cur


# ── normalizar_cpf ─────────────────────────────────────────────────────────

class TestNormalizarCpf:

    def test_cpf_11_digitos(self):
        assert normalizar_cpf("12345678901") == "12345678901"

    def test_cpf_com_mascara(self):
        assert normalizar_cpf("123.456.789-01") == "12345678901"

    def test_cpf_curto_preenche_zeros(self):
        assert normalizar_cpf("1234") == "00000001234"

    def test_cpf_none_retorna_vazio(self):
        assert normalizar_cpf(None) == ""

    def test_cpf_so_letras_retorna_vazio(self):
        assert normalizar_cpf("abc") == ""

    def test_cpf_misto(self):
        assert normalizar_cpf("  47.177.098/453 ") == "47177098453"


class TestNormalizarUc:

    def test_uc_10_digitos(self):
        assert normalizar_uc("1165486367") == "1165486367"

    def test_uc_curta_preenche_zeros(self):
        assert normalizar_uc("123") == "0000000123"

    def test_uc_com_letras_extrai_digitos(self):
        assert normalizar_uc("UC000123") == "0000000123"

    def test_uc_none_retorna_vazio(self):
        assert normalizar_uc(None) == ""

    def test_uc_com_zeros_a_esquerda(self):
        assert normalizar_uc("0000000001") == "0000000001"


# ── construir_indice_meta ──────────────────────────────────────────────────

class TestConstruirIndiceMeta:

    def test_indice_simples(self, meta_lote_simples):
        idx = construir_indice_meta(meta_lote_simples)
        assert ("11111111111", "0000000001") in idx
        assert idx[("11111111111", "0000000001")] == 1

    def test_cpf_e_uc_normalizados(self):
        """
        construir_indice_meta usa str.zfill() (não normalizar_cpf),
        pois o meta JSON vem do banco e sempre contém dígitos limpos.
        CPF mascarado no meta não é um caso real de produção.
        """
        meta = {"registros": [
            {"macro_id": 99, "cpf": "11111111111", "codigo cliente": "1"},
        ]}
        idx = construir_indice_meta(meta)
        assert ("11111111111", "0000000001") in idx

    def test_cpf_curto_no_meta_preenche_zeros(self):
        """CPF numérico curto no meta é preenchido com zeros à esquerda via zfill."""
        meta = {"registros": [
            {"macro_id": 77, "cpf": "1234", "codigo cliente": "5678"},
        ]}
        idx = construir_indice_meta(meta)
        assert ("00000001234", "0000005678") in idx

    def test_meta_vazio(self):
        assert construir_indice_meta({"registros": []}) == {}

    def test_multiplos_registros_mesmo_cpf(self, meta_lote_com_multiplas_ucs):
        idx = construir_indice_meta(meta_lote_com_multiplas_ucs)
        # Ambas as UCs do macro_id=1 devem estar no índice
        assert ("11111111111", "0000000001") in idx
        assert ("11111111111", "0000000099") in idx
        assert idx[("11111111111", "0000000001")] == 1
        assert idx[("11111111111", "0000000099")] == 1


# ── processar(): lógica de agregação ─────────────────────────────────────

class TestAgregarPorMacroId:
    """
    Verifica a regra: um macro_id com múltiplas UCs → mantém o status
    de maior prioridade (consolidado > reprocessar > excluido > pendente).
    """

    def test_uma_uc_ativa_consolida(self, meta_lote_com_multiplas_ucs, mapa_respostas_fixo):
        """
        macro_id=1 tem 2 UCs:
          UC 0000000001 → 000 (excluido)
          UC 0000000099 → 003 (consolidado)
        Resultado esperado: consolidado (maior prioridade).
        """
        conn, _ = _mock_conn(mapa_respostas_fixo)
        df = _df([
            {"cpf": "11111111111", "codigo cliente": "0000000001", "empresa": "celpe", "resposta": _resp("000")},
            {"cpf": "11111111111", "codigo cliente": "0000000099", "empresa": "celpe", "resposta": _resp("003")},
            {"cpf": "22222222222", "codigo cliente": "0000000002", "empresa": "coelba", "resposta": _resp("000")},
            {"cpf": "33333333333", "codigo cliente": "0000000003", "empresa": "cosern", "resposta": _resp("000")},
        ])
        stats = processar(conn, df, meta_lote_com_multiplas_ucs, dry_run=True)
        assert stats["consolidado"] == 1   # macro_id=1
        assert stats["excluido"] == 2      # macro_id=2 e 3

    def test_duas_ucs_ambas_excluidas(self, meta_lote_com_multiplas_ucs, mapa_respostas_fixo):
        """Ambas as UCs do mesmo cliente excluidas → excluido."""
        conn, _ = _mock_conn(mapa_respostas_fixo)
        df = _df([
            {"cpf": "11111111111", "codigo cliente": "0000000001", "empresa": "celpe", "resposta": _resp("000")},
            {"cpf": "11111111111", "codigo cliente": "0000000099", "empresa": "celpe", "resposta": _resp("001")},
            {"cpf": "22222222222", "codigo cliente": "0000000002", "empresa": "coelba", "resposta": _resp("003")},
            {"cpf": "33333333333", "codigo cliente": "0000000003", "empresa": "cosern", "resposta": _resp("004")},
        ])
        stats = processar(conn, df, meta_lote_com_multiplas_ucs, dry_run=True)
        assert stats["excluido"] == 1      # macro_id=1
        assert stats["consolidado"] == 1   # macro_id=2
        assert stats["reprocessar"] == 1   # macro_id=3

    def test_reprocessar_nao_sobrescreve_consolidado(self, meta_lote_com_multiplas_ucs, mapa_respostas_fixo):
        """Uma UC consolida, outra reprocessa → mantém consolidado."""
        conn, _ = _mock_conn(mapa_respostas_fixo)
        df = _df([
            {"cpf": "11111111111", "codigo cliente": "0000000001", "empresa": "celpe", "resposta": _resp("003")},
            {"cpf": "11111111111", "codigo cliente": "0000000099", "empresa": "celpe", "resposta": _resp("004")},
            {"cpf": "22222222222", "codigo cliente": "0000000002", "empresa": "coelba", "resposta": _resp("000")},
            {"cpf": "33333333333", "codigo cliente": "0000000003", "empresa": "cosern", "resposta": _resp("000")},
        ])
        stats = processar(conn, df, meta_lote_com_multiplas_ucs, dry_run=True)
        assert stats["consolidado"] == 1
        assert stats["reprocessar"] == 0

    def test_prioridade_completa(self, mapa_respostas_fixo):
        """
        Testa todas as combinações da hierarquia de prioridade.
        consolidado(3) > reprocessar(2) > excluido(1) > pendente(0)
        """
        from interpretar_resposta import _STATUS_RESPOSTAS_PARA_ENUM
        STATUS_PRIORIDADE = {"consolidado": 3, "reprocessar": 2, "excluido": 1, "pendente": 0}

        pares = [
            ("consolidado", "reprocessar", "consolidado"),
            ("consolidado", "excluido",    "consolidado"),
            ("consolidado", "pendente",    "consolidado"),
            ("reprocessar", "excluido",    "reprocessar"),
            ("reprocessar", "pendente",    "reprocessar"),
            ("excluido",    "pendente",    "excluido"),
        ]
        for s1, s2, esperado in pares:
            p1 = STATUS_PRIORIDADE[s1]
            p2 = STATUS_PRIORIDADE[s2]
            melhor = s1 if p1 >= p2 else s2
            assert melhor == esperado, f"prioridade({s1}, {s2}) → esperado {esperado}, obtido {melhor}"


class TestProcessarSemMatch:

    def test_linha_sem_cpf_correspondente(self, meta_lote_simples, mapa_respostas_fixo):
        """Linha no resultado que não está no meta → contada como sem_match."""
        conn, _ = _mock_conn(mapa_respostas_fixo)
        df = _df([
            # CPF não está no meta
            {"cpf": "99999999999", "codigo cliente": "0000099999", "empresa": "celpe", "resposta": _resp("003")},
        ])
        stats = processar(conn, df, meta_lote_simples, dry_run=True)
        assert stats["sem_match"] == 1
        assert stats["consolidado"] == 0

    def test_todos_sem_match(self, meta_lote_simples, mapa_respostas_fixo):
        conn, _ = _mock_conn(mapa_respostas_fixo)
        df = _df([
            {"cpf": "88888888888", "codigo cliente": "0000000088", "empresa": "celpe", "resposta": _resp("003")},
            {"cpf": "77777777777", "codigo cliente": "0000000077", "empresa": "celpe", "resposta": _resp("000")},
        ])
        stats = processar(conn, df, meta_lote_simples, dry_run=True)
        assert stats["sem_match"] == 2


class TestProcessarRecuperados:

    def test_registro_no_lote_sem_resultado(self, meta_lote_simples, mapa_respostas_fixo):
        """macro_id=3 estava no lote mas não apareceu no resultado → recuperado."""
        conn, _ = _mock_conn(mapa_respostas_fixo)
        df = _df([
            # Apenas 2 dos 3 registros do lote retornaram resultado
            {"cpf": "11111111111", "codigo cliente": "0000000001", "empresa": "celpe",  "resposta": _resp("003")},
            {"cpf": "22222222222", "codigo cliente": "0000000002", "empresa": "coelba", "resposta": _resp("000")},
        ])
        stats = processar(conn, df, meta_lote_simples, dry_run=True)
        assert stats["recuperados"] == 1   # macro_id=3 não veio no resultado


class TestProcessarDryRun:

    def test_dry_run_nao_executa_update(self, meta_lote_simples, mapa_respostas_fixo):
        """Em dry_run=True, cursor.execute() não deve ser chamado para updates."""
        conn, cur = _mock_conn(mapa_respostas_fixo)
        df = _df([
            {"cpf": "11111111111", "codigo cliente": "0000000001", "empresa": "celpe",  "resposta": _resp("003")},
            {"cpf": "22222222222", "codigo cliente": "0000000002", "empresa": "coelba", "resposta": _resp("000")},
            {"cpf": "33333333333", "codigo cliente": "0000000003", "empresa": "cosern", "resposta": _resp("004")},
        ])
        stats = processar(conn, df, meta_lote_simples, dry_run=True)
        # Cursor pode ser chamado para carregar mapa_respostas, mas NÃO para UPDATE
        update_calls = [c for c in cur.execute.call_args_list if "UPDATE" in str(c)]
        assert update_calls == [], "dry_run=True não deve executar UPDATE"

    def test_dry_run_stats_corretas(self, meta_lote_simples, mapa_respostas_fixo):
        """Stats devem ser calculadas mesmo em dry_run."""
        conn, _ = _mock_conn(mapa_respostas_fixo)
        df = _df([
            {"cpf": "11111111111", "codigo cliente": "0000000001", "empresa": "celpe",  "resposta": _resp("003")},
            {"cpf": "22222222222", "codigo cliente": "0000000002", "empresa": "coelba", "resposta": _resp("000")},
            {"cpf": "33333333333", "codigo cliente": "0000000003", "empresa": "cosern", "resposta": _resp("001")},
        ])
        stats = processar(conn, df, meta_lote_simples, dry_run=True)
        assert stats["consolidado"] == 1
        assert stats["excluido"] == 2
        assert stats["sem_match"] == 0
        assert stats["recuperados"] == 0


class TestDeteccaoColunas:

    def test_coluna_resposta_alternativa(self, meta_lote_simples, mapa_respostas_fixo):
        """Coluna com nome contendo 'resposta' deve ser detectada automaticamente."""
        conn, _ = _mock_conn(mapa_respostas_fixo)
        df = _df([
            {"cpf": "11111111111", "codigo cliente": "0000000001", "empresa": "celpe",
             "resposta_api": _resp("003")},
        ])
        stats = processar(conn, df, meta_lote_simples, dry_run=True)
        assert stats["consolidado"] == 1

    def test_coluna_cpf_cnpj(self, meta_lote_simples, mapa_respostas_fixo):
        """Coluna 'cpf_cnpj' deve ser aceita como CPF."""
        conn, _ = _mock_conn(mapa_respostas_fixo)
        df = _df([
            {"cpf_cnpj": "11111111111", "codigo cliente": "0000000001", "empresa": "celpe",
             "resposta": _resp("000")},
        ])
        stats = processar(conn, df, meta_lote_simples, dry_run=True)
        assert stats["excluido"] == 1
