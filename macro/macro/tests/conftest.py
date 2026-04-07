"""
conftest.py — Fixtures compartilhadas entre todos os testes.

Uso:
    pytest macro/macro/tests/
"""
import json
import sys
from pathlib import Path

import pytest

# ── Caminhos base ──────────────────────────────────────────────────────────
MACRO_DIR   = Path(__file__).resolve().parents[1]          # macro/macro/
PROJETO_DIR = MACRO_DIR.parents[1]                         # raiz do projeto

# Adiciona ao sys.path para que os módulos ETL sejam encontrados
for p in [
    str(PROJETO_DIR),
    str(PROJETO_DIR / "etl" / "transformation" / "macro"),
    str(PROJETO_DIR / "etl" / "load" / "macro"),
    str(PROJETO_DIR / "etl" / "extraction" / "macro"),
]:
    if p not in sys.path:
        sys.path.insert(0, p)


# ── Fixtures de dados ──────────────────────────────────────────────────────

@pytest.fixture
def mapa_respostas_fixo():
    """
    Mapa de respostas idêntico ao da tabela `respostas` do banco.
    Permite rodar testes unitários sem conexão ao banco.
    """
    return {
        0:  {"mensagem": "Conta Contrato nao existe",                    "status": "excluir"},
        1:  {"mensagem": "Doc. fiscal não existe",                       "status": "excluir"},
        2:  {"mensagem": "Titularidade não confirmada",                  "status": "excluir"},
        3:  {"mensagem": "Titularidade confirmada com contrato ativo",   "status": "consolidado"},
        4:  {"mensagem": "Titularidade confirmada com contrato inativo", "status": "reprocessar"},
        5:  {"mensagem": "Titularidade confirmada com inst. suspensa",   "status": "reprocessar"},
        6:  {"mensagem": "Aguardando processamento",                     "status": "pendente"},
        7:  {"mensagem": "Doc. Fiscal nao cadastrado no SAP",            "status": "excluir"},
        8:  {"mensagem": "Parceiro informado nao possui conta contrato", "status": "excluir"},
        9:  {"mensagem": "Status instalacao: desligado",                 "status": "reprocessar"},
        10: {"mensagem": "Status instalacao: ligado",                    "status": "consolidado"},
        11: {"mensagem": "ERRO",                                         "status": "pendente"},
    }


@pytest.fixture
def resposta_api():
    """Fábrica de respostas JSON da API Neo Energia."""
    def _make(codigo: str, msg: str = "", status: str = "INATIVO") -> str:
        return json.dumps({
            "Error": "false",
            "CodigoRetorno": codigo,
            "Msg": msg or f"Mensagem cod {codigo}",
            "Status": status,
        })
    return _make


@pytest.fixture
def meta_lote_simples():
    """Meta JSON de lote com 3 registros distintos."""
    return {
        "gerado_em": "2026-04-07T12:00:00",
        "tamanho": 3,
        "registros": [
            {"macro_id": 1, "cpf": "11111111111", "codigo cliente": "0000000001", "empresa": "celpe"},
            {"macro_id": 2, "cpf": "22222222222", "codigo cliente": "0000000002", "empresa": "coelba"},
            {"macro_id": 3, "cpf": "33333333333", "codigo cliente": "0000000003", "empresa": "cosern"},
        ],
    }


@pytest.fixture
def meta_lote_com_multiplas_ucs():
    """
    Meta JSON onde o macro_id=1 aparece para 2 UCs diferentes do mesmo cliente.
    Simula o JOIN 1:N de cliente_uc.
    """
    return {
        "gerado_em": "2026-04-07T12:00:00",
        "tamanho": 4,
        "registros": [
            {"macro_id": 1, "cpf": "11111111111", "codigo cliente": "0000000001", "empresa": "celpe"},
            {"macro_id": 1, "cpf": "11111111111", "codigo cliente": "0000000099", "empresa": "celpe"},
            {"macro_id": 2, "cpf": "22222222222", "codigo cliente": "0000000002", "empresa": "coelba"},
            {"macro_id": 3, "cpf": "33333333333", "codigo cliente": "0000000003", "empresa": "cosern"},
        ],
    }


def pytest_configure(config):
    config.addinivalue_line("markers", "integracao: testes que exigem banco de dados real")
    config.addinivalue_line("markers", "conectividade: testes que exigem VPN/SSH/API ativos")
    config.addinivalue_line("markers", "lento: testes com esperas ou processamento pesado")
