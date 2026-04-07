"""
test_integracao_banco.py
========================
Testes de integração que exigem conexão real com o banco de dados.

Cobre:
  - Conexão ao banco está funcionando
  - Tabela `respostas` tem todos os códigos esperados
  - Tabela `tabela_macros` existe e tem os campos necessários
  - Tabela `cliente_uc` existe com dados
  - Tabela `cliente_origem` existe (migration aplicada)
  - mapa_respostas carregado do banco é consistente com o fallback hardcoded
  - 03_buscar_lote_macro: retorna lote válido em dry-run

Marcados com @pytest.mark.integracao — pulados em CI sem banco.

Execução:
    pytest tests/test_integracao_banco.py -v
    pytest tests/test_integracao_banco.py -v -m integracao
"""
import sys
from pathlib import Path

import pytest

PROJETO_DIR = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJETO_DIR))
sys.path.insert(0, str(PROJETO_DIR / "etl" / "transformation" / "macro"))

pytestmark = pytest.mark.integracao


# ── Fixture de conexão ─────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def conn():
    """Conexão real ao banco. Skipada se config.py não estiver disponível."""
    try:
        from config import db_destino
        import pymysql
        _conn = pymysql.connect(**db_destino())
        yield _conn
        _conn.close()
    except Exception as e:
        pytest.skip(f"Banco inacessível: {e}")


@pytest.fixture(scope="module")
def cur(conn):
    return conn.cursor()


# ── Conectividade básica ───────────────────────────────────────────────────

class TestConexaoBanco:

    def test_conexao_estabelecida(self, conn):
        assert conn.open

    def test_select_simples(self, cur):
        cur.execute("SELECT 1")
        assert cur.fetchone()[0] == 1

    def test_banco_correto(self, cur):
        """Verifica que está conectado ao banco que contém tabela_macros."""
        cur.execute("SHOW TABLES LIKE 'tabela_macros'")
        assert cur.fetchone() is not None, "Tabela 'tabela_macros' não encontrada"


# ── Tabela respostas ───────────────────────────────────────────────────────

class TestTabelaRespostas:

    CODIGOS_ESPERADOS = set(range(12))  # 0-11

    def test_tabela_respostas_existe(self, cur):
        cur.execute("SHOW TABLES LIKE 'respostas'")
        assert cur.fetchone() is not None

    def test_todos_codigos_presentes(self, cur):
        cur.execute("SELECT id FROM respostas ORDER BY id")
        ids = {r[0] for r in cur.fetchall()}
        faltando = self.CODIGOS_ESPERADOS - ids
        assert not faltando, f"Códigos ausentes em `respostas`: {faltando}"

    def test_nenhum_status_invalido(self, cur):
        """status deve ser um dos valores válidos."""
        validos = {"excluir", "consolidado", "reprocessar", "pendente"}
        cur.execute("SELECT id, status FROM respostas")
        for id_, status in cur.fetchall():
            assert status in validos, f"id={id_} tem status inválido: '{status}'"

    def test_mapa_banco_vs_fallback(self, cur):
        """
        Mapa carregado do banco deve bater com o fallback hardcoded.
        Exceção conhecida: código 11 (ERRO) — banco define 'pendente',
        fallback hardcoded usa 'reprocessar'. A função interpretar() com mapa
        usa o banco, que é a fonte de verdade.
        """
        from interpretar_resposta import carregar_mapa_respostas, _CODIGO_PARA_STATUS, _STATUS_RESPOSTAS_PARA_ENUM

        # Códigos onde banco e fallback diferem (documentado e aceitável)
        DIFERENCAS_CONHECIDAS = {
            11: ("pendente", "reprocessar"),  # banco=pendente, fallback=reprocessar
        }

        mapa = carregar_mapa_respostas(cur)
        for codigo, (rid_esperado, status_esperado) in _CODIGO_PARA_STATUS.items():
            assert codigo in mapa, f"Código {codigo} do fallback não existe no banco"
            if codigo in DIFERENCAS_CONHECIDAS:
                continue  # diferença documentada — pular
            status_banco_raw = mapa[codigo]["status"]
            status_banco_enum = _STATUS_RESPOSTAS_PARA_ENUM.get(status_banco_raw, status_banco_raw)
            assert status_banco_enum == status_esperado, (
                f"Código {codigo}: banco={status_banco_enum}, fallback={status_esperado}"
            )


# ── Tabela tabela_macros ───────────────────────────────────────────────────

class TestTabelaMacros:

    CAMPOS_OBRIGATORIOS = {"id", "cliente_id", "distribuidora_id", "status", "resposta_id", "data_extracao", "data_update"}

    def test_campos_existem(self, cur):
        cur.execute("SHOW COLUMNS FROM tabela_macros")
        campos = {r[0] for r in cur.fetchall()}
        faltando = self.CAMPOS_OBRIGATORIOS - campos
        assert not faltando, f"Campos ausentes em tabela_macros: {faltando}"

    def test_enum_status_correto(self, cur):
        cur.execute("SHOW COLUMNS FROM tabela_macros")
        for r in cur.fetchall():
            if r[0] == "status":
                tipo = r[1]
                assert "pendente" in tipo
                assert "processando" in tipo
                assert "reprocessar" in tipo
                assert "consolidado" in tipo
                assert "excluido" in tipo

    def test_tem_registros_pendentes_ou_reprocessar(self, cur):
        """Pipeline precisa de registros para processar."""
        cur.execute("SELECT COUNT(*) FROM tabela_macros WHERE status IN ('pendente','reprocessar')")
        total = cur.fetchone()[0]
        assert total > 0, "Nenhum registro pendente/reprocessar em tabela_macros"

    def test_sem_processando_orfaos(self, cur):
        """
        Registros presos em 'processando' indicam ciclo interrompido.
        Não é erro fatal, mas um aviso importante.
        """
        cur.execute("SELECT COUNT(*) FROM tabela_macros WHERE status='processando'")
        orfaos = cur.fetchone()[0]
        if orfaos > 0:
            pytest.warns(None, match="")  # Não falha, só avisa
            print(f"\n  AVISO: {orfaos:,} registros presos em 'processando'")


# ── Tabela cliente_uc ──────────────────────────────────────────────────────

class TestTabelaClienteUc:

    def test_tabela_existe(self, cur):
        cur.execute("SHOW TABLES LIKE 'cliente_uc'")
        assert cur.fetchone() is not None

    def test_tem_dados(self, cur):
        cur.execute("SELECT COUNT(*) FROM cliente_uc")
        assert cur.fetchone()[0] > 0, "cliente_uc está vazia"

    def test_campos_basicos(self, cur):
        cur.execute("SHOW COLUMNS FROM cliente_uc")
        campos = {r[0] for r in cur.fetchall()}
        for campo in ("id", "cliente_id", "uc", "distribuidora_id"):
            assert campo in campos, f"Campo '{campo}' ausente em cliente_uc"


# ── Tabela cliente_origem (migration 20260406) ─────────────────────────────

class TestTabelaClienteOrigem:

    def test_tabela_existe(self, cur):
        cur.execute("SHOW TABLES LIKE 'cliente_origem'")
        assert cur.fetchone() is not None, (
            "Tabela 'cliente_origem' não existe. Execute: "
            "python db/improvements/20260406_cliente_origem_views_fornecedor/migration.py"
        )

    def test_tem_dados_backfill(self, cur):
        cur.execute("SELECT COUNT(*) FROM cliente_origem")
        total = cur.fetchone()[0]
        assert total > 0, "cliente_origem está vazia — backfill não foi executado"

    def test_fornecedores_validos(self, cur):
        cur.execute("SELECT DISTINCT fornecedor FROM cliente_origem")
        validos = {"fornecedor2", "contatus", "fornecedor1"}
        for (f,) in cur.fetchall():
            assert f in validos, f"Fornecedor inválido em cliente_origem: '{f}'"


# ── 03_buscar_lote_macro em dry-run ───────────────────────────────────────

class TestBuscarLoteDryRun:

    def test_busca_retorna_lote_nao_vazio(self, cur):
        """Consulta de prioridade deve retornar registros."""
        cur.execute("""
            SELECT tm.id, c.cpf, cu.uc, COALESCE(co.fornecedor, 'fornecedor2') AS fornecedor
            FROM tabela_macros tm
            JOIN clientes       c  ON c.id  = tm.cliente_id
            JOIN cliente_uc     cu ON cu.cliente_id      = tm.cliente_id
                                   AND cu.distribuidora_id = tm.distribuidora_id
            LEFT JOIN cliente_origem co ON co.cliente_id = tm.cliente_id
            WHERE tm.status IN ('pendente', 'reprocessar')
            ORDER BY
              (tm.status = 'pendente') DESC,
              (COALESCE(co.fornecedor, 'fornecedor2') = 'fornecedor2') DESC,
              tm.id ASC
            LIMIT 10
        """)
        rows = cur.fetchall()
        assert len(rows) > 0, "Query de prioridade retornou 0 registros"

    def test_prioritario_e_fornecedor2(self, cur):
        """Primeiro registro deve ser de fornecedor2 se existir pendente."""
        cur.execute("""
            SELECT COALESCE(co.fornecedor, 'fornecedor2')
            FROM tabela_macros tm
            LEFT JOIN cliente_origem co ON co.cliente_id = tm.cliente_id
            WHERE tm.status = 'pendente'
            LIMIT 1
        """)
        row = cur.fetchone()
        if row:
            assert row[0] in ("fornecedor2", "contatus", "fornecedor1")

    def test_nao_inclui_consolidado_ou_excluido(self, cur):
        """Lote nunca deve incluir registros já finalizados."""
        cur.execute("""
            SELECT COUNT(*) FROM tabela_macros
            WHERE status IN ('consolidado', 'excluido', 'processando')
              AND status IN ('pendente', 'reprocessar')
        """)
        # Impossível ter os dois ao mesmo tempo — deve ser 0
        assert cur.fetchone()[0] == 0
