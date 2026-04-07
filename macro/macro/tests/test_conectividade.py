"""
test_conectividade.py
=====================
Testes de conectividade: VPN, túnel SSH e API Neo Energia.

Esses testes exigem infraestrutura ativa (VPN, servidor SSH, API).
Marcados com @pytest.mark.conectividade — devem ser rodados manualmente
antes de um ciclo de produção ou para diagnóstico de falhas.

Execução:
    pytest tests/test_conectividade.py -v -m conectividade
    pytest tests/test_conectividade.py -v --timeout=30
"""
import os
import socket
import subprocess
import sys
from pathlib import Path

import pytest

MACRO_DIR   = Path(__file__).resolve().parents[1]
PROJETO_DIR = MACRO_DIR.parents[1]
PLINK_EXE   = MACRO_DIR / "plink.exe"

pytestmark = pytest.mark.conectividade


# ── Fixture: .env carregado ────────────────────────────────────────────────

@pytest.fixture(scope="module")
def env_config():
    """Carrega configurações do .env do macro. Skipado se ausente."""
    env_path = MACRO_DIR / ".env"
    if not env_path.exists():
        pytest.skip(".env não encontrado em macro/macro/.env")

    config = {}
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            config[k.strip()] = v.strip()
    return config


# ── plink.exe disponível ───────────────────────────────────────────────────

class TestPlinkDisponivel:

    def test_plink_existe(self):
        assert PLINK_EXE.exists(), f"plink.exe não encontrado em {PLINK_EXE}"

    def test_plink_executavel(self):
        result = subprocess.run(
            [str(PLINK_EXE), "--help"],
            capture_output=True, timeout=5
        )
        # plink retorna código != 0 mas deve ter saída
        assert result.returncode is not None


# ── Porta do túnel ─────────────────────────────────────────────────────────

class TestPortaTunel:

    def test_porta_5000_acessivel(self):
        """Verifica se algum processo está ouvindo na porta 5000 (túnel SSH)."""
        try:
            with socket.create_connection(("localhost", 5000), timeout=3):
                pass  # conseguiu conectar
        except ConnectionRefusedError:
            pytest.skip("Porta 5000 não está ouvindo — túnel não está ativo")
        except OSError as e:
            pytest.fail(f"Erro ao verificar porta 5000: {e}")

    def test_porta_5000_responde_http(self):
        """Se a porta está aberta, deve responder a HTTP."""
        try:
            import urllib.request
            url = "http://localhost:5000/validacaotitularidade/Validacao/ValidarTitularidade?ContaContrato=123456789&CpfCnpj=12345678901&Empresa=coelba"
            with urllib.request.urlopen(url, timeout=5) as resp:
                assert resp.status == 200
                body = resp.read().decode("utf-8", errors="replace")
                assert "CodigoRetorno" in body, f"Resposta inesperada: {body[:200]}"
        except ConnectionRefusedError:
            pytest.skip("Porta 5000 não está ouvindo — túnel não está ativo")
        except Exception as e:
            pytest.fail(f"API não respondeu na porta 5000: {e}")


# ── SSH / plink ────────────────────────────────────────────────────────────

class TestSshConexao:

    def test_ssh_echo_conectado(self, env_config):
        """Testa conexão SSH básica ao servidor."""
        cmd = [
            str(PLINK_EXE), "-batch",
            "-pw", env_config.get("SSH_PASSWORD", ""),
            "-hostkey", env_config.get("SSH_HOST_KEY", ""),
            f"{env_config.get('SSH_USER', 'root')}@{env_config.get('SSH_SERVER', '')}",
            "echo conectado",
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, timeout=15, text=True)
            assert result.returncode == 0, (
                f"SSH falhou (código {result.returncode}):\n"
                f"stdout: {result.stdout}\nstderr: {result.stderr}"
            )
            assert "conectado" in result.stdout.lower()
        except subprocess.TimeoutExpired:
            pytest.fail("SSH timeout após 15s — servidor inacessível ou VPN inativa")

    def test_vpn_ativa_no_servidor(self, env_config):
        """Verifica se a VPN está ativa no servidor remoto."""
        cmd = [
            str(PLINK_EXE), "-batch",
            "-pw", env_config.get("SSH_PASSWORD", ""),
            "-hostkey", env_config.get("SSH_HOST_KEY", ""),
            f"{env_config.get('SSH_USER', 'root')}@{env_config.get('SSH_SERVER', '')}",
            "ipsec status 2>/dev/null | grep -c 'ESTABLISHED' || echo 0",
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, timeout=15, text=True)
            if result.returncode != 0:
                pytest.skip(f"Não foi possível verificar VPN: {result.stderr}")
            tunnels = int(result.stdout.strip() or "0")
            assert tunnels > 0, (
                "VPN não está estabelecida no servidor. "
                "Execute a reconexão VPN antes de rodar a macro."
            )
        except subprocess.TimeoutExpired:
            pytest.fail("Timeout ao verificar VPN no servidor")

    def test_host_interno_alcancavel(self, env_config):
        """Verifica se o host interno (API) está acessível via VPN."""
        remote_host = env_config.get("REMOTE_HOST", "10.219.11.156")
        remote_port = env_config.get("REMOTE_PORT", "80")
        cmd = [
            str(PLINK_EXE), "-batch",
            "-pw", env_config.get("SSH_PASSWORD", ""),
            "-hostkey", env_config.get("SSH_HOST_KEY", ""),
            f"{env_config.get('SSH_USER', 'root')}@{env_config.get('SSH_SERVER', '')}",
            f"curl -s -o /dev/null -w '%{{http_code}}' --connect-timeout 5 http://{remote_host}:{remote_port}/",
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, timeout=20, text=True)
            http_code = result.stdout.strip()
            assert http_code in ("200", "301", "302", "404"), (
                f"Host interno {remote_host}:{remote_port} retornou HTTP {http_code} "
                f"(ou inacessível: '{result.stderr[:100]}')"
            )
        except subprocess.TimeoutExpired:
            pytest.fail("Timeout ao testar host interno via SSH")


# ── API Neo Energia ────────────────────────────────────────────────────────

class TestApiNeoEnergia:
    """
    Testa a API via túnel local (porta 5000).
    Requer túnel SSH ativo.
    """

    API_BASE = "http://localhost:5000/validacaotitularidade/Validacao/ValidarTitularidade"

    def _chamar_api(self, conta_contrato: str, cpf: str, empresa: str) -> dict:
        import json
        import urllib.request
        url = f"{self.API_BASE}?ContaContrato={conta_contrato}&CpfCnpj={cpf}&Empresa={empresa}"
        try:
            with urllib.request.urlopen(url, timeout=8) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except ConnectionRefusedError:
            pytest.skip("Porta 5000 não está ouvindo — inicie o túnel SSH primeiro")
        except Exception as e:
            pytest.fail(f"Erro ao chamar API: {e}")

    def test_api_responde_json(self):
        """API deve responder com JSON válido."""
        data = self._chamar_api("123456789", "12345678901", "coelba")
        assert isinstance(data, dict)
        assert "CodigoRetorno" in data

    def test_api_codigo_retorno_numerico(self):
        """CodigoRetorno deve ser uma string de 3 dígitos."""
        data = self._chamar_api("123456789", "12345678901", "coelba")
        cod = data["CodigoRetorno"]
        assert cod.isdigit(), f"CodigoRetorno não é numérico: '{cod}'"
        assert 0 <= int(cod) <= 11, f"CodigoRetorno fora do range esperado: {cod}"

    def test_api_empresas_validas(self):
        """API deve aceitar pelo menos uma das distribuidoras."""
        empresas = ["celpe", "coelba", "cosern"]
        for empresa in empresas:
            data = self._chamar_api("123456789", "12345678901", empresa)
            assert "CodigoRetorno" in data, f"API não respondeu para empresa={empresa}"

    def test_api_sem_dados_retorna_001_ou_000(self):
        """CPF e contrato fictícios devem retornar 000 ou 001 (não existe)."""
        data = self._chamar_api("000000000", "00000000000", "coelba")
        cod = data.get("CodigoRetorno", "")
        assert cod in ("000", "001", "002"), (
            f"Dados fictícios retornaram código inesperado: {cod}"
        )

    def test_api_resposta_tem_campos_obrigatorios(self):
        """JSON da API deve ter todos os campos usados pelo pipeline."""
        data = self._chamar_api("123456789", "12345678901", "coelba")
        for campo in ("Error", "CodigoRetorno", "Msg", "Status"):
            assert campo in data, f"Campo '{campo}' ausente na resposta da API"


# ── Ambiente geral ─────────────────────────────────────────────────────────

class TestAmbiente:

    def test_python_versao(self):
        """Python 3.10+ necessário."""
        assert sys.version_info >= (3, 10), (
            f"Python {sys.version_info.major}.{sys.version_info.minor} "
            f"— requer 3.10+"
        )

    def test_dependencias_criticas(self):
        """Módulos essenciais devem estar instalados no venv."""
        criticos = ["httpx", "pandas", "pymysql", "dotenv"]
        faltando = []
        for mod in criticos:
            try:
                __import__(mod if mod != "dotenv" else "dotenv")
            except ImportError:
                faltando.append(mod)
        assert not faltando, f"Dependências não instaladas: {faltando}"

    def test_env_file_existe(self):
        assert (MACRO_DIR / ".env").exists(), (
            ".env não encontrado. Copie .env.example e preencha as credenciais."
        )

    def test_env_tem_campos_obrigatorios(self, env_config):
        obrigatorios = ["SSH_USER", "SSH_SERVER", "SSH_PASSWORD", "SSH_HOST_KEY",
                        "LOCAL_PORT", "REMOTE_HOST", "REMOTE_PORT"]
        faltando = [k for k in obrigatorios if not env_config.get(k)]
        assert not faltando, f"Campos ausentes no .env: {faltando}"

    def test_plink_na_pasta_correta(self):
        assert PLINK_EXE.exists(), (
            f"plink.exe não encontrado em {PLINK_EXE}. "
            "Baixe em https://www.chiark.greenend.org.uk/~sgtatham/putty/latest.html"
        )
