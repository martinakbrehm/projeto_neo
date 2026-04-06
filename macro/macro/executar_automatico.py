#!/usr/bin/env python3
"""
Neo Energia - Orquestrador Automático  (executar_automatico.py)
===============================================================
Coordenar todos os passos do ciclo de consulta automatizado:

  PASSO 1  [ETL]    etl/load/macro/03_buscar_lote_macro.py
                    → Busca lote priorizado do banco (fornecedor2 > contatus,
                      pendente > reprocessar), exporta macro/dados/lote_pendente.csv

  PASSO 2  [MACRO]  macro/macro/consulta_contrato.py --arquivo --saida
                    → SSH + túnil + chamadas à API Neo Energia
                    → Salva macro/dados/resultado_lote.csv

  PASSO 3  [ETL]    etl/load/macro/04_processar_retorno_macro.py
                    → Interpreta respostas, atualiza tabela_macros no banco
                    → Arquiva os arquivos de lote

Transformation utilizada internamente pelo passo 3:
  etl/transformation/macro/interpretar_resposta.py

Uso:
  EXECUTAR.bat               (modo normal)
  python executar_automatico.py
  python executar_automatico.py --tamanho 1000
  python executar_automatico.py --dry-run          (apenas passo 1 sem gravar)
"""

import subprocess
import time
import os
import sys
import signal
from pathlib import Path
from dotenv import load_dotenv

# Carrega variáveis do arquivo .env
load_dotenv()

# Configurações
SSH_USER    = os.getenv("SSH_USER", "root")
SSH_SERVER  = os.getenv("SSH_SERVER")
SSH_PASSWORD= os.getenv("SSH_PASSWORD")
LOCAL_PORT  = int(os.getenv("LOCAL_PORT", 5000))
REMOTE_HOST = os.getenv("REMOTE_HOST")
REMOTE_PORT = int(os.getenv("REMOTE_PORT", 80))

# Caminhos
SCRIPT_DIR   = Path(__file__).parent
# Caminhos — detecta SO para usar o executável correto do venv
import platform as _platform
if _platform.system() == "Windows":
    PYTHON_EXE   = SCRIPT_DIR / ".venv" / "Scripts" / "python.exe"
else:
    PYTHON_EXE   = SCRIPT_DIR / ".venv" / "bin" / "python"
PYTHON_SCRIPT= SCRIPT_DIR / "consulta_contrato.py"

# Raíz do projeto (3 níveis acima de macro/macro/)
PROJETO_DIR  = SCRIPT_DIR.parents[1]

# Arquivos de lote (interface entre ETL e macro)
LOTE_CSV     = SCRIPT_DIR.parent / "dados" / "lote_pendente.csv"
RESULTADO_CSV= SCRIPT_DIR.parent / "dados" / "resultado_lote.csv"

# Scripts ETL
ETL_BUSCAR   = PROJETO_DIR / "etl" / "load" / "macro" / "03_buscar_lote_macro.py"
ETL_RETORNO  = PROJETO_DIR / "etl" / "load" / "macro" / "04_processar_retorno_macro.py"

if not all([SSH_SERVER, SSH_PASSWORD, REMOTE_HOST]):
    print("❌ Erro: Variáveis de ambiente não encontradas no arquivo .env")
    print("Certifique-se de que o arquivo .env existe e contém:")
    print("SSH_SERVER, SSH_PASSWORD, REMOTE_HOST")
    sys.exit(1)

IS_WINDOWS = _platform.system() == "Windows"


def kill_existing_ssh():
    """Encerra processos SSH/túnel existentes (Windows e Linux)."""
    killed_any = False

    if IS_WINDOWS:
        for proc in ["ssh.exe", "plink.exe"]:
            try:
                r = subprocess.run(["taskkill", "/IM", proc, "/F"],
                                   capture_output=True, text=True)
                if r.returncode == 0:
                    print(f"✓ Processo {proc} finalizado")
                    killed_any = True
            except Exception as e:
                print(f"⚠️  Erro ao finalizar {proc}: {e}")
        # Libera a porta pelo PID (netstat)
        try:
            r = subprocess.run(["netstat", "-ano"], capture_output=True, text=True)
            for line in r.stdout.split("\n"):
                if f":{LOCAL_PORT}" in line and "LISTENING" in line:
                    parts = line.split()
                    if len(parts) >= 5:
                        try:
                            subprocess.run(["taskkill", "/PID", parts[-1], "/F"],
                                           capture_output=True, text=True)
                            print(f"✓ Porta {LOCAL_PORT} liberada (PID {parts[-1]})")
                            killed_any = True
                        except Exception:
                            pass
        except Exception:
            pass
    else:
        # Linux: mata ssh e sshpass que estejam usando a porta do túnel
        try:
            r = subprocess.run(["pkill", "-f", f"ssh.*{LOCAL_PORT}"],
                               capture_output=True, text=True)
            if r.returncode == 0:
                print(f"✓ Processo ssh túnel (porta {LOCAL_PORT}) finalizado")
                killed_any = True
        except Exception as e:
            print(f"⚠️  Erro ao matar ssh: {e}")
        # Fallback: lsof para liberar a porta
        try:
            r = subprocess.run(["lsof", "-t", f"-i:{LOCAL_PORT}"],
                               capture_output=True, text=True)
            for pid in r.stdout.strip().split():
                subprocess.run(["kill", pid], capture_output=True)
                print(f"✓ PID {pid} na porta {LOCAL_PORT} finalizado")
                killed_any = True
        except Exception:
            pass

    if killed_any:
        print("✓ Conexões anteriores limpas")
        time.sleep(2)
    else:
        print("ℹ️  Nenhuma conexão anterior encontrada")

def check_tunnel_working():
    """Verifica se o túnel está funcionando"""
    try:
        # Verifica se a porta está sendo usada
        result = subprocess.run(["netstat", "-an"], capture_output=True, text=True)
        port_in_use = f":{LOCAL_PORT}" in result.stdout
        
        if port_in_use:
            print(f"✓ Porta {LOCAL_PORT} está sendo usada")
            
            # Teste adicional: tenta conectar na porta local
            import socket
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(3)
                result = sock.connect_ex(('127.0.0.1', LOCAL_PORT))
                sock.close()
                
                if result == 0:
                    print(f"✓ Conexão local na porta {LOCAL_PORT} funcionando")
                    return True
                else:
                    print(f"⚠️  Porta {LOCAL_PORT} aberta mas não aceitando conexões")
                    return False
            except Exception as e:
                print(f"⚠️  Erro ao testar conexão local: {e}")
                return port_in_use
        else:
            print(f"❌ Porta {LOCAL_PORT} não está sendo usada")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao verificar túnel: {e}")
        return False

def _ssh_cmd_remoto(comando: str) -> list:
    """Retorna o comando para executar remotamente via SSH (Windows=plink, Linux=sshpass+ssh)."""
    if IS_WINDOWS:
        return ["plink", "-batch", "-pw", SSH_PASSWORD,
                f"{SSH_USER}@{SSH_SERVER}", comando]
    else:
        return ["sshpass", "-p", SSH_PASSWORD,
                "ssh", "-o", "StrictHostKeyChecking=no",
                f"{SSH_USER}@{SSH_SERVER}", comando]


def verificar_ativar_vpn():
    """Verifica e ativa VPN no servidor SSH (Windows e Linux)."""
    print("🔍 1. Verificando status da VPN no servidor...")

    try:
        r = subprocess.run(_ssh_cmd_remoto('ipsec status | grep "vpn"'),
                           capture_output=True, text=True, timeout=10)
        if r.returncode == 0 and "vpn" in r.stdout:
            print("✅ VPN já está ativa")
            return True
    except Exception:
        pass

    print("⚠️ VPN não está ativa, tentando conectar...")
    try:
        r = subprocess.run(_ssh_cmd_remoto("ipsec up vpn"),
                           capture_output=True, text=True, timeout=15)
        if r.returncode == 0:
            print("✅ VPN conectada com sucesso")
            print(f"📋 Saída: {r.stdout.strip()}")
            time.sleep(4)
            return True
        else:
            print("⚠️ VPN falhou, continuando sem VPN...")
            print(f"📋 Erro: {r.stderr}")
            return True  # Continua mesmo se VPN falhar
    except Exception as e:
        print(f"⚠️ Erro ao ativar VPN: {e}")
        return True  # Continua mesmo se VPN falhar


def create_ssh_tunnel():
    """Cria túnel SSH local → remoto (Windows usa plink, Linux usa sshpass+ssh)."""
    print(f"🔗 Criando túnel SSH: localhost:{LOCAL_PORT} → {REMOTE_HOST}:{REMOTE_PORT}")

    if IS_WINDOWS:
        cmd = [
            "plink", "-batch", "-pw", SSH_PASSWORD,
            "-L", f"{LOCAL_PORT}:{REMOTE_HOST}:{REMOTE_PORT}",
            f"{SSH_USER}@{SSH_SERVER}", "-N"
        ]
        kwargs = {"creationflags": subprocess.CREATE_NO_WINDOW}
    else:
        cmd = [
            "sshpass", "-p", SSH_PASSWORD,
            "ssh", "-N",
            "-o", "StrictHostKeyChecking=no",
            "-o", "ServerAliveInterval=30",
            "-L", f"{LOCAL_PORT}:{REMOTE_HOST}:{REMOTE_PORT}",
            f"{SSH_USER}@{SSH_SERVER}"
        ]
        kwargs = {}

    print(f"🔧 Comando: {' '.join(cmd[:6])} ... (senha ocultada)")

    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            **kwargs
        )
        print(f"🔄 Processo SSH iniciado (PID: {process.pid})")
        print("⏳ Aguardando túnel estabelecer...")

        for i in range(10):
            time.sleep(1)
            if process.poll() is not None:
                stdout, stderr = process.communicate()
                print(f"❌ Processo plink terminou inesperadamente")
                print(f"📤 STDOUT: {stdout.decode('utf-8', errors='ignore')}")
                print(f"📥 STDERR: {stderr.decode('utf-8', errors='ignore')}")
                return None
            
            # Verifica se o túnel já está funcionando
            if check_tunnel_working():
                print("✅ Túnel SSH criado com sucesso!")
                return process
                
            print(f"⏳ Tentativa {i+1}/10...")
        
        # Se chegou aqui, o túnel não foi estabelecido
        print("❌ Falha ao criar túnel SSH - timeout")
        
        # Captura logs do processo
        try:
            stdout, stderr = process.communicate(timeout=2)
            print(f"📤 STDOUT: {stdout.decode('utf-8', errors='ignore')}")
            print(f"📥 STDERR: {stderr.decode('utf-8', errors='ignore')}")
        except subprocess.TimeoutExpired:
            print("⚠️  Processo não respondeu para captura de logs")
        
        process.terminate()
        return None
            
    except FileNotFoundError:
        print("❌ PuTTY/plink não encontrado. Tentando método alternativo...")
        return create_ssh_tunnel_sshpass()
    except Exception as e:
        print(f"❌ Erro ao criar túnel: {e}")
        return None

def create_ssh_tunnel_sshpass():
    """Método alternativo usando sshpass ou entrada manual"""
    print("⚠️  Método automático falhou")
    print("📝 Instruções manuais:")
    print(f"1. Abra um novo terminal")
    print(f"2. Execute: ssh -L {LOCAL_PORT}:{REMOTE_HOST}:{REMOTE_PORT} {SSH_USER}@{SSH_SERVER} -N")
    print(f"3. Digite a senha: {SSH_PASSWORD}")
    print(f"4. Pressione ENTER aqui quando o túnel estiver ativo...")
    
    input("Pressione ENTER quando o túnel SSH estiver funcionando...")
    
    if check_tunnel_working():
        print("✅ Túnel confirmado!")
        return True
    else:
        print("❌ Túnel não detectado")
        return None

def testar_api():
    """Testa a API antes de executar o script principal"""
    print("\n🔍 3. Testando conectividade da API...")
    
    # URL de teste com parâmetros reais do script antigo
    url_teste = f"http://localhost:{LOCAL_PORT}/validacaotitularidade/Validacao/ValidarTitularidade?ContaContrato=7081339311&CpfCnpj=1743511&Empresa=coelba"
    
    try:
        # Usa curl se disponível
        result = subprocess.run([
            "curl", "-s", "--max-time", "10", url_teste
        ], capture_output=True, text=True, timeout=15)
        
        if result.returncode == 0:
            print("✅ API respondendo via túnel")
            resposta = result.stdout.strip()
            if len(resposta) > 100:
                print(f"📋 Resposta: {resposta[:100]}...")
            else:
                print(f"📋 Resposta: {resposta}")
            return True
        else:
            print("⚠️ API não responde rapidamente (normal se estiver lenta)")
            return testar_api_python(url_teste)
            
    except FileNotFoundError:
        print("⚠️ curl não encontrado, testando com Python...")
        return testar_api_python(url_teste)
    except Exception as e:
        print(f"⚠️ Erro no teste da API: {e}")
        return testar_api_python(url_teste)

def testar_api_python(url):
    """Teste da API usando Python httpx"""
    try:
        import httpx
        with httpx.Client(timeout=15.0) as client:
            response = client.get(url)
            print("✅ API respondendo via túnel (teste Python)")
            print(f"📋 Status: {response.status_code}")
            if len(response.text) > 100:
                print(f"📋 Resposta: {response.text[:100]}...")
            else:
                print(f"📋 Resposta: {response.text}")
            return True
    except Exception as e:
        print(f"❌ API não respondeu: {e}")
        return False

def run_python_script():
    """Executa a macro (passo 2) com os argumentos de entrada/saída automáticos.
    Os caminhos são passados via --arquivo e --saida para bypassar o dialog.
    """
    try:
        print("🐍 Executando macro (consulta_contrato.py)...")
        cmd = [
            str(PYTHON_EXE),
            str(PYTHON_SCRIPT),
            "--arquivo", str(LOTE_CSV),
            "--saida",   str(RESULTADO_CSV),
        ]
        print(f"   Comando: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            cwd=SCRIPT_DIR,
            capture_output=False
        )

        if result.returncode == 0:
            print("✅ Macro executada com sucesso!")
            return True
        else:
            print(f"❌ Macro falhou com código: {result.returncode}")
            return False

    except Exception as e:
        print(f"❌ Erro ao executar macro: {e}")
        return False


def run_etl_buscar_lote(tamanho: int = 2000) -> bool:
    """PASSO 1 — ETL: busca lote priorizado e exporta CSV para a macro.
    Script: etl/load/macro/03_buscar_lote_macro.py
    """
    print(f"\n🗔 [PASSO 1] Buscando lote do banco (tamanho={tamanho})...")
    if not ETL_BUSCAR.exists():
        print(f"❌ Script ETL não encontrado: {ETL_BUSCAR}")
        return False
    try:
        # Usa o python do .venv para garantir as dependências
        result = subprocess.run(
            [str(PYTHON_EXE), str(ETL_BUSCAR), "--tamanho", str(tamanho)],
            cwd=str(PROJETO_DIR),
            capture_output=False,
        )
        if result.returncode == 0:
            if LOTE_CSV.exists():
                print(f"✅ Lote exportado: {LOTE_CSV}")
                return True
            else:
                # Script concluíu sem erro mas sem CSV = lote vazio
                print("ℹ️ Lote vazio — nada a processar.")
                return False
        else:
            print(f"❌ ETL buscar lote falhou (código {result.returncode})")
            return False
    except Exception as e:
        print(f"❌ Erro no passo 1: {e}")
        return False


def run_etl_processar_retorno() -> bool:
    """PASSO 3 — ETL: lê resultado da macro, interpreta respostas, atualiza banco.
    Script: etl/load/macro/04_processar_retorno_macro.py
    """
    print("\n🗔 [PASSO 3] Processando retorno no banco...")
    if not ETL_RETORNO.exists():
        print(f"❌ Script ETL não encontrado: {ETL_RETORNO}")
        return False
    if not RESULTADO_CSV.exists():
        print(f"❌ Arquivo de resultado não encontrado: {RESULTADO_CSV}")
        print("   A macro pode ter falhado antes de gerar saída.")
        return False
    try:
        result = subprocess.run(
            [str(PYTHON_EXE), str(ETL_RETORNO)],
            cwd=str(PROJETO_DIR),
            capture_output=False,
        )
        ok = result.returncode == 0
        if ok:
            print("✅ Retorno processado com sucesso!")
        else:
            print(f"❌ ETL processar retorno falhou (código {result.returncode})")
        return ok
    except Exception as e:
        print(f"❌ Erro no passo 3: {e}")
        return False

def main():
    """Orquestrador principal: PASSO 1 (ETL) → PASSO 2 (SSH+macro) → PASSO 3 (ETL)."""
    import argparse
    parser = argparse.ArgumentParser(
        description="Neo Energia — orquestrador automático (ETL + SSH + macro + ETL)"
    )
    parser.add_argument("--tamanho", type=int, default=2000,
                        help="Tamanho do lote (padrão: 2000)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Executa apenas o passo 1 (busca) sem gravar no banco")
    args = parser.parse_args()

    print("=" * 60)
    print("🚀 NEO ENERGIA — EXECUÇÃO AUTOMÁTICA")
    print("   Prioridade: fornecedor2 → contatus  |  pendente → reprocessar")
    print("=" * 60)
    print()

    # ------------------------------------------------------------------
    # PASSO 1 — ETL: buscar lote priorizado do banco
    # ------------------------------------------------------------------
    print("\n" + "=" * 50)
    print("PASSO 1 — Buscar lote do banco")
    print("=" * 50)
    if not run_etl_buscar_lote(tamanho=args.tamanho):
        print("\nℹ️ Lote vazio ou erro no passo 1 — encerrando.")
        return 0

    if args.dry_run:
        print("\n[DRY-RUN] Encerrando após passo 1.")
        return 0

    try:
        # ------------------------------------------------------------------
        # Limpa conexões anteriores
        # ------------------------------------------------------------------
        print("\n🧹 Limpando conexões anteriores...")
        kill_existing_ssh()

        # ------------------------------------------------------------------
        # PASSO 2a — VPN + túnel SSH
        # ------------------------------------------------------------------
        if not verificar_ativar_vpn():
            print("❌ Falha ao configurar VPN")
            return 1
        time.sleep(3)

        ssh_process = create_ssh_tunnel()
        if ssh_process is None:
            return 1

        if not testar_api():
            print("⚠️ API não respondeu no teste — tentando mesmo assim...")

        # ------------------------------------------------------------------
        # PASSO 2b — Macro: consulta de titularidade via API
        # ------------------------------------------------------------------
        print("\n" + "=" * 50)
        print("PASSO 2 — Macro: consulta de titularidade")
        print("=" * 50)
        macro_ok = run_python_script()

        # ------------------------------------------------------------------
        # Limpa conexões (VPN + SSH)
        # ------------------------------------------------------------------
        print("\n🧹 Limpando conexões...")
        try:
            subprocess.run(
                _ssh_cmd_remoto("ipsec down vpn"),
                capture_output=True, timeout=5
            )
        except Exception:
            pass
        kill_existing_ssh()

        # ------------------------------------------------------------------
        # PASSO 3 — ETL: processar retorno (mesmo se macro teve erro parcial)
        # ------------------------------------------------------------------
        print("\n" + "=" * 50)
        print("PASSO 3 — Processar retorno no banco")
        print("=" * 50)
        retorno_ok = run_etl_processar_retorno()

        if macro_ok and retorno_ok:
            print("\n🎉 CICLO COMPLETO CONCLUÍDO COM SUCESSO!")
            return 0
        else:
            print("\n⚠️ Ciclo concluído com advertências. Verifique os logs.")
            return 1

    except KeyboardInterrupt:
        print("\n⚠️ Interrompido pelo usuário")
        kill_existing_ssh()
        # Tenta processar o que já foi gerado antes de sair
        if RESULTADO_CSV.exists():
            print("  Processando resultado parcial...")
            run_etl_processar_retorno()
        return 1
    except Exception as e:
        print(f"\n❌ Erro inesperado: {e}")
        kill_existing_ssh()
        return 1

if __name__ == "__main__":
    sys.exit(main())