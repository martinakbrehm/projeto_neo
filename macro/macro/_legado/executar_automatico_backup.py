#!/usr/bin/env python3
"""
Neo Energia - Script de Execução Automática
Automatiza SSH, túnel e execução do script Python sem interação manual
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
SSH_USER = os.getenv("SSH_USER", "root")
SSH_SERVER = os.getenv("SSH_SERVER")
SSH_PASSWORD = os.getenv("SSH_PASSWORD")
LOCAL_PORT = int(os.getenv("LOCAL_PORT", 5000))
REMOTE_HOST = os.getenv("REMOTE_HOST")
REMOTE_PORT = int(os.getenv("REMOTE_PORT", 80))

# Verifica se as variáveis essenciais foram carregadas
if not all([SSH_SERVER, SSH_PASSWORD, REMOTE_HOST]):
    print("❌ Erro: Variáveis de ambiente não encontradas no arquivo .env")
    print("Certifique-se de que o arquivo .env existe e contém:")
    print("SSH_SERVER, SSH_PASSWORD, REMOTE_HOST")
    sys.exit(1)


# Caminhos
SCRIPT_DIR = Path(__file__).parent
PYTHON_EXE = SCRIPT_DIR / ".venv" / "Scripts" / "python.exe"
PYTHON_SCRIPT = SCRIPT_DIR / "consulta_contrato.py"

def kill_existing_ssh():
    """Mata processos SSH existentes"""
    killed_any = False
    
    # Lista de processos para matar
    processes_to_kill = ["ssh.exe", "plink.exe"]
    
    for process_name in processes_to_kill:
        try:
            result = subprocess.run(
                ["taskkill", "/IM", process_name, "/F"], 
                capture_output=True, text=True
            )
            if result.returncode == 0:
                print(f"✓ Processo {process_name} finalizado")
                killed_any = True
        except Exception as e:
            print(f"⚠️  Erro ao finalizar {process_name}: {e}")
    
    # Verifica se alguma porta específica precisa ser liberada
    try:
        result = subprocess.run(["netstat", "-ano"], capture_output=True, text=True)
        lines = result.stdout.split('\n')
        for line in lines:
            if f":{LOCAL_PORT}" in line and "LISTENING" in line:
                # Extrai o PID da linha
                parts = line.split()
                if len(parts) >= 5:
                    pid = parts[-1]
                    try:
                        subprocess.run(["taskkill", "/PID", pid, "/F"], 
                                     capture_output=True, text=True)
                        print(f"✓ Processo usando porta {LOCAL_PORT} (PID: {pid}) finalizado")
                        killed_any = True
                    except:
                        pass
    except:
        pass
    
    if killed_any:
        print("✓ Conexões anteriores limpas")
        time.sleep(2)  # Aguarda um pouco para liberar recursos
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

def verificar_ativar_vpn():
    """Verifica e ativa VPN no servidor SSH"""
    print("🔍 1. Verificando status da VPN no servidor...")
    
    # Verifica se VPN já está ativa
    try:
        result = subprocess.run([
            "plink", "-batch", "-pw", SSH_PASSWORD,
            f"{SSH_USER}@{SSH_SERVER}",
            'ipsec status | grep "vpn"'
        ], capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0 and "vpn" in result.stdout:
            print("✅ VPN já está ativa")
            return True
    except Exception:
        pass
    
    # Ativa a VPN
    print("⚠️ VPN não está ativa, tentando conectar...")
    try:
        result = subprocess.run([
            "plink", "-batch", "-pw", SSH_PASSWORD,
            f"{SSH_USER}@{SSH_SERVER}",
            "ipsec up vpn"
        ], capture_output=True, text=True, timeout=15)
        
        if result.returncode == 0:
            print("✅ VPN conectada com sucesso")
            print(f"📋 Saída: {result.stdout.strip()}")
            time.sleep(4)  # Aguarda VPN estabilizar
            return True
        else:
            print("⚠️ VPN falhou, mas continuando com túnel SSH...")
            print(f"📋 Erro: {result.stderr}")
            print("   (O script pode funcionar mesmo sem VPN ativa)")
            return True  # Continua mesmo se VPN falhar
    except Exception as e:
        print(f"⚠️ Erro ao ativar VPN: {e}")
        return True  # Continua mesmo se VPN falhar

def create_ssh_tunnel():
    """Cria túnel SSH usando plink (PuTTY) com senha automática"""
    
    # Comando plink com senha embutida
    plink_cmd = [
        "plink",
        "-batch",
        "-pw", SSH_PASSWORD,
        "-L", f"{LOCAL_PORT}:{REMOTE_HOST}:{REMOTE_PORT}",
        f"{SSH_USER}@{SSH_SERVER}",
        "-N"
    ]
    
    try:
        print(f"🔗 Criando túnel SSH: {LOCAL_PORT} -> {REMOTE_HOST}:{REMOTE_PORT}")
        print(f"🔧 Comando: plink -batch -L {LOCAL_PORT}:{REMOTE_HOST}:{REMOTE_PORT} {SSH_USER}@{SSH_SERVER} -N")
        
        # Inicia plink em background
        process = subprocess.Popen(
            plink_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            creationflags=subprocess.CREATE_NO_WINDOW
        )
        
        print(f"🔄 Processo plink iniciado (PID: {process.pid})")
        
        # Aguarda um pouco para o túnel estabelecer
        print("⏳ Aguardando túnel estabelecer...")
        
        # Verifica o status do processo durante o estabelecimento
        for i in range(10):  # 10 tentativas de 1 segundo cada
            time.sleep(1)
            
            # Verifica se o processo ainda está rodando
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
    """Executa o script Python principal"""
    try:
        print("🐍 Executando script Python...")
        print(f"Comando: {PYTHON_EXE} {PYTHON_SCRIPT}")
        
        result = subprocess.run(
            [str(PYTHON_EXE), str(PYTHON_SCRIPT)],
            cwd=SCRIPT_DIR,
            capture_output=False
        )
        
        if result.returncode == 0:
            print("✅ Script Python executado com sucesso!")
            return True
        else:
            print(f"❌ Script Python falhou com código: {result.returncode}")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao executar script Python: {e}")
        return False

def main():
    """Função principal"""
    print("=" * 50)
    print("🚀 NEO ENERGIA - EXECUÇÃO AUTOMÁTICA")
    print("=" * 50)
    print()
    
    # Verifica dependências
    if not PYTHON_EXE.exists():
        print(f"❌ Python não encontrado em: {PYTHON_EXE}")
        return 1
        
    if not PYTHON_SCRIPT.exists():
        print(f"❌ Script não encontrado em: {PYTHON_SCRIPT}")
        return 1
    
    try:
        # 1. Limpa conexões anteriores
        print("🧹 Limpando conexões anteriores...")
        kill_existing_ssh()
        
        # 2. Verifica e ativa VPN
        if not verificar_ativar_vpn():
            print("❌ Falha ao configurar VPN")
            return 1
        
        # Pausa para VPN estabilizar
        time.sleep(3)
        
        # 3. Cria túnel SSH
        ssh_process = create_ssh_tunnel()
        if ssh_process is None:
            return 1
        
        # 4. Testa API
        if not testar_api():
            print("⚠️ API não está respondendo adequadamente")
            print("Tentando executar mesmo assim...")
        
        # 5. Executa script Python
        success = run_python_script()
        
        # 6. Limpa conexões (incluindo VPN)
        print("\n🧹 Limpando conexões...")
        try:
            subprocess.run([
                "plink", "-batch", "-pw", SSH_PASSWORD,
                f"{SSH_USER}@{SSH_SERVER}",
                "ipsec down vpn"
            ], capture_output=True, timeout=5)
        except:
            pass
        kill_existing_ssh()
        
        if success:
            print("\n🎉 EXECUÇÃO CONCLUÍDA COM SUCESSO!")
            return 0
        else:
            print("\n❌ EXECUÇÃO FALHOU")
            return 1
            
    except KeyboardInterrupt:
        print("\n⚠️  Execução interrompida pelo usuário")
        kill_existing_ssh()
        return 1
    except Exception as e:
        print(f"\n❌ Erro inesperado: {e}")
        kill_existing_ssh()
        return 1

if __name__ == "__main__":
    sys.exit(main())