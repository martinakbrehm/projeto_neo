#!/usr/bin/env python3
"""
Teste de Conectividade da API Neo Energia
Verifica se a API está respondendo corretamente via túnel SSH
"""

import requests
import time
import sys

def testar_api():
    """Testa se a API está funcionando"""
    
    # URLs de teste
    urls_teste = [
        "http://localhost:5000/validacaotitularidade/Validacao/ValidarTitularidade?ContaContrato=7081339311&CpfCnpj=1743511&Empresa=coelba",
        "http://localhost:5000",  # Teste básico
    ]
    
    print("🔍 TESTANDO CONECTIVIDADE DA API...")
    print("=" * 50)
    
    for i, url in enumerate(urls_teste, 1):
        print(f"\n{i}. Testando: {url}")
        print("-" * 30)
        
        try:
            start_time = time.time()
            response = requests.get(url, timeout=30)
            end_time = time.time()
            
            print(f"✅ Status: {response.status_code}")
            print(f"⏱️  Tempo: {end_time - start_time:.2f}s")
            print(f"📦 Tamanho: {len(response.text)} bytes")
            
            if len(response.text) < 200:
                print(f"📄 Resposta: {response.text}")
            else:
                print(f"📄 Resposta: {response.text[:200]}...")
                
        except requests.exceptions.Timeout:
            print("❌ TIMEOUT: API não respondeu em 30 segundos")
        except requests.exceptions.ConnectionError:
            print("❌ CONEXÃO: Não foi possível conectar (túnel SSH inativo?)")
        except Exception as e:
            print(f"❌ ERRO: {e}")
    
    print("\n" + "=" * 50)
    print("🔍 TESTE DE CONECTIVIDADE CONCLUÍDO")

def verificar_tunel():
    """Verifica se o túnel SSH está ativo"""
    import subprocess
    
    print("🔗 VERIFICANDO TÚNEL SSH...")
    print("-" * 30)
    
    try:
        result = subprocess.run(
            ["netstat", "-an"], 
            capture_output=True, 
            text=True, 
            timeout=10
        )
        
        if ":5000" in result.stdout:
            print("✅ Túnel SSH ativo na porta 5000")
            return True
        else:
            print("❌ Túnel SSH não encontrado na porta 5000")
            print("💡 Execute: python executar_automatico.py")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao verificar túnel: {e}")
        return False

if __name__ == "__main__":
    print("🚀 DIAGNÓSTICO DE API - NEO ENERGIA")
    print("=" * 50)
    
    # 1. Verifica túnel
    if not verificar_tunel():
        print("\n❌ PROBLEMA: Túnel SSH não está ativo!")
        print("🔧 SOLUÇÃO: Execute primeiro o executar_automatico.py")
        sys.exit(1)
    
    # 2. Testa API
    testar_api()
    
    print("\n💡 DICAS:")
    print("- Se API der timeout, ela pode estar sobrecarregada")
    print("- Se conexão falhar, verifique o túnel SSH")
    print("- Timeouts são normais em APIs lentas")