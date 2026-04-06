"""
Launcher do dashboard de aproveitamento das macros.
Os dados são carregados diretamente do banco de dados — nenhuma pasta
de arquivos precisa ser selecionada.

Usage: double-click this file or run `python run_dashboard_launcher.py`.
"""
import os
import sys
import subprocess
import time
import urllib.request
import webbrowser

ROOT = os.path.abspath(os.path.dirname(__file__))
DASHBOARD_PY = os.path.join(ROOT, 'dashboard.py')


if __name__ == '__main__':
    cmd = [sys.executable, DASHBOARD_PY]
    try:
        subprocess.Popen(cmd)
        print('Dashboard iniciado. Aguardando o servidor responder (abrindo no navegador automaticamente)...')
        url = 'http://127.0.0.1:8050'
        for _ in range(30):  # ~15 s (30 × 0.5 s)
            try:
                with urllib.request.urlopen(url, timeout=1):
                    break
            except Exception:
                time.sleep(0.5)
        try:
            webbrowser.open(url)
            print(f'Abrindo {url} no navegador padrão.')
        except Exception:
            print(f'Servidor iniciado, mas não foi possível abrir o navegador automaticamente. Acesse: {url}')
    except Exception as e:
        print('Falha ao iniciar o dashboard:', e)
        sys.exit(1)
