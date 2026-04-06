#!/bin/bash
# setup_venv.sh — Configura o ambiente virtual no Linux/Mac

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "============================================="
echo "  NEO ENERGIA - CONFIGURACAO DO AMBIENTE"
echo "============================================="
echo

# Verifica Python 3.12+
if ! command -v python3 &>/dev/null; then
    echo "[ERRO] python3 não encontrado. Instale Python 3.12+."
    exit 1
fi

echo "[1/4] Verificando Python..."
python3 --version

# Remove venv antigo
if [ -d ".venv" ]; then
    echo "[2/4] Removendo ambiente virtual antigo..."
    rm -rf .venv
fi

# Cria venv
echo "[2/4] Criando ambiente virtual..."
python3 -m venv .venv

# Atualiza pip
echo "[3/4] Atualizando pip..."
.venv/bin/python -m pip install --upgrade pip --quiet

# Instala dependências
echo "[4/4] Instalando dependências..."
.venv/bin/pip install -r requirements.txt

# sshpass (necessário para túnel SSH no Linux)
if ! command -v sshpass &>/dev/null; then
    echo
    echo "[AVISO] sshpass não encontrado. Instale com:"
    echo "  Ubuntu/Debian: sudo apt install sshpass"
    echo "  CentOS/RHEL:   sudo yum install sshpass"
fi

echo
echo "============================================="
echo "  AMBIENTE CONFIGURADO COM SUCESSO!"
echo "============================================="
echo
echo "Próximo passo: configure o arquivo .env"
echo "  cp .env.example .env && nano .env"
echo "  Preencha: SSH_USER, SSH_SERVER, SSH_PASSWORD, REMOTE_HOST"
echo
echo "Para executar:"
echo "  python executar_automatico.py"
echo "  python executar_automatico.py --tamanho 1000"
echo
