"""
setup_dashboard.py
==================
Instalador interativo do Dashboard de Macros.

Executa na ordem:
  1. Verifica versão do Python
  2. Instala dependências Python (requirements.txt do dashboard)
  3. Coleta credenciais do banco e escreve/atualiza config.py
  4. Testa a conexão com o banco
  5. Configura a senha do dashboard (basic auth)
  6. Baixa o cloudflared.exe (se não encontrado)
  7. Configura o túnel Cloudflare (token salvo, ou quick-tunnel)
  8. Gera scripts de inicialização prontos para uso

Execute: python setup_dashboard.py
  ou:    double-click em setup_dashboard.bat
"""

import sys
import os
import subprocess
import shutil
import textwrap
import re
import json
import urllib.request
import zipfile
import io
import base64

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
ROOT            = os.path.dirname(os.path.abspath(__file__))
CONFIG_PY       = os.path.join(ROOT, "config.py")
CONFIG_EXAMPLE  = os.path.join(ROOT, "config.example.py")
CLOUDFLARED_EXE = os.path.join(ROOT, "cloudflared.exe")
TUNNEL_CFG_FILE = os.path.join(ROOT, "cloudflare_tunnel.json")
STARTUP_DIR     = os.path.join(ROOT, "startup_scripts")
REQUIREMENTS    = os.path.join(ROOT, "dashboard_macros", "requirements.txt")

CLOUDFLARED_DOWNLOAD_URL = (
    "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-windows-amd64.exe"
)

# ---------------------------------------------------------------------------
# Helpers de saída colorida (funciona no Windows Terminal / cmd moderno)
# ---------------------------------------------------------------------------
BOLD  = "\033[1m"
GREEN = "\033[92m"
CYAN  = "\033[96m"
YELLOW= "\033[93m"
RED   = "\033[91m"
RESET = "\033[0m"

def title(msg):
    print(f"\n{BOLD}{CYAN}{'='*60}{RESET}")
    print(f"{BOLD}{CYAN}  {msg}{RESET}")
    print(f"{BOLD}{CYAN}{'='*60}{RESET}")

def ok(msg):
    print(f"  {GREEN}✔  {msg}{RESET}")

def warn(msg):
    print(f"  {YELLOW}⚠  {msg}{RESET}")

def err(msg):
    print(f"  {RED}✘  {msg}{RESET}")

def ask(prompt, default=None):
    if default is not None:
        full_prompt = f"  {BOLD}{prompt}{RESET} [{default}]: "
    else:
        full_prompt = f"  {BOLD}{prompt}{RESET}: "
    value = input(full_prompt).strip()
    return value if value else default

def ask_secret(prompt):
    import getpass
    return getpass.getpass(f"  {BOLD}{prompt}{RESET}: ").strip()

def confirm(prompt, default=True):
    hint = "S/n" if default else "s/N"
    ans = ask(f"{prompt} ({hint})", "").lower()
    if ans == "":
        return default
    return ans in ("s", "sim", "y", "yes")

# ---------------------------------------------------------------------------
# ETAPA 1 – Verificar Python
# ---------------------------------------------------------------------------
def check_python():
    title("ETAPA 1 · Verificando Python")
    v = sys.version_info
    if v < (3, 8):
        err(f"Python {v.major}.{v.minor} encontrado. Mínimo necessário: 3.8")
        sys.exit(1)
    ok(f"Python {v.major}.{v.minor}.{v.micro}  ({sys.executable})")

# ---------------------------------------------------------------------------
# ETAPA 2 – Instalar dependências
# ---------------------------------------------------------------------------
def install_dependencies():
    title("ETAPA 2 · Instalando dependências Python")
    if not os.path.exists(REQUIREMENTS):
        warn(f"requirements.txt não encontrado em {REQUIREMENTS}")
        warn("Instalando dependências base manualmente…")
        pkgs = ["dash>=2.10", "plotly>=5.15", "pandas>=1.5", "pymysql", "flask>=2.0"]
    else:
        with open(REQUIREMENTS) as f:
            pkgs_raw = [l.strip() for l in f if l.strip() and not l.startswith("#")]
        pkgs = pkgs_raw
        ok(f"requirements.txt lido: {len(pkgs)} pacote(s)")

    print()
    result = subprocess.run(
        [sys.executable, "-m", "pip", "install", "--upgrade"] + pkgs,
        capture_output=True, text=True
    )
    if result.returncode != 0:
        err("Falha ao instalar dependências:")
        print(result.stderr[-2000:])
        sys.exit(1)
    ok("Todas as dependências instaladas com sucesso.")

# ---------------------------------------------------------------------------
# ETAPA 3 – Coletar credenciais e escrever config.py
# ---------------------------------------------------------------------------
def _read_existing_config():
    """Tenta ler valores atuais do config.py para usar como defaults."""
    defaults = {}
    if not os.path.exists(CONFIG_PY):
        return defaults
    with open(CONFIG_PY, encoding="utf-8") as f:
        content = f.read()
    patterns = {
        "DB_DESTINO_HOST":     r'DB_DESTINO_HOST\s*=\s*["\'](.+?)["\']',
        "DB_DESTINO_PORT":     r'DB_DESTINO_PORT\s*=\s*(\d+)',
        "DB_DESTINO_USER":     r'DB_DESTINO_USER\s*=\s*["\'](.+?)["\']',
        "DB_DESTINO_DATABASE": r'DB_DESTINO_DATABASE\s*=\s*["\'](.+?)["\']',
    }
    for key, pat in patterns.items():
        m = re.search(pat, content)
        if m:
            defaults[key] = m.group(1)
    return defaults

def configure_database():
    title("ETAPA 3 · Configuração do banco de dados")
    print("  Informe as credenciais do banco MySQL/RDS que o dashboard vai usar.\n")

    d = _read_existing_config()

    host = ask("Host do banco",     d.get("DB_DESTINO_HOST", ""))
    port = ask("Porta",              d.get("DB_DESTINO_PORT", "3306"))
    user = ask("Usuário",            d.get("DB_DESTINO_USER", ""))
    password = ask_secret("Senha   (oculta)")
    database = ask("Database",       d.get("DB_DESTINO_DATABASE", "bd_Automacoes_time_dadosV2"))

    return {
        "host": host,
        "port": int(port),
        "user": user,
        "password": password,
        "database": database,
    }

def write_config(db):
    title("Escrevendo config.py")
    # Lê config.example.py como template
    if os.path.exists(CONFIG_EXAMPLE):
        with open(CONFIG_EXAMPLE, encoding="utf-8") as f:
            template = f.read()
    else:
        template = _CONFIG_TEMPLATE

    content = template
    replacements = {
        r'DB_DESTINO_HOST\s*=\s*["\'].+?["\']':     f'DB_DESTINO_HOST     = "{db["host"]}"',
        r'DB_DESTINO_PORT\s*=\s*\d+':                f'DB_DESTINO_PORT     = {db["port"]}',
        r'DB_DESTINO_USER\s*=\s*["\'].+?["\']':     f'DB_DESTINO_USER     = "{db["user"]}"',
        r'DB_DESTINO_PASSWORD\s*=\s*["\'].+?["\']': f'DB_DESTINO_PASSWORD = "{db["password"]}"',
        r'DB_DESTINO_DATABASE\s*=\s*["\'].+?["\']': f'DB_DESTINO_DATABASE = "{db["database"]}"',
    }
    for pat, repl in replacements.items():
        content = re.sub(pat, repl, content)

    with open(CONFIG_PY, "w", encoding="utf-8") as f:
        f.write(content)
    ok(f"config.py salvo em {CONFIG_PY}")

# Fallback caso config.example.py não exista
_CONFIG_TEMPLATE = textwrap.dedent("""\
    DB_DESTINO_HOST     = "HOST"
    DB_DESTINO_PORT     = 3306
    DB_DESTINO_USER     = "USER"
    DB_DESTINO_PASSWORD = "PASSWORD"
    DB_DESTINO_DATABASE = "DATABASE"
    DB_ORIGEM_HOST     = ""
    DB_ORIGEM_PORT     = 3306
    DB_ORIGEM_USER     = ""
    DB_ORIGEM_PASSWORD = ""
    DB_ORIGEM_DATABASE = ""

    def db_destino(**kwargs) -> dict:
        return dict(host=DB_DESTINO_HOST, port=DB_DESTINO_PORT, user=DB_DESTINO_USER,
                    password=DB_DESTINO_PASSWORD, database=DB_DESTINO_DATABASE,
                    charset="utf8mb4", **kwargs)

    def db_origem(**kwargs) -> dict:
        return dict(host=DB_ORIGEM_HOST, port=DB_ORIGEM_PORT, user=DB_ORIGEM_USER,
                    password=DB_ORIGEM_PASSWORD, database=DB_ORIGEM_DATABASE,
                    charset="utf8mb4", **kwargs)
""")

# ---------------------------------------------------------------------------
# ETAPA 4 – Testar conexão com o banco
# ---------------------------------------------------------------------------
def test_database(db):
    title("ETAPA 4 · Testando conexão com o banco")
    try:
        import pymysql
        conn = pymysql.connect(
            host=db["host"], port=db["port"],
            user=db["user"], password=db["password"],
            database=db["database"], charset="utf8mb4",
            connect_timeout=10,
        )
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM tabela_macros WHERE status != 'pendente'")
        count = cur.fetchone()[0]
        conn.close()
        ok(f"Conexão bem-sucedida.  Registros processados em tabela_macros: {count:,}")
        return True
    except Exception as e:
        err(f"Falha na conexão: {e}")
        if not confirm("Continuar mesmo assim?", default=False):
            sys.exit(1)
        return False

# ---------------------------------------------------------------------------
# ETAPA 5 – Configurar credenciais do dashboard (basic auth)
# ---------------------------------------------------------------------------
def configure_dashboard_auth():
    title("ETAPA 5 · Autenticação do Dashboard")
    print("  O dashboard usa HTTP Basic Auth (usuário/senha no navegador).\n")
    print("  Credenciais padrão:  neo / dashboard2026\n")

    if not confirm("Alterar as credenciais do dashboard?", default=False):
        ok("Mantendo credenciais padrão (neo / dashboard2026)")
        return

    username = ask("Novo usuário", "neo")
    password = ask_secret("Nova senha   (oculta)")

    # Atualiza dashboard.py
    dashboard_file = os.path.join(ROOT, "dashboard_macros", "dashboard.py")
    if os.path.exists(dashboard_file):
        with open(dashboard_file, encoding="utf-8") as f:
            content = f.read()
        token = base64.b64encode(f"{username}:{password}".encode()).decode()
        content = re.sub(
            r"VALID_CREDENTIALS\s*=\s*base64\.b64encode\(b'[^']+'\)\.decode\('\S+'\)",
            f"VALID_CREDENTIALS = base64.b64encode(b'{username}:{password}').decode('utf-8')",
            content,
        )
        with open(dashboard_file, "w", encoding="utf-8") as f:
            f.write(content)
        ok(f"Credenciais atualizadas: {username} / {'*' * len(password)}")
    else:
        warn("dashboard.py não encontrado — credenciais não foram alteradas.")

# ---------------------------------------------------------------------------
# ETAPA 6 – Baixar cloudflared.exe
# ---------------------------------------------------------------------------
def download_cloudflared():
    title("ETAPA 6 · Cloudflared")

    if shutil.which("cloudflared") or os.path.exists(CLOUDFLARED_EXE):
        path = shutil.which("cloudflared") or CLOUDFLARED_EXE
        ok(f"cloudflared já presente: {path}")
        return path

    print(f"  cloudflared não encontrado. Baixando de:\n  {CLOUDFLARED_DOWNLOAD_URL}\n")
    if not confirm("Baixar agora?", default=True):
        warn("cloudflared não será configurado. O túnel Cloudflare não estará disponível.")
        return None

    try:
        print("  Aguarde…", end="", flush=True)
        urllib.request.urlretrieve(CLOUDFLARED_DOWNLOAD_URL, CLOUDFLARED_EXE)
        print()
        ok(f"cloudflared.exe salvo em {CLOUDFLARED_EXE}")
        return CLOUDFLARED_EXE
    except Exception as e:
        print()
        err(f"Falha no download: {e}")
        warn("Baixe manualmente: https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/")
        return None

# ---------------------------------------------------------------------------
# ETAPA 7 – Configurar túnel Cloudflare
# ---------------------------------------------------------------------------
def configure_cloudflare_tunnel(cloudflared_path):
    title("ETAPA 7 · Configuração do Túnel Cloudflare")

    if cloudflared_path is None:
        warn("cloudflared não disponível — etapa ignorada.")
        return None, "quick"

    print(textwrap.dedent("""\
      Opções de túnel:

        [1] Token de túnel  (recomendado para produção)
            Você já criou um túnel no painel Cloudflare Zero Trust e tem o token.
            O token é estável e o hostname personalizado (ex: dashboard.empresa.com.br).

        [2] Quick tunnel  (sem conta necessária)
            URL temporária gerada na hora (ex: https://abc123.trycloudflare.com).
            Muda a cada reinicialização. Útil para testes rápidos.

        [3] Pular / configurar depois
    """))

    mode = ask("Escolha", "1")

    if mode == "1":
        # Verificar se já há token salvo
        saved_token = ""
        if os.path.exists(TUNNEL_CFG_FILE):
            try:
                with open(TUNNEL_CFG_FILE) as f:
                    saved = json.load(f)
                saved_token = saved.get("token", "")
            except Exception:
                pass

        if saved_token:
            print(f"\n  Token já configurado (últimos 8 chars: …{saved_token[-8:]})")
            if not confirm("Substituir pelo novo token?", default=False):
                ok("Mantendo token existente.")
                return saved_token, "token"

        print(textwrap.dedent("""\
          Para obter o token:
            1. Acesse https://one.dash.cloudflare.com
            2. Networks → Tunnels → (seu túnel) → Configure → Install connector
            3. Copie o token do comando mostrado  (começa com eyJ…)
        """))
        token = ask_secret("Cole o token do túnel aqui")
        if not token:
            warn("Token vazio — túnel não configurado.")
            return None, "none"

        with open(TUNNEL_CFG_FILE, "w") as f:
            json.dump({"token": token, "mode": "token"}, f)
        ok(f"Token salvo em {TUNNEL_CFG_FILE}")
        return token, "token"

    elif mode == "2":
        ok("Modo quick tunnel selecionado (URL temporária na inicialização).")
        with open(TUNNEL_CFG_FILE, "w") as f:
            json.dump({"mode": "quick"}, f)
        return None, "quick"

    else:
        warn("Túnel Cloudflare não configurado. Configure manualmente depois.")
        return None, "none"

# ---------------------------------------------------------------------------
# ETAPA 8 – Gerar scripts de inicialização
# ---------------------------------------------------------------------------
def generate_startup_scripts(cloudflared_path, tunnel_token, tunnel_mode):
    title("ETAPA 8 · Gerando scripts de inicialização")
    os.makedirs(STARTUP_DIR, exist_ok=True)

    python = sys.executable

    # ---- start_dashboard.bat ----
    start_dashboard = os.path.join(STARTUP_DIR, "start_dashboard.bat")
    with open(start_dashboard, "w", encoding="utf-8") as f:
        f.write(textwrap.dedent(f"""\
            @echo off
            title Dashboard de Macros
            cd /d "{ROOT}"
            echo Iniciando Dashboard de Macros...
            "{python}" -m dashboard_macros
            pause
        """))
    ok(f"start_dashboard.bat  →  {start_dashboard}")

    # ---- start_tunnel.bat ----
    if cloudflared_path and tunnel_mode != "none":
        start_tunnel = os.path.join(STARTUP_DIR, "start_tunnel.bat")
        if tunnel_mode == "token":
            tunnel_cmd = f'"{cloudflared_path}" tunnel run --token {tunnel_token}'
        else:  # quick
            tunnel_cmd = f'"{cloudflared_path}" tunnel --url http://localhost:8050'

        with open(start_tunnel, "w", encoding="utf-8") as f:
            f.write(textwrap.dedent(f"""\
                @echo off
                title Cloudflare Tunnel
                cd /d "{ROOT}"
                echo Iniciando túnel Cloudflare...
                {tunnel_cmd}
                pause
            """))
        ok(f"start_tunnel.bat      →  {start_tunnel}")

        # ---- start_all.bat ---- (abre ambos em janelas separadas)
        start_all = os.path.join(STARTUP_DIR, "start_all.bat")
        with open(start_all, "w", encoding="utf-8") as f:
            f.write(textwrap.dedent(f"""\
                @echo off
                title Iniciar Dashboard + Túnel
                cd /d "{ROOT}"
                echo Iniciando Dashboard de Macros...
                start "Dashboard de Macros" cmd /k "{python} -m dashboard_macros"
                timeout /t 4 /nobreak >nul
                echo Iniciando túnel Cloudflare...
                start "Cloudflare Tunnel" cmd /k "{tunnel_cmd}"
                echo Tudo iniciado!
            """))
        ok(f"start_all.bat         →  {start_all}")
    else:
        start_all = None

    # ---- install_service.bat (Task Scheduler, opcional) ----
    install_service = os.path.join(STARTUP_DIR, "instalar_inicializar_com_windows.bat")
    task_cmd_dashboard = f'"{python}" -m dashboard_macros'
    tunnel_task = ""
    if cloudflared_path and tunnel_mode == "token":
        tunnel_task = textwrap.dedent(f"""\

            echo Criando tarefa de inicializacao do tunel Cloudflare...
            schtasks /create /tn "Cloudflare Tunnel Dashboard" ^
              /tr "\\"{cloudflared_path}\\" tunnel run --token {tunnel_token}" ^
              /sc onlogon /ru SYSTEM /f
            echo Tunel Cloudflare agendado com sucesso.
        """)

    with open(install_service, "w", encoding="utf-8") as f:
        f.write(textwrap.dedent(f"""\
            @echo off
            :: Registra o dashboard e o tunel para iniciar automaticamente com o Windows
            :: Execute como ADMINISTRADOR

            echo Criando tarefa de inicializacao do Dashboard de Macros...
            schtasks /create /tn "Dashboard de Macros" ^
              /tr "\\"{python}\\" -m dashboard_macros" ^
              /sc onlogon /ru SYSTEM /f
            echo Dashboard agendado com sucesso.
            {tunnel_task}
            echo.
            echo Pronto! As tarefas foram criadas no Agendador de Tarefas do Windows.
            pause
        """))
    ok(f"instalar_inicializar_com_windows.bat → {install_service}")

    return start_dashboard, start_all

# ---------------------------------------------------------------------------
# Resumo final
# ---------------------------------------------------------------------------
def print_summary(start_dashboard, start_all, tunnel_mode):
    title("INSTALAÇÃO CONCLUÍDA")
    print(textwrap.dedent(f"""\
      {GREEN}Tudo configurado com sucesso!{RESET}

      Para iniciar:

        {BOLD}Só o dashboard:{RESET}
          {start_dashboard}
          — ou —
          python -m dashboard_macros   (na pasta {ROOT})

    """))
    if start_all:
        print(f"  {BOLD}Dashboard + Túnel Cloudflare juntos:{RESET}")
        print(f"    {start_all}\n")

    if tunnel_mode == "quick":
        print(textwrap.dedent(f"""\
          {YELLOW}Túnel Quick:{RESET}
            A URL pública será exibida no console ao iniciar o túnel.
            Ela muda a cada reinicialização.
        """))
    elif tunnel_mode == "token":
        print(textwrap.dedent(f"""\
          {GREEN}Túnel com token:{RESET}
            Seu hostname personalizado estará disponível após iniciar o túnel.
        """))

    print(textwrap.dedent(f"""\
      {CYAN}Para registrar como serviço do Windows (iniciar com o servidor):{RESET}
        Execute como Administrador:
          {os.path.join(STARTUP_DIR, 'instalar_inicializar_com_windows.bat')}
    """))

# ---------------------------------------------------------------------------
# Ponto de entrada
# ---------------------------------------------------------------------------
def main():
    # Habilitar cores no cmd do Windows
    os.system("")

    print(textwrap.dedent(f"""
    {BOLD}{CYAN}
    ╔══════════════════════════════════════════════════════╗
    ║      SETUP – Dashboard de Macros                     ║
    ║      Configuração automática do servidor             ║
    ╚══════════════════════════════════════════════════════╝
    {RESET}
    Este script irá:
      1. Verificar o Python
      2. Instalar dependências
      3. Configurar credenciais do banco de dados
      4. Testar a conexão
      5. Configurar a senha do dashboard
      6. Baixar o cloudflared (se necessário)
      7. Configurar o túnel Cloudflare
      8. Gerar scripts de inicialização

    Diretório de trabalho: {ROOT}
    """))

    if not confirm("Continuar?", default=True):
        print("Instalação cancelada.")
        sys.exit(0)

    # Executar etapas
    check_python()
    install_dependencies()
    db = configure_database()
    write_config(db)
    test_database(db)
    configure_dashboard_auth()
    cloudflared_path = download_cloudflared()
    tunnel_token, tunnel_mode = configure_cloudflare_tunnel(cloudflared_path)
    start_dashboard, start_all = generate_startup_scripts(
        cloudflared_path, tunnel_token, tunnel_mode
    )
    print_summary(start_dashboard, start_all, tunnel_mode)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n\n{YELLOW}Setup interrompido pelo usuário.{RESET}\n")
        sys.exit(0)
