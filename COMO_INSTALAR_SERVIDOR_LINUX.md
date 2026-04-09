# Como instalar o Dashboard no servidor Linux

Guia completo para colocar o **Dashboard de Macros** em produção num servidor Linux (Ubuntu/Debian),
acessível publicamente via túnel Cloudflare.

---

## Pré-requisitos

- Servidor Linux com Ubuntu 20.04+ ou Debian 11+ (pode ser VPS, AWS EC2, etc.)
- Acesso SSH ao servidor
- Conta na Cloudflare (gratuita em [dash.cloudflare.com](https://dash.cloudflare.com))
- Os arquivos do projeto (pasta `projeto_banco_neo/`)

---

## PARTE 1 — Preparar o servidor

### 1.1 Atualizar o sistema

```bash
sudo apt update && sudo apt upgrade -y
```

### 1.2 Instalar Python 3 e pip

```bash
sudo apt install -y python3 python3-pip python3-venv git
```

Verificar:

```bash
python3 --version   # deve ser 3.8+
```

### 1.3 Instalar o Git (se necessário)

```bash
sudo apt install -y git
```

---

## PARTE 2 — Copiar o projeto para o servidor

### Opção A — Via Git (recomendado)

```bash
cd /opt
sudo git clone <url-do-repositorio> dashboard_neo
sudo chown -R $USER:$USER /opt/dashboard_neo
cd /opt/dashboard_neo
```

### Opção B — Via SCP (copiar do seu computador)

No **seu computador** (Windows), abra o PowerShell:

```powershell
scp -r "C:\Users\marti\Desktop\Bases fornecedor novo\pipeline_bases_neo\projeto_banco_neo" usuario@IP_DO_SERVIDOR:/opt/dashboard_neo
```

---

## PARTE 3 — Criar ambiente virtual e instalar dependências

```bash
cd /opt/dashboard_neo

# Criar ambiente virtual
python3 -m venv .venv

# Ativar
source .venv/bin/activate

# Instalar todas as dependências do dashboard
pip install --upgrade pip
pip install -r dashboard_macros/requirements.txt
```

> **Nota:** sempre que abrir um novo terminal, ative o venv antes de qualquer comando Python:
> ```bash
> source /opt/dashboard_neo/.venv/bin/activate
> ```

---

## PARTE 4 — Configurar as credenciais do banco

### 4.1 Copiar o template

```bash
cp config.example.py config.py
nano config.py
```

### 4.2 Preencher os valores

Edite as linhas abaixo com os dados reais do banco:

```python
DB_DESTINO_HOST     = "seu-host.rds.amazonaws.com"
DB_DESTINO_PORT     = 3306
DB_DESTINO_USER     = "usuario"
DB_DESTINO_PASSWORD = "sua_senha_aqui"
DB_DESTINO_DATABASE = "bd_Automacoes_time_dadosV2"
```

Salvar: `Ctrl+O` → `Enter` → `Ctrl+X`

### 4.3 Testar a conexão

```bash
python3 -c "
import sys; sys.path.insert(0, '.')
from config import db_destino
import pymysql
conn = pymysql.connect(**db_destino())
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM tabela_macros')
print('Conexão OK. Total de registros:', cur.fetchone()[0])
conn.close()
"
```

Se aparecer `Conexão OK`, está tudo certo. Se der erro, revise os dados em `config.py`.

---

## PARTE 5 — Testar o dashboard manualmente

```bash
cd /opt/dashboard_neo
source .venv/bin/activate
python3 -m dashboard_macros
```

No terminal deve aparecer:

```
Dash is running on http://0.0.0.0:8050/
 * Running on http://127.0.0.1:8050
```

Pressione `Ctrl+C` para parar. Se funcionou, siga para a próxima etapa.

---

## PARTE 6 — Instalar o Cloudflare Tunnel (cloudflared)

### 6.1 Baixar e instalar o cloudflared

```bash
# Baixar o binário para Linux x64
wget -O cloudflared.deb https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64.deb

# Instalar
sudo dpkg -i cloudflared.deb

# Verificar
cloudflared --version
```

> Se preferir sem .deb:
> ```bash
> wget -O cloudflared https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64
> chmod +x cloudflared
> sudo mv cloudflared /usr/local/bin/
> ```

### 6.2 Criar o túnel no painel Cloudflare

1. Acesse [one.dash.cloudflare.com](https://one.dash.cloudflare.com)
2. Vá em **Networks → Tunnels → Create a tunnel**
3. Escolha **Cloudflared** como conector
4. Dê um nome ao túnel (ex: `dashboard-macros`)
5. Em "Install connector", selecione **Linux** — copie o comando com o token, que tem este formato:
   ```
   cloudflared tunnel run --token eyJhGciO...
   ```
6. Em **Public Hostnames**, configure:
   - **Subdomain**: `dashboard` (ou o que quiser)
   - **Domain**: seu domínio (ex: `empresa.com.br`)
   - **Service**: `http://localhost:8050`
7. Salve.

### 6.3 Testar o túnel manualmente

Abra um segundo terminal SSH e execute (trocando pelo seu token):

```bash
cloudflared tunnel run --token SEU_TOKEN_AQUI
```

Se aparecer `Connection registered`, o túnel está funcionando.
Acesse `https://dashboard.empresa.com.br` no navegador para confirmar.

---

## PARTE 7 — Rodar como serviço (iniciado automaticamente com o servidor)

Vamos criar dois serviços `systemd`: um para o dashboard e um para o túnel.

### 7.1 Serviço do Dashboard

```bash
sudo nano /etc/systemd/system/dashboard-macros.service
```

Cole o conteúdo abaixo (ajuste o usuário se necessário):

```ini
[Unit]
Description=Dashboard de Macros
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/opt/dashboard_neo
ExecStart=/opt/dashboard_neo/.venv/bin/python3 -m dashboard_macros
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

> Substitua `ubuntu` pelo seu usuário Linux (`whoami` para descobrir).

### 7.2 Serviço do Cloudflare Tunnel

```bash
sudo nano /etc/systemd/system/cloudflare-tunnel.service
```

Cole (substituindo `SEU_TOKEN_AQUI` pelo token real):

```ini
[Unit]
Description=Cloudflare Tunnel - Dashboard de Macros
After=network.target

[Service]
Type=simple
User=ubuntu
ExecStart=/usr/local/bin/cloudflared tunnel run --token SEU_TOKEN_AQUI
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

> Se instalou via `.deb`, o binário fica em `/usr/bin/cloudflared` — use esse caminho.

### 7.3 Ativar e iniciar os serviços

```bash
# Recarregar o systemd
sudo systemctl daemon-reload

# Habilitar para iniciar com o servidor
sudo systemctl enable dashboard-macros
sudo systemctl enable cloudflare-tunnel

# Iniciar agora
sudo systemctl start dashboard-macros
sudo systemctl start cloudflare-tunnel
```

### 7.4 Verificar o status

```bash
sudo systemctl status dashboard-macros
sudo systemctl status cloudflare-tunnel
```

Deve aparecer `Active: active (running)` nos dois.

---

## PARTE 8 — Verificar e monitorar

### Ver logs do dashboard em tempo real

```bash
sudo journalctl -u dashboard-macros -f
```

### Ver logs do túnel em tempo real

```bash
sudo journalctl -u cloudflare-tunnel -f
```

### Testar o endpoint do dashboard

```bash
curl -u neo:dashboard2026 http://localhost:8050/_debug/data | python3 -m json.tool | head -30
```

---

## Comandos úteis do dia a dia

| Ação | Comando |
|------|---------|
| Reiniciar dashboard | `sudo systemctl restart dashboard-macros` |
| Parar dashboard | `sudo systemctl stop dashboard-macros` |
| Reiniciar túnel | `sudo systemctl restart cloudflare-tunnel` |
| Ver logs (últimas 50 linhas) | `sudo journalctl -u dashboard-macros -n 50` |
| Verificar porta 8050 em uso | `ss -tlnp \| grep 8050` |
| Atualizar o código | `cd /opt/dashboard_neo && git pull && sudo systemctl restart dashboard-macros` |

---

## Solução de problemas

### Dashboard não inicia

```bash
# Ver o erro completo
sudo journalctl -u dashboard-macros -n 100 --no-pager

# Testar manualmente fora do serviço
cd /opt/dashboard_neo
source .venv/bin/activate
python3 -m dashboard_macros
```

### Túnel desconecta com frequência

Verifique se o token está correto e se o servidor tem acesso à Internet na porta 443:
```bash
curl -I https://cloudflare.com
```

### Porta 8050 já em uso

```bash
ss -tlnp | grep 8050
# Pegar o PID e matar
kill -9 <PID>
```

### Erro de conexão com o banco

```bash
cd /opt/dashboard_neo
source .venv/bin/activate
python3 -c "from config import db_destino; import pymysql; conn = pymysql.connect(**db_destino()); print('OK')"
```

---

## Resumo da estrutura final no servidor

```
/opt/dashboard_neo/
├── config.py                        ← credenciais do banco (não commitar!)
├── dashboard_macros/                ← código do dashboard
├── .venv/                           ← ambiente virtual Python
└── cloudflared_tunnel.json          ← config do túnel (gerado pelo setup)

/etc/systemd/system/
├── dashboard-macros.service         ← serviço do dashboard
└── cloudflare-tunnel.service        ← serviço do túnel
```

URL pública: **https://dashboard.empresa.com.br** (com login: `neo` / `dashboard2026`)
