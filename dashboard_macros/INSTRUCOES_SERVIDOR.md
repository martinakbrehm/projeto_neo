# Como Instalar Servidor Linux para Dashboard de Macros

Este documento descreve como instalar e configurar o servidor Linux para o Dashboard de Macros do projeto Neo Energia.

## Pré-requisitos

- Servidor Linux (Ubuntu 20.04+ recomendado)
- Python 3.8+
- Acesso root ou sudo
- Git

## Instalação

### 1. Clonar o repositório

```bash
git clone <url-do-repositorio>
cd projeto_banco_neo
```

### 2. Instalar dependências do sistema

```bash
sudo apt update
sudo apt install python3 python3-pip python3-venv
```

### 3. Criar ambiente virtual

```bash
python3 -m venv venv
source venv/bin/activate
```

### 4. Instalar dependências Python

```bash
pip install -r dashboard_macros/requirements.txt
```

### 5. Configurar banco de dados

Edite o arquivo `config.py` com as configurações do banco MySQL:

```python
def db_destino():
    return {
        'host': 'localhost',
        'user': 'seu_usuario',
        'password': 'sua_senha',
        'database': 'neo_dashboard',
        'charset': 'utf8mb4'
    }
```

### 6. Executar migrações do banco

```bash
python db/setup_database.py
```

### 7. Iniciar o dashboard

```bash
python -m dashboard_macros
```

O dashboard estará disponível em `http://localhost:8050`

## Configuração como serviço systemd

Para executar o dashboard como serviço:

### Criar arquivo de serviço

```bash
sudo nano /etc/systemd/system/dashboard-macros.service
```

Conteúdo:

```
[Unit]
Description=Dashboard de Macros Neo Energia
After=network.target

[Service]
User=ubuntu
WorkingDirectory=/caminho/para/projeto_banco_neo
ExecStart=/caminho/para/projeto_banco_neo/venv/bin/python -m dashboard_macros
Restart=always

[Install]
WantedBy=multi-user.target
```

### Habilitar e iniciar o serviço

```bash
sudo systemctl daemon-reload
sudo systemctl enable dashboard-macros
sudo systemctl start dashboard-macros
```

### Verificar status

```bash
sudo systemctl status dashboard-macros
```

## Configuração Nginx (opcional)

Para expor o dashboard na web:

```bash
sudo apt install nginx
sudo nano /etc/nginx/sites-available/dashboard-macros
```

Conteúdo:

```
server {
    listen 80;
    server_name seu-dominio.com;

    location / {
        proxy_pass http://127.0.0.1:8050;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

```bash
sudo ln -s /etc/nginx/sites-available/dashboard-macros /etc/nginx/sites-enabled/
sudo systemctl restart nginx
```

## Logs

Logs do dashboard estão em `/var/log/syslog` ou via `journalctl -u dashboard-macros`

## Troubleshooting

- Verifique se todas as dependências estão instaladas
- Confirme configurações do banco de dados
- Verifique portas (8050 deve estar livre)
- Use `python -c "import dash; print('Dash OK')"` para testar imports