# Macro Neo Energia — Consulta de Titularidade

Script de automação para consulta de contratos na API da Neo Energia via túnel SSH.  
Integrado ao pipeline `projeto_banco_neo` como a etapa central do ciclo automatizado.

> **Este documento cobre Windows e Linux.** Seções com variações entre sistemas operacionais possuem abas separadas identificadas por 🪟 Windows e 🐧 Linux.

---

## Como funciona no pipeline

```
03_buscar_lote_macro.py  →  executar_automatico.py  →  04_processar_retorno_macro.py
       (ETL)                   (este projeto)                   (ETL)
         │                           │                             │
   Exporta lote                SSH + API Neo                 Interpreta respostas
   do banco para               Energia. Salva                e atualiza tabela_macros
   lote_pendente.csv           resultado_lote.csv            no banco
```

Para uso integrado basta executar `EXECUTAR.bat` (Windows) ou `python executar_automatico.py` (Linux). O passo a passo abaixo é necessário apenas na **primeira configuração**.

---

## Pré-requisitos

| Requisito | 🪟 Windows | 🐧 Linux |
|---|---|---|
| Python | 3.7+ | 3.7+ |
| Cliente SSH | `plink.exe` (PuTTY) | `ssh` (nativo) |
| sshpass | não necessário | `sshpass` (para senha automática) |
| Acesso SSH | usuário + senha + servidor | usuário + senha + servidor |
| Credenciais API | fornecidas pelo gestor | fornecidas pelo gestor |

---

## 1. Instalar o cliente SSH

### 🪟 Windows — PuTTY (plink)

O `plink.exe` é o cliente SSH de linha de comando do PuTTY. É obrigatório para o túnel automático no Windows.

**Opção A — Instalador oficial (recomendado)**

1. Acesse **https://www.putty.org/**
2. Baixe o instalador `.msi` para Windows 64-bit
3. Execute como Administrador
4. Abra um **novo terminal** e verifique: `plink -V`

**Opção B — Winget (Windows 10/11)**

```powershell
winget install PuTTY.PuTTY
```

Abra um novo terminal após instalar.

**Opção C — Executável direto (fallback)**

Um `plink.exe` já está incluído nesta pasta (`macro/macro/`). Se as opções acima falharem, ele será usado automaticamente.

Download manual: https://the.earth.li/~sgtatham/putty/latest/w64/plink.exe

> **Verificação:** `plink -V` deve retornar a versão sem erros.

---

### 🐧 Linux — OpenSSH + sshpass

O `ssh` já vem instalado na maioria das distribuições. O `sshpass` é necessário para passar a senha automaticamente (sem interação manual).

**Ubuntu / Debian:**
```bash
sudo apt update && sudo apt install -y openssh-client sshpass
```

**CentOS / RHEL / Fedora:**
```bash
sudo dnf install -y openssh-clients sshpass
# ou em versões mais antigas:
sudo yum install -y openssh-clients sshpass
```

**Arch Linux:**
```bash
sudo pacman -S openssh sshpass
```

> **Verificação:**
> ```bash
> ssh -V        # deve mostrar a versão do OpenSSH
> sshpass -V    # deve mostrar a versão do sshpass
> ```

> ⚠️ Em alguns ambientes corporativos o `sshpass` pode não estar disponível nos repositórios padrão. Nesse caso, compile da fonte: https://sourceforge.net/projects/sshpass/

---

## 2. Configurar as credenciais (.env)

1. Copie o arquivo de template:
   ```
   .env.example  →  .env
   ```

2. Abra o `.env` e preencha todas as variáveis:

   ```dotenv
   # Usuário SSH do servidor
   SSH_USER=root

   # Endereço do servidor SSH (IP ou hostname)
   SSH_SERVER=192.168.1.100

   # Senha SSH (usada pelo plink para autenticação automática)
   SSH_PASSWORD=sua_senha_aqui

   # Porta local onde o túnel ficará disponível
   # A API será acessada em http://localhost:LOCAL_PORT
   LOCAL_PORT=5000

   # IP/hostname do servidor da API no lado remoto
   REMOTE_HOST=10.0.0.1

   # Porta da API no servidor remoto
   REMOTE_PORT=80
   ```

3. **Nunca versione o `.env`** — ele está no `.gitignore` e contém credenciais.

---

## 3. Aceitar a chave do servidor SSH (primeira vez)

Na primeira conexão o cliente SSH exige confirmação manual da chave do servidor. Se isso não for feito antes, o túnel não abre automaticamente.

### 🪟 Windows

```powershell
cd macro\macro
plink -pw SUA_SENHA SEU_USUARIO@SEU_SERVIDOR echo "chave aceita"
```

Quando aparecer:
```
The server's host key is not cached in the registry.
Store key in cache? (y/n)
```
Digite `y` e Enter. A chave fica gravada no registro do Windows.

> Repita este passo se trocar de servidor ou reinstalar o Windows.

### 🐧 Linux

```bash
cd macro/macro
ssh -o StrictHostKeyChecking=accept-new SEU_USUARIO@SEU_SERVIDOR echo "chave aceita"
```

A chave fica gravada em `~/.ssh/known_hosts`. Para conexões futuras automatizadas com `sshpass`, a flag `-o StrictHostKeyChecking=no` pode ser usada (menos seguro) ou mantenha a chave em `known_hosts` (recomendado).

> Se o servidor mudar de chave (ex.: reinstalação), remova a entrada antiga:
> ```bash
> ssh-keygen -R SEU_SERVIDOR
> ```
> e reexecute o comando acima.

---

## 4. Instalar as dependências Python

### 🪟 Windows

```powershell
cd macro\macro
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt
```

> O `executar_automatico.py` usa especificamente `.venv\Scripts\python.exe`.  
> Não use o Python global para rodar os scripts desta pasta.

### 🐧 Linux

```bash
cd macro/macro
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

> O `executar_automatico.py` detecta automaticamente `.venv/bin/python` quando executado no Linux.

---

## 5. Testar o túnel manualmente (opcional)

Para verificar se tudo está correto antes de rodar o pipeline completo.

### 🪟 Windows

```powershell
cd macro\macro

# Abre o túnel em segundo plano
plink -batch -pw SUA_SENHA -L 5000:REMOTE_HOST:80 USUARIO@SERVIDOR -N

# Em outro terminal PowerShell, testa a API
curl "http://localhost:5000/validacaotitularidade/Validacao/ValidarTitularidade?ContaContrato=123&CpfCnpj=12345678901&Empresa=coelba"

# Fechar o túnel
taskkill /IM plink.exe /F
```

### 🐧 Linux

```bash
cd macro/macro

# Abre o túnel em segundo plano (& = background)
sshpass -p 'SUA_SENHA' ssh -N -f \
  -L 5000:REMOTE_HOST:80 \
  -o StrictHostKeyChecking=no \
  USUARIO@SERVIDOR

# Testa a API
curl "http://localhost:5000/validacaotitularidade/Validacao/ValidarTitularidade?ContaContrato=123&CpfCnpj=12345678901&Empresa=coelba"

# Fechar o túnel
kill $(lsof -t -i:5000)
# ou
pkill -f 'ssh.*5000'
```

Resposta esperada em ambos os sistemas: qualquer JSON ou texto da API (mesmo que diga "não encontrado" — o importante é receber uma resposta HTTP).

---

## 6. Executar

### Modo automático (integrado ao pipeline — uso normal)

#### 🪟 Windows

```
EXECUTAR.bat
```

Ou diretamente no PowerShell:

```powershell
cd macro\macro
.venv\Scripts\python.exe executar_automatico.py
```

#### 🐧 Linux

```bash
cd macro/macro
.venv/bin/python executar_automatico.py
```

O orquestrador executa em sequência:
1. **Passo 1** — busca lote priorizado do banco (`03_buscar_lote_macro.py`)
2. **Passo 2** — VPN + túnel SSH + consulta de titularidade via API
3. **Passo 3** — interpreta respostas e atualiza o banco (`04_processar_retorno_macro.py`)

Parâmetros opcionais (iguais em ambos os sistemas):

```bash
# Lote menor (padrão: 2000)
python executar_automatico.py --tamanho 500

# Apenas verifica o lote sem rodar a macro
python executar_automatico.py --dry-run
```

### Modo manual (sem banco — arquivo Excel/CSV avulso)

#### 🪟 Windows

```powershell
cd macro\macro
.venv\Scripts\python.exe consulta_contrato.py
```

#### 🐧 Linux

```bash
cd macro/macro
.venv/bin/python consulta_contrato.py
```

Abre o dialog de seleção de arquivo (requer display/X11 no Linux — em servidor headless use o modo automático com `--arquivo`). A planilha deve ter as colunas: `cpf` | `codigo cliente` | `empresa`

---

## Estrutura de arquivos

```
macro/
├── macro/
│   ├── .env.example          ← template de credenciais
│   ├── .env                  ← credenciais reais (não versionar)
│   ├── EXECUTAR.bat          ← atalho para execução
│   ├── executar_automatico.py← orquestrador (ETL + SSH + macro + ETL)
│   ├── consulta_contrato.py  ← macro principal (chamada pelo orquestrador)
│   ├── plink.exe             ← cliente SSH (fallback local)
│   ├── requirements.txt      ← dependências Python
│   └── .venv/                ← ambiente virtual (não versionar)
└── dados/
    ├── lote_pendente.csv     ← entrada: gerado por 03_buscar_lote_macro.py
    ├── resultado_lote.csv    ← saída:  lido por 04_processar_retorno_macro.py
    ├── lote_meta.json        ← correlação macro_id ↔ cpf+uc
    └── arquivo/              ← histórico arquivado com timestamp
```

---

## Diagnóstico de problemas comuns

| Sintoma | Causa provável | 🪟 Windows | 🐧 Linux |
|---|---|---|---|
| `plink não encontrado` | PuTTY não instalado | Ver seção 1 | Não se aplica — usa `ssh` nativo |
| `ssh: command not found` | OpenSSH não instalado | Não se aplica | `sudo apt install openssh-client` |
| `sshpass: command not found` | sshpass não instalado | Não se aplica | `sudo apt install sshpass` |
| Túnel abre mas fecha imediatamente | Chave SSH não aceita | Ver seção 3 | Ver seção 3 |
| `Host key verification failed` | Servidor trocou de chave | Reveja registro via `regedit` | `ssh-keygen -R SEU_SERVIDOR` |
| `API não respondeu` | VPN inativa | O script tenta `ipsec up vpn` automaticamente | Idem |
| `lote_pendente.csv não encontrado` | Banco vazio ou ETL falhou | `python etl/load/macro/03_buscar_lote_macro.py` | `python3 etl/load/macro/03_buscar_lote_macro.py` |
| `resultado_lote.csv não encontrado` | Macro abortou antes de completar | Registros voltam para `reprocessar` no próximo ciclo | Idem |
| Porta 5000 já em uso | Processo anterior não encerrado | `taskkill /IM plink.exe /F` | `kill $(lsof -t -i:5000)` |
| Dialog não abre (modo manual) | Sem display gráfico | Não se aplica | Use `--arquivo` no modo automático |
