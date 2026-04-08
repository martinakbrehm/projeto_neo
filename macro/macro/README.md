# Neo Energia — Macro de Consulta de Titularidade

Automação do ciclo completo de consulta de contratos na API Neo Energia:  
**banco → SSH + túnel → API → banco**

> Para detalhes técnicos, decisões de arquitetura e histórico de testes, veja [DIARIO_TECNICO.md](DIARIO_TECNICO.md).

---

## Início rápido (nova máquina)

### Pré-requisitos

| Requisito | Windows | Linux |
|---|---|---|
| Python 3.7+ | [python.org](https://www.python.org/downloads/) | `apt install python3` |
| plink (SSH) | incluído na pasta como `plink.exe` | `apt install sshpass` |

> **Não é necessário instalar o PuTTY** — o `plink.exe` já está incluído na pasta `macro/macro/`.

---

### Passo 1 — Clonar / copiar o projeto

```powershell
# Se via Git:
git clone https://github.com/martinakbrehm/projeto_neo.git
cd projeto_neo/macro/macro
```

---

### Passo 2 — Criar o ambiente virtual

```bat
# Windows:
setup_venv.bat

# Linux:
bash setup_venv.sh
```

Isso cria `.venv/` com todas as dependências do `requirements.txt`.

---

### Passo 3 — Configurar as credenciais

```bat
# Windows:
copy .env.example .env

# Linux:
cp .env.example .env
```

Edite `.env` e preencha:

```env
SSH_USER=root
SSH_SERVER=191.252.200.81       # IP do servidor SSH
SSH_PASSWORD=sua_senha
SSH_HOST_KEY=ssh-ed25519 255 SHA256:85SZsIcG+...   # fingerprint
LOCAL_PORT=5000
REMOTE_HOST=10.219.11.156       # IP da API dentro da rede
REMOTE_PORT=80
```

> **Como obter o `SSH_HOST_KEY`:** execute `plink.exe -pw SENHA root@SERVIDOR "echo ok"` uma vez e copie a fingerprint exibida.

---

### Passo 4 — Executar

```bat
# Windows — modo normal (lote padrão de 2000 registros):
EXECUTAR.bat

# Com tamanho de lote customizado:
EXECUTAR.bat --tamanho 500

# Dry-run (só busca o lote, não consulta a API nem grava):
EXECUTAR.bat --dry-run

# Linux:
.venv/bin/python executar_automatico.py
.venv/bin/python executar_automatico.py --tamanho 500
.venv/bin/python executar_automatico.py --dry-run
```

---

## Abrir o túnel manualmente

Para usar `consulta_contrato.py` de forma independente (sem o orquestrador), abra o túnel em um terminal separado:

```bat
# Windows — abre e mantém o túnel aberto:
TUNEL_MANUAL.bat

# Ou diretamente:
plink.exe -batch -pw "SUA_SENHA" -hostkey "ssh-ed25519 255 SHA256:..." ^
    -L 5000:10.219.11.156:80 root@191.252.200.81 -N
```

```bash
# Linux:
sshpass -p "SUA_SENHA" ssh -N \
    -o StrictHostKeyChecking=no \
    -L 5000:10.219.11.156:80 \
    root@191.252.200.81
```

Com o túnel ativo, rode em outro terminal:

```bat
# Com lote CSV já gerado:
.venv\Scripts\python.exe consulta_contrato.py ^
    --arquivo ..\dados\lote_pendente.csv ^
    --saida ..\dados\resultado_lote.csv
```

---

## Estrutura da pasta

```
macro/macro/
├── .env                    # Credenciais (NÃO versionado)
├── .env.example            # Template de credenciais (versionado)
├── .gitignore              # Garante que .env não suba ao Git
├── .venv/                  # Ambiente virtual Python (NÃO versionado)
│
├── executar_automatico.py  # Orquestrador: ETL + SSH + macro + ETL
├── consulta_contrato.py    # Consulta à API (passo 2)
│
├── EXECUTAR.bat            # Lançador Windows (repassa argumentos)
├── TUNEL_MANUAL.bat        # Abre só o túnel SSH (uso manual)
├── setup_venv.bat          # Configura venv no Windows
├── setup_venv.sh           # Configura venv no Linux
│
├── plink.exe               # Cliente SSH portável (PuTTY)
├── requirements.txt        # Dependências pinadas
│
├── README.md               # Este arquivo
├── DIARIO_TECNICO.md       # Decisões, testes, inspeções técnicas
├── CONFIGURACAO.md         # Guia detalhado de instalação e configuração
│
└── _legado/                # Arquivos descontinuados (apenas referência histórica)
    ├── README.md           # Descrição de cada arquivo legado
    ├── executar_automatico_backup.py
    ├── muito_rapido_tratado.py
    ├── demora.py
    └── cli.txt
```

```
macro/dados/
├── lote_pendente.csv       # Gerado pelo passo 1 (NÃO versionado)
├── resultado_lote.csv      # Gerado pelo passo 2 (NÃO versionado)
└── arquivo/                # Arquivos arquivados após ciclo (NÃO versionado)
```

---

## O que o orquestrador faz (ciclo completo)

1. **Passo 1 — EXTRACTION** (`03_buscar_lote_macro.py`)  
   Busca registros com `status='pendente'` ou `'reprocessar'` em `tabela_macros`, priorizando `fornecedor2`. Exporta `lote_pendente.csv`.

2. **Passo 2a — VPN + Túnel SSH**  
   Verifica/ativa a VPN no servidor (`ipsec up vpn`) e abre o túnel `localhost:5000 → API`.

3. **Passo 2b — MACRO** (`consulta_contrato.py`)  
   Lê `lote_pendente.csv`, consulta a API para cada contrato, salva `resultado_lote.csv`.

4. **Limpeza** — Encerra VPN e túnel SSH.

5. **Passo 3 — LOAD** (`04_processar_retorno_macro.py`)  
   Lê `resultado_lote.csv`, interpreta respostas e atualiza `tabela_macros` no banco.

---

## Volumes e tempo estimado

| Lote | Registros | Tempo estimado |
|---|---|---|
| Teste | 50 | ~5 min |
| Operacional | 500 | ~30 min |
| Carga | 2000 | ~2–4 horas |

---

## Validação antes de rodar

Do diretório raiz do projeto:

```powershell
python _validar_macro.py
```

Verifica: banco, qualidade de dados, `.env`, venv, scripts ETL, SSH e túnel. Saída: `PRONTA PARA PRODUÇÃO` ou lista de problemas.

---

## Troubleshooting

| Problema | Causa provável | Solução |
|---|---|---|
| `plink: comando não encontrado` | plink não está no PATH | Use `.\plink.exe` (está na pasta) |
| Túnel não estabiliza | Porta já em uso | Feche processos plink anteriores: `taskkill /IM plink.exe /F` |
| API não responde | VPN não ativou | Verifique VPN no servidor: `plink ... "ipsec status"` |
| `ModuleNotFoundError` | Dependência faltando no venv | Execute `setup_venv.bat` novamente |
| Exit Code 1 no plink foreground | Comportamento normal no PS | Use `EXECUTAR.bat` ou via `subprocess.Popen` (orquestrador faz isso) |

## 🔒 Segurança

- As credenciais ficam no arquivo `.env` que não é versionado
- Chaves SSH e outros arquivos sensíveis são ignorados pelo Git
- Sempre use senhas fortes e mantenha as credenciais seguras

## 📝 Logs

O script exibe logs detalhados durante a execução:
- ✅ Operações bem-sucedidas
- ⚠️ Avisos e tentativas alternativas
- ❌ Erros que impedem a execução
- 🔍 Informações de debug

## 🆘 Solução de Problemas

### Problemas com PuTTY/plink:
1. **Erro "PuTTY/plink não encontrado"**: 
   - Instale o PuTTY seguindo as [instruções acima](#-instalação-do-putty)
   - Reinicie o terminal após a instalação
   - Teste digitando `plink` no terminal

2. **"'plink' is not recognized"**: 
   - Verifique se o PuTTY foi instalado corretamente
   - Adicione manualmente ao PATH: `C:\Program Files\PuTTY\`
   - Ou coloque o `plink.exe` na pasta do projeto

### Outros problemas:
3. **Erro de conexão SSH**: Verifique as credenciais no arquivo `.env`
4. **API não responde**: Verifique se a VPN está ativa no servidor
5. **Porta ocupada**: O script limpa conexões anteriores automaticamente
6. **Timeout na conexão**: Verifique firewall e conectividade de rede

### Teste rápido do PuTTY:
```cmd
plink -V
```
Deve mostrar a versão do PuTTY se estiver instalado corretamente.