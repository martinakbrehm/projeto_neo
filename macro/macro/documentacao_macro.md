# Neo Energia — Macro de Consulta de Titularidade

Documentação do ciclo completo de consulta de contratos na API Neo Energia:  
**banco → SSH + túnel → API → banco**

> Para decisões de arquitetura, histórico de testes e inspeções técnicas, veja [DIARIO_TECNICO.md](DIARIO_TECNICO.md).  
> Para configuração detalhada de SSH, VPN e variáveis de ambiente, veja [CONFIGURACAO.md](CONFIGURACAO.md).

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
├── DOCUMENTACAO.md         # Este arquivo
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

5. **Passo 3 — LOAD** (`etl/load/macro/04_processar_retorno_macro.py`)  
   Lê `resultado_lote.csv`, interpreta respostas via `etl/transformation/macro/interpretar_resposta.py` e atualiza `tabela_macros` no banco.

---

## Lógica de status

O campo `status` em `tabela_macros` segue o fluxo abaixo:

```
pendente → processando → consolidado   (titularidade confirmada, contrato ativo)
                       → reprocessar   (titularidade confirmada mas inativa/suspensa,
                                        ou erro de comunicação — timeout, LIMIT_EXCEEDED)
                       → excluido      (contrato/doc não existe, titularidade não confirmada)
                       → pendente      (sem resposta — API não retornou nada,
                                        ou macro interrompida antes de processar)
```

**Invariante:** todo registro com `status = 'reprocessar'` ou `'excluido'` **deve ter `resposta_id NOT NULL`**.  
Registros sem resposta da API voltam como `pendente` (não `reprocessar`) para reentrar na fila normalmente.

### Mapeamento CodigoRetorno → status

| Código | Mensagem | Status |
|---|---|---|
| 000 | Conta Contrato não existe | `excluido` |
| 001 | Doc. fiscal não existe | `excluido` |
| 002 | Titularidade não confirmada | `excluido` |
| 003 | Titularidade confirmada — contrato ativo | `consolidado` |
| 004 | Titularidade confirmada — contrato inativo | `reprocessar` |
| 005 | Titularidade confirmada — instalação suspensa | `reprocessar` |
| 006 | Aguardando processamento | `pendente` |
| 007 | Doc. Fiscal não cadastrado no SAP | `excluido` |
| 008 | Parceiro não possui conta contrato | `excluido` |
| 009 | Status instalação: desligado | `reprocessar` |
| 010 | Status instalação: ligado | `consolidado` |
| 011 | ERRO | `reprocessar` |
| — | Sem resposta / macro interrompida | `pendente` |
| — | Timeout, LIMIT_EXCEEDED, ERRO_RETRY | `reprocessar` (id=11) |

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

---

## Segurança

- As credenciais ficam no arquivo `.env`, que **não é versionado** (`.gitignore` garante isso)
- O `SSH_HOST_KEY` no `.env` evita ataques MITM — não use `-batch` sem ele em produção
- Nunca compartilhe o `.env` nem o inclua em logs ou prints