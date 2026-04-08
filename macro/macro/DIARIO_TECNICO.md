# Diário Técnico — Macro Neo Energia

> **Projeto:** `projeto_banco_neo / macro/macro/`  
> **Responsável:** Martina  
> **Período:** 2026-04-06 a 2026-04-08  
> **Objetivo:** Automatizar a consulta de titularidade de contratos na API Neo Energia via túnel SSH, integrado ao banco de dados do pipeline principal.

---

## Índice

1. [Arquitetura do Pipeline](#1-arquitetura-do-pipeline)
2. [Inspeção das Tabelas](#2-inspeção-das-tabelas)
3. [Decisões Técnicas](#3-decisões-técnicas)
4. [Configuração de Credenciais e Segurança](#4-configuração-de-credenciais-e-segurança)
5. [Testes de Viabilidade](#5-testes-de-viabilidade)
6. [Resultados da Validação Final](#6-resultados-da-validação-final)
7. [Problemas Encontrados e Soluções](#7-problemas-encontrados-e-soluções)
8. [Estado Atual e Próximos Passos](#8-estado-atual-e-próximos-passos)

---

## 1. Arquitetura do Pipeline

O ciclo completo de consulta de titularidade envolve três etapas sequenciais:

```
┌─────────────────────────────────────────────────────────────────┐
│                    executar_automatico.py                        │
│                    (orquestrador do ciclo)                       │
└───────────┬─────────────────┬─────────────────┬─────────────────┘
            │                 │                  │
            ▼                 ▼                  ▼
    ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐
    │  PASSO 1     │  │  PASSO 2     │  │  PASSO 3             │
    │  EXTRACTION  │  │  MACRO       │  │  LOAD                │
    │              │  │              │  │                      │
    │ 03_buscar_   │  │ consulta_    │  │ 04_processar_        │
    │ lote_macro.py│  │ contrato.py  │  │ retorno_macro.py     │
    │              │  │              │  │                      │
    │ Banco →      │  │ SSH + túnel  │  │ resultado_lote.csv   │
    │ lote_        │  │ → API Neo    │  │ → tabela_macros      │
    │ pendente.csv │  │ Energia →    │  │ (atualiza status)    │
    └──────────────┘  │ resultado_   │  └──────────────────────┘
                      │ lote.csv     │
                      └──────────────┘
```

### Infraestrutura de Rede

```
  Sua máquina (Windows)
  ┌─────────────────────────────────────────┐
  │  localhost:5000  ◄──── túnel SSH ──────┐│
  │  (consulta_contrato.py conecta aqui)   ││
  └─────────────────────────────────────────┘
           │ plink.exe -L 5000:10.x.x.x:80
           ▼
  Servidor SSH Público (191.252.200.81)
  ┌─────────────────────────────────────────┐
  │  SSH jump host / bastion                │
  │  Executa: ipsec up vpn (StrongSwan)     │
  └─────────────────────────────────────────┘
           │ forward para rede interna
           ▼
  API Neo Energia (10.219.11.156:80)
  ┌─────────────────────────────────────────┐
  │  /validacaotitularidade/Validacao/       │
  │  ValidarTitularidade?                   │
  │    ContaContrato=...                    │
  │    CpfCnpj=...                          │
  │    Empresa=...                          │
  └─────────────────────────────────────────┘
```

### Fluxo de dados

| Etapa | Entrada | Saída | Script |
|---|---|---|---|
| Passo 1 — Buscar lote | `tabela_macros` (status=pendente/reprocessar) | `macro/dados/lote_pendente.csv` | `etl/extraction/macro/03_buscar_lote_macro.py` |
| Passo 2 — Consultar API | `lote_pendente.csv` | `macro/dados/resultado_lote.csv` | `macro/macro/consulta_contrato.py` |
| Passo 3 — Processar retorno | `resultado_lote.csv` | `tabela_macros` (atualiza status + resposta_id) | `etl/load/macro/04_processar_retorno_macro.py` |

---

## 2. Inspeção das Tabelas

### 2.1 `tabela_macros` — Schema

Inspecionado em 2026-04-08 via `DESCRIBE tabela_macros`:

| Coluna | Tipo | Observação |
|---|---|---|
| `id` | int | PK — identificador do registro de consulta |
| `cliente_id` | int | FK → `clientes.id` |
| `distribuidora_id` | tinyint unsigned | FK → `distribuidoras.id` |
| `resposta_id` | tinyint unsigned | FK → tabela de respostas (enum de resultado) |
| `qtd_faturas` | int | Quantidade de faturas do contrato |
| `valor_debito` | decimal(10,2) | Valor de débito associado |
| `valor_credito` | decimal(10,2) | Valor de crédito associado |
| `data_update` | datetime | Última atualização do registro |
| `data_extracao` | datetime | Data em que o dado foi extraído da fonte |
| `data_criacao` | datetime | Data de criação do registro |
| `status` | enum | `pendente` / `processando` / `reprocessar` / `consolidado` / `excluido` |
| `extraido` | tinyint(1) | Flag de extração |
| `data_inic_parc` | date | Data início de parcelamento |
| `qtd_parcelas` | int | Quantidade de parcelas |
| `valor_parcelas` | decimal(10,2) | Valor das parcelas |
| `data_criacao_data` | date | Data de criação (campo redundante legado) |

> **Observação:** Os dados para a API (cpf, uc, empresa) **não estão em `tabela_macros`** diretamente — são obtidos via JOIN com `clientes`, `cliente_uc` e `distribuidoras` no passo 1.

### 2.2 `tabela_macros` — Distribuição por status (2026-04-08)

| Status | Quantidade |
|---|---:|
| `consolidado` | 89.804 |
| **`pendente`** | **34.324** |
| `reprocessar` | 29.490 |
| `excluido` | 667 |
| **Total** | **154.285** |

### 2.3 Lógica de priorização do lote

O lote de cada ciclo é montado com quatro níveis de prioridade, aplicados nesta ordem:

#### Nível 1 — Status: `pendente` antes de `reprocessar`

| Status | Significado | Prioridade |
|---|---|---|
| `pendente` | Nunca foi consultado na API | **1ª (máxima)** |
| `reprocessar` | Já foi consultado, mas a resposta foi inválida ou inconclusiva e precisa ser refeita | 2ª |
| `consolidado` | Resposta válida recebida e gravada — não entra no lote | — |
| `excluido` | Descartado manualmente — não entra no lote | — |

**Decisão:** priorizar `pendente` porque são registros que nunca tiveram nenhuma tentativa. Os `reprocessar` já tiveram pelo menos uma chamada à API; podem aguardar um ciclo sem impacto operacional. Em um lote de 2.000, se houver mais de 2.000 pendentes, nenhum `reprocessar` entrará naquele ciclo.

#### Nível 2 — Fornecedor: `fornecedor2` antes de `contatus`

Dentro de cada grupo de status, os registros de `fornecedor2` aparecem antes dos de `contatus`.

| Fornecedor | Prioridade |
|---|---|
| `fornecedor2` | **1ª** |
| `contatus` | 2ª |

**Decisão:** `fornecedor2` é o fornecedor operacional principal do projeto. Os dados do `contatus` são históricos/migração e têm menor urgência. Além disso, o pipeline operacional diário gera somente registros de `fornecedor2` — ao processar esses primeiro, mantemos o ciclo operacional em dia antes de atacar o histórico.

O fornecedor é identificado via `LEFT JOIN` na tabela `cliente_origem` (aplicada na melhoria `20260406`). Registros sem entrada em `cliente_origem` são tratados como `fornecedor2` via `COALESCE(co.fornecedor, 'fornecedor2')`.

#### Nível 3 — Antiguidade: `data_update` ASC

Dentro do mesmo status e fornecedor, os registros mais antigos (menor `data_update`) têm prioridade. Isso garante que nenhum registro fique represado indefinidamente — os que esperaram mais tempo são consultados primeiro.

#### Nível 4 — Desempate: `id` ASC

Para registros com exatamente o mesmo `data_update`, o `id` menor garante uma ordenação determinística e reproduzível.

#### Resumo da ordem de prioridade

```
1º  pendente   + fornecedor2  (mais antigo primeiro)
2º  pendente   + contatus     (mais antigo primeiro)
3º  reprocessar + fornecedor2 (mais antigo primeiro)
4º  reprocessar + contatus    (mais antigo primeiro)
```

### 2.4 Query do Passo 1 — montagem do lote

O script `03_buscar_lote_macro.py` implementa a lógica acima com o seguinte SQL:

```sql
SELECT
    tm.id                              AS macro_id,
    c.cpf                              AS cpf,
    cu.uc                              AS `codigo cliente`,
    d.nome                             AS empresa,
    tm.status                          AS status_atual,
    COALESCE(co.fornecedor, 'fornecedor2') AS fornecedor
FROM tabela_macros tm
JOIN clientes       c  ON c.id          = tm.cliente_id
JOIN cliente_uc     cu ON cu.cliente_id = tm.cliente_id
                       AND cu.distribuidora_id = tm.distribuidora_id
JOIN distribuidoras d  ON d.id          = tm.distribuidora_id
LEFT JOIN cliente_origem co ON co.cliente_id = tm.cliente_id
WHERE tm.status IN ('pendente', 'reprocessar')
ORDER BY
    -- Nível 1: status (pendente=1 vem antes de reprocessar=0)
    (tm.status = 'pendente')                               DESC,
    -- Nível 2: fornecedor (fornecedor2=1 vem antes de contatus=0)
    (COALESCE(co.fornecedor,'fornecedor2') = 'fornecedor2') DESC,
    -- Nível 3: mais antigos primeiro
    tm.data_update                                          ASC,
    -- Nível 4: desempate determinístico
    tm.id                                                   ASC
LIMIT %s
```

> **Como o `ORDER BY` booleano funciona no MySQL:** a expressão `(tm.status = 'pendente')` retorna `1` quando verdadeira e `0` quando falsa. Com `DESC`, os `1` (pendente) vêm antes dos `0` (reprocessar). O mesmo se aplica ao critério de fornecedor.

**Fallback:** se a tabela `cliente_origem` não existir (melhoria `20260406` não aplicada), usa SQL sem o LEFT JOIN, com `fornecedor` fixo como `'fornecedor2'` — mantendo os níveis 1, 3 e 4 mas sem o nível 2 de priorização por fornecedor.

### 2.5 Validação de qualidade dos dados (2026-04-08)

Verificado via `_validar_macro.py`: **0 registros** com cpf / uc / distribuidora não resolvidos via JOIN. Todos os 34.324 pendentes têm dados válidos para a API.

Amostra confirmada:
```
macro_id=121240  cpf=13852507472  uc=1248951838  empresa=celpe  status=pendente
macro_id=121241  cpf=18020810463  uc=1248957944  empresa=celpe  status=pendente
macro_id=121241  cpf=18020810463  uc=1963228344  empresa=celpe  status=pendente
```

---

## 3. Decisões Técnicas

### 3.1 SSH: plink vs openssh nativo

**Situação:** Windows sem suporte fácil a `ssh -L` em batch mode sem prompt interativo.

**Decisão:** Usar `plink.exe` (PuTTY) com flags `-batch -pw SENHA -hostkey FINGERPRINT`.

- `-batch`: modo não interativo (não pede confirmação de chave)
- `-hostkey`: valida fingerprint sem armazenar no registry (funciona em qualquer máquina, inclusive sem instalação do PuTTY)
- `plink.exe` incluído na pasta `macro/macro/` como fallback portável

**Alternativa Linux:** `sshpass + ssh -N -L` (implementado em `executar_automatico.py` com detecção de SO via `platform.system()`).

### 3.2 `consulta_contrato.py`: remoção do dialog de arquivo

**Situação original:** Script abria um dialog gráfico (`tkinter.filedialog`) para o usuário selecionar o arquivo CSV manualmente. Incompatível com execução automática.

**Decisão:** Adicionados os argumentos `--arquivo` e `--saida` ao script. Quando presentes, o dialog é bypassado. O orquestrador passa os caminhos fixos:
- entrada: `macro/dados/lote_pendente.csv`
- saída: `macro/dados/resultado_lote.csv`

### 3.3 Dois venvs separados

| Ambiente | Localização | Uso |
|---|---|---|
| venv da macro | `macro/macro/.venv/` | `httpx`, `pandas`, `openpyxl`, `pymysql` — executa `consulta_contrato.py` |
| Python do sistema | `python` no PATH | `pymysql`, `pandas` — executa scripts ETL (passos 1 e 3) aprovados pelo AppLocker |

**Motivo:** Restrições de AppLocker na máquina impedem que o venv da macro execute scripts ETL que acessam o banco. O `executar_automatico.py` detecta o Python do sistema via `shutil.which("python")` para os passos ETL.

### 3.4 VPN no servidor (StrongSwan ipsec)

A API Neo Energia (`10.219.11.156`) só é acessível dentro de uma VPN gerenciada pelo servidor SSH (`ipsec up vpn`). O orquestrador:
1. Verifica se a VPN está ativa (`ipsec status | grep vpn`)
2. Ativa se necessário (`ipsec up vpn`)
3. Desativa ao final (`ipsec down vpn`)

Se a VPN falhar, o script **continua** (não aborta), pois em alguns casos a VPN já está ativa de sessões anteriores.

### 3.5 Prioridade de consulta fornecedor2 > contatus

O campo `fornecedor` do LEFT JOIN em `cliente_origem` é usado para priorizar registros do `fornecedor2` antes do `contatus` no lote. Isso porque o `fornecedor2` é o fornecedor operacional principal.

### 3.6 Processamento do retorno mesmo em falha parcial

Se a macro (passo 2) retornar erro mas já tiver gerado `resultado_lote.csv`, o passo 3 é executado mesmo assim. Isso evita perder resultados parciais de lotes grandes.

---

## 4. Configuração de Credenciais e Segurança

### 4.1 Arquivo `.env`

Localizado em `macro/macro/.env` — **nunca versionado**.

Coberto por dois `.gitignore`:
- `macro/macro/.gitignore`: regra `.env` e `*.env`
- `.gitignore` raiz do projeto: regra `macro/macro/.env`

Template disponível em `macro/macro/.env.example` (este sim é versionado).

### 4.2 Variáveis necessárias

| Variável | Descrição |
|---|---|
| `SSH_USER` | Usuário do servidor SSH (normalmente `root`) |
| `SSH_SERVER` | IP público do servidor SSH |
| `SSH_PASSWORD` | Senha do servidor SSH |
| `SSH_HOST_KEY` | Fingerprint da chave do servidor (modo batch sem prompt) |
| `LOCAL_PORT` | Porta local do túnel (padrão: `5000`) |
| `REMOTE_HOST` | IP da API dentro da rede interna |
| `REMOTE_PORT` | Porta da API remota (padrão: `80`) |

### 4.3 Como obter o `SSH_HOST_KEY`

Execute uma vez interativamente (aceite a chave quando pedido) ou:
```powershell
.\plink.exe -pw SUA_SENHA root@SEU_SERVIDOR "echo ok"
# A chave aparece no formato: ssh-ed25519 255 SHA256:XXXX...
# Cole esse valor no .env
```

---

## 5. Testes de Viabilidade

### 5.1 Teste de conectividade SSH (2026-04-08)

```powershell
cd macro\macro
.\plink.exe -batch -pw "****" -hostkey "ssh-ed25519 255 SHA256:85SZs..." root@191.252.200.81 "echo conectado"
# Resultado: "conectado" — Exit Code: 0
```

✅ **Aprovado**

### 5.2 Teste de túnel SSH (2026-04-08)

```powershell
.\plink.exe -batch -pw "****" -hostkey "ssh-ed25519 255 SHA256:85SZs..." `
    -L 5000:10.219.11.156:80 root@191.252.200.81 -N
# Em background (& no PowerShell) — Exit Code: 0
# Get-NetTCPConnection -LocalPort 5000 → porta ativa
```

Observação: em foreground (sem `&`) o plink retornava Exit Code 1. Em background (processo Popen/`&`) funcionou normalmente. O `executar_automatico.py` usa `subprocess.Popen` com `CREATE_NO_WINDOW` o que resolve o problema.

✅ **Aprovado** (via Popen em background)

### 5.3 Dry-run do orquestrador (2026-04-08)

```powershell
cd macro\macro
python executar_automatico.py --tamanho 2000 --dry-run
# Passo 1: buscou 2.000 registros do banco — OK
# Encerrou após passo 1 (--dry-run)
# Exit Code: 0
```

✅ **Aprovado**

### 5.4 Validação completa via `_validar_macro.py` (2026-04-08)

Script de validação automática executado a partir da raiz do projeto:

```
python _validar_macro.py
```

Resultados:

| Check | Status |
|---|---|
| `tabela_macros`: 34.324 pendentes | ✅ OK |
| Qualidade dos dados (cpf + uc + distribuidora via JOIN) | ✅ OK — 0 nulos |
| `.env`: SSH_SERVER, SSH_PASSWORD, REMOTE_HOST, SSH_HOST_KEY | ✅ OK |
| venv: httpx 0.28.1, pandas 3.0.2, openpyxl 3.1.5 | ✅ OK |
| Script passo 1 (`03_buscar_lote_macro.py`) | ✅ OK |
| Script passo 3 (`04_processar_retorno_macro.py`) | ✅ OK |
| Script `consulta_contrato.py` | ✅ OK |
| SSH — `plink echo SSH_OK` | ✅ OK — Exit Code 0 |
| Túnel SSH — porta 5000 ativa | ✅ OK — ativo na tentativa 1 |

**Conclusão: ✅ PRONTA PARA PRODUÇÃO**

---

## 6. Resultados da Validação Final

### 6.1 Checklist pré-produção

- [x] `.env` configurado com todas as credenciais
- [x] SSH_HOST_KEY preenchida (plink sem prompt interativo)
- [x] `plink.exe` presente na pasta (portável)
- [x] venv recriado com `pymysql` incluído no `requirements.txt`
- [x] `consulta_contrato.py` aceita `--arquivo` e `--saida` (sem dialog)
- [x] `03_buscar_lote_macro.py` com fallback de `cliente_origem`
- [x] Dry-run validado (2.000 registros buscados com sucesso)
- [x] Conectividade SSH testada
- [x] Túnel SSH testado (porta 5000 ativa na tentativa 1)
- [x] 34.324 registros pendentes com dados válidos no banco
- [x] Processamento parcial: passo 3 roda mesmo se macro tiver erro parcial
- [x] KeyboardInterrupt tratado: flush do resultado parcial antes de sair

### 6.2 Volumes esperados por ciclo

| Configuração | Tamanho do lote | Tempo estimado |
|---|---|---|
| Teste inicial | 50–100 | ~5–15 min |
| Operacional padrão | 500–1000 | ~30–60 min |
| Carga máxima | 2000 | ~2–4 horas |

---

## 7. Problemas Encontrados e Soluções

### 7.1 `plink -N` foreground retornou Exit Code 1

**Sintoma:** `.\plink.exe ... -L 5000:... -N` (sem `&`) encerrava com código 1.

**Causa provável:** Alguma saída no stderr sendo interpretada como erro pelo PowerShell, ou a sessão SSH sem comando remoto fechando antes de estabilizar.

**Solução:** Usar `subprocess.Popen` com `CREATE_NO_WINDOW` e verificar a porta via socket após o processo iniciar. O orquestrador tenta 10 vezes com intervalo de 1 segundo antes de considerar falha.

### 7.2 `tabela_macros` sem colunas `conta_contrato`/`cpf_cnpj`

**Sintoma:** Primeiro check de qualidade do `_validar_macro.py` falhou com `Unknown column 'conta_contrato'`.

**Causa:** `tabela_macros` não tem essas colunas diretamente — os dados vêm de JOIN com `clientes`, `cliente_uc` e `distribuidoras`.

**Solução:** Check reescrito para fazer o mesmo JOIN que o passo 1 e verificar se os registros pendentes têm dados resolvíveis.

### 7.3 Encoding UTF-8 no `.gitignore` raiz

**Sintoma:** Arquivo `.gitignore` mostrava `â€"` em vez de `—` no terminal.

**Solução:** Arquivo reescrito com `[System.IO.File]::WriteAllText(..., UTF8Encoding($false))` para garantir UTF-8 sem BOM.

### 7.4 Scripts ETL não podiam rodar no venv da macro (AppLocker)

**Sintoma:** Tentar rodar `03_buscar_lote_macro.py` com o Python do venv falhou.

**Causa:** Política de AppLocker na máquina bloqueia executáveis em pastas do perfil de usuário.

**Solução:** `executar_automatico.py` detecta o Python do sistema via `shutil.which("python")` e o usa exclusivamente para os scripts ETL. Só `consulta_contrato.py` roda no venv.

---

## 8. Execuções Reais em Produção

### 8.1 Primeiro ciclo real — lote 50 (2026-04-08)

**Comando:**
```bat
EXECUTAR.bat --tamanho 50
```

**Resultado:**
- ✅ 50/50 registros consultados na API
- 4 timeouts resolvidos na segunda tentativa (retry automático)
- **7 consolidados** (titularidade confirmada)
- **21 excluídos** (contrato não existe / CPF inválido / distribuição inativa)
- 0 erros de processamento
- Arquivos arquivados com timestamp `20260408_113504`

**Conclusão:** Pipeline funcionando ponta a ponta confirmado.

---

### 8.2 Ciclo completo — lote 2000 (2026-04-08)

Lançado em background após confirmação do lote 50:
```powershell
.venv\Scripts\python.exe -u executar_automatico.py --tamanho 2000 *> ciclo_run.log
```
Log crescendo normalmente — ~578+ de 2000 processados quando modo contínuo foi implementado.

---

## 9. Modo Contínuo (Loop Forever)

### 9.1 Motivação

Com ~34.000+ registros pendentes, rodar ciclos manuais é inviável. O modo `--continuar`
executa ciclos indefinidamente com mecanismos de segurança para falhas de SSH e lotes vazios.

### 9.2 Implementação

**Commit:** `7c220b0` — `macro: modo continuo (--continuar) com reconexao SSH automatica`

**Nova função extraída:** `_executar_um_ciclo(tamanho: int) -> str`
- Encapsula o ciclo completo (passo 1 → SSH → API → passo 3)
- Retorna: `'ok'` / `'vazio'` / `'erro_ssh'` / `'erro'`

**Flags de `main()`:**

| Flag | Padrão | Descrição |
|---|---|---|
| `--tamanho N` | 200 | Registros por ciclo (antes era 2000) |
| `--continuar` | off | Ativa o loop infinito |
| `--pausa N` | 30 | Segundos entre ciclos |
| `--max-erros N` | 3 | Erros consecutivos antes de reconectar SSH |

### 9.3 Lógica de Segurança do Loop

```
resultado = _executar_um_ciclo(tamanho)

'ok'       → erros_seguidos = 0  |  aguarda --pausa segundos
'vazio'    → aguarda 5 min (300s), verifica novamente
'erro_ssh' → kill SSH  |  backoff = min(120, 30 × erros_seguidos)
'erro'     → erros_seguidos++  |  aguarda --pausa segundos

erros_seguidos >= max_erros:
  → kill SSH + aguarda 60s + reconecta VPN
  → reinicia contagem de erros

Ctrl+C → graceful: kill SSH + processa resultado parcial se CSV existir
```

### 9.4 Como Usar

**Modo padrão — produção (recomendado):**
```bat
EXECUTAR.bat
```
Equivale a `executar_automatico.py --continuar --tamanho 200 --pausa 30 --max-erros 3`.

**Com pausa maior entre ciclos:**
```bat
EXECUTAR.bat --tamanho 100 --pausa 60
```

**Ciclo único sem loop (para testes):**
```bat
EXECUTAR.bat --tamanho 50
```

### 9.5 Estimativa de Tempo para Zerar Pendentes

Com ~34.000 pendentes, lotes de 200, pausa de 30s e ~2-3 min/ciclo de API:

- Ciclos necessários: ~170
- Tempo estimado: **5–9 horas contínuas**

---

## 10. Estado Final e Próximas Ações

### Estado em 2026-04-08

| Item | Status |
|---|---|
| Infraestrutura da macro | ✅ Pronta |
| Credenciais e segurança | ✅ Organizadas (.env, .gitignore) |
| Testes de viabilidade | ✅ 9/9 aprovados |
| Primeiro ciclo real (lote 50) | ✅ 7 consolidados, 21 excluídos |
| Ciclo completo (lote 2000) | ✅ Rodando em background |
| Modo contínuo (loop forever) | ✅ Commit `7c220b0` |
| Documentação profissional | ✅ README, DIARIO_TECNICO, EXECUTAR.bat |

### Próximas ações

1. **Rodar em modo contínuo** até zerar os ~34.000 pendentes:
   ```bat
   EXECUTAR.bat
   ```

2. **Integrar com pipeline operacional:** após carga diária do staging
   (`pipeline_carga_operacional_fornecedor2.py`), executar `EXECUTAR.bat`
   para processar os novos pendentes inseridos automaticamente.

3. **Agendar execução** via Agendador de Tarefas do Windows para rodar
   diariamente após a carga do ETL.
