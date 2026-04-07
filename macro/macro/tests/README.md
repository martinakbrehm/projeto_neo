# Tests — Macro Pipeline Neo Energia

Testes automatizados para validar a confiabilidade e integridade do pipeline.

## Estrutura

```
tests/
├── conftest.py                  # Fixtures compartilhadas e configuração de marcadores
├── test_interpretar_resposta.py # Unit tests: mapeamento de respostas da API
├── test_processar_retorno.py    # Unit tests: agregação por macro_id, normalização, dry-run
├── test_integracao_banco.py     # Integration tests: banco real (tabelas, dados, queries)
└── test_conectividade.py        # Connectivity tests: VPN, SSH, API Neo Energia
```

## Tipos de teste

| Marcador         | Requer                   | Quando rodar                          |
|------------------|--------------------------|---------------------------------------|
| *(sem marcador)* | Nada (100% unitário)     | Sempre — a cada alteração de código   |
| `integracao`     | Banco de dados acessível | Antes de subir ao repositório         |
| `conectividade`  | VPN + SSH + API ativos   | Antes de iniciar um ciclo de produção |

## Como rodar

```powershell
# Instalar dependências de teste (uma vez)
.venv\Scripts\pip install pytest pytest-timeout

# ── Testes unitários (sem banco, sem VPN) ─────────────────────────────────
cd macro\macro
python -m pytest tests\test_interpretar_resposta.py tests\test_processar_retorno.py -v

# ── Todos os testes unitários ─────────────────────────────────────────────
python -m pytest -m "not integracao and not conectividade" -v

# ── Testes de integração (banco) ──────────────────────────────────────────
python -m pytest -m integracao -v

# ── Testes de conectividade (VPN + SSH + API) ─────────────────────────────
python -m pytest -m conectividade -v

# ── Todos os testes ───────────────────────────────────────────────────────
python -m pytest -v

# ── Com timeout por teste ─────────────────────────────────────────────────
python -m pytest --timeout=30 -v
```

## O que cada arquivo testa

### `test_interpretar_resposta.py` — Mapeamento da API
- Todos os `CodigoRetorno` documentados (000–011) mapeiam para o status correto
- Erros de comunicação (`ERRO_RETRY`, `LIMIT_EXCEEDED`, timeout) → `reprocessar`
- Respostas vazias / None → `reprocessar`
- JSON malformado cai no fallback de texto
- Tradução `excluir` (banco) → `excluido` (ENUM) aplicada corretamente
- Comportamento idêntico com e sem mapa carregado do banco

### `test_processar_retorno.py` — Lógica de processamento
- `normalizar_cpf` / `normalizar_uc` com máscaras, espaços e zeros
- `construir_indice_meta` indexa corretamente CPF+UC → macro_id
- **Agregação por macro_id**: cliente com múltiplas UCs recebe o melhor status
  - Se uma UC confirmar titularidade (003) → `consolidado`, mesmo que outras retornem 000
  - Prioridade: `consolidado` > `reprocessar` > `excluido` > `pendente`
- Registros sem match no lote (`sem_match`)
- Registros do lote sem resultado (`recuperados`)
- `dry_run=True` calcula stats mas não executa UPDATE
- Detecção automática das colunas por nome

### `test_integracao_banco.py` — Banco de dados
- Conexão estabelecida
- Tabela `respostas` tem todos os 12 códigos (0–11) e status válidos
- Mapa do banco é consistente com o fallback hardcoded do código
- `tabela_macros` tem todos os campos obrigatórios e ENUM correto
- `cliente_uc` existe e tem dados
- `cliente_origem` existe (migration 20260406 aplicada)
- Query de prioridade retorna registros e na ordem correta

### `test_conectividade.py` — Infraestrutura
- `plink.exe` presente e executável
- Porta 5000 localmente acessível (túnel ativo)
- API responde HTTP 200 com JSON válido
- SSH echo ao servidor funciona
- VPN estabelecida no servidor remoto
- Host interno alcançável via VPN
- API aceita as 3 distribuidoras (celpe, coelba, cosern)
- Campos obrigatórios na resposta da API presentes

## Antes de rodar em produção

```powershell
# Checklist completo pré-ciclo:
python -m pytest tests\test_conectividade.py -v -m conectividade
python -m pytest tests\test_integracao_banco.py -v -m integracao
```

Se tudo verde, pode rodar:
```powershell
python executar_automatico.py --tamanho 2000
```
