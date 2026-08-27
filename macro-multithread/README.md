# Macro Multithread

## Descricao
Scripts de execução multithread/paralela da macro de entrada para processar múltiplas instâncias simultaneamente.

Otimizado para rodar 4 macros em paralelo com workers independentes para máxima eficiência.

## Scripts

### 1. rodar_4_macros_paralelo.py
Executa 4 instâncias da macro entrada em paralelo.

```bash
python rodar_4_macros_paralelo.py
```

**Funcionalidades:**
- 4 workers simultâneos
- Processamento paralelo de dados
- Monitoramento de status

### 2. executar_paralelo.py
Executor genérico de processamento paralelo.

```bash
python executar_paralelo.py
```

**Uso flexível para multiplos tipos de processamento paralelo.**

### 3. rodar_macro_continuo.py
Executa a macro continuamente (monitoramento e re-execução).

```bash
python rodar_macro_continuo.py
```

**Funcionalidades:**
- Monitoramento contínuo
- Re-execução automática
- Tratamento de falhas

## Performance
- 4 macros em paralelo = ~4x melhor throughput
- Cada worker processa independentemente
- Sem contenção de recursos

## Requisitos
- Python 3.8+
- Threading/Multiprocessing disponível
- Workers em portas: 5000, 5001, 5002, 5003

## Uso em Produção
Para máxima eficiência, execute `rodar_4_macros_paralelo.py` como serviço de background.
