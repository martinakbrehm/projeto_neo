"""
rodar_4_macros_paralelo.py
==========================
Roda 4 macros em paralelo até processar todos os pendentes.
"""

import subprocess
import time
import pandas as pd
from pathlib import Path
from datetime import datetime
import threading

MACRO_DIR = Path("macro_entrada_local")
ENTRADA = MACRO_DIR / "dados" / "entrada_pendentes_total.csv"
RESULTADO = MACRO_DIR / "dados" / "resultado_lote.csv"

processos = []
lock = threading.Lock()

def contar_pendentes():
    """Conta quantos registros da entrada ainda não foram processados."""
    if not ENTRADA.exists() or not RESULTADO.exists():
        return None

    try:
        df_ent = pd.read_csv(ENTRADA, dtype=str)
        df_res = pd.read_csv(RESULTADO, dtype=str)

        df_ent['key'] = df_ent['cpf'].str.strip() + '|' + df_ent['codigo cliente'].str.strip()
        df_res['key'] = df_res['cpf'].str.strip() + '|' + df_res['codigo cliente'].str.strip()

        entrada_keys = set(df_ent['key'])
        resultado_keys = set(df_res['key'])
        pendentes = len(entrada_keys - resultado_keys)
        total = len(df_ent)

        return pendentes, total
    except Exception:
        return None

def rodar_macro_worker(worker_id):
    """Executa macro em uma thread."""
    cmd = [
        "python",
        "executar_automatico_local.py",
        "--continuar",
        "--tamanho", "1000"
    ]

    print(f"  [Worker {worker_id}] Iniciando...")
    proc = subprocess.Popen(cmd, cwd=str(MACRO_DIR))

    with lock:
        processos.append((worker_id, proc))

    proc.wait()

    with lock:
        processos.remove((worker_id, proc))

    print(f"  [Worker {worker_id}] Concluído")

def main():
    print("=" * 70)
    print("RODAR 4 MACROS EM PARALELO - Dia 15-06 (111.555 pendentes)")
    print("=" * 70)

    ciclo = 0
    while True:
        ciclo += 1
        print(f"\n[CICLO {ciclo}] {datetime.now().strftime('%H:%M:%S')}")

        # Verificar pendentes
        resultado = contar_pendentes()
        if resultado:
            pendentes, total = resultado
            pct = (total - pendentes) / total * 100
            print(f"  Status: {total - pendentes:,} / {total:,} ({pct:.1f}%)")

            if pendentes == 0:
                print("\n[SUCESSO] Todos os pendentes foram processados!")
                print(f"  Total: {total:,} registros")
                print(f"  Ciclos: {ciclo - 1}")
                break

        # Iniciar 4 workers em paralelo
        print(f"  Iniciando 4 workers em paralelo...")
        threads = []
        for i in range(1, 5):
            t = threading.Thread(target=rodar_macro_worker, args=(i,))
            t.start()
            threads.append(t)

        # Aguardar conclusão de todos os workers
        for t in threads:
            t.join()

        print(f"  Todos os 4 workers concluídos. Aguardando 5s...")
        time.sleep(5)

if __name__ == "__main__":
    main()
