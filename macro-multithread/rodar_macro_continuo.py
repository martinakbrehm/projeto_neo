"""
rodar_macro_continuo.py
=======================
Roda a macro em loop contínuo até processar todos os pendentes do dia 15-06.
"""

import subprocess
import time
import pandas as pd
from pathlib import Path
from datetime import datetime

MACRO_DIR = Path("macro_entrada_local")
ENTRADA = MACRO_DIR / "dados" / "entrada_pendentes_total.csv"
RESULTADO = MACRO_DIR / "dados" / "resultado_lote.csv"

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
    except Exception as e:
        print(f"Erro ao contar pendentes: {e}")
        return None

def rodar_macro():
    """Executa a macro uma vez."""
    cmd = [
        "python",
        "executar_automatico_local.py",
        "--continuar",
        "--tamanho", "1000"
    ]

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Iniciando macro...")
    proc = subprocess.Popen(cmd, cwd=str(MACRO_DIR))
    proc.wait()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Macro concluída")

def main():
    print("=" * 70)
    print("RODAR MACRO CONTINUO - Dia 15-06 (111.555 pendentes)")
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

        # Rodar macro
        rodar_macro()

        # Pausa antes do próximo ciclo
        print(f"  Aguardando 10s antes do próximo ciclo...")
        time.sleep(10)

if __name__ == "__main__":
    main()
