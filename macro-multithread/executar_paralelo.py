"""
executar_paralelo.py
====================
Orquestrador de múltiplos workers para a macro de titularidade Neo Energia.

Uso:
    python executar_paralelo.py               # 4 workers, porta base 5000
    python executar_paralelo.py --workers 2   # 2 workers
    python executar_paralelo.py --workers 4 --porta-base 5000

O script:
  1. Lê o arquivo de entrada (ENTRADA_CONFIG.txt ou --entrada)
  2. Subtrai linhas já processadas (resultado_lote.csv existentes)
  3. Divide o pendente em N fatias
  4. Cria/atualiza dirs workers/worker_500X/ com scripts e .env isolados
  5. Dispara N processos em paralelo
  6. Exibe progresso consolidado a cada 30s
  7. Ao concluir, mescla todos resultado_lote.csv em um único

Dependências: mesmas de macro_entrada_local (requirements.txt)
"""

import argparse
import os
import shutil
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

# --- Caminhos base ------------------------------------------------------------
REPO_DIR   = Path(__file__).parent
BASE_DIR   = REPO_DIR / "macro_entrada_local"
WORKERS_DIR = REPO_DIR / "workers"

# Arquivos que cada worker precisa (copiados do BASE_DIR)
ARQUIVOS_WORKER = [
    "executar_automatico_local.py",
    "consulta_contrato.py",
    "plink.exe",
    ".env",
    "requirements.txt",
]

# --- Helpers ------------------------------------------------------------------

def ler_entrada(caminho_entrada: Path) -> pd.DataFrame:
    """Lê o CSV de entrada."""
    return pd.read_csv(caminho_entrada, dtype=str)


def ler_processados() -> pd.DataFrame:
    """Junta todos resultado_lote.csv existentes (base + workers + arquivo/) para deduplicação.
    Lê um arquivo por vez para economizar memória."""
    import glob as _glob
    chaves = set()

    def _processar(path):
        try:
            df = pd.read_csv(path, dtype=str, usecols=["cpf", "codigo cliente", "resposta"])
            df.columns = [c.strip() for c in df.columns]
            df = df[df["resposta"].notna() & (df["resposta"].str.strip() != "")]
            for cpf, cod in zip(df["cpf"].str.strip(), df["codigo cliente"].str.strip()):
                chaves.add((cpf, cod))
        except Exception:
            pass

    # worker base
    _processar(BASE_DIR / "dados" / "resultado_lote.csv")
    # workers orquestrados - resultado atual + arquivos históricos
    if WORKERS_DIR.exists():
        for wd in sorted(WORKERS_DIR.iterdir()):
            _processar(wd / "dados" / "resultado_lote.csv")
            for arq in _glob.glob(str(wd / "dados" / "arquivo" / "*.csv")):
                _processar(arq)
    # macro_entrada_local_2 legado
    _processar(REPO_DIR / "macro_entrada_local_2" / "dados" / "resultado_lote.csv")

    if not chaves:
        return pd.DataFrame(columns=["cpf", "codigo cliente", "empresa"])
    df_out = pd.DataFrame(list(chaves), columns=["cpf", "codigo cliente"])
    df_out["empresa"] = ""
    return df_out


def calcular_pendentes(df_entrada: pd.DataFrame, df_proc: pd.DataFrame) -> pd.DataFrame:
    """Remove de df_entrada os pares já processados com resposta válida."""
    if df_proc.empty:
        return df_entrada
    chave_proc = set(zip(df_proc["cpf"].str.strip(), df_proc["codigo cliente"].str.strip()))
    mask = ~df_entrada.apply(
        lambda r: (str(r["cpf"]).strip(), str(r["codigo cliente"]).strip()) in chave_proc,
        axis=1
    )
    return df_entrada[mask].reset_index(drop=True)


def setup_worker(worker_dir: Path, porta: int, slice_df: pd.DataFrame):
    """Cria/atualiza o diretório de um worker."""
    worker_dir.mkdir(parents=True, exist_ok=True)
    (worker_dir / "dados").mkdir(exist_ok=True)
    (worker_dir / "dados" / "arquivo").mkdir(exist_ok=True)

    # Copia scripts
    for arq in ARQUIVOS_WORKER:
        src = BASE_DIR / arq
        if src.exists():
            shutil.copy2(src, worker_dir / arq)

    # .env com porta correta
    env_src = BASE_DIR / ".env"
    env_txt = env_src.read_text(encoding="utf-8")
    import re
    env_txt = re.sub(r"LOCAL_PORT\s*=\s*\d+", f"LOCAL_PORT={porta}", env_txt)
    (worker_dir / ".env").write_text(env_txt, encoding="utf-8")

    # Arquivo de entrada do worker
    entrada_path = worker_dir / "dados" / "entrada.csv"
    slice_df.to_csv(entrada_path, index=False)

    # ENTRADA_CONFIG.txt
    (worker_dir / "ENTRADA_CONFIG.txt").write_text("dados\\entrada.csv", encoding="utf-8")

    # ENTRADA_REGISTRO.txt (preserva se existe - permite retomar)
    reg = worker_dir / "ENTRADA_REGISTRO.txt"
    if not reg.exists():
        reg.write_text("", encoding="utf-8")


# --- Monitor de um worker ----------------------------------------------------

SEM_PROGRESSO_MAX = 10  # minutos sem novo lote para considerar travado

class WorkerMonitor(threading.Thread):
    def __init__(self, worker_id: int, porta: int, proc: subprocess.Popen, worker_dir: Path,
                 python_exe: str):
        super().__init__(daemon=True)
        self.worker_id    = worker_id
        self.porta        = porta
        self.proc         = proc
        self.worker_dir   = Path(worker_dir)
        self.python_exe   = python_exe
        self.linhas_ok    = 0
        self.erros        = 0
        self.ultimo_log   = ""
        self.finalizado   = False
        self.reinicios    = 0
        self._lock        = threading.Lock()
        self._ultimo_arq_count = self._contar_arquivos()
        self._ultimo_progresso = time.time()

    def _contar_arquivos(self) -> int:
        arq_dir = self.worker_dir / "dados" / "arquivo"
        if arq_dir.exists():
            return len(list(arq_dir.glob("*.csv")))
        return 0

    def verificar_progresso(self):
        """Retorna True se houve progresso desde a última verificação."""
        atual = self._contar_arquivos()
        if atual > self._ultimo_arq_count:
            self._ultimo_arq_count = atual
            self._ultimo_progresso = time.time()
            return True
        return False

    def esta_travado(self) -> bool:
        if self.finalizado:
            return False
        self.verificar_progresso()
        minutos_parado = (time.time() - self._ultimo_progresso) / 60
        return minutos_parado >= SEM_PROGRESSO_MAX

    def reiniciar(self):
        """Mata o processo atual e reinicia o worker."""
        with self._lock:
            self.reinicios += 1
            print(f"\n[WATCHDOG] Worker {self.worker_id+1} (:{self.porta}) "
                  f"travado - reiniciando (#{self.reinicios})...")

            # Mata processo atual e filhos (plink/ssh)
            try:
                self.proc.terminate()
                self.proc.wait(timeout=5)
            except Exception:
                try:
                    self.proc.kill()
                except Exception:
                    pass

            # Mata túnel SSH da porta desse worker
            try:
                r = subprocess.run(["netstat", "-ano"], capture_output=True, text=True)
                for line in r.stdout.splitlines():
                    if f":{self.porta}" in line and "LISTENING" in line:
                        pid = line.strip().split()[-1]
                        subprocess.run(["taskkill", "/PID", pid, "/F"], capture_output=True)
            except Exception:
                pass

            time.sleep(3)

            # Relança o worker
            env = os.environ.copy()
            env["LOCAL_PORT"] = str(self.porta)
            new_proc = subprocess.Popen(
                [self.python_exe, str(self.worker_dir / "executar_automatico_local.py"),
                 "--continuar", "--pausa", "5"],
                cwd=str(self.worker_dir),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                env=env,
            )
            self.proc = new_proc
            self._ultimo_progresso = time.time()
            self._ultimo_arq_count = self._contar_arquivos()
            self.finalizado = False
            print(f"  > Worker {self.worker_id+1} (:{self.porta}) reiniciado PID={new_proc.pid}")

            # Relança a thread de leitura do stdout
            t = threading.Thread(target=self._ler_stdout, daemon=True)
            t.start()

    def _ler_stdout(self):
        my_proc = self.proc  # captura referência no início - não muda se houver reinício
        for linha in my_proc.stdout:
            self.ultimo_log = linha.rstrip()
            if "Status HTTP: 200" in linha:
                self.linhas_ok += 1
            elif "ERRO" in linha.upper() or "Timeout" in linha:
                self.erros += 1
        my_proc.wait()
        # Só marca finalizado se este ainda é o processo atual (não foi reiniciado)
        if self.proc is my_proc:
            self.finalizado = True

    def run(self):
        self._ler_stdout()


# --- Progresso + Watchdog ----------------------------------------------------

def exibir_progresso(monitors: list[WorkerMonitor], total_por_worker: list[int]):
    while True:
        time.sleep(30)
        print("\n" + "="*70)
        print(f"[PROGRESSO] {datetime.now().strftime('%H:%M:%S')}")
        total_ok = 0
        for m in monitors:
            r = m.worker_dir / "dados" / "resultado_lote.csv"
            linhas_csv = 0
            if r.exists():
                try:
                    linhas_csv = sum(1 for _ in open(r, encoding="utf-8")) - 1
                except Exception:
                    pass
            total_ok += linhas_csv
            minutos_parado = (time.time() - m._ultimo_progresso) / 60
            if m.finalizado:
                status = "[OK] CONCLUÍDO"
            elif minutos_parado >= SEM_PROGRESSO_MAX:
                status = f"[WARN]  TRAVADO ({minutos_parado:.0f}min)"
            else:
                status = f"[RUNNING] rodando ({minutos_parado:.0f}min sem novo lote)"
            pct = f"{linhas_csv/total_por_worker[m.worker_id]*100:.1f}%" if total_por_worker[m.worker_id] else "-"
            reinicstr = f" | reinícios: {m.reinicios}" if m.reinicios else ""
            print(f"  Worker {m.worker_id+1} (:{m.porta}) {status} - {linhas_csv}/{total_por_worker[m.worker_id]} ({pct}){reinicstr}")

            # Watchdog: reinicia se travado
            if m.esta_travado():
                m.reiniciar()

        print(f"  TOTAL: {total_ok} linhas processadas")
        print("="*70)
        if all(m.finalizado for m in monitors):
            break


# --- Merge final ------------------------------------------------------------

def merge_resultados(monitors: list[WorkerMonitor], saida: Path):
    frames = []
    # resultado existente na base (worker 1 legado)
    r_base = BASE_DIR / "dados" / "resultado_lote.csv"
    if r_base.exists():
        frames.append(pd.read_csv(r_base, dtype=str))

    for m in monitors:
        r = m.worker_dir / "dados" / "resultado_lote.csv"
        if r.exists():
            frames.append(pd.read_csv(r, dtype=str))

    if not frames:
        print("[AVISO] Nenhum resultado encontrado para mesclar.")
        return

    merged = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["cpf", "codigo cliente"])
    merged.to_csv(saida, index=False)
    print(f"\n[OK] Merge concluido -> {saida} ({len(merged)} linhas)")


# --- Main --------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Orquestrador paralelo de workers Neo Energia")
    parser.add_argument("--workers",    type=int, default=4,    help="Número de workers (padrão: 4)")
    parser.add_argument("--porta-base", type=int, default=5000, help="Porta do worker 1 (padrão: 5000)")
    parser.add_argument("--entrada",    type=str, default=None, help="Caminho do CSV de entrada (padrão: lê ENTRADA_CONFIG.txt)")
    parser.add_argument("--sem-dedup",  action="store_true",    help="Não subtrai já-processados antes de dividir")
    parser.add_argument("--so-merge",   action="store_true",    help="Apenas mescla resultados existentes e sai")
    args = parser.parse_args()

    N = args.workers
    PORTA_BASE = args.porta_base
    portas = [PORTA_BASE + i for i in range(N)]

    # -- Saída merged ---------------------------------------------------------
    saida_merge = BASE_DIR / "dados" / "resultado_lote_merged.csv"

    if args.so_merge:
        dirs = [WORKERS_DIR / f"worker_{p}" for p in portas]
        monitors_fake = [type("M", (), {"worker_dir": d, "porta": p, "worker_id": i, "finalizado": True})()
                         for i, (d, p) in enumerate(zip(dirs, portas))]
        merge_resultados(monitors_fake, saida_merge)
        return

    # -- Lê entrada -----------------------------------------------------------
    if args.entrada:
        entrada_path = Path(args.entrada)
    else:
        cfg = BASE_DIR / "ENTRADA_CONFIG.txt"
        rel = cfg.read_text(encoding="utf-8").strip()
        entrada_path = BASE_DIR / rel

    print(f"[INFO] Entrada: {entrada_path}")
    df_entrada = ler_entrada(entrada_path)
    print(f"[INFO] Total entrada: {len(df_entrada)} linhas")

    # -- Deduplica já-processados ----------------------------------------------
    if not args.sem_dedup:
        df_proc = ler_processados()
        print(f"[INFO] Já processados (com resposta válida): {len(df_proc)}")
        df_pendente = calcular_pendentes(df_entrada, df_proc)
        print(f"[INFO] Pendentes após dedup: {len(df_pendente)}")
    else:
        df_pendente = df_entrada

    if df_pendente.empty:
        print("[INFO] Nenhuma linha pendente. Encerrando.")
        return

    # -- Divide em N fatias ----------------------------------------------------
    total = len(df_pendente)
    tamanho = (total + N - 1) // N
    slices = [df_pendente.iloc[i*tamanho:(i+1)*tamanho].reset_index(drop=True) for i in range(N)]
    slices = [s for s in slices if not s.empty]
    N_real = len(slices)
    portas = portas[:N_real]

    print(f"\n[INFO] Dividindo {total} linhas em {N_real} workers:")
    for i, (s, p) in enumerate(zip(slices, portas)):
        print(f"  Worker {i+1} (porta {p}): {len(s)} linhas")

    # -- Setup de cada worker --------------------------------------------------
    worker_dirs = []
    for i, (s, p) in enumerate(zip(slices, portas)):
        wd = WORKERS_DIR / f"worker_{p}"
        print(f"[SETUP] Worker {i+1} -> {wd}")
        setup_worker(wd, p, s)
        worker_dirs.append(wd)

    # -- Lança processos -------------------------------------------------------
    python_exe = sys.executable
    processos  = []
    monitors   = []
    total_por_worker = [len(s) for s in slices]

    print(f"\n[START] Iniciando {N_real} workers em paralelo...\n")
    for i, (wd, p) in enumerate(zip(worker_dirs, portas)):
        env = os.environ.copy()
        # garante que o worker carrega o .env correto via CWD
        env["LOCAL_PORT"] = str(p)

        proc = subprocess.Popen(
            [python_exe, str(wd / "executar_automatico_local.py"), "--continuar", "--pausa", "5"],
            cwd=str(wd),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=env,
        )
        monitor = WorkerMonitor(i, p, proc, wd, python_exe)
        monitor.start()
        processos.append(proc)
        monitors.append(monitor)
        print(f"  > Worker {i+1} (:{p}) PID={proc.pid}")
        time.sleep(8)  # escalonamento para evitar conflito de túnel na inicialização

    # -- Monitor de progresso --------------------------------------------------
    prog_thread = threading.Thread(
        target=exibir_progresso,
        args=(monitors, total_por_worker),
        daemon=True
    )
    prog_thread.start()

    # -- Aguarda todos ---------------------------------------------------------
    print("\n[INFO] Workers em execução. Aguardando conclusão...\n")
    # Espera pelos monitors (não pelos procs originais) para suportar reinícios do watchdog
    while not all(m.finalizado for m in monitors):
        time.sleep(5)

    prog_thread.join(timeout=5)

    # -- Merge final -----------------------------------------------------------
    print("\n[INFO] Todos os workers concluídos. Mesclando resultados...")
    merge_resultados(monitors, saida_merge)
    print("\n[OK] Pipeline paralelo finalizado.")


if __name__ == "__main__":
    main()
