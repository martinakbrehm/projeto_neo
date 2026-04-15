"""
Painel de controle  -  Neo Energia Orquestrador Macro
Botão liga/desliga para o modo contínuo de consulta de titularidade.

Uso:
    python painel.py
    (ou: PAINEL.bat)
"""
import tkinter as tk
from tkinter import scrolledtext, font as tkfont, ttk
import subprocess
import threading
import queue
import signal
import os
import sys
import shutil
import time
import re
from pathlib import Path

# 
# Constantes
# 
HERE       = Path(__file__).parent
VENV_PY    = HERE / ".venv" / "Scripts" / "python.exe"
SCRIPT     = HERE / "executar_automatico.py"

# Caminhos para salvar resultados no banco apos parada
RESULTADO_CSV = HERE.parent / "dados" / "resultado_lote.csv"
PROJETO_DIR   = HERE.parents[1]
ETL_RETORNO   = PROJETO_DIR / "etl" / "load" / "macro" / "04_processar_retorno_macro.py"
_sys_python   = shutil.which("python") or shutil.which("python3") or sys.executable

COR_BG      = "#1e1e2e"  # fundo geral
COR_PAINEL  = "#2a2a3e"  # painel interno
COR_VERDE   = "#40c057"  # botão ON
COR_CINZA   = "#495057"  # botão OFF
COR_HOVER_V = "#2f9e44"
COR_HOVER_C = "#343a40"
COR_TEXTO   = "#ced4da"
COR_LOG_BG  = "#12121f"
COR_LOG_FG  = "#d0d0d0"
COR_TITULO  = "#74c0fc"
COR_AVISO   = "#ffd43b"
COR_ERRO    = "#ff6b6b"
COR_OK      = "#69db7c"
COR_PARANDO = "#e67700"  # laranja - aguardando fim do lote
COR_INPUT_BG= "#2c2c42"
COR_INPUT_FG= "#e0e0e0"

# 
# Classe principal
# 
class PainelMacro:
    def __init__(self, root: tk.Tk):
        self.root       = root
        self.processo   = None
        self.rodando    = False
        self.parando    = False  # True enquanto aguarda o lote atual terminar
        self._q         = queue.Queue()
        self._thread    = None

        # contadores
        self.ciclos     = 0
        self.ok         = 0
        self.erros      = 0
        self.inicio     = None

        self._construir_ui()
        self._poll_queue()           # inicia polling do log
        self._atualizar_timer()      # inicia timer de tempo rodando

        root.protocol("WM_DELETE_WINDOW", self._fechar)

    #  UI 

    def _construir_ui(self):
        self.root.title("Neo Energia  -  Orquestrador Macro")
        self.root.configure(bg=COR_BG)
        self.root.resizable(True, True)
        self.root.minsize(620, 540)

        f_titulo = tkfont.Font(family="Segoe UI", size=13, weight="bold")
        f_btn    = tkfont.Font(family="Segoe UI", size=28, weight="bold")
        f_status = tkfont.Font(family="Segoe UI", size=11, weight="bold")
        f_label  = tkfont.Font(family="Segoe UI", size=9)
        f_mono   = tkfont.Font(family="Consolas",  size=9)

        #  Título 
        tk.Label(
            self.root, text="NEO ENERGIA  -  Orquestrador Macro",
            bg=COR_BG, fg=COR_TITULO, font=f_titulo, pady=10
        ).pack(fill=tk.X)

        #  Botão toggle 
        frm_btn = tk.Frame(self.root, bg=COR_BG, pady=6)
        frm_btn.pack()

        self.btn = tk.Button(
            frm_btn,
            text="DESLIGADO",
            font=f_btn,
            bg=COR_CINZA, fg="white",
            activebackground=COR_HOVER_C, activeforeground="white",
            relief="flat", bd=0,
            padx=50, pady=18,
            cursor="hand2",
            command=self._toggle
        )
        self.btn.pack()
        self.btn.bind("<Enter>", self._btn_hover)
        self.btn.bind("<Leave>", self._btn_leave)

        #  Status 
        self.var_status = tk.StringVar(value="Aguardando...")
        tk.Label(
            self.root, textvariable=self.var_status,
            bg=COR_BG, fg=COR_TEXTO, font=f_status, pady=2
        ).pack()

        #  Configurações 
        frm_cfg = tk.Frame(self.root, bg=COR_PAINEL, padx=16, pady=10)
        frm_cfg.pack(fill=tk.X, padx=20, pady=(8, 4))

        self._label_entry(frm_cfg, "Lote (registros):", "200",  0, "var_tam")
        self._label_entry(frm_cfg, "Pausa (segundos):", "30",   1, "var_pausa")
        self._label_entry(frm_cfg, "Max erros seguidos:", "3",  2, "var_erros")

        #  Contadores 
        frm_stats = tk.Frame(self.root, bg=COR_BG)
        frm_stats.pack(fill=tk.X, padx=20, pady=4)

        self.var_ciclos = tk.StringVar(value="Ciclos: 0")
        self.var_ok     = tk.StringVar(value="OK: 0")
        self.var_err    = tk.StringVar(value="Erros: 0")
        self.var_tempo  = tk.StringVar(value="Tempo: --:--:--")

        for var, cor in [
            (self.var_ciclos, COR_TEXTO),
            (self.var_ok,     COR_OK),
            (self.var_err,    COR_ERRO),
            (self.var_tempo,  COR_AVISO),
        ]:
            tk.Label(
                frm_stats, textvariable=var, bg=COR_BG, fg=cor,
                font=tkfont.Font(family="Segoe UI", size=10, weight="bold"),
                padx=14
            ).pack(side=tk.LEFT)

        #  Log 
        frm_log = tk.Frame(self.root, bg=COR_BG)
        frm_log.pack(fill=tk.BOTH, expand=True, padx=20, pady=(4, 14))

        tk.Label(
            frm_log, text="Log de saída", bg=COR_BG, fg=COR_TEXTO,
            font=f_label, anchor="w"
        ).pack(fill=tk.X)

        self.log = tk.Text(
            frm_log,
            bg=COR_LOG_BG, fg=COR_LOG_FG,
            font=f_mono,
            relief="flat", bd=0,
            state=tk.DISABLED,
            wrap=tk.WORD,
        )
        sb = tk.Scrollbar(frm_log, command=self.log.yview)
        self.log.configure(yscrollcommand=sb.set)
        sb.pack(side=tk.RIGHT, fill=tk.Y)
        self.log.pack(fill=tk.BOTH, expand=True)

        # tags de cor no log
        self.log.tag_configure("ok",     foreground=COR_OK)
        self.log.tag_configure("erro",   foreground=COR_ERRO)
        self.log.tag_configure("aviso",  foreground=COR_AVISO)
        self.log.tag_configure("titulo", foreground=COR_TITULO)
        self.log.tag_configure("normal", foreground=COR_LOG_FG)

    def _label_entry(self, parent, label, default, col, attr):
        f = tkfont.Font(family="Segoe UI", size=9)
        tk.Label(
            parent, text=label, bg=COR_PAINEL, fg=COR_TEXTO, font=f
        ).grid(row=0, column=col * 2, sticky="e", padx=(12, 4))
        var = tk.StringVar(value=default)
        setattr(self, attr, var)
        tk.Entry(
            parent, textvariable=var, width=6,
            bg=COR_INPUT_BG, fg=COR_INPUT_FG,
            insertbackground=COR_INPUT_FG,
            relief="flat", font=f
        ).grid(row=0, column=col * 2 + 1, padx=(0, 8))

    #  Toggle ON / OFF 

    def _toggle(self):
        if self.parando:
            return  # ja enviou sinal, aguardando lote terminar
        if self.rodando:
            self._parar()
        else:
            self._iniciar()

    def _iniciar(self):
        try:
            tamanho = int(self.var_tam.get())
            pausa   = int(self.var_pausa.get())
            max_err = int(self.var_erros.get())
        except ValueError:
            self._log_append("Valores de configuração inválidos.\n", "erro")
            return

        if not VENV_PY.exists() and not _sys_python:
            self._log_append(f"Python nao encontrado: {VENV_PY}\n", "erro")
            return
        if not SCRIPT.exists():
            self._log_append(f"Script nao encontrado: {SCRIPT}\n", "erro")
            return

        # Usa o Python do sistema (aprovado pelo AppLocker) em vez do venv,
        # pois o AppLocker bloqueia DLLs do numpy dentro do venv.
        python_exe = str(VENV_PY) if VENV_PY.exists() else _sys_python
        cmd = [
            python_exe, "-u", str(SCRIPT),
            "--continuar",
            "--tamanho", str(tamanho),
            "--pausa",   str(pausa),
            "--max-erros", str(max_err),
        ]

        self._log_append(
            f">>> Iniciando: {' '.join(cmd[2:])}\n", "titulo"
        )

        self.ciclos = 0
        self.ok     = 0
        self.erros  = 0
        self.inicio = time.time()
        self._atualizar_contadores()

        try:
            self.processo = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd=str(HERE),
                encoding="utf-8",
                errors="replace",
                env={**os.environ, "PYTHONIOENCODING": "utf-8"},
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW,
            )
        except Exception as e:
            self._log_append(f"Erro ao iniciar processo: {e}\n", "erro")
            return

        self.rodando = True
        self._atualizar_btn()

        # thread de leitura do stdout
        self._thread = threading.Thread(
            target=self._ler_stdout, daemon=True
        )
        self._thread.start()

    def _matar_processo_tree(self):
        """Mata o processo e todos os filhos (taskkill /T /F no Windows)."""
        if not self.processo:
            return
        pid = self.processo.pid
        try:
            import subprocess as _sp
            _sp.run(
                ["taskkill", "/T", "/F", "/PID", str(pid)],
                capture_output=True
            )
        except Exception:
            try:
                self.processo.terminate()
            except Exception:
                pass

    def _salvar_no_banco(self, motivo="parada"):
        """Roda 04_processar_retorno_macro.py se resultado_lote.csv existir.
        Executa em thread separada para nao travar a UI.
        """
        if not RESULTADO_CSV.exists() or RESULTADO_CSV.stat().st_size < 10:
            self._log_append(f"[BANCO] Nenhum resultado para salvar ({motivo}).\n", "aviso")
            return
        if not ETL_RETORNO.exists():
            self._log_append(f"[BANCO] Script ETL nao encontrado: {ETL_RETORNO}\n", "erro")
            return

        self._log_append(f"[BANCO] Salvando resultados no banco ({motivo})...\n", "aviso")
        self.var_status.set("Salvando no banco...")

        def _run():
            try:
                r = subprocess.run(
                    [_sys_python, "-u", str(ETL_RETORNO)],
                    cwd=str(PROJETO_DIR),
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=120,
                )
                self._q.put(f"[BANCO] Saida ETL:\n{r.stdout[-800:]}\n" if r.stdout else "")
                if r.returncode == 0:
                    self._q.put("[BANCO] Resultados salvos no banco com sucesso.\n")
                else:
                    self._q.put(f"[BANCO][ERRO] ETL encerrou com codigo {r.returncode}\n{r.stderr[-400:]}\n")
            except subprocess.TimeoutExpired:
                self._q.put("[BANCO][ERRO] Timeout ao salvar no banco (120s).\n")
            except Exception as e:
                self._q.put(f"[BANCO][ERRO] {e}\n")

        threading.Thread(target=_run, daemon=True).start()

    def _parar(self):
        """Para todo o ciclo atual matando a arvore de processos e salva no banco."""
        if self.processo and self.processo.poll() is None:
            self._log_append(">>> Parando  -  encerrando processos...\n", "aviso")
            self._matar_processo_tree()
        self.parando = True
        self.rodando = False
        self._atualizar_btn()
        # Salva no banco o que foi processado ate agora
        self._salvar_no_banco(motivo="parada pelo usuario")

    def _ler_stdout(self):
        """Lê stdout do subprocess linha a linha e coloca na fila."""
        try:
            for line in self.processo.stdout:
                self._q.put(line)
        except Exception:
            pass
        finally:
            self.processo.wait()
            self._q.put(None)  # sinal de fim

    #  Polling da fila (roda na thread principal via after) 

    def _poll_queue(self):
        try:
            while True:
                item = self._q.get_nowait()
                if item is None:
                    # processo terminou de verdade
                    self.rodando = False
                    self.parando = False
                    self._atualizar_btn()
                    self.var_status.set("Encerrado.")
                    self._log_append(">>> Lote concluido. Processo encerrado.\n", "aviso")
                    # Salva no banco automaticamente ao final de cada ciclo
                    self._salvar_no_banco(motivo="fim de ciclo")
                else:
                    self._processar_linha(item)
        except queue.Empty:
            pass
        self.root.after(100, self._poll_queue)

    def _processar_linha(self, linha: str):
        """Classifica a linha, atualiza contadores e adiciona ao log."""
        l = linha.rstrip("\n")

        # ciclo do loop principal
        if re.search(r"CICLO #\d+", l):
            self.ciclos += 1
            self._atualizar_contadores()
            self._log_append(linha, "titulo")
            return
        if re.search(r"Ciclo #\d+ conclu|CICLO COMPLETO CONCLU", l):
            self.ok += 1
            self._atualizar_contadores()
            self._log_append(linha, "ok")
            return
        if re.search(r"Ciclo #\d+.*erros|erro_ssh|Erro inesperado|FALHA", l, re.I):
            self.erros += 1
            self._atualizar_contadores()
            self._log_append(linha, "erro")
            return

        # progresso da macro (consulta_contrato.py)
        if re.search(r"\[Linha \d+\] Enviando|Enviando:.*CPF=", l):
            self._log_append(linha, "normal")
            return
        if re.search(r"\[PROG\]|\[STATUS\]|Lote \d+ \|.*Total:", l):
            self._log_append(linha, "aviso")
            return
        if re.search(r"Salvando \d+ result|resultado.*salvo|arquivo.*criado|anexado", l, re.I):
            self._log_append(linha, "ok")
            return
        if re.search(r"Resposta:|Status HTTP:|API respond|Tunel SSH", l, re.I):
            self._log_append(linha, "aviso")
            return

        # padroes gerais
        if re.search(r"pendente|aguardando|pausa|reconnect|VPN|SSH|tunel|tunnel", l, re.I):
            self._log_append(linha, "aviso")
            return
        if re.search(r"\[OK\]|success|sucesso|consolidado|certo", l, re.I):
            self._log_append(linha, "ok")
            return
        if re.search(r"\[ERRO\]|error|fail|falha|excep", l, re.I):
            self._log_append(linha, "erro")
            return
        self._log_append(linha, "normal")

    def _log_append(self, text: str, tag: str = "normal"):
        self.log.configure(state=tk.NORMAL)
        ts = time.strftime("%H:%M:%S")
        lf = text if text.endswith("\n") else text + "\n"
        self.log.insert(tk.END, f"[{ts}] {lf}", tag)
        # mantém no máximo 2000 linhas
        linhas = int(self.log.index("end-1c").split(".")[0])
        if linhas > 2000:
            self.log.delete("1.0", f"{linhas - 1800}.0")
        self.log.see(tk.END)
        self.log.configure(state=tk.DISABLED)

    #  Helpers UI 

    def _atualizar_btn(self):
        if self.rodando:
            self.btn.configure(
                text="LIGADO",
                bg=COR_VERDE,
                activebackground=COR_HOVER_V,
                state=tk.NORMAL,
            )
            self.var_status.set("Rodando em modo continuo...")
        elif self.parando:
            self.btn.configure(
                text="PARANDO...",
                bg=COR_PARANDO,
                activebackground=COR_PARANDO,
                state=tk.DISABLED,
            )
            self.var_status.set("Aguardando o lote atual terminar...")
        else:
            self.btn.configure(
                text="DESLIGADO",
                bg=COR_CINZA,
                activebackground=COR_HOVER_C,
                state=tk.NORMAL,
            )
            if self.inicio:
                self.var_status.set("Parado.")
            else:
                self.var_status.set("Aguardando...")

    def _atualizar_contadores(self):
        self.var_ciclos.set(f"Ciclos: {self.ciclos}")
        self.var_ok.set(f"OK: {self.ok}")
        self.var_err.set(f"Erros: {self.erros}")

    def _atualizar_timer(self):
        if self.rodando and self.inicio:
            seg = int(time.time() - self.inicio)
            h, r = divmod(seg, 3600)
            m, s = divmod(r, 60)
            self.var_tempo.set(f"Tempo: {h:02d}:{m:02d}:{s:02d}")
        self.root.after(1000, self._atualizar_timer)

    def _btn_hover(self, _):
        if self.parando:
            return
        if self.rodando:
            self.btn.configure(bg=COR_HOVER_V)
        else:
            self.btn.configure(bg=COR_HOVER_C)

    def _btn_leave(self, _):
        self._atualizar_btn()

    def _fechar(self):
        if self.rodando or self.parando:
            self._matar_processo_tree()
        # espera max 5s pelo pipe fechar, depois destrói
        deadline = time.time() + 5
        def _aguardar_e_fechar():
            if self.processo and self.processo.poll() is None and time.time() < deadline:
                self.root.after(300, _aguardar_e_fechar)
            else:
                self.root.destroy()
        self.root.after(300, _aguardar_e_fechar)


# 
# Entry point
# 
if __name__ == "__main__":
    root = tk.Tk()
    app  = PainelMacro(root)
    root.mainloop()
