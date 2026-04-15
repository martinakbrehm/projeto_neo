"""
consulta_contrato.py
====================
MACRO — Consulta de titularidade na API Neo Energia via túnel SSH.

Esta é a etapa central do ciclo automatizado. Os scripts de suporte
que integram este processo com o banco de dados são:

  ANTES   (alimenta esta macro):
    etl/extraction/macro/03_buscar_lote_macro.py
      → busca lote priorizado do banco, exporta macro/dados/lote_pendente.csv

  APÓS    (processa o resultado desta macro):
    etl/load/macro/04_processar_retorno_macro.py
      → lê macro/dados/resultado_lote.csv, interpreta respostas, atualiza banco

  TRANSFORMATION (interpretação das respostas):
    etl/transformation/macro/interpretar_resposta.py

Modos de execução:
  Modo automático (chamado por executar_automatico.py):
    python consulta_contrato.py --arquivo macro/dados/lote_pendente.csv
                                --saida   macro/dados/resultado_lote.csv

  Modo manual (dialog de seleção de arquivo — comportamento original):
    python consulta_contrato.py

Orquestrador completo:
    macro/macro/executar_automatico.py  (chama os 3 passos em sequência)
"""

import argparse
import asyncio
import os.path
import json
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import shutil
from datetime import datetime
import subprocess
import socket
import sys
import threading

import httpx
import pandas as pd
import time

# Garante UTF-8 no pipe para o painel (evita UnicodeEncodeError no Windows)
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')




class JanelaControle:
    """Janela de controle para parar/encerrar a aplicação"""
    
    def __init__(self):
        self.parar_processo = False
        self.encerrar_aplicacao = False
        self.root = None
        self.thread_janela = None
        
        # Controles de tempo real
        self.inicio_processamento = None
        self.total_processadas = 0
        self.lotes_processados = 0  # Contador de lotes
        
    def criar_janela(self):
        """Cria a janela de controle em thread separada"""
        self.root = tk.Tk()
        self.root.title("Controle de Consulta - Neo Energia")
        self.root.geometry("450x250")  # Aumentado para acomodar novos campos
        self.root.resizable(False, False)
        
        # Centraliza a janela
        self.root.eval('tk::PlaceWindow . center')
        
        # Frame principal
        frame_main = ttk.Frame(self.root, padding="20")
        frame_main.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Título
        ttk.Label(frame_main, text="🚀 Consulta Neo Energia", 
                 font=("Arial", 14, "bold")).grid(row=0, column=0, columnspan=2, pady=(0, 20))
        
        # Status principal
        self.label_status = ttk.Label(frame_main, text="🔄 Processando consultas...", 
                                     font=("Arial", 10))
        self.label_status.grid(row=1, column=0, columnspan=2, pady=(0, 10))
        
        # Informações em tempo real
        self.label_linha_atual = ttk.Label(frame_main, text="� Lote atual: Aguardando...", 
                                          font=("Arial", 9), foreground="blue")
        self.label_linha_atual.grid(row=2, column=0, columnspan=2, pady=(0, 5))
        
        self.label_velocidade = ttk.Label(frame_main, text="⚡ Velocidade: Calculando...", 
                                         font=("Arial", 9), foreground="green")
        self.label_velocidade.grid(row=3, column=0, columnspan=2, pady=(0, 15))
        
        # Botões
        ttk.Button(frame_main, text="⏸️ Parar Processo", 
                  command=self.parar, width=20).grid(row=4, column=0, padx=(0, 10))
        
        ttk.Button(frame_main, text="❌ Encerrar Aplicação", 
                  command=self.encerrar, width=20).grid(row=4, column=1, padx=(10, 0))
        
        # Informações
        ttk.Label(frame_main, text="• Parar: Finaliza consultas atuais e salva resultados\n• Encerrar: Fecha aplicação imediatamente", 
                 font=("Arial", 8), foreground="gray").grid(row=5, column=0, columnspan=2, pady=(20, 0))
        
        # Protocolo de fechamento
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        
        # Inicia loop
        self.root.mainloop()
    
    def parar(self):
        """Sinaliza para parar o processo atual"""
        self.parar_processo = True
        self.label_status.config(text="⏸️ Parando processo...")
        print("🛑 Botão PARAR pressionado - Finalizando consultas atuais...")
        
        # Força atualização da interface
        if self.root:
            self.root.update()
        
        messagebox.showinfo("Parar Processo", "Processo será interrompido após a consulta atual.")
    
    def encerrar(self):
        """Encerra a aplicação imediatamente"""
        print("🛑 Botão ENCERRAR pressionado...")
        
        if messagebox.askyesno("Encerrar Aplicação", "Deseja realmente encerrar a aplicação?\n\nTodos os dados não salvos serão perdidos."):
            self.encerrar_aplicacao = True
            self.parar_processo = True
            print("❌ Encerrando aplicação por solicitação do usuário...")
            
            # Força atualização da interface
            if self.root:
                self.root.update()
                self.root.quit()
                
            sys.exit(0)
    
    def on_closing(self):
        """Trata o fechamento da janela"""
        self.encerrar()
    
    def iniciar_em_thread(self):
        """Inicia a janela em thread separada"""
        self.thread_janela = threading.Thread(target=self.criar_janela, daemon=True)
        self.thread_janela.start()
    
    def iniciar_cronometro(self):
        """Inicia o cronômetro para calcular velocidade"""
        self.inicio_processamento = time.time()
        self.total_processadas = 0
        self.lotes_processados = 0
    
    def atualizar_informacoes_tempo_real(self, lote_atual=None, total_processadas=None):
        """Atualiza informações em tempo real na tela"""
        if self.root:
            try:
                # Atualiza lote atual se fornecido
                if lote_atual is not None:
                    self.lotes_processados = lote_atual
                    self.label_linha_atual.config(text=f"� Lote atual: {lote_atual}")
                
                # Calcula e atualiza velocidade baseada no total processado
                if total_processadas is not None:
                    self.total_processadas = total_processadas
                
                if self.inicio_processamento and self.total_processadas > 0:
                    tempo_decorrido = time.time() - self.inicio_processamento
                    if tempo_decorrido > 0:
                        consultas_por_minuto = (self.total_processadas / tempo_decorrido) * 60
                        self.label_velocidade.config(text=f"⚡ Velocidade: {consultas_por_minuto:.1f} consultas/min")
                    else:
                        self.label_velocidade.config(text="⚡ Velocidade: Calculando...")
                else:
                    # Quando ainda não processou nenhuma, mostra aguardando
                    self.label_velocidade.config(text="⚡ Velocidade: Aguardando...")
                
                self.root.update_idletasks()
            except:
                pass
    
    def atualizar_status(self, texto):
        """Atualiza o status da janela"""
        if self.root:
            try:
                self.label_status.config(text=texto)
                self.root.update_idletasks()
            except:
                pass
    
    def fechar_janela(self):
        """Fecha a janela"""
        if self.root:
            try:
                self.root.quit()
                self.root.destroy()
            except:
                pass


class JanelaControleHeadless:
    """Substituto sem UI para modo automatico (chamado por executar_automatico.py).

    Tem a mesma interface de JanelaControle mas nao cria nenhuma janela.
    O status e exibido no stdout (capturado pelo painel).
    parar_processo e encerrar_aplicacao ficam sempre False — a parada
    e controlada pelo CTRL_BREAK_EVENT enviado pelo orquestrador.
    """
    def __init__(self):
        self.parar_processo      = False
        self.encerrar_aplicacao  = False
        self._inicio             = None
        self._total              = 0
        self._ultimo_log         = 0

    def iniciar_em_thread(self):
        pass

    def iniciar_cronometro(self):
        self._inicio = time.time()
        self._total  = 0

    def atualizar_informacoes_tempo_real(self, lote_atual=None, total_processadas=None):
        if total_processadas is not None:
            self._total = total_processadas
        # Loga progresso a cada 10s
        agora = time.time()
        if agora - self._ultimo_log >= 10:
            self._ultimo_log = agora
            if self._inicio and self._total:
                decorrido = agora - self._inicio
                vel = (self._total / decorrido) * 60 if decorrido > 0 else 0
                print(f"[PROG] Lote {lote_atual} | Processados: {self._total} | {vel:.0f}/min")

    def atualizar_status(self, texto):
        # Remove emojis simples para nao quebrar o pipe
        import re
        limpo = re.sub(r'[^\x00-\x7F\u00C0-\u024F\u2010-\u2027]', '', texto).strip()
        if limpo:
            print(f"[STATUS] {limpo}")

    def fechar_janela(self):
        pass


class ConsultaContratoAsync:
    def __init__(self, limite_concorrencia=3, arquivo_entrada=None, arquivo_saida=None):
        # --- Modo automático (pipeline) vs. modo manual (dialog) ---
        # arquivo_entrada: caminho do CSV gerado por 03_buscar_lote_macro.py
        #                  Se None → abre dialog (comportamento original)
        # arquivo_saida:   caminho onde salvar resultado_lote.csv
        #                  Se None → usa caminho_saida padrão (pasta com timestamp)
        self.caminho_excel = arquivo_entrada  # preenchido pelo arg CLI ou pelo dialog
        self.arquivo_saida_fixo = arquivo_saida  # None = modo original (gera nome com timestamp)

        # Pasta de saída padrão (modo manual — mantém comportamento original)
        _pasta_padrao = os.path.join(os.path.dirname(__file__), "..", "dados")
        self.caminho_saida = os.path.normpath(
            arquivo_saida if arquivo_saida else _pasta_padrao
        )
        if arquivo_saida is None and not os.path.exists(self.caminho_saida):
            os.makedirs(self.caminho_saida)
            print(f"📁 Diretório criado: {self.caminho_saida}")

        self.resultados = []  # Lista simples para acumular resultados
        self.limite_concorrencia = limite_concorrencia
        self.semaforo = None
        self.linha_inicial = 0
        self.contador_processados = 0

        # Auto-reconexão: detecta rajadas de erros de conexão e reinicia o túnel
        self.erros_consecutivos = 0       # incrementa a cada erro de conexão, zera no sucesso
        self._pedir_reconexao = False     # flag setada pelo consultar_linha_rapida
        self.reconexoes_realizadas = 0    # contador de reconexões (para log)
        self.LIMITE_RECONECTAR = 10       # erros consecutivos que disparam reconexão

        # Janela de controle: UI real no modo manual, headless no modo automatico
        if arquivo_entrada is None:
            self.janela_controle = JanelaControle()
        else:
            self.janela_controle = JanelaControleHeadless()

    def verificar_tunel_ssh(self):
        """Verifica se o túnel SSH está ativo na porta 5000"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3)
            resultado = sock.connect_ex(('127.0.0.1', 5000))
            sock.close()
            return resultado == 0
        except Exception:
            return False

    def _reconectar_tunel(self):
        """Mata o plink atual e reinicia o túnel SSH.
        Chamado automaticamente quando erros_consecutivos >= LIMITE_RECONECTAR.
        """
        from dotenv import load_dotenv
        import platform
        load_dotenv()

        self.reconexoes_realizadas += 1
        print(f"\n🔄 [RECONEXÃO #{self.reconexoes_realizadas}] "
              f"{self.erros_consecutivos} erros consecutivos — reiniciando túnel SSH...")
        self.janela_controle.atualizar_status(
            f"⚠️ Reconectando túnel... (#{self.reconexoes_realizadas})")

        is_windows = platform.system() == "Windows"
        script_dir = os.path.dirname(__file__)

        # Encerra processo SSH atual
        if is_windows:
            subprocess.run(["taskkill", "/IM", "plink.exe", "/F"], capture_output=True)
        else:
            subprocess.run(["pkill", "-f", "ssh.*5000"], capture_output=True)
        time.sleep(2)

        # Lê credenciais do .env
        ssh_user     = os.getenv("SSH_USER", "root")
        ssh_server   = os.getenv("SSH_SERVER")
        ssh_password = os.getenv("SSH_PASSWORD")
        ssh_host_key = os.getenv("SSH_HOST_KEY", "")
        local_port   = int(os.getenv("LOCAL_PORT", 5000))
        remote_host  = os.getenv("REMOTE_HOST")
        remote_port  = int(os.getenv("REMOTE_PORT", 80))

        if not all([ssh_server, ssh_password, remote_host]):
            print("  [AVISO] Variáveis SSH não encontradas no .env — reconexão abortada")
            self.erros_consecutivos = 0
            self._pedir_reconexao = False
            return

        # Monta comando plink (Windows) ou sshpass+ssh (Linux)
        if is_windows:
            plink = os.path.join(script_dir, "plink.exe")
            if not os.path.exists(plink):
                plink = "plink"
            cmd = [plink, "-batch", "-pw", ssh_password]
            if ssh_host_key:
                cmd += ["-hostkey", ssh_host_key]
            cmd += ["-L", f"{local_port}:{remote_host}:{remote_port}",
                    f"{ssh_user}@{ssh_server}", "-N"]
            kwargs = {"creationflags": subprocess.CREATE_NO_WINDOW}
        else:
            cmd = ["sshpass", "-p", ssh_password,
                   "ssh", "-N", "-o", "StrictHostKeyChecking=no",
                   "-L", f"{local_port}:{remote_host}:{remote_port}",
                   f"{ssh_user}@{ssh_server}"]
            kwargs = {}

        try:
            subprocess.Popen(cmd, stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL, **kwargs)
            print("  Túnel reiniciado — aguardando estabilização (5s)...")
            time.sleep(5)
            if self.verificar_tunel_ssh():
                print(f"  ✅ Túnel ativo na porta {local_port} — retomando processamento")
                self.janela_controle.atualizar_status(
                    f"✅ Reconectado! ({self.reconexoes_realizadas}x)")
            else:
                print("  ⚠️ Túnel não confirmado — continuando assim mesmo")
        except Exception as e:
            print(f"  [ERRO] Falha ao reiniciar túnel: {e}")

        self.erros_consecutivos = 0
        self._pedir_reconexao = False

    def iniciar_tunel_automatico(self):
        """Tenta iniciar o túnel SSH automaticamente"""
        script_tunel = os.path.join(os.path.dirname(__file__), "executar_automatico.py")
        
        if os.path.exists(script_tunel):
            print("🔗 Tentando iniciar túnel SSH automaticamente...")
            try:
                processo = subprocess.Popen(
                    [sys.executable, script_tunel],
                    creationflags=subprocess.CREATE_NO_WINDOW,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                
                time.sleep(5)  # Aguarda túnel estabelecer
                
                if self.verificar_tunel_ssh():
                    print("✅ Túnel SSH iniciado com sucesso!")
                    return True
                else:
                    print("❌ Falha ao iniciar túnel SSH automaticamente")
                    return False
                    
            except Exception as e:
                print(f"❌ Erro ao iniciar túnel: {e}")
                return False
        else:
            print("❌ Script executar_automatico.py não encontrado")
            return False

    def verificar_conectividade_api(self):
        """Verifica se a API está acessível"""
        if not self.verificar_tunel_ssh():
            print("❌ Túnel SSH não está ativo na porta 5000")
            
            try:
                root = tk.Tk()
                root.withdraw()
                
                resposta = messagebox.askyesno(
                    "Túnel SSH Inativo",
                    "O túnel SSH não está ativo na porta 5000.\n\n"
                    "Deseja tentar iniciar automaticamente?"
                )
                
                root.destroy()
                
                if resposta:
                    if self.iniciar_tunel_automatico():
                        return self.testar_api_real()
                    else:
                        print("❌ Falha ao iniciar túnel. Inicie manualmente.")
                        return False
                else:
                    print("ℹ️ Por favor, inicie o túnel SSH manualmente.")
                    return False
                    
            except Exception:
                resposta = input("Tentar iniciar túnel automaticamente? (s/n): ").lower().strip()
                if resposta in ['s', 'sim', 'y', 'yes']:
                    if self.iniciar_tunel_automatico():
                        return self.testar_api_real()
                    else:
                        print("❌ Falha ao iniciar túnel.")
                        return False
                else:
                    print("ℹ️ Por favor, inicie o túnel SSH manualmente.")
                    return False
        else:
            print("✅ Túnel SSH ativo na porta 5000")
            return self.testar_api_real()

    def testar_api_real(self):
        """Testa se a API está respondendo (com retry)"""
        print("🧪 Testando conectividade com a API...")
        
        url_teste = "http://localhost:5000/validacaotitularidade/Validacao/ValidarTitularidade?ContaContrato=123456789&CpfCnpj=12345678901&Empresa=coelba"
        max_tentativas = 3

        for tentativa in range(1, max_tentativas + 1):
            try:
                timeout_config = httpx.Timeout(
                    connect=5.0,
                    read=8.0,
                    write=3.0,
                    pool=3.0
                )
                
                with httpx.Client(timeout=timeout_config) as client:
                    response = client.get(url_teste)
                    
                    print(f"📊 Status HTTP: {response.status_code}")
                    print(f"🔍 URL de teste: {url_teste}")
                    print(f"📄 Resposta de teste: {response.text[:200]}...")
                    
                    if response.status_code == 200:
                        print("✅ API respondendo corretamente!")
                        
                        if "INATIVO" in response.text or "não existe" in response.text:
                            print("⚠️ ATENÇÃO: API está retornando que os dados de teste não existem")
                            print("💡 Isso é normal para dados fictícios. Verifique se os dados da planilha estão corretos.")
                        
                        return True
                    else:
                        print(f"❌ API retornou status {response.status_code}")
                        if tentativa < max_tentativas:
                            print(f"🔄 Tentando novamente ({tentativa}/{max_tentativas})...")
                            time.sleep(3)
                            continue
                        return False
                        
            except Exception as e:
                print(f"❌ Erro ao testar API (tentativa {tentativa}/{max_tentativas}): {e}")
                if tentativa < max_tentativas:
                    print(f"🔄 Aguardando 3s antes de tentar novamente...")
                    time.sleep(3)
                else:
                    print("❌ API não respondeu após todas as tentativas.")
                    return False
        return False

    def selecionar_arquivo(self):
        """Seleciona o arquivo de entrada.

        Modo automático: usa self.caminho_excel definido via --arquivo (bypassa dialog).
        Modo manual:     abre dialog de seleção (comportamento original).
        """
        # --- MODO AUTOMÁTICO (pipeline) ---
        # Arquivo já definido por 03_buscar_lote_macro.py via arg --arquivo
        if self.caminho_excel is not None:
            if os.path.exists(self.caminho_excel):
                print(f"✅ Arquivo de entrada (automático): {self.caminho_excel}")
                return True
            else:
                print(f"❌ Arquivo de entrada não encontrado: {self.caminho_excel}")
                return False

        # --- MODO MANUAL (comportamento original — dialog) ---
        try:
            root = tk.Tk()
            root.withdraw()

            arquivo = filedialog.askopenfilename(
                title="Selecione a planilha para consulta",
                filetypes=[
                    ("Arquivos Excel", "*.xlsx *.xls"),
                    ("CSV", "*.csv"),
                    ("Todos os arquivos", "*.*")
                ]
            )

            root.destroy()

            if not arquivo:
                print("❌ Nenhum arquivo selecionado.")
                return False

            self.caminho_excel = arquivo
            print(f"✅ Arquivo selecionado: {arquivo}")
            return True

        except Exception as e:
            print(f"❌ Erro ao abrir diálogo: {e}")
            return False

    def _caminho_resultado(self) -> str:
        """Retorna o caminho do arquivo de resultado.

        Modo automático: usa self.arquivo_saida_fixo definido via --saida.
        Modo manual:     gera arquivo com timestamp na pasta caminho_saida.
        """
        if self.arquivo_saida_fixo:
            return self.arquivo_saida_fixo
        if not hasattr(self, '_arquivo_resultado_gerado'):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self._arquivo_resultado_gerado = os.path.join(
                self.caminho_saida, f"resultado_rapido_{timestamp}.csv"
            )
        return self._arquivo_resultado_gerado

    def salvar_resultados_em_lote(self):
        """Salva 1000 resultados de uma vez — MÁXIMA PERFORMANCE.

        Modo automático: salva em CSV (caminho fixo definido por --saida).
        Modo manual:     mantém comportamento anterior (CSV com timestamp).
        """
        try:
            if len(self.resultados) >= 1000:
                print(f"\n💾 Salvando lote de 1000 resultados...")

                colunas = ["cpf", "codigo cliente", "empresa", "resposta"]
                lote_para_salvar = self.resultados[:1000]
                caminho = self._caminho_resultado()

                df_lote = pd.DataFrame(lote_para_salvar, columns=colunas)

                if not os.path.exists(caminho):
                    # Primeiro lote — cria arquivo
                    df_lote.to_csv(caminho, index=False, encoding="utf-8")
                    print(f"📊 Arquivo criado: {os.path.basename(caminho)}")
                else:
                    # Lotes seguintes — anexa sem re-escrever cabeçalho
                    df_lote.to_csv(caminho, mode="a", index=False,
                                   encoding="utf-8", header=False)
                    print(f"📊 Lote anexado: {os.path.basename(caminho)}")

                self.resultados = self.resultados[1000:]
                print(f"✅ Lote de 1000 salvo! Restam {len(self.resultados)} em memória")
                return True

        except Exception as e:
            print(f"❌ Erro ao salvar lote: {e}")
            return False

    def salvar_resultados_finais(self):
        """Salva todos os resultados restantes — VERSÃO FINAL.

        Salva em CSV (compatível com 04_processar_retorno_macro.py).
        """
        try:
            if not self.resultados:
                print("⚠️ Nenhum resultado final para salvar")
                return False

            print(f"\n💾 Salvando {len(self.resultados)} resultados finais...")

            colunas = ["cpf", "codigo cliente", "empresa", "resposta"]
            caminho = self._caminho_resultado()
            df_final = pd.DataFrame(self.resultados, columns=colunas)

            if os.path.exists(caminho):
                # Anexa ao arquivo existente (pode haver lotes anteriores)
                df_final.to_csv(caminho, mode="a", index=False,
                                encoding="utf-8", header=False)
                total = sum(1 for _ in open(caminho, encoding="utf-8")) - 1
                print(f"✅ Resultados finais anexados!")
                print(f"📊 Total estimado no arquivo: {total:,} registros")
            else:
                df_final.to_csv(caminho, index=False, encoding="utf-8")
                print(f"✅ Arquivo final criado!")
                print(f"📊 Total: {len(df_final):,} registros")

            print(f"📁 Arquivo: {caminho}")
            self.resultados.clear()
            return True

        except Exception as e:
            print(f"❌ Erro ao salvar resultados finais: {e}")
            return False

    @staticmethod
    def padronizar_cpf_cnpj(valor):
        """Padroniza CPF/CNPJ - VERSÃO RÁPIDA"""
        if pd.isna(valor):
            return ""

        valor_str = str(valor)
        somente_digitos = "".join(c for c in valor_str if c.isdigit())

        if len(somente_digitos) <= 11:
            return somente_digitos.zfill(11)
        else:
            return somente_digitos.zfill(14)

    @staticmethod
    def padronizar_contrato(valor):
        """Padroniza código do contrato - Remove .0 e caracteres inválidos"""
        if pd.isna(valor):
            return ""
        
        valor_str = str(valor)
        
        # Remove .0 se existir no final (comum no Excel)
        if valor_str.endswith('.0'):
            valor_str = valor_str[:-2]
        
        # Remove qualquer caractere que não seja dígito
        somente_digitos = "".join(c for c in valor_str if c.isdigit())
        
        return somente_digitos

    async def consultar_linha_rapida(self, cpf, contrato, empresa, linha_display, client):
        """Consulta uma linha - COM RETRY AUTOMÁTICO PARA TIMEOUTS"""
        
        url = (
            f"http://localhost:5000/validacaotitularidade/Validacao/"
            f"ValidarTitularidade?ContaContrato={contrato}&CpfCnpj={cpf}&Empresa={empresa}"
        )

        # 🔍 DEBUG: Log dos dados sendo enviados
        print(f"🔍 [Linha {linha_display}] Enviando: CPF={cpf}, Contrato={contrato}, Empresa={empresa}")
        print(f"🌐 [Linha {linha_display}] URL: {url}")

        # ⚡ TIMEOUT AJUSTADO: 4 segundos
        timeout_rapido = 4.0
        
        async with self.semaforo:
            # 🔄 PRIMEIRA TENTATIVA
            try:
                response = await client.get(url, timeout=timeout_rapido)
                
                # ⚡ SEM TRATAMENTO: Pega resposta bruta
                resposta_bruta = response.text
                
                # 🔍 DEBUG: Log da resposta recebida
                print(f"📄 [Linha {linha_display}] Resposta: {resposta_bruta[:100]}...")  # Primeiros 100 chars
                print(f"✅ [Linha {linha_display}] Status HTTP: {response.status_code}")
                
                # 🔍 VERIFICA ERRO DE LIMITE DE CONEXÕES SAP
                if "peak connections limit" in resposta_bruta and "exceeded" in resposta_bruta:
                    print(f"⚠️ [Linha {linha_display}] Limite SAP excedido - Tentativa com delay...")
                    
                    # 🕐 AGUARDA PARA LIBERAR CONEXÕES SAP
                    await asyncio.sleep(3)
                    
                    try:
                        # 🔄 TERCEIRA TENTATIVA COM DELAY E TIMEOUT MAIOR
                        response_retry = await client.get(url, timeout=10.0)
                        resposta_retry = response_retry.text
                        
                        # Verifica se ainda tem erro de limite
                        if "peak connections limit" in resposta_retry and "exceeded" in resposta_retry:
                            print(f"❌ [Linha {linha_display}] Limite SAP persistente")
                            resultado = [cpf, contrato, empresa, "LIMIT_EXCEEDED"]
                        else:
                            print(f"✅ [Linha {linha_display}] Recuperado após delay SAP")
                            resultado = [cpf, contrato, empresa, resposta_retry]
                        
                        self.resultados.append(resultado)
                        self.contador_processados += 1
                        
                        if len(self.resultados) >= 1000:
                            self.salvar_resultados_em_lote()
                        
                        return
                        
                    except Exception as e:
                        print(f"❌ [Linha {linha_display}] Erro na tentativa SAP: {type(e).__name__}")
                        resultado = [cpf, contrato, empresa, "LIMIT_EXCEEDED"]
                        self.resultados.append(resultado)
                        self.contador_processados += 1
                        return
                
                # ✅ Resultado normal: resposta válida
                resultado = [cpf, contrato, empresa, resposta_bruta]
                self.resultados.append(resultado)
                self.erros_consecutivos = 0  # sucesso — zera contador
                self.contador_processados += 1
                
                # ⚡ SALVA EM LOTES DE 1000 para máxima performance
                if len(self.resultados) >= 1000:
                    self.salvar_resultados_em_lote()
                
                return  # ✅ Sucesso na primeira tentativa
                
            except httpx.ReadTimeout:
                print(f"⏱️ [Linha {linha_display}] Timeout (>4s) - Tentando novamente...")
                
                # 🔄 SEGUNDA TENTATIVA COM TIMEOUT MAIOR
                try:
                    # ⚡ RETRY: Timeout de 8s para segunda tentativa
                    response = await client.get(url, timeout=8.0)
                    
                    resposta_bruta = response.text
                    
                    print(f"✅ [Linha {linha_display}] OK (2ª tentativa)")
                    
                    # 🔍 VERIFICA ERRO DE LIMITE SAP TAMBÉM NA 2ª TENTATIVA
                    if "peak connections limit" in resposta_bruta and "exceeded" in resposta_bruta:
                        print(f"⚠️ [Linha {linha_display}] Limite SAP na 2ª tentativa - Delay extra...")
                        
                        await asyncio.sleep(5)  # Delay maior na segunda tentativa
                        
                        try:
                            response_sap = await client.get(url, timeout=12.0)
                            resposta_sap = response_sap.text
                            
                            if "peak connections limit" in resposta_sap and "exceeded" in resposta_sap:
                                resultado = [cpf, contrato, empresa, "LIMIT_EXCEEDED"]
                            else:
                                resultado = [cpf, contrato, empresa, resposta_sap]
                            
                        except Exception:
                            resultado = [cpf, contrato, empresa, "LIMIT_EXCEEDED"]
                    else:
                        resultado = [cpf, contrato, empresa, resposta_bruta]
                    
                    self.resultados.append(resultado)
                    self.contador_processados += 1
                    self.erros_consecutivos = 0  # 2ª tentativa teve sucesso
                    
                    if len(self.resultados) >= 1000:
                        self.salvar_resultados_em_lote()
                    
                    return  # ✅ Sucesso na segunda tentativa
                    
                except httpx.ReadTimeout:
                    print(f"❌ [Linha {linha_display}] Timeout final (>8s) - Desistindo")
                    resultado_timeout = [cpf, contrato, empresa, "TIMEOUT_FINAL"]
                    self.resultados.append(resultado_timeout)
                    self.contador_processados += 1
                    # timeout não é erro de conexão — não incrementa contador de reconexão
                    
                except Exception as e:
                    print(f"❌ [Linha {linha_display}] Erro na 2ª tentativa: {type(e).__name__}")
                    resultado_erro = [cpf, contrato, empresa, f"ERRO_RETRY: {type(e).__name__}"]
                    self.resultados.append(resultado_erro)
                    self.contador_processados += 1
                    self.erros_consecutivos += 1
                    if self.erros_consecutivos >= self.LIMITE_RECONECTAR:
                        print(f"⚠️ [{linha_display}] Limite de erros atingido ({self.erros_consecutivos}) — reconexão solicitada")
                        self._pedir_reconexao = True
                
            except Exception as e:
                print(f"❌ [Linha {linha_display}] Erro: {type(e).__name__}")
                resultado_erro = [cpf, contrato, empresa, f"ERRO: {type(e).__name__}"]
                self.resultados.append(resultado_erro)
                self.contador_processados += 1
                self.erros_consecutivos += 1
                if self.erros_consecutivos >= self.LIMITE_RECONECTAR:
                    print(f"⚠️ [{linha_display}] Limite de erros atingido ({self.erros_consecutivos}) — reconexão solicitada")
                    self._pedir_reconexao = True

    async def consultar_cadastro(self):
        """Método principal.

        Modo automático: lê CSV de lote_pendente.csv (sem dialog).
        Modo manual:     abre dialog e lê Excel (comportamento original).
        """
        if isinstance(self.janela_controle, JanelaControle):
            print("Iniciando janela de controle...")
            self.janela_controle.iniciar_em_thread()
            time.sleep(0.5)

        try:
            # Verificações básicas
            self.janela_controle.atualizar_status("🔍 Verificando API...")
            if not self.verificar_conectividade_api():
                print("❌ API não acessível")
                self.janela_controle.atualizar_status("❌ API não conectada")
                return

            self.janela_controle.atualizar_status("📁 Selecionando arquivo...")
            if not self.selecionar_arquivo():
                self.janela_controle.atualizar_status("❌ Arquivo não selecionado")
                return

            # Carrega dados — CSV (automático) ou Excel (manual)
            self.janela_controle.atualizar_status("📊 Carregando dados...")
            try:
                ext = os.path.splitext(self.caminho_excel)[1].lower()
                if ext == ".csv":
                    df = pd.read_csv(self.caminho_excel, dtype=str)
                    print(f"📄 CSV carregado: {len(df)} linhas")
                else:
                    df = pd.read_excel(self.caminho_excel)
                    print(f"📄 Excel carregado: {len(df)} linhas")
            except Exception as e:
                print(f"❌ Erro ao ler arquivo: {e}")
                return

            await self._processar_rapido(df)
            
        except Exception as e:
            print(f"❌ Erro geral: {e}")
            
        finally:
            self.janela_controle.atualizar_status("Finalizado")
            if isinstance(self.janela_controle, JanelaControle):
                time.sleep(2)
            self.janela_controle.fechar_janela()
    
    async def _processar_rapido(self, df):
        """Processamento assíncrono com auto-reconexão SSH.

        Detecta rajadas de erros de conexão e reinicia o túnel automaticamente,
        retomando do ponto exato onde parou (self.linha_inicial).
        """
        df.columns = df.columns.str.strip()
        print(f"\U0001f4cb Colunas: {list(df.columns)}")

        colunas_esperadas = {"codigo cliente", "cpf", "empresa"}
        if not colunas_esperadas.issubset(df.columns):
            print(f"\u274c Colunas inválidas. Esperadas: {colunas_esperadas}")
            return

        print(f"\n\U0001f50d VERIFICAÇÃO DOS DADOS:")
        for i in range(min(3, len(df))):
            linha = df.iloc[i]
            print(f"   Linha {i+2}: CPF={self.padronizar_cpf_cnpj(linha['cpf'])}, "
                  f"Contrato={self.padronizar_contrato(linha['codigo cliente'])}, "
                  f"Empresa={linha['empresa']}")
        print()

        timeout_config = httpx.Timeout(connect=30.0, read=30.0, write=30.0, pool=30.0)
        self.semaforo = asyncio.Semaphore(self.limite_concorrencia)

        # Loop de reconexão: reinicia túnel e retoma quando _pedir_reconexao=True
        while True:
            self._pedir_reconexao = False
            linhas_para_processar = df.iloc[self.linha_inicial:]

            if linhas_para_processar.empty:
                break

            print(f"\U0001f4ca Total: {len(df)} | Retomando em: {self.linha_inicial} | "
                  f"Reconexões: {self.reconexoes_realizadas} | "
                  f"Concorrência: {self.limite_concorrencia} | "
                  f"Limite reconectar: {self.LIMITE_RECONECTAR} erros")

            self.janela_controle.iniciar_cronometro()
            self.janela_controle.atualizar_informacoes_tempo_real(
                lote_atual=0, total_processadas=self.contador_processados)

            async with httpx.AsyncClient(timeout=timeout_config) as client:
                tasks = []
                lote_contador = 0
                linhas_nesta_rodada = 0

                try:
                    for idx, (_, row) in enumerate(linhas_para_processar.iterrows()):

                        if self.janela_controle.parar_processo:
                            print(f"\n\u23f8\ufe0f Parado pelo usuário")
                            break
                        if self.janela_controle.encerrar_aplicacao:
                            sys.exit(0)

                        contrato      = self.padronizar_contrato(row["codigo cliente"])
                        cpf           = self.padronizar_cpf_cnpj(row["cpf"])
                        empresa       = row["empresa"]
                        linha_display = self.linha_inicial + idx + 2

                        if idx % 10 == 0:
                            self.janela_controle.atualizar_status(
                                f"\U0001f504 Lote {lote_contador + 1}...")
                            self.janela_controle.atualizar_informacoes_tempo_real(
                                lote_atual=lote_contador,
                                total_processadas=self.contador_processados)
                            if self.janela_controle.parar_processo or \
                               self.janela_controle.encerrar_aplicacao:
                                break

                        tasks.append(self.consultar_linha_rapida(
                            cpf, contrato, empresa, linha_display, client))
                        linhas_nesta_rodada += 1

                        if len(tasks) >= 50:
                            await asyncio.gather(*tasks, return_exceptions=True)
                            tasks = []
                            lote_contador += 1
                            print(f"\u26a1 Lote {lote_contador} | Total: {self.contador_processados} "
                                  f"| Erros consec.: {self.erros_consecutivos}")
                            self.janela_controle.atualizar_informacoes_tempo_real(
                                lote_atual=lote_contador,
                                total_processadas=self.contador_processados)

                            # ---- Auto-reconexão ----
                            if self._pedir_reconexao:
                                self.linha_inicial += linhas_nesta_rodada
                                print(f"\u26a0\ufe0f Reconexão solicitada. Retomando em linha {self.linha_inicial}")
                                break

                            if self.janela_controle.parar_processo or \
                               self.janela_controle.encerrar_aplicacao:
                                break

                    # Lote final (tasks restantes)
                    if not self._pedir_reconexao and tasks and \
                       not self.janela_controle.parar_processo and \
                       not self.janela_controle.encerrar_aplicacao:
                        lote_contador += 1
                        print(f"\u26a1 Lote final {lote_contador} ({len(tasks)} tasks)")
                        await asyncio.gather(*tasks, return_exceptions=True)
                        self.janela_controle.atualizar_informacoes_tempo_real(
                            lote_atual=lote_contador,
                            total_processadas=self.contador_processados)

                except KeyboardInterrupt:
                    print("\n\u26d4 Interrompido pelo usuário")

                finally:
                    # Salva buffer em memória (parcial na reconexão, ou final)
                    if self.resultados:
                        print(f"\n\U0001f4be Salvando {len(self.resultados)} resultados...")
                        self.salvar_resultados_finais()
                        self.janela_controle.atualizar_status("\u2705 Resultados salvos!")

            # Saiu do async with: conexões antigas fechadas
            if self._pedir_reconexao:
                self._reconectar_tunel()
                continue  # nova iteração com novo AsyncClient e nova posição
            else:
                break  # processamento concluído

        print(f"\n\U0001f4ca ESTATÍSTICAS FINAIS:")
        print(f"   \u2022 Total processado  : {self.contador_processados} linhas")
        print(f"   \u2022 Reconexões SSH    : {self.reconexoes_realizadas}")
        print(f"   \u2022 Concorrência      : {self.limite_concorrencia} simultâneas")
        print(f"   \u2022 Retry timeout     : ativo (2\u00aa tentativa 8s)")
        print(f"   \u2022 Auto-reconexão    : ativo após {self.LIMITE_RECONECTAR} erros consecutivos")


if __name__ == "__main__":
    # ---------------------------------------------------------------------------
    # CLI — suporte a modo automático (pipeline) e modo manual (comportamento
    # original com dialog de seleção de arquivo).
    #
    # Modo automático chamado por executar_automatico.py:
    #   python consulta_contrato.py \
    #     --arquivo ../dados/lote_pendente.csv \
    #     --saida   ../dados/resultado_lote.csv
    #
    # Modo manual (sem argumentos): abre dialog — comportamento original.
    # ---------------------------------------------------------------------------
    _parser = argparse.ArgumentParser(
        description="Macro Neo Energia — consulta de titularidade"
    )
    _parser.add_argument(
        "--arquivo", default=None,
        help="Caminho do CSV de entrada (gerado por 03_buscar_lote_macro.py). "
             "Se omitido, abre dialog de seleção."
    )
    _parser.add_argument(
        "--saida", default=None,
        help="Caminho do CSV de saída (lido por 04_processar_retorno_macro.py). "
             "Se omitido, gera arquivo com timestamp na pasta padrão."
    )
    _parser.add_argument(
        "--concorrencia", type=int, default=3,
        help="Número de consultas simultâneas (padrão: 3)"
    )
    _args = _parser.parse_args()

    print("=" * 60)
    print("🚀 CONSULTA RÁPIDA - NEO ENERGIA")
    if _args.arquivo:
        print(f"   Modo: AUTOMÁTICO  |  arquivo: {_args.arquivo}")
    else:
        print("   Modo: MANUAL (dialog)")
    print("=" * 60)
    print()

    consulta = ConsultaContratoAsync(
        limite_concorrencia=_args.concorrencia,
        arquivo_entrada=_args.arquivo,
        arquivo_saida=_args.saida,
    )

    try:
        asyncio.run(consulta.consultar_cadastro())
    except Exception as e:
        print(f"❌ Erro na execução: {e}")

    print("\n" + "=" * 60)
    print("⚡ EXECUÇÃO FINALIZADA")
    print("=" * 60)