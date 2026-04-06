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

from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio


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


class ConsultaContratoAsync:
    def __init__(self, limite_concorrencia=3):  # ⚡ REDUZIDO: 3 consultas simultâneas
        self.caminho_excel = None
        self.caminho_saida = r"C:\Users\gismi\OneDrive\Desktop\auto_api_neo\consulta_neo_reinan\dados"
        self.resultados = []  # Lista simples para acumular resultados
        self.limite_concorrencia = limite_concorrencia
        self.semaforo = None
        self.linha_inicial = 0
        self.contador_processados = 0
        
        # Janela de controle
        self.janela_controle = JanelaControle()
        
        # Garante que o diretório de saída existe
        if not os.path.exists(self.caminho_saida):
            os.makedirs(self.caminho_saida)
            print(f"📁 Diretório criado: {self.caminho_saida}")

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
        """Testa se a API está respondendo"""
        print("🧪 Testando conectividade com a API...")
        
        try:
            url_teste = "http://localhost:5000/validacaotitularidade/Validacao/ValidarTitularidade?ContaContrato=123456789&CpfCnpj=12345678901&Empresa=coelba"
            
            # ⚡ CORREÇÃO: Timeout configurado corretamente
            timeout_config = httpx.Timeout(
                connect=3.0,
                read=5.0,
                write=3.0,
                pool=3.0
            )
            
            with httpx.Client(timeout=timeout_config) as client:
                response = client.get(url_teste)
                
                print(f"📊 Status HTTP: {response.status_code}")
                print(f"🔍 URL de teste: {url_teste}")
                print(f"📄 Resposta de teste: {response.text[:200]}...")  # Mostra resposta
                
                if response.status_code == 200:
                    print("✅ API respondendo corretamente!")
                    
                    # 🔍 Verifica se a resposta contém dados válidos
                    if "INATIVO" in response.text or "não existe" in response.text:
                        print("⚠️ ATENÇÃO: API está retornando que os dados de teste não existem")
                        print("💡 Isso é normal para dados fictícios. Verifique se os dados da planilha estão corretos.")
                    
                    return True
                else:
                    print(f"❌ API retornou status {response.status_code}")
                    return False
                    
        except Exception as e:
            print(f"❌ Erro ao testar API: {e}")
            return False

    def selecionar_arquivo(self):
        """Abre diálogo para seleção do arquivo Excel"""
        try:
            root = tk.Tk()
            root.withdraw()
            
            arquivo = filedialog.askopenfilename(
                title="Selecione a planilha para consulta",
                filetypes=[
                    ("Arquivos Excel", "*.xlsx *.xls"),
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

    def salvar_resultados_em_lote(self):
        """Salva 1000 resultados na planilha de uma vez - MÁXIMA PERFORMANCE"""
        try:
            if len(self.resultados) >= 1000:
                print(f"\n💾 Salvando lote de 1000 resultados...")
                
                # Cria DataFrame com apenas as 4 colunas necessárias
                colunas = ["cpf", "codigo_cliente", "empresa", "resposta"]
                
                # Pega os primeiros 1000 resultados
                lote_para_salvar = self.resultados[:1000]
                
                # Se é o primeiro lote, cria arquivo novo
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                nome_arquivo = f"resultado_rapido_{timestamp}.xlsx"
                caminho_completo = os.path.join(self.caminho_saida, nome_arquivo)
                
                if not hasattr(self, 'arquivo_resultado'):
                    self.arquivo_resultado = caminho_completo
                    
                    # Primeiro lote - cria arquivo
                    df_lote = pd.DataFrame(lote_para_salvar, columns=colunas)
                    df_lote.to_excel(self.arquivo_resultado, index=False, engine='openpyxl')
                    print(f"📊 Arquivo criado: {os.path.basename(self.arquivo_resultado)}")
                    
                else:
                    # Lotes seguintes - anexa ao arquivo existente
                    df_existente = pd.read_excel(self.arquivo_resultado)
                    df_lote = pd.DataFrame(lote_para_salvar, columns=colunas)
                    df_final = pd.concat([df_existente, df_lote], ignore_index=True)
                    df_final.to_excel(self.arquivo_resultado, index=False, engine='openpyxl')
                    print(f"📊 Lote anexado ao arquivo existente")
                
                # Remove os 1000 primeiros da lista
                self.resultados = self.resultados[1000:]
                
                print(f"✅ Lote de 1000 salvo! Restam {len(self.resultados)} em memória")
                return True
                
        except Exception as e:
            print(f"❌ Erro ao salvar lote: {e}")
            return False

    def salvar_resultados_finais(self):
        """Salva todos os resultados restantes - VERSÃO FINAL"""
        try:
            if not self.resultados:
                print("⚠️ Nenhum resultado final para salvar")
                return False
            
            print(f"\n💾 Salvando {len(self.resultados)} resultados finais...")
            
            colunas = ["cpf", "codigo_cliente", "empresa", "resposta"]
            
            if hasattr(self, 'arquivo_resultado') and os.path.exists(self.arquivo_resultado):
                # Anexa ao arquivo existente
                df_existente = pd.read_excel(self.arquivo_resultado)
                df_final = pd.DataFrame(self.resultados, columns=colunas)
                df_completo = pd.concat([df_existente, df_final], ignore_index=True)
                df_completo.to_excel(self.arquivo_resultado, index=False, engine='openpyxl')
                
                print(f"✅ Resultados finais anexados!")
                print(f"📊 Total final: {len(df_completo)} registros")
                print(f"📁 Arquivo: {self.arquivo_resultado}")
                
            else:
                # Cria arquivo novo (caso não tenha criado nenhum lote antes)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                nome_arquivo = f"resultado_rapido_{timestamp}.xlsx"
                caminho_completo = os.path.join(self.caminho_saida, nome_arquivo)
                
                df_final = pd.DataFrame(self.resultados, columns=colunas)
                df_final.to_excel(caminho_completo, index=False, engine='openpyxl')
                
                print(f"✅ Arquivo final criado!")
                print(f"📊 Total: {len(df_final)} registros")
                print(f"📁 Arquivo: {caminho_completo}")
                
            # Limpa resultados da memória
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
                
                # ⚡ RESULTADO SIMPLES: Apenas 4 campos
                resultado = [cpf, contrato, empresa, resposta_bruta]
                self.resultados.append(resultado)
                
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
                    
                    resultado = [cpf, contrato, empresa, resposta_bruta]
                    self.resultados.append(resultado)
                    
                    self.contador_processados += 1
                    
                    if len(self.resultados) >= 1000:
                        self.salvar_resultados_em_lote()
                    
                    return  # ✅ Sucesso na segunda tentativa
                    
                except httpx.ReadTimeout:
                    print(f"❌ [Linha {linha_display}] Timeout final (>8s) - Desistindo")

                    # ❌ TIMEOUT FINAL: Marca como timeout definitivo
                    resultado_timeout = [cpf, contrato, empresa, "TIMEOUT_FINAL"]
                    self.resultados.append(resultado_timeout)
                    self.contador_processados += 1
                    
                except Exception as e:
                    print(f"❌ [Linha {linha_display}] Erro na 2ª tentativa: {type(e).__name__}")
                    
                    resultado_erro = [cpf, contrato, empresa, f"ERRO_RETRY: {type(e).__name__}"]
                    self.resultados.append(resultado_erro)
                    self.contador_processados += 1
                
            except Exception as e:
                print(f"❌ [Linha {linha_display}] Erro: {type(e).__name__}")
                
                # ⚡ ERRO SIMPLES: Apenas marca como erro (sem retry para outros erros)
                resultado_erro = [cpf, contrato, empresa, f"ERRO: {type(e).__name__}"]
                self.resultados.append(resultado_erro)
                self.contador_processados += 1

    async def consultar_cadastro(self):
        """Método principal - VERSÃO ULTRA RÁPIDA"""
        
        print("🚀 Iniciando janela de controle...")
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

            # Carrega planilha
            self.janela_controle.atualizar_status("📊 Carregando planilha...")
            try:
                df = pd.read_excel(self.caminho_excel)
                print(f"📄 Planilha carregada: {len(df)} linhas")
            except Exception as e:
                print(f"❌ Erro ao ler Excel: {e}")
                return

            await self._processar_rapido(df)
            
        except Exception as e:
            print(f"❌ Erro geral: {e}")
            
        finally:
            self.janela_controle.atualizar_status("✅ Finalizado")
            time.sleep(2)
            self.janela_controle.fechar_janela()
    
    async def _processar_rapido(self, df):
        """Processamento ultra rápido - SEM TRATAMENTO DE DADOS"""
        
        # Limpa colunas
        df.columns = df.columns.str.strip()
        print(f"📋 Colunas: {list(df.columns)}")

        # Verifica colunas obrigatórias
        colunas_esperadas = {"codigo cliente", "cpf", "empresa"}
        if not colunas_esperadas.issubset(df.columns):
            print(f"❌ Colunas inválidas. Esperadas: {colunas_esperadas}")
            return

        # 🔍 DEBUG: Mostra algumas linhas da planilha para verificação
        print(f"\n🔍 VERIFICAÇÃO DOS DADOS DA PLANILHA:")
        print(f"📊 Primeiras 3 linhas da planilha:")
        for i in range(min(3, len(df))):
            linha = df.iloc[i]
            cpf_exemplo = self.padronizar_cpf_cnpj(linha["cpf"])
            contrato_exemplo = self.padronizar_contrato(linha["codigo cliente"])  # 🔧 CORREÇÃO: Mostra contrato tratado
            print(f"   Linha {i+2}: CPF={cpf_exemplo}, Contrato ORIGINAL={linha['codigo cliente']}, Contrato TRATADO={contrato_exemplo}, Empresa={linha['empresa']}")
        print()

        # ⚡ TIMEOUT AJUSTADO para reduzir timeouts
        timeout_config = httpx.Timeout(
            connect=4.0,
            read=4.0,
            write=3.0,
            pool=3.0
        )
        
        # ⚡ SEMÁFORO: 5 consultas simultâneas
        self.semaforo = asyncio.Semaphore(self.limite_concorrencia)
        
        async with httpx.AsyncClient(timeout=timeout_config) as client:
            
            try:
                total_linhas = len(df)
                linhas_para_processar = df.iloc[self.linha_inicial:]
                
                print(f"📊 Total: {total_linhas} linhas")
                print(f"🔄 Processando: {len(linhas_para_processar)} linhas")
                print(f"⚡ Concorrência: {self.limite_concorrencia} consultas simultâneas")
                print(f"⏱️ Timeout: 4s por consulta (8s na 2ª tentativa)")
                print(f"🔄 Retry automático: Sim (para timeouts)")
                print(f"💾 Salvamento: A cada 1000 resultados")
                print(f"📋 Colunas resultado: cpf, codigo_cliente, empresa, resposta")
                print()
                
                # 🕐 INICIA CRONÔMETRO PARA TEMPO REAL
                self.janela_controle.iniciar_cronometro()
                
                # � ATUALIZAÇÃO INICIAL: Mostra que vai começar o primeiro lote
                self.janela_controle.atualizar_informacoes_tempo_real(lote_atual=0, total_processadas=0)
                
                # ⚡ PROCESSAMENTO EM LOTES ASSÍNCRONOS
                tasks = []
                lote_contador = 0  # Contador de lotes processados
                for idx, (_, row) in enumerate(linhas_para_processar.iterrows()):
                    
                    # 🛑 VERIFICA PARADA COM MAIS FREQUÊNCIA
                    if self.janela_controle.parar_processo:
                        print(f"\n⏸️ Parado pelo usuário na linha {idx + 2}")
                        break
                    
                    if self.janela_controle.encerrar_aplicacao:
                        print(f"\n❌ Encerrando aplicação...")
                        sys.exit(0)
                    
                    contrato = self.padronizar_contrato(row["codigo cliente"])  # 🔧 CORREÇÃO: Remove .0
                    cpf = self.padronizar_cpf_cnpj(row["cpf"])
                    empresa = row["empresa"]
                    linha_display = idx + 2
                    
                    # Atualiza status e informações em tempo real
                    if idx % 10 == 0:  # Atualiza a cada 10 linhas para ser mais responsivo
                        self.janela_controle.atualizar_status(f"🔄 Processando lote {lote_contador + 1}...")
                        self.janela_controle.atualizar_informacoes_tempo_real(lote_atual=lote_contador, total_processadas=self.contador_processados)
                        
                        # 🛑 VERIFICA PARADA DURANTE ATUALIZAÇÕES
                        if self.janela_controle.parar_processo or self.janela_controle.encerrar_aplicacao:
                            break
                    
                    # ⚡ CRIA TASK ASSÍNCRONA
                    task = self.consultar_linha_rapida(cpf, contrato, empresa, linha_display, client)
                    tasks.append(task)
                    
                    # ⚡ PROCESSA EM LOTES DE 50 PARA CONTROLE
                    if len(tasks) >= 50:
                        await asyncio.gather(*tasks, return_exceptions=True)
                        tasks = []
                        lote_contador += 1  # Incrementa contador de lote
                        print(f"⚡ Lote {lote_contador} processado - Total: {self.contador_processados}")
                        # Atualiza informações em tempo real após cada lote
                        self.janela_controle.atualizar_informacoes_tempo_real(lote_atual=lote_contador, total_processadas=self.contador_processados)
                        
                        # 🛑 VERIFICA PARADA APÓS CADA LOTE
                        if self.janela_controle.parar_processo or self.janela_controle.encerrar_aplicacao:
                            break
                
                # Processa tasks restantes
                if tasks and not self.janela_controle.parar_processo and not self.janela_controle.encerrar_aplicacao:
                    lote_contador += 1  # Incrementa para o lote final
                    print(f"⚡ Processando lote final {lote_contador} com {len(tasks)} tasks restantes...")
                    await asyncio.gather(*tasks, return_exceptions=True)
                    # Atualiza para o lote final
                    self.janela_controle.atualizar_informacoes_tempo_real(lote_atual=lote_contador, total_processadas=self.contador_processados)
                
            except KeyboardInterrupt:
                print("\n⛔ Interrompido pelo usuário")
                
            finally:
                # ⚡ SALVA RESULTADOS FINAIS
                print(f"\n💾 Salvando resultados finais...")
                if self.salvar_resultados_finais():
                    print(f"✅ Todos os resultados salvos!")
                    self.janela_controle.atualizar_status("✅ Resultados salvos!")
                
                print(f"\n📊 ESTATÍSTICAS FINAIS:")
                print(f"   • Total processado: {self.contador_processados} linhas")
                print(f"   • Concorrência: {self.limite_concorrencia} simultâneas")
                print(f"   • Timeout: 4s (6s na 2ª tentativa)")
                print(f"   • Retry automático: Ativo para timeouts")
                print(f"   • Formato: Resposta bruta da API")


if __name__ == "__main__":
    print("=" * 60)
    print("🚀 CONSULTA RÁPIDA - NEO ENERGIA - SEM TRATAMENTO")
    print("=" * 60)
    print()
    
    # ⚡ INSTÂNCIA EQUILIBRADA: 3 consultas simultâneas, timeout 4s
    consulta = ConsultaContratoAsync(limite_concorrencia=3)
    
    try:
        asyncio.run(consulta.consultar_cadastro())
    except Exception as e:
        print(f"❌ Erro na execução: {e}")
    
    print("\n" + "=" * 60)
    print("⚡ EXECUÇÃO ULTRA RÁPIDA FINALIZADA")
    print("=" * 60)