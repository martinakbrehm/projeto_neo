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
        
    def criar_janela(self):
        """Cria a janela de controle em thread separada"""
        self.root = tk.Tk()
        self.root.title("Controle de Consulta - Neo Energia")
        self.root.geometry("400x200")
        self.root.resizable(False, False)
        
        # Centraliza a janela
        self.root.eval('tk::PlaceWindow . center')
        
        # Frame principal
        frame_main = ttk.Frame(self.root, padding="20")
        frame_main.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Título
        ttk.Label(frame_main, text="🚀 Consulta Neo Energia", 
                 font=("Arial", 14, "bold")).grid(row=0, column=0, columnspan=2, pady=(0, 20))
        
        # Status
        self.label_status = ttk.Label(frame_main, text="🔄 Processando consultas...", 
                                     font=("Arial", 10))
        self.label_status.grid(row=1, column=0, columnspan=2, pady=(0, 20))
        
        # Botões
        ttk.Button(frame_main, text="⏸️ Parar Processo", 
                  command=self.parar, width=20).grid(row=2, column=0, padx=(0, 10))
        
        ttk.Button(frame_main, text="❌ Encerrar Aplicação", 
                  command=self.encerrar, width=20).grid(row=2, column=1, padx=(10, 0))
        
        # Informações
        ttk.Label(frame_main, text="• Parar: Finaliza consultas atuais e salva resultados\n• Encerrar: Fecha aplicação imediatamente", 
                 font=("Arial", 8), foreground="gray").grid(row=3, column=0, columnspan=2, pady=(20, 0))
        
        # Protocolo de fechamento
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        
        # Inicia loop
        self.root.mainloop()
    
    def parar(self):
        """Sinaliza para parar o processo atual"""
        self.parar_processo = True
        self.label_status.config(text="⏸️ Parando processo...")
        messagebox.showinfo("Parar Processo", "Processo será interrompido após a consulta atual.")
    
    def encerrar(self):
        """Encerra a aplicação imediatamente"""
        if messagebox.askyesno("Encerrar Aplicação", "Deseja realmente encerrar a aplicação?\n\nTodos os dados não salvos serão perdidos."):
            self.encerrar_aplicacao = True
            self.parar_processo = True
            self.root.quit()
            sys.exit(0)
    
    def on_closing(self):
        """Trata o fechamento da janela"""
        self.encerrar()
    
    def iniciar_em_thread(self):
        """Inicia a janela em thread separada"""
        self.thread_janela = threading.Thread(target=self.criar_janela, daemon=True)
        self.thread_janela.start()
    
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
    def __init__(self, limite_concorrencia=1):
        self.caminho_excel = None
        self.caminho_saida = r"C:\Users\gismi\OneDrive\Desktop\auto_api_neo\consulta_neo_reinan\dados"
        self.resultados = []
        self.limite_concorrencia = limite_concorrencia
        self.semaforo = None
        self.linha_inicial = 0  # Inicia da primeira linha de dados (índice 0 = linha 2 do Excel)
        self.cache_memoria = {}  # Cache em memória para respostas antes de gravar
        self.contador_cache = 0  # Contador para saber quando gravar
        
        # **NOVO**: Janela de controle
        self.janela_controle = JanelaControle()
        
        # Garante que o diretório de saída existe
        if not os.path.exists(self.caminho_saida):
            os.makedirs(self.caminho_saida)
            print(f"📁 Diretório criado: {self.caminho_saida}")
        
        # Garante que o diretório de saída existe
        if not os.path.exists(self.caminho_saida):
            os.makedirs(self.caminho_saida)
            print(f"📁 Diretório criado: {self.caminho_saida}")
        
        # Colunas esperadas da resposta da API
        self.colunas_api = [
            "Error", "CodigoRetorno", "Msg", "Status", 
            "QtdFaturas", "VlrDebito", "VlrCredito", 
            "DtAtivacaoContrato", "ParcelamentoAtivo", "DetalheParcelamento"
        ]

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
        """Tenta iniciar o túnel SSH automaticamente usando o script executar_automatico.py"""
        script_tunel = os.path.join(os.path.dirname(__file__), "executar_automatico.py")
        
        if os.path.exists(script_tunel):
            print("🔗 Tentando iniciar túnel SSH automaticamente...")
            try:
                # Executa o script de túnel em background
                processo = subprocess.Popen(
                    [sys.executable, script_tunel],
                    creationflags=subprocess.CREATE_NO_WINDOW,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                
                # Aguarda um pouco para o túnel estabelecer
                time.sleep(10)
                
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
            
            # Pergunta se quer tentar iniciar automaticamente
            try:
                root = tk.Tk()
                root.withdraw()
                
                resposta = messagebox.askyesno(
                    "Túnel SSH Inativo",
                    "O túnel SSH não está ativo na porta 5000.\n\n"
                    "Deseja tentar iniciar automaticamente?\n\n"
                    "Se escolher 'Não', você precisará iniciar o túnel manualmente."
                )
                
                root.destroy()
                
                if resposta:
                    if self.iniciar_tunel_automatico():
                        return self.testar_api_real()
                    else:
                        print("❌ Falha ao iniciar túnel. Inicie manualmente e tente novamente.")
                        return False
                else:
                    print("ℹ️ Por favor, inicie o túnel SSH manualmente e execute novamente.")
                    return False
                    
            except Exception:
                # Fallback para terminal
                resposta = input("Tentar iniciar túnel automaticamente? (s/n): ").lower().strip()
                if resposta in ['s', 'sim', 'y', 'yes']:
                    if self.iniciar_tunel_automatico():
                        return self.testar_api_real()
                    else:
                        print("❌ Falha ao iniciar túnel. Inicie manualmente e tente novamente.")
                        return False
                else:
                    print("ℹ️ Por favor, inicie o túnel SSH manualmente e execute novamente.")
                    return False
        else:
            print("✅ Túnel SSH ativo na porta 5000")
            return self.testar_api_real()

    def testar_api_real(self):
        """Testa se a API está realmente respondendo com uma requisição real"""
        print("🧪 Testando conectividade com a API real...")
        
        try:
            import httpx
            
            # URL de teste da API
            url_teste = "http://localhost:5000/validacaotitularidade/Validacao/ValidarTitularidade?ContaContrato=123456789&CpfCnpj=12345678901&Empresa=coelba"
            
            print(f"🔗 Testando URL: {url_teste}")
            
            # Configuração de timeout mais baixo para teste
            timeout_config = httpx.Timeout(connect=10.0, read=15.0, write=10.0, pool=10.0)
            
            with httpx.Client(timeout=timeout_config) as client:
                response = client.get(url_teste)
                
                print(f"📊 Status Code: {response.status_code}")
                print(f"📋 Headers: {dict(response.headers)}")
                print(f"📄 Resposta (primeiros 200 chars): {response.text[:200]}...")
                
                if response.status_code == 200:
                    # Verifica se a resposta parece ser da API correta
                    if "Error" in response.text or "CodigoRetorno" in response.text:
                        print("✅ API respondendo corretamente!")
                        return True
                    else:
                        print("⚠️ API respondeu, mas formato inesperado")
                        print(f"Resposta completa: {response.text}")
                        return True  # Considera sucesso mesmo assim
                else:
                    print(f"❌ API retornou status {response.status_code}")
                    print(f"Resposta: {response.text}")
                    return False
                    
        except httpx.ConnectError as e:
            print(f"❌ Erro de conexão: {e}")
            print("💡 Possíveis causas:")
            print("   - Túnel SSH não está encaminhando corretamente")
            print("   - API não está rodando no servidor")
            print("   - Firewall bloqueando conexão")
            return False
            
        except httpx.TimeoutException:
            print("⏱️ Timeout na conexão com a API")
            print("💡 A API pode estar muito lenta ou não respondendo")
            return False
            
        except Exception as e:
            print(f"❌ Erro inesperado ao testar API: {e}")
            return False

    def selecionar_arquivo(self):
        """Abre diálogo para seleção do arquivo Excel"""
        try:
            root = tk.Tk()
            root.withdraw()  # Esconde a janela principal
            
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

    async def gravar_cache_na_planilha(self):
        """Grava todo o cache de memória na planilha de uma vez"""
        try:
            print(f"\n📝 Gravando {len(self.cache_memoria)} itens do cache na planilha...")
            
            # Carrega a planilha atual
            df = pd.read_excel(self.caminho_excel)
            
            # Garante que as colunas da API existem
            df = self.preparar_colunas_planilha(df)
            
            # Aplica todas as mudanças do cache
            itens_gravados = 0
            for row_index, dados in self.cache_memoria.items():
                dados_api = dados['dados_api']
                
                # Atualiza o DataFrame com os dados da API
                for coluna in self.colunas_api:
                    if coluna in df.columns:
                        valor = dados_api.get(coluna, "")
                        if pd.isna(valor) or valor is None:
                            valor = ""
                        df.loc[row_index, coluna] = str(valor)
                
                itens_gravados += 1
            
            # Salva a planilha
            if self.salvar_planilha_seguro(df, parcial=True):
                print(f"✅ {itens_gravados} itens gravados com sucesso!")
                
                # Limpa o cache
                self.cache_memoria.clear()
                self.contador_cache = 0
                print(f"🧹 Cache limpo. Próxima gravação em 100 itens.")
                
            else:
                print(f"❌ Erro ao gravar cache na planilha!")
                
        except Exception as e:
            print(f"❌ Erro ao gravar cache: {e}")
            import traceback
            print(f"   Traceback: {traceback.format_exc()}")

    def criar_planilha_resultado(self):
        """Cria planilha de resultado com todos os dados consultados"""
        try:
            if not self.resultados:
                print("⚠️ Nenhum resultado para salvar")
                return False
            
            # Define colunas da planilha de resultado
            colunas_resultado = [
                "cpf", "codigo_cliente", "empresa", "Error", "CodigoRetorno", "Msg", "Status", 
                "QtdFaturas", "VlrDebito", "VlrCredito", 
                "DtAtivacaoContrato", "ParcelamentoAtivo", "DetalheParcelamento"
            ]
            
            # Cria DataFrame com os resultados
            df_resultado = pd.DataFrame(self.resultados, columns=colunas_resultado)
            
            # Gera nome do arquivo com timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            nome_arquivo = f"resultado_consulta_{timestamp}.xlsx"
            caminho_completo = os.path.join(self.caminho_saida, nome_arquivo)
            
            # Salva a planilha
            df_resultado.to_excel(caminho_completo, index=False, engine='openpyxl')
            
            print(f"✅ Planilha de resultado criada: {caminho_completo}")
            print(f"📊 Total de registros salvos: {len(self.resultados)}")
            
            # REMOVIDO: Não abre mais automaticamente
            # try:
            #     subprocess.run(["start", "", caminho_completo], shell=True)
            #     print("📂 Arquivo aberto automaticamente")
            # except:
            #     pass
                
            return True
            
        except Exception as e:
            print(f"❌ Erro ao criar planilha de resultado: {e}")
            return False

    def salvar_planilha_seguro(self, df, parcial=False):
        """Salva a planilha de forma segura para evitar corrupção"""
        try:
            # Cria arquivo temporário com extensão .xlsx
            base_name = os.path.splitext(self.caminho_excel)[0]
            temp_path = f"{base_name}_temp_{int(time.time())}.xlsx"
            
            # Force conversion of any problematic data types
            df_copy = df.copy()
            for col in self.colunas_api:
                if col in df_copy.columns:
                    # Converte para string primeiro, depois substitui NaN por string vazia
                    df_copy[col] = df_copy[col].astype(str).replace('nan', '')
            
            # Salva no arquivo temporário
            df_copy.to_excel(temp_path, index=False, engine='openpyxl')
            
            # Se salvou com sucesso, substitui o original
            if os.path.exists(temp_path) and os.path.getsize(temp_path) > 0:
                # Remove o arquivo original se existir
                if os.path.exists(self.caminho_excel):
                    os.remove(self.caminho_excel)
                
                # Move o temporário para o lugar do original
                shutil.move(temp_path, self.caminho_excel)
                
                if not parcial:
                    print(f"💾 Planilha salva com segurança: {os.path.basename(self.caminho_excel)}")
                
                return True
            else:
                print("❌ Erro: arquivo temporário não foi criado ou está vazio")
                return False
                
        except Exception as e:
            print(f"❌ Erro ao salvar planilha: {e}")
            import traceback
            print(f"   Traceback: {traceback.format_exc()}")
            
            # Remove arquivo temporário se existir
            try:
                if 'temp_path' in locals() and os.path.exists(temp_path):
                    os.remove(temp_path)
            except:
                pass
                    
            return False

    def preparar_colunas_planilha(self, df):
        """Prepara as colunas da planilha adicionando colunas da API se necessário"""
        colunas_existentes = df.columns.tolist()
        
        # Verifica se as colunas da API já existem
        colunas_faltando = []
        for coluna in self.colunas_api:
            if coluna not in colunas_existentes:
                colunas_faltando.append(coluna)
        
        if colunas_faltando:
            print(f"➕ Adicionando colunas: {', '.join(colunas_faltando)}")
            
            # Adiciona as colunas faltando após a coluna C (empresa)
            for i, coluna in enumerate(colunas_faltando):
                # Insere após a coluna "empresa" (índice 2)
                pos = 3 + i
                # **CORREÇÃO**: Remove dtype inválido do insert()
                df.insert(pos, coluna, "")
                
        # **CORREÇÃO**: Garante que todas as colunas da API sejam do tipo string
        for coluna in self.colunas_api:
            if coluna in df.columns:
                df[coluna] = df[coluna].astype('object')
                
        return df

    @staticmethod
    def padronizar_cpf_cnpj(valor):
        if pd.isna(valor):
            return ""

        valor_str = str(valor)
        somente_digitos = "".join(
            c for c in valor_str if c.isdigit()
        )

        if len(somente_digitos) <= 11:
            return somente_digitos.zfill(11)  # cpf
        else:
            return somente_digitos.zfill(14)  # cnpj

    def parsear_resposta_api(self, resposta_text):
        """Parseia a resposta da API de forma otimizada - foca apenas nos dados necessários"""
        try:
            # Verifica se é JSON válido de forma rápida
            if not resposta_text or len(resposta_text) < 2:
                return {"Error": "true", "Msg": "Resposta vazia da API"}
            
            # Parse rápido do JSON
            if resposta_text.strip().startswith('{') and resposta_text.strip().endswith('}'):
                dados = json.loads(resposta_text)
                
                # Extrai apenas os campos necessários para otimizar velocidade
                resultado = {
                    "Error": str(dados.get("Error", "false")).lower(),
                    "CodigoRetorno": str(dados.get("CodigoRetorno", "")),
                    "Msg": str(dados.get("Msg", "")),
                    "Status": str(dados.get("Status", "")),
                    "QtdFaturas": str(dados.get("QtdFaturas", "")),
                    "VlrDebito": str(dados.get("VlrDebito", "")),
                    "VlrCredito": str(dados.get("VlrCredito", "")),
                    "DtAtivacaoContrato": str(dados.get("DtAtivacaoContrato", "")),
                    "ParcelamentoAtivo": str(dados.get("ParcelamentoAtivo", "")),
                    "DetalheParcelamento": str(dados.get("DetalheParcelamento", ""))
                }
                return resultado
            else:
                # Se não for JSON, retorna erro
                return {"Error": "true", "Msg": f"Resposta inválida: {resposta_text[:100]}..."}
                
        except json.JSONDecodeError as e:
            return {"Error": "true", "Msg": f"JSON inválido: {str(e)[:50]}..."}
        except Exception as e:
            return {"Error": "true", "Msg": f"Erro no parsing: {type(e).__name__}"}

    async def consultar_linhas(self, row_index_df, linha_display, contrato, cpf, empresa, client):
        """Consulta uma linha - OTIMIZADO PARA VELOCIDADE MÁXIMA"""
        
        url = (
            f"http://localhost:5000/validacaotitularidade/Validacao/"
            f"ValidarTitularidade?ContaContrato={contrato}&CpfCnpj={cpf}&Empresa={empresa}"
        )

        # **TIMEOUT OTIMIZADO**: Reduzido para evitar ReadTimeout
        timeout_dinamico = 6.0  # 6 segundos - mais conservador para evitar timeout
        
        print(f"🔗 [Linha {linha_display}] UC: {contrato}")
        
        async with self.semaforo:
            try:
                # **RESPOSTA DINÂMICA**: Aguarda apenas o necessário
                response = await client.get(url, timeout=timeout_dinamico)
                
                resultado_texto = response.text
                print(f"✅ [Linha {linha_display}] OK")
                
                # Parseia a resposta
                dados_api = self.parsear_resposta_api(resultado_texto)
                
                # **NOVO**: Acumula resultado para planilha de saída
                resultado = [
                    cpf, contrato, empresa,  # Dados de entrada
                    dados_api.get("Error", ""),
                    dados_api.get("CodigoRetorno", ""),
                    dados_api.get("Msg", ""),
                    dados_api.get("Status", ""),
                    dados_api.get("QtdFaturas", ""),
                    dados_api.get("VlrDebito", ""),
                    dados_api.get("VlrCredito", ""),
                    dados_api.get("DtAtivacaoContrato", ""),
                    dados_api.get("ParcelamentoAtivo", ""),
                    dados_api.get("DetalheParcelamento", "")
                ]
                self.resultados.append(resultado)
                
                # **CACHE CORRIGIDO**: Salva com o índice correto do DataFrame
                self.cache_memoria[row_index_df] = {
                    'contrato': contrato,
                    'cpf': cpf,
                    'empresa': empresa,
                    'dados_api': dados_api,
                    'sucesso': True
                }
                
                self.contador_cache += 1
                
            except httpx.ReadTimeout:
                # **TIMEOUT ESPECÍFICO**: Trata ReadTimeout separadamente
                print(f"⏱️ [Linha {linha_display}] Timeout - API demorou mais que 2s")
                
                # **NOVO**: Acumula resultado de timeout para planilha de saída
                resultado_timeout = [
                    cpf, contrato, empresa,  # Dados de entrada
                    "true", "", "Timeout - API não respondeu em 6s", "TIMEOUT",
                    "", "", "", "", "", ""  # Campos vazios
                ]
                self.resultados.append(resultado_timeout)
                
                self.cache_memoria[row_index_df] = {
                    'contrato': contrato,
                    'cpf': cpf,
                    'empresa': empresa,
                    'dados_api': {"Error": "true", "Msg": "Timeout - API não respondeu"},
                    'sucesso': False
                }
                self.contador_cache += 1
                
            except Exception as e:
                # **FALHA RÁPIDA**: Outros erros
                print(f"❌ [Linha {linha_display}] Erro: {type(e).__name__}")
                
                # **NOVO**: Acumula resultado de erro para planilha de saída
                resultado_erro = [
                    cpf, contrato, empresa,  # Dados de entrada
                    "true", "", f"Erro: {type(e).__name__}", "ERRO",
                    "", "", "", "", "", ""  # Campos vazios
                ]
                self.resultados.append(resultado_erro)
                
                self.cache_memoria[row_index_df] = {
                    'contrato': contrato,
                    'cpf': cpf,
                    'empresa': empresa,
                    'dados_api': {"Error": "true", "Msg": f"Erro: {type(e).__name__}"},
                    'sucesso': False
                }
                self.contador_cache += 1

        # **GRAVAÇÃO A CADA 100**: Grava quando cache chega a 100 itens
        if self.contador_cache >= 100:
            await self.gravar_cache_na_planilha()

    async def consultar_cadastro(self):
        """Método principal para consulta - COM CONTROLE DE INTERFACE"""
        
        # **NOVO**: Inicia janela de controle
        print("🚀 Iniciando janela de controle...")
        self.janela_controle.iniciar_em_thread()
        time.sleep(1)  # Aguarda janela inicializar
        
        try:
            # Verifica conectividade com a API antes de tudo
            self.janela_controle.atualizar_status("🔍 Verificando conectividade com API...")
            print("🔍 Verificando conectividade com a API...")
            if not self.verificar_conectividade_api():
                print("❌ Não é possível continuar sem conectividade com a API")
                self.janela_controle.atualizar_status("❌ Erro: API não conectada")
                return
            
            # Seleciona arquivo
            self.janela_controle.atualizar_status("📁 Selecionando arquivo de entrada...")
            if not self.selecionar_arquivo():
                self.janela_controle.atualizar_status("❌ Nenhum arquivo selecionado")
                return

            # **SIMPLIFICADO**: Sempre inicia da linha 2 (índice 1)
            print("🎯 Configuração: SEMPRE inicia da linha 2")

            # Cria o semáforo
            self.semaforo = asyncio.Semaphore(self.limite_concorrencia)

            # Carrega planilha
            self.janela_controle.atualizar_status("📊 Carregando planilha...")
            try:
                df = pd.read_excel(self.caminho_excel)
                print(f"📄 Planilha carregada: {len(df)} linhas")
            except Exception as e:
                print(f"❌ Erro ao ler o Excel: {e}")
                self.janela_controle.atualizar_status("❌ Erro ao ler planilha")
                return

            # Processa dados...
            await self._processar_consultas(df)
            
        except Exception as e:
            print(f"❌ Erro geral: {e}")
            self.janela_controle.atualizar_status(f"❌ Erro: {str(e)[:50]}...")
            
        finally:
            # Fecha janela de controle
            self.janela_controle.atualizar_status("✅ Processo finalizado")
            time.sleep(2)
            self.janela_controle.fechar_janela()
    
    async def _processar_consultas(self, df):
        print("🔍 Verificando conectividade com a API...")
        if not self.verificar_conectividade_api():
            print("❌ Não é possível continuar sem conectividade com a API")
            return
        
        # Seleciona arquivo
        if not self.selecionar_arquivo():
            return

        # **SIMPLIFICADO**: Sempre inicia da linha 2 (índice 1)
        print("🎯 Configuração: SEMPRE inicia da linha 2")

        # Cria o semáforo
        self.semaforo = asyncio.Semaphore(self.limite_concorrencia)

        try:
            df = pd.read_excel(self.caminho_excel)
            print(f"📄 Planilha carregada: {len(df)} linhas")
        except Exception as e:
            print(f"❌ Erro ao ler o Excel: {e}")
            return

        # Limpa os nomes das colunas (remove espaços extras)
        df.columns = df.columns.str.strip()
        
        print(f"📋 Colunas após limpeza: {list(df.columns)}")

        # Verifica colunas obrigatórias
        colunas_esperadas = {"codigo cliente", "cpf", "empresa"}
        if not colunas_esperadas.issubset(df.columns):
            print(f"❌ Erro: o arquivo precisa conter as colunas {colunas_esperadas}")
            print(f"Colunas encontradas: {list(df.columns)}")
            return

        # Prepara colunas da planilha
        df = self.preparar_colunas_planilha(df)
        
        # Salva a planilha com as novas colunas se foram adicionadas
        self.salvar_planilha_seguro(df, parcial=True)

        # **TIMEOUT OTIMIZADO**: Ajustado para evitar ReadTimeout
        timeout_config = httpx.Timeout(connect=8.0, read=12.0, write=8.0, pool=8.0)
        
        async with httpx.AsyncClient(timeout=timeout_config) as client:
            
            try:
                total_linhas = len(df)
                
                # **CORREÇÃO CRUCIAL**: Garante que sempre processe da linha 2 real do Excel
                # Usa reset_index() para garantir índices sequenciais
                df_reset = df.reset_index(drop=True)
                
                # Agora sim, pega da linha 1 em diante (linha 2 do Excel)
                linhas_para_processar = df_reset.iloc[self.linha_inicial:]
                
                print(f"📊 Total de linhas: {total_linhas}")
                print(f"🎯 SEMPRE iniciando da linha: {self.linha_inicial + 1} (linha 2)")
                print(f"🔄 Linhas a processar: {len(linhas_para_processar)}")
                print(f"⚡ Processamento sequencial")
                print(f"⏱️ Timeout: 6s por requisição (otimizado para estabilidade)")
                print(f"💾 Cache em memória: gravação a cada 100 respostas")
                print()
                
                # **PROCESSAMENTO SEQUENCIAL COM ÍNDICES CORRETOS**
                for idx, (_, row) in enumerate(tqdm(linhas_para_processar.iterrows(), desc="Consultando", total=len(linhas_para_processar))):
                    contrato = row["codigo cliente"]
                    cpf = self.padronizar_cpf_cnpj(row["cpf"])
                    empresa = row["empresa"]

                    # Para exibição: linha real do Excel (começa em 2)
                    linha_display = idx + 2  # idx=0 -> linha 2, idx=1 -> linha 3, etc.
                    
                    # Para gravação: usa o índice correto sequencial
                    row_index_gravacao = self.linha_inicial + idx
                    
                    # Processa uma linha por vez (sequencial)
                    await self.consultar_linhas(row_index_gravacao, linha_display, contrato, cpf, empresa, client)
                
            except KeyboardInterrupt:
                print("\n⛔ Execução interrompida pelo usuário.")
                # Grava cache restante antes de sair
                if self.cache_memoria:
                    print("💾 Gravando cache restante...")
                    await self.gravar_cache_na_planilha()
                
            finally:
                # Grava qualquer cache restante
                if self.cache_memoria:
                    print(f"\n💾 Gravando {len(self.cache_memoria)} itens restantes do cache...")
                    await self.gravar_cache_na_planilha()
                
                print(f"\n✅ Processamento finalizado!")
                
                # **NOVO**: Cria planilha de resultado separada
                print(f"\n📊 Criando planilha de resultado...")
                if self.criar_planilha_resultado():
                    print(f"✅ Planilha de resultado criada com sucesso!")
                else:
                    print(f"❌ Erro ao criar planilha de resultado")
                
                print(f"ℹ️ Sempre iniciará da linha 2 na próxima execução.")

    async def _processar_consultas(self, df):
        """Processa as consultas com verificação de parada"""
        
        # Limpa os nomes das colunas (remove espaços extras)
        df.columns = df.columns.str.strip()
        print(f"📋 Colunas após limpeza: {list(df.columns)}")

        # Verifica colunas obrigatórias
        colunas_esperadas = {"codigo cliente", "cpf", "empresa"}
        if not colunas_esperadas.issubset(df.columns):
            print(f"❌ Erro: o arquivo precisa conter as colunas {colunas_esperadas}")
            print(f"Colunas encontradas: {list(df.columns)}")
            self.janela_controle.atualizar_status("❌ Colunas inválidas na planilha")
            return

        # Prepara colunas da planilha
        df = self.preparar_colunas_planilha(df)
        
        # Salva a planilha com as novas colunas se foram adicionadas
        self.salvar_planilha_seguro(df, parcial=True)

        # **OTIMIZAÇÃO MÁXIMA**: Timeout muito baixo
        timeout_config = httpx.Timeout(connect=1.0, read=2.0, write=1.0, pool=1.0)
        
        async with httpx.AsyncClient(timeout=timeout_config) as client:
            
            try:
                total_linhas = len(df)
                
                # **CORREÇÃO CRUCIAL**: Garante que sempre processe da linha 2 real do Excel
                df_reset = df.reset_index(drop=True)
                linhas_para_processar = df_reset.iloc[self.linha_inicial:]
                
                print(f"📊 Total de linhas: {total_linhas}")
                print(f"🎯 Iniciando da primeira linha de dados (linha 2 do Excel)")
                print(f"🔄 Linhas a processar: {len(linhas_para_processar)}")
                print(f"⚡ Processamento sequencial")
                print(f"⏱️ Timeout otimizado: 2s por requisição")
                print(f"💾 Cache em memória: gravação a cada 100 respostas")
                print()
                
                # **PROCESSAMENTO SEQUENCIAL COM VERIFICAÇÃO DE PARADA**
                for idx, (_, row) in enumerate(tqdm(linhas_para_processar.iterrows(), desc="Consultando", total=len(linhas_para_processar))):
                    
                    # **NOVO**: Verifica se deve parar
                    if self.janela_controle.parar_processo:
                        print(f"\n⏸️ Processo interrompido pelo usuário na linha {idx + 2}")
                        self.janela_controle.atualizar_status("⏸️ Processo parado pelo usuário")
                        break
                    
                    # **NOVO**: Verifica se deve encerrar
                    if self.janela_controle.encerrar_aplicacao:
                        print(f"\n❌ Aplicação encerrada pelo usuário")
                        sys.exit(0)
                    
                    contrato = row["codigo cliente"]
                    cpf = self.padronizar_cpf_cnpj(row["cpf"])
                    empresa = row["empresa"]

                    # Para exibição: linha real do Excel (começa em 2)
                    linha_display = idx + 2  # idx=0 -> linha 2, idx=1 -> linha 3, etc.
                    
                    # Para gravação: usa o índice correto sequencial
                    row_index_gravacao = self.linha_inicial + idx
                    
                    # **NOVO**: Atualiza status na janela
                    self.janela_controle.atualizar_status(f"🔄 Processando linha {linha_display} ({idx+1}/{len(linhas_para_processar)})")
                    
                    # Processa uma linha por vez (sequencial)
                    await self.consultar_linhas(row_index_gravacao, linha_display, contrato, cpf, empresa, client)
                
            except KeyboardInterrupt:
                print("\n⛔ Execução interrompida pelo usuário.")
                self.janela_controle.atualizar_status("⛔ Interrompido (Ctrl+C)")
                # Grava cache restante antes de sair
                if self.cache_memoria:
                    print("💾 Gravando cache restante...")
                    await self.gravar_cache_na_planilha()
                
            finally:
                # Grava qualquer cache restante
                if self.cache_memoria:
                    print(f"\n💾 Gravando {len(self.cache_memoria)} itens restantes do cache...")
                    await self.gravar_cache_na_planilha()
                
                print(f"\n✅ Processamento finalizado!")
                
                # **NOVO**: Cria planilha de resultado separada
                self.janela_controle.atualizar_status("📊 Criando planilha de resultado...")
                print(f"\n📊 Criando planilha de resultado...")
                if self.criar_planilha_resultado():
                    print(f"✅ Planilha de resultado criada com sucesso!")
                    self.janela_controle.atualizar_status("✅ Planilha de resultado criada!")
                else:
                    print(f"❌ Erro ao criar planilha de resultado")
                    self.janela_controle.atualizar_status("❌ Erro ao criar planilha")


if __name__ == "__main__":
    print("=" * 60)
    print("🚀 CONSULTA DE CONTRATOS - NEO ENERGIA - OTIMIZADO")

    print("=" * 60)
    print()
    
    # Cria instância da consulta com concorrência 1 para garantir ordem
    consulta = ConsultaContratoAsync(limite_concorrencia=1)
    
    try:
        # Executa a consulta
        asyncio.run(consulta.consultar_cadastro())
    except Exception as e:
        print(f"❌ Erro na execução: {e}")
    
    print("\n" + "=" * 60)
    print("✅ EXECUÇÃO FINALIZADA")
    print("=" * 60)