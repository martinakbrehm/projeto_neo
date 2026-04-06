# Neo Energia - Script de Consulta Automática

Este projeto automatiza o processo de consulta de contratos na API da Neo Energia através de túnel SSH.

## 📋 Pré-requisitos

- Python 3.7+
- **PuTTY (plink) instalado no Windows** - [Como instalar](#-instalação-do-putty)
- Acesso SSH ao servidor
- Credenciais de acesso

### 🔧 Instalação do PuTTY

O PuTTY **não vem instalado por padrão** no Windows. Escolha uma das opções:

#### Opção 1: Instalador Oficial (Recomendado)
1. Acesse: https://www.putty.org/
2. Baixe o instalador Windows (.msi)
3. Execute o instalador como Administrador
4. O `plink.exe` ficará disponível no PATH automaticamente

#### Opção 2: Chocolatey (se você usa)
```powershell
choco install putty
```

#### Opção 3: Winget (Windows 10/11)
```powershell
winget install PuTTY.PuTTY
```

#### Opção 4: Download Direto
1. Baixe apenas o `plink.exe` de: https://the.earth.li/~sgtatham/putty/latest/w64/plink.exe
2. Coloque na pasta do projeto ou em uma pasta no PATH

**⚠️ Importante**: Após a instalação, abra um novo terminal para que o `plink` seja reconhecido.

## 🚀 Instalação

1. Clone ou baixe o projeto
2. Copie o arquivo `.env.example` para `.env`
3. Edite o arquivo `.env` com suas credenciais:

```env
SSH_USER=seu_usuario
SSH_SERVER=seu_servidor_ssh
SSH_PASSWORD=sua_senha
LOCAL_PORT=5000
REMOTE_HOST=ip_do_host_remoto
REMOTE_PORT=80
```

4. Instale as dependências:
```bash
pip install -r requirements.txt
```

## 🔧 Uso

Execute o script principal:
```bash
python executar_automatico.py
```

O script irá automaticamente:
1. Verificar e ativar a VPN no servidor
2. Criar túnel SSH
3. Testar a conectividade da API
4. Executar o script de consulta
5. Limpar as conexões

## 📁 Estrutura do Projeto

- `executar_automatico.py` - Script principal de automação
- `consulta_contrato.py` - Script de consulta de contratos
- `.env` - Arquivo de configurações (não versionado)
- `.env.example` - Template de configurações
- `requirements.txt` - Dependências do Python
- `.gitignore` - Arquivos ignorados pelo Git

## 🔒 Segurança

- As credenciais ficam no arquivo `.env` que não é versionado
- Chaves SSH e outros arquivos sensíveis são ignorados pelo Git
- Sempre use senhas fortes e mantenha as credenciais seguras

## 📝 Logs

O script exibe logs detalhados durante a execução:
- ✅ Operações bem-sucedidas
- ⚠️ Avisos e tentativas alternativas
- ❌ Erros que impedem a execução
- 🔍 Informações de debug

## 🆘 Solução de Problemas

### Problemas com PuTTY/plink:
1. **Erro "PuTTY/plink não encontrado"**: 
   - Instale o PuTTY seguindo as [instruções acima](#-instalação-do-putty)
   - Reinicie o terminal após a instalação
   - Teste digitando `plink` no terminal

2. **"'plink' is not recognized"**: 
   - Verifique se o PuTTY foi instalado corretamente
   - Adicione manualmente ao PATH: `C:\Program Files\PuTTY\`
   - Ou coloque o `plink.exe` na pasta do projeto

### Outros problemas:
3. **Erro de conexão SSH**: Verifique as credenciais no arquivo `.env`
4. **API não responde**: Verifique se a VPN está ativa no servidor
5. **Porta ocupada**: O script limpa conexões anteriores automaticamente
6. **Timeout na conexão**: Verifique firewall e conectividade de rede

### Teste rápido do PuTTY:
```cmd
plink -V
```
Deve mostrar a versão do PuTTY se estiver instalado corretamente.