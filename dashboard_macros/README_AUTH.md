# Dashboard de Macros - Autenticação

## Acesso ao Dashboard

O dashboard agora está protegido por autenticação básica.

### Credenciais de Acesso:
- **Usuário**: `neo`
- **Senha**: `dashboard2026`

### Como Acessar:
1. Abra o navegador em `http://127.0.0.1:8050` (local) ou a URL do Cloudflare
2. Quando solicitado, digite:
   - Usuário: `neo`
   - Senha: `dashboard2026`

### Para Deploy no Cloudflare:
- Configure o túnel Cloudflare para apontar para `http://127.0.0.1:8050`
- A autenticação será solicitada automaticamente pelo navegador
- Recomende usar HTTPS para proteger as credenciais

### Segurança:
- Esta é uma autenticação básica (HTTP Basic Auth)
- Recomenda-se usar HTTPS em produção para criptografar as credenciais
- Considere implementar autenticação mais robusta se necessário