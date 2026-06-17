# Kiro API Gateway

API Gateway centralizado com Load Balancing para gerenciar múltiplas contas Kiro.

## Funcionalidades

- **Round Robin Load Balancing** - Distribui requisições entre múltiplas contas Kiro
- **Failover Automático** - Se uma conta falha (429, 401, 403), pula para a próxima
- **Streaming (SSE)** - Respostas em tempo real para evitar timeout no Claude Desktop
- **Autenticação Interna** - Protege o gateway com X-API-Key
- **Cooldown Automático** - Contas desabilitadas são reativadas após 5 minutos
- **Health Check** - Endpoint `/health` para monitoramento do Render

## Deploy no Render.com

### 1. Push para GitHub

```bash
git init
git add .
git commit -m "Initial commit: Kiro API Gateway"
git remote add origin https://github.com/SEU_USER/kiro-gateway.git
git push -u origin main
```

### 2. Conectar ao Render

1. Acesse [render.com](https://render.com) e crie um **Web Service**
2. Conecte o repositório GitHub
3. O Render detectará o `render.yaml` automaticamente

### 3. Configurar Variáveis de Ambiente

No painel do Render, adicione:

| Variável | Descrição | Exemplo |
|----------|-----------|---------|
| `KIRO_TOKENS` | Tokens separados por vírgula | `token1,token2,token3` |
| `GATEWAY_API_KEY` | Chave de acesso ao gateway | `minha-chave-secreta-123` |
| `KIRO_BASE_URL` | URL base da API Kiro | `https://api.kiro.dev` |

Opcionais:

| Variável | Default | Descrição |
|----------|---------|-----------|
| `MAX_RETRIES` | `3` | Tentativas antes de falhar |
| `REQUEST_TIMEOUT` | `120` | Timeout em segundos |
| `ACCOUNT_COOLDOWN_SECONDS` | `300` | Tempo para reativar conta |
| `WEB_CONCURRENCY` | `2` | Número de workers |

### Formatos de Token

**Opção A** - Variável única (recomendado):
```
KIRO_TOKENS=token_abc123,token_def456,token_ghi789
```

**Opção B** - Variáveis separadas:
```
KIRO_TOKEN_1=token_abc123
KIRO_TOKEN_2=token_def456
KIRO_TOKEN_3=token_ghi789
```

## Uso nos Notebooks

Configure seus notebooks remotos para apontar para o gateway:

```python
import requests

GATEWAY_URL = "https://kiro-gateway.onrender.com"
API_KEY = "minha-chave-secreta-123"

response = requests.post(
    f"{GATEWAY_URL}/v1/messages",
    headers={
        "X-API-Key": API_KEY,
        "Content-Type": "application/json",
    },
    json={"model": "claude-3-5-sonnet", "messages": [...]},
    stream=True,
)

for chunk in response.iter_content():
    print(chunk.decode(), end="")
```

## Endpoints

| Método | Path | Auth | Descrição |
|--------|------|------|-----------|
| GET | `/health` | Não | Health check + stats |
| GET | `/stats` | Sim | Estatísticas detalhadas |
| ANY | `/{path}` | Sim | Proxy para API Kiro |

## Monitoramento

Acesse `/health` para ver o status do pool:

```json
{
  "status": "healthy",
  "pool": {
    "total_accounts": 3,
    "active_accounts": 2,
    "disabled_accounts": ["account_2"],
    "total_requests_served": 147
  }
}
```

## Desenvolvimento Local

```bash
pip install -r requirements.txt
export KIRO_TOKENS="token1,token2"
export GATEWAY_API_KEY="dev-key"
uvicorn main:app --reload --port 8000
```
