# Kiro Multi-Account Gateway

Gateway centralizado que transforma múltiplas contas Kiro em uma API compatível com OpenAI.

## Como funciona

1. Você loga em várias contas Kiro (gratuitas ou pagas)
2. Extrai o `refreshToken` de cada uma
3. Configura neste gateway
4. O gateway faz Round Robin entre as contas, refresh automático de tokens, e failover

## Funcionalidades

- **OpenAI Compatible** - Endpoint `/v1/chat/completions` funciona com qualquer cliente OpenAI
- **Multi-Account Pool** - Round Robin entre N contas
- **Auto Token Refresh** - Refresh automático quando token expira (a cada ~1h)
- **Failover** - Se conta dá erro/esgota, pula pra próxima
- **Streaming (SSE)** - Respostas em tempo real
- **Cooldown** - Contas com erro voltam após 5 min

## Como obter o Refresh Token

### Método 1: Arquivo de cache (mais fácil)

1. Instale o Kiro IDE e faça login (Google/GitHub)
2. Encontre o arquivo:
   - **Windows**: `%APPDATA%\Kiro\User\globalStorage\` ou `%USERPROFILE%\.aws\sso\cache\kiro-auth-token.json`
   - **macOS**: `~/Library/Application Support/Kiro/User/globalStorage/` ou `~/.aws/sso/cache/kiro-auth-token.json`
   - **Linux**: `~/.config/Kiro/User/globalStorage/` ou `~/.aws/sso/cache/kiro-auth-token.json`
3. Copie o valor de `refreshToken`

### Método 2: DevTools do Kiro IDE

1. No Kiro IDE, abra DevTools (Help → Toggle Developer Tools)
2. Na aba Network, filtre por `refreshToken`
3. Copie o refresh token da request/response

## Deploy no Render.com

### 1. Push para GitHub (já feito!)

O código já está em: https://github.com/stivencortez2026-afk/kiromanager

### 2. Criar Web Service no Render

1. [render.com](https://render.com) → New → Web Service
2. Conecte o repo `kiromanager`
3. Configure as variáveis de ambiente:

### 3. Variáveis de Ambiente

**Obrigatórias:**

| Variável | Descrição |
|----------|-----------|
| `KIRO_ACCOUNTS` | JSON com suas contas (veja formato abaixo) |
| `GATEWAY_API_KEY` | Chave que VOCÊ inventa para proteger o gateway |

**Formato do `KIRO_ACCOUNTS`:**

```json
[
  {"refresh_token": "SEU_REFRESH_TOKEN_CONTA_1", "profile_arn": ""},
  {"refresh_token": "SEU_REFRESH_TOKEN_CONTA_2", "profile_arn": ""},
  {"refresh_token": "SEU_REFRESH_TOKEN_CONTA_3", "profile_arn": ""}
]
```

**Alternativa simples (só tokens):**

```
KIRO_REFRESH_TOKENS=token1,token2,token3
```

**Opcionais:**

| Variável | Default | Descrição |
|----------|---------|-----------|
| `KIRO_REGION` | `us-east-1` | Região AWS |
| `MAX_RETRIES` | `3` | Tentativas por request |
| `REQUEST_TIMEOUT` | `300` | Timeout em segundos |
| `ACCOUNT_COOLDOWN_SECONDS` | `300` | Cooldown de conta com erro |

## Uso com Clientes OpenAI

### Python (OpenAI SDK)

```python
from openai import OpenAI

client = OpenAI(
    base_url="https://kiromanager.onrender.com/v1",
    api_key="SUA_GATEWAY_API_KEY",  # O que você definiu em GATEWAY_API_KEY
)

response = client.chat.completions.create(
    model="claude-sonnet-4",
    messages=[{"role": "user", "content": "Olá!"}],
    stream=True,
)

for chunk in response:
    if chunk.choices[0].delta.content:
        print(chunk.choices[0].delta.content, end="")
```

### Claude Desktop / Continue.dev

Configure como provedor OpenAI-compatible:
- Base URL: `https://kiromanager.onrender.com/v1`
- API Key: Sua `GATEWAY_API_KEY`

### curl

```bash
curl https://kiromanager.onrender.com/v1/chat/completions \
  -H "X-API-Key: SUA_GATEWAY_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "claude-sonnet-4",
    "messages": [{"role": "user", "content": "Olá!"}],
    "stream": true
  }'
```

## Endpoints

| Método | Path | Auth | Descrição |
|--------|------|------|-----------|
| GET | `/health` | Não | Health check |
| GET | `/stats` | Sim | Estatísticas do pool |
| GET | `/v1/models` | Sim | Lista modelos |
| POST | `/v1/chat/completions` | Sim | Chat (OpenAI format) |

## Modelos Disponíveis

- `auto` - Modelo padrão do plano
- `claude-sonnet-4` - Claude Sonnet 4
- `claude-sonnet-4.5` - Claude Sonnet 4.5
- `claude-haiku-4.5` - Claude Haiku 4.5
- `claude-opus-4.5` - Claude Opus 4.5 (requer plano pago)
- `claude-3.7-sonnet` - Claude 3.7 Sonnet (legacy)

## Dev Local

```bash
pip install -r requirements.txt
set KIRO_REFRESH_TOKENS=seu_token_aqui
set GATEWAY_API_KEY=dev-key
uvicorn main:app --reload --port 8000
```
