"""
Kiro API Gateway - Multi-Account Pool com Auto Token Refresh
=============================================================
Gateway centralizado que gerencia múltiplas contas Kiro com:
- Round Robin entre contas
- Refresh automático de tokens (Kiro Desktop Auth)
- Failover quando conta esgota créditos
- Streaming (SSE) para Claude Desktop / OpenAI clients
- Compatível com formato OpenAI /v1/chat/completions

Deploy: Render.com com Gunicorn + Uvicorn workers
"""

import os
import json
import time
import asyncio
import hashlib
import uuid
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List, Any

import httpx
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.security import APIKeyHeader

# ─── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("kiro-gateway")

# ─── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Kiro Multi-Account Gateway",
    description="API Gateway com pool de contas Kiro, auto-refresh e streaming",
    version="2.0.0",
)

# ─── Configuração ─────────────────────────────────────────────────────────────
GATEWAY_API_KEY = os.getenv("GATEWAY_API_KEY", "")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "300"))
ACCOUNT_COOLDOWN = int(os.getenv("ACCOUNT_COOLDOWN_SECONDS", "300"))
REGION = os.getenv("KIRO_REGION", "us-east-1")

# URLs da API Kiro (baseado no código do kiro-gateway original)
KIRO_REFRESH_URL = f"https://prod.{REGION}.auth.desktop.kiro.dev/refreshToken"
KIRO_API_HOST = f"https://codewhisperer.{REGION}.amazonaws.com"
KIRO_Q_HOST = f"https://q.{REGION}.amazonaws.com"

# ─── Security ──────────────────────────────────────────────────────────────────
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(api_key: Optional[str] = Depends(api_key_header)):
    if not GATEWAY_API_KEY:
        return True
    if api_key != GATEWAY_API_KEY:
        raise HTTPException(status_code=401, detail="X-API-Key inválida ou ausente.")
    return True


# ─── Fingerprint (simula o que o Kiro IDE envia) ──────────────────────────────

def get_machine_fingerprint() -> str:
    """Gera um fingerprint de máquina consistente."""
    machine_id = os.getenv("MACHINE_ID", str(uuid.uuid4()))
    return hashlib.sha256(machine_id.encode()).hexdigest()[:32]


FINGERPRINT = get_machine_fingerprint()


# ─── Kiro Account Manager ─────────────────────────────────────────────────────

class KiroAccount:
    """Gerencia uma conta Kiro individual com auto-refresh de token."""

    def __init__(self, account_id: str, refresh_token: str, profile_arn: str = ""):
        self.id = account_id
        self.refresh_token = refresh_token
        self.profile_arn = profile_arn
        self.access_token: Optional[str] = None
        self.expires_at: Optional[datetime] = None
        self.requests_served: int = 0
        self.errors: int = 0
        self.last_used: Optional[float] = None
        self.disabled: bool = False
        self.disabled_at: Optional[float] = None
        self._lock = asyncio.Lock()

    def is_token_valid(self) -> bool:
        """Verifica se o access token ainda é válido."""
        if not self.access_token or not self.expires_at:
            return False
        now = datetime.now(timezone.utc)
        # Refresh 10 min antes de expirar
        return now < (self.expires_at - timedelta(minutes=10))

    async def get_access_token(self) -> str:
        """Retorna um access token válido, fazendo refresh se necessário."""
        async with self._lock:
            if self.is_token_valid():
                return self.access_token

            await self._refresh()
            return self.access_token

    async def _refresh(self):
        """Faz refresh do token via Kiro Desktop Auth endpoint."""
        logger.info(f"[{self.id}] Fazendo refresh do token...")

        headers = {
            "Content-Type": "application/json",
            "User-Agent": f"KiroIDE-0.7.45-{FINGERPRINT}",
        }
        payload = {"refreshToken": self.refresh_token}

        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(KIRO_REFRESH_URL, json=payload, headers=headers)

            if response.status_code != 200:
                error_text = response.text[:200]
                logger.error(f"[{self.id}] Refresh falhou: HTTP {response.status_code} - {error_text}")
                raise Exception(f"Token refresh failed: HTTP {response.status_code}")

            data = response.json()

        new_access = data.get("accessToken")
        new_refresh = data.get("refreshToken")
        expires_in = data.get("expiresIn", 3600)
        new_profile = data.get("profileArn")

        if not new_access:
            raise Exception(f"[{self.id}] Refresh não retornou accessToken")

        self.access_token = new_access
        if new_refresh:
            self.refresh_token = new_refresh
        if new_profile:
            self.profile_arn = new_profile

        self.expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in - 60)
        logger.info(f"[{self.id}] Token atualizado. Expira: {self.expires_at.isoformat()}")

    async def force_refresh(self):
        """Força refresh (usado após 403)."""
        async with self._lock:
            await self._refresh()
            return self.access_token


# ─── Account Pool ─────────────────────────────────────────────────────────────

class AccountPool:
    """Pool de contas Kiro com Round Robin e failover."""

    def __init__(self):
        self.accounts: List[KiroAccount] = []
        self._cycle_index: int = 0
        self.request_count: int = 0
        self._load_accounts()

    def _load_accounts(self):
        """
        Carrega contas das variáveis de ambiente.

        Formato 1 (JSON): KIRO_ACCOUNTS=[{"refresh_token":"...", "profile_arn":"..."},...]
        Formato 2 (simples): KIRO_REFRESH_TOKENS=token1,token2,token3
        Formato 3 (individual): KIRO_ACCOUNT_1_REFRESH_TOKEN=... KIRO_ACCOUNT_1_PROFILE_ARN=...
        """
        # Formato 1: JSON completo
        accounts_json = os.getenv("KIRO_ACCOUNTS", "")
        if accounts_json:
            try:
                accounts_data = json.loads(accounts_json)
                for i, acc in enumerate(accounts_data):
                    self.accounts.append(KiroAccount(
                        account_id=f"account_{i+1}",
                        refresh_token=acc["refresh_token"],
                        profile_arn=acc.get("profile_arn", ""),
                    ))
            except (json.JSONDecodeError, KeyError) as e:
                logger.error(f"Erro ao parsear KIRO_ACCOUNTS: {e}")

        # Formato 2: Tokens separados por vírgula
        if not self.accounts:
            tokens = os.getenv("KIRO_REFRESH_TOKENS", "")
            if tokens:
                for i, token in enumerate(t.strip() for t in tokens.split(",") if t.strip()):
                    self.accounts.append(KiroAccount(
                        account_id=f"account_{i+1}",
                        refresh_token=token,
                    ))

        # Formato 3: Variáveis individuais
        if not self.accounts:
            i = 1
            while True:
                token = os.getenv(f"KIRO_ACCOUNT_{i}_REFRESH_TOKEN", "")
                if not token:
                    break
                profile = os.getenv(f"KIRO_ACCOUNT_{i}_PROFILE_ARN", "")
                self.accounts.append(KiroAccount(
                    account_id=f"account_{i}",
                    refresh_token=token,
                    profile_arn=profile,
                ))
                i += 1

        if not self.accounts:
            logger.error("NENHUMA CONTA KIRO CONFIGURADA!")
            logger.error("Configure: KIRO_ACCOUNTS (JSON), KIRO_REFRESH_TOKENS, ou KIRO_ACCOUNT_N_REFRESH_TOKEN")
        else:
            logger.info(f"Pool inicializado com {len(self.accounts)} conta(s)")

    def _check_cooldown(self):
        """Reativa contas após cooldown."""
        now = time.time()
        for acc in self.accounts:
            if acc.disabled and acc.disabled_at:
                if now - acc.disabled_at > ACCOUNT_COOLDOWN:
                    acc.disabled = False
                    acc.disabled_at = None
                    logger.info(f"[{acc.id}] Reativada após cooldown")

    def get_active_accounts(self) -> List[KiroAccount]:
        self._check_cooldown()
        return [acc for acc in self.accounts if not acc.disabled]

    def get_next_account(self) -> Optional[KiroAccount]:
        """Round Robin entre contas ativas."""
        active = self.get_active_accounts()
        if not active:
            return None
        self._cycle_index = self._cycle_index % len(active)
        account = active[self._cycle_index]
        self._cycle_index = (self._cycle_index + 1) % len(active)
        self.request_count += 1
        account.requests_served += 1
        account.last_used = time.time()
        return account

    def disable_account(self, account: KiroAccount, reason: str = ""):
        account.errors += 1
        account.disabled = True
        account.disabled_at = time.time()
        logger.warning(f"[{account.id}] Desabilitada: {reason}")

    def get_stats(self) -> dict:
        return {
            "total_accounts": len(self.accounts),
            "active_accounts": len(self.get_active_accounts()),
            "total_requests": self.request_count,
            "accounts": [
                {
                    "id": acc.id,
                    "active": not acc.disabled,
                    "requests": acc.requests_served,
                    "errors": acc.errors,
                    "token_valid": acc.is_token_valid(),
                }
                for acc in self.accounts
            ],
        }


# ─── Instância global ─────────────────────────────────────────────────────────
pool = AccountPool()


# ─── Kiro API Client ──────────────────────────────────────────────────────────

async def call_kiro_api(
    account: KiroAccount,
    messages: List[Dict],
    model: str = "auto",
    stream: bool = True,
    max_tokens: int = 8192,
    temperature: float = 0.7,
):
    """
    Chama a API do Kiro (generateAssistantResponse) com streaming.
    Converte formato OpenAI para formato Kiro interno.
    """
    access_token = await account.get_access_token()

    # Converte mensagens OpenAI → Kiro
    kiro_messages = []
    for msg in messages:
        role = msg.get("role", "user")
        content = msg.get("content", "")

        if role == "system":
            # System message vai como context do Kiro
            kiro_messages.append({
                "role": "user",
                "content": [{"text": f"[System]: {content}"}]
            })
        elif role == "user":
            kiro_messages.append({
                "role": "user",
                "content": [{"text": content if isinstance(content, str) else json.dumps(content)}]
            })
        elif role == "assistant":
            kiro_messages.append({
                "role": "assistant",
                "content": [{"text": content if isinstance(content, str) else json.dumps(content)}]
            })

    # Payload para a API Kiro
    payload = {
        "conversationState": {
            "chatTriggerType": "MANUAL",
            "currentMessage": {
                "userInputMessage": {
                    "content": kiro_messages[-1]["content"] if kiro_messages else [{"text": ""}],
                    "userIntent": "CHAT",
                },
            },
            "history": kiro_messages[:-1] if len(kiro_messages) > 1 else [],
        },
    }

    # Se modelo específico foi pedido
    if model and model != "auto":
        payload["modelId"] = model

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "User-Agent": f"KiroIDE-0.7.45-{FINGERPRINT}",
        "Accept": "application/json, text/event-stream",
    }

    if account.profile_arn:
        headers["x-amz-codewhisperer-profile-arn"] = account.profile_arn

    url = f"{KIRO_API_HOST}/generateAssistantResponse"

    return url, headers, payload


# ─── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
async def health_check():
    """Health check (sem auth)."""
    return {"status": "healthy", "pool": pool.get_stats()}


@app.get("/stats", dependencies=[Depends(verify_api_key)])
async def get_stats():
    return pool.get_stats()


@app.get("/v1/models", dependencies=[Depends(verify_api_key)])
async def list_models():
    """Lista modelos disponíveis (formato OpenAI)."""
    models = [
        {"id": "auto", "object": "model", "owned_by": "kiro"},
        {"id": "claude-sonnet-4", "object": "model", "owned_by": "kiro"},
        {"id": "claude-sonnet-4.5", "object": "model", "owned_by": "kiro"},
        {"id": "claude-haiku-4.5", "object": "model", "owned_by": "kiro"},
        {"id": "claude-opus-4.5", "object": "model", "owned_by": "kiro"},
        {"id": "claude-3.7-sonnet", "object": "model", "owned_by": "kiro"},
    ]
    return {"object": "list", "data": models}


@app.post("/v1/chat/completions", dependencies=[Depends(verify_api_key)])
async def chat_completions(request: Request):
    """
    Endpoint principal - compatível com OpenAI /v1/chat/completions.
    Faz Round Robin entre contas com failover e streaming.
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON inválido no body")

    messages = body.get("messages", [])
    model = body.get("model", "auto")
    stream = body.get("stream", False)
    max_tokens = body.get("max_tokens", 8192)
    temperature = body.get("temperature", 0.7)

    if not messages:
        raise HTTPException(status_code=400, detail="'messages' é obrigatório")

    last_error = None
    tried: set = set()
    max_attempts = min(MAX_RETRIES, len(pool.accounts)) if pool.accounts else 1

    for attempt in range(max_attempts):
        account = pool.get_next_account()
        if not account:
            raise HTTPException(status_code=503, detail="Nenhuma conta disponível")

        if account.id in tried:
            continue
        tried.add(account.id)

        logger.info(f"[Attempt {attempt+1}] {account.id} | model={model} | stream={stream}")

        try:
            url, headers, payload = await call_kiro_api(
                account, messages, model, stream, max_tokens, temperature
            )

            if stream:
                return await _stream_response(url, headers, payload, account)
            else:
                return await _normal_response(url, headers, payload, account)

        except AccountExhaustedException as e:
            last_error = str(e)
            pool.disable_account(account, reason=str(e))
            continue
        except TokenRefreshFailedException as e:
            last_error = str(e)
            pool.disable_account(account, reason=str(e))
            continue
        except httpx.TimeoutException:
            last_error = "Timeout"
            logger.warning(f"[{account.id}] Timeout")
            continue
        except Exception as e:
            last_error = str(e)
            logger.error(f"[{account.id}] Erro: {e}")
            continue

    raise HTTPException(status_code=502, detail=f"Todas tentativas falharam: {last_error}")


# ─── Exceções ──────────────────────────────────────────────────────────────────

class AccountExhaustedException(Exception):
    pass

class TokenRefreshFailedException(Exception):
    pass


# ─── Streaming Response ────────────────────────────────────────────────────────

async def _stream_response(url: str, headers: dict, payload: dict, account: KiroAccount):
    """Faz request para Kiro e retorna streaming no formato OpenAI SSE."""

    async def generate():
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            async with client.stream("POST", url, json=payload, headers=headers) as response:
                if response.status_code in (401, 403):
                    # Tenta refresh e retry
                    try:
                        await account.force_refresh()
                        headers["Authorization"] = f"Bearer {account.access_token}"
                    except Exception as e:
                        raise TokenRefreshFailedException(str(e))
                    raise AccountExhaustedException(f"HTTP {response.status_code} após refresh")

                if response.status_code == 429:
                    raise AccountExhaustedException("Rate limited (429)")

                if response.status_code >= 400:
                    error = await response.aread()
                    raise AccountExhaustedException(
                        f"HTTP {response.status_code}: {error.decode('utf-8', errors='replace')[:200]}"
                    )

                # Processa o stream do Kiro e converte para formato OpenAI SSE
                buffer = ""
                async for chunk in response.aiter_text():
                    buffer += chunk
                    # Kiro envia eventos separados por newlines
                    while "\n" in buffer:
                        line, buffer = buffer.split("\n", 1)
                        line = line.strip()
                        if not line:
                            continue

                        # Tenta parsear como JSON (evento Kiro)
                        content_delta = _extract_content_from_kiro_event(line)
                        if content_delta:
                            # Formato OpenAI SSE
                            sse_data = {
                                "id": f"chatcmpl-{account.id}-{int(time.time())}",
                                "object": "chat.completion.chunk",
                                "created": int(time.time()),
                                "model": "kiro",
                                "choices": [{
                                    "index": 0,
                                    "delta": {"content": content_delta},
                                    "finish_reason": None,
                                }],
                            }
                            yield f"data: {json.dumps(sse_data)}\n\n"

                # Final chunk
                final = {
                    "id": f"chatcmpl-{account.id}-{int(time.time())}",
                    "object": "chat.completion.chunk",
                    "created": int(time.time()),
                    "model": "kiro",
                    "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
                }
                yield f"data: {json.dumps(final)}\n\n"
                yield "data: [DONE]\n\n"

    return StreamingResponse(
        content=generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Account-Id": account.id,
        },
    )


async def _normal_response(url: str, headers: dict, payload: dict, account: KiroAccount):
    """Request normal (sem streaming), retorna formato OpenAI."""
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        response = await client.post(url, json=payload, headers=headers)

        if response.status_code in (401, 403):
            try:
                await account.force_refresh()
            except Exception as e:
                raise TokenRefreshFailedException(str(e))
            raise AccountExhaustedException(f"HTTP {response.status_code}")

        if response.status_code == 429:
            raise AccountExhaustedException("Rate limited (429)")

        if response.status_code >= 400:
            raise AccountExhaustedException(
                f"HTTP {response.status_code}: {response.text[:200]}"
            )

        # Extrai conteúdo da resposta Kiro
        content = _extract_full_content_from_kiro_response(response.text)

        return JSONResponse({
            "id": f"chatcmpl-{account.id}-{int(time.time())}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": "kiro",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": content},
                "finish_reason": "stop",
            }],
            "usage": {
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
            },
        })


# ─── Parsers de resposta Kiro ──────────────────────────────────────────────────

def _extract_content_from_kiro_event(line: str) -> Optional[str]:
    """Extrai texto de um evento de streaming do Kiro."""
    try:
        data = json.loads(line)

        # Formato 1: assistantResponseEvent com content
        if "assistantResponseEvent" in data:
            event = data["assistantResponseEvent"]
            if "content" in event:
                return event["content"]

        # Formato 2: contentBlockDelta (similar ao Anthropic)
        if "contentBlockDelta" in data:
            delta = data["contentBlockDelta"].get("delta", {})
            if "text" in delta:
                return delta["text"]

        # Formato 3: messageStream com text
        if "messageStream" in data:
            stream = data["messageStream"]
            if isinstance(stream, dict):
                if "contentBlockDelta" in stream:
                    delta = stream["contentBlockDelta"].get("delta", {})
                    return delta.get("text")
                if "assistantResponseEvent" in stream:
                    return stream["assistantResponseEvent"].get("content")

        # Formato 4: texto direto
        if "text" in data:
            return data["text"]

    except (json.JSONDecodeError, KeyError, TypeError):
        # Pode ser texto puro
        if line and not line.startswith("{") and not line.startswith(":"):
            return line

    return None


def _extract_full_content_from_kiro_response(response_text: str) -> str:
    """Extrai todo o conteúdo de uma resposta Kiro não-streaming."""
    parts = []
    for line in response_text.split("\n"):
        line = line.strip()
        if not line:
            continue
        content = _extract_content_from_kiro_event(line)
        if content:
            parts.append(content)

    return "".join(parts) if parts else response_text


# ─── Proxy genérico (fallback) ─────────────────────────────────────────────────

@app.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH"],
    dependencies=[Depends(verify_api_key)],
)
async def proxy_fallback(request: Request, path: str):
    """Proxy genérico para outros endpoints da API Kiro."""
    body = await request.body()
    last_error = None
    tried: set = set()
    max_attempts = min(MAX_RETRIES, len(pool.accounts)) if pool.accounts else 1

    for attempt in range(max_attempts):
        account = pool.get_next_account()
        if not account:
            raise HTTPException(status_code=503, detail="Nenhuma conta disponível")
        if account.id in tried:
            continue
        tried.add(account.id)

        try:
            access_token = await account.get_access_token()

            headers = {}
            skip = {"host", "x-api-key", "content-length", "transfer-encoding"}
            for key, value in request.headers.items():
                if key.lower() not in skip:
                    headers[key] = value
            headers["Authorization"] = f"Bearer {access_token}"
            if account.profile_arn:
                headers["x-amz-codewhisperer-profile-arn"] = account.profile_arn

            url = f"{KIRO_API_HOST}/{path}"

            async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
                response = await client.request(
                    method=request.method,
                    url=url,
                    content=body if body else None,
                    headers=headers,
                )

            if response.status_code in (401, 403, 429):
                pool.disable_account(account, f"HTTP {response.status_code}")
                last_error = f"HTTP {response.status_code}"
                continue

            return StreamingResponse(
                content=iter([response.content]),
                status_code=response.status_code,
                media_type=response.headers.get("content-type", "application/json"),
            )

        except Exception as e:
            last_error = str(e)
            continue

    raise HTTPException(status_code=502, detail=f"Falhou: {last_error}")


# ─── Startup ──────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup_event():
    logger.info("=" * 60)
    logger.info("  Kiro Multi-Account Gateway v2.0")
    logger.info(f"   Contas: {len(pool.accounts)}")
    logger.info(f"   Region: {REGION}")
    logger.info(f"   API Host: {KIRO_API_HOST}")
    logger.info(f"   Auth: {'ON' if GATEWAY_API_KEY else 'OFF'}")
    logger.info(f"   Timeout: {REQUEST_TIMEOUT}s")
    logger.info("=" * 60)

    # Tenta fazer refresh de todos os tokens no startup
    for acc in pool.accounts:
        try:
            await acc.get_access_token()
            logger.info(f"   [{acc.id}] Token OK")
        except Exception as e:
            logger.warning(f"   [{acc.id}] Refresh falhou (tentará novamente na 1ª request): {e}")
