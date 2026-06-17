"""
Kiro API Gateway - Centralized Load Balancer with Account Pool
Gerencia múltiplas contas Kiro com Round Robin, failover automático e streaming.
"""

import os
import time
import logging
from typing import Optional

import httpx
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import StreamingResponse
from fastapi.security import APIKeyHeader

# ─── Configuração de Logging ───────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("kiro-gateway")

# ─── App FastAPI ───────────────────────────────────────────────────────────────
app = FastAPI(
    title="Kiro API Gateway",
    description="API Gateway centralizado com Load Balancing para múltiplas contas Kiro",
    version="1.0.0",
)

# ─── Configuração ─────────────────────────────────────────────────────────────
KIRO_BASE_URL = os.getenv("KIRO_BASE_URL", "https://api.kiro.dev")
GATEWAY_API_KEY = os.getenv("GATEWAY_API_KEY", "")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "120"))

# ─── Security: Autenticação via X-API-Key ──────────────────────────────────────
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(api_key: Optional[str] = Depends(api_key_header)):
    """Verifica se a requisição possui um X-API-Key válido."""
    if not GATEWAY_API_KEY:
        logger.warning("GATEWAY_API_KEY não configurada! Gateway operando sem autenticação.")
        return True
    if api_key != GATEWAY_API_KEY:
        raise HTTPException(status_code=401, detail="X-API-Key inválida ou ausente.")
    return True


# ─── Account Pool: State Persistence ──────────────────────────────────────────

class AccountPool:
    """
    Gerencia o pool de contas Kiro com Round Robin e failover.
    Estado mantido em variável global para persistir entre requisições.
    """

    def __init__(self):
        self.accounts: list[dict] = []
        self.disabled_accounts: dict[str, float] = {}
        self._cycle_index: int = 0
        self.request_count: int = 0
        self._load_accounts()

    def _load_accounts(self):
        """
        Carrega tokens das variáveis de ambiente.
        Suporta: KIRO_TOKENS (vírgula) ou KIRO_TOKEN_1, KIRO_TOKEN_2, ...
        """
        # Método 1: KIRO_TOKENS (separados por vírgula)
        tokens_env = os.getenv("KIRO_TOKENS", "")
        if tokens_env:
            tokens = [t.strip() for t in tokens_env.split(",") if t.strip()]
            for i, token in enumerate(tokens):
                self.accounts.append({
                    "id": f"account_{i+1}",
                    "token": token,
                    "requests_served": 0,
                    "errors": 0,
                    "last_used": None,
                })

        # Método 2: KIRO_TOKEN_1, KIRO_TOKEN_2, etc.
        if not self.accounts:
            i = 1
            while True:
                token = os.getenv(f"KIRO_TOKEN_{i}", "")
                if not token:
                    break
                self.accounts.append({
                    "id": f"account_{i}",
                    "token": token,
                    "requests_served": 0,
                    "errors": 0,
                    "last_used": None,
                })
                i += 1

        if not self.accounts:
            logger.error("NENHUM TOKEN KIRO ENCONTRADO! Configure KIRO_TOKENS ou KIRO_TOKEN_N.")
        else:
            logger.info(f"Pool inicializado com {len(self.accounts)} conta(s) Kiro.")

    def get_active_accounts(self) -> list[dict]:
        """Retorna contas ativas. Reabilita contas após cooldown."""
        now = time.time()
        cooldown = int(os.getenv("ACCOUNT_COOLDOWN_SECONDS", "300"))
        reactivated = []
        for token_id, disabled_at in list(self.disabled_accounts.items()):
            if now - disabled_at > cooldown:
                reactivated.append(token_id)
                del self.disabled_accounts[token_id]
        if reactivated:
            logger.info(f"Contas reativadas após cooldown: {reactivated}")
        return [acc for acc in self.accounts if acc["id"] not in self.disabled_accounts]

    def get_next_account(self) -> Optional[dict]:
        """Retorna a próxima conta no rodízio Round Robin."""
        active = self.get_active_accounts()
        if not active:
            return None
        self._cycle_index = self._cycle_index % len(active)
        account = active[self._cycle_index]
        self._cycle_index = (self._cycle_index + 1) % len(active)
        self.request_count += 1
        account["requests_served"] += 1
        account["last_used"] = time.time()
        return account

    def disable_account(self, account: dict, reason: str = ""):
        """Desabilita temporariamente uma conta."""
        account["errors"] += 1
        self.disabled_accounts[account["id"]] = time.time()
        logger.warning(f"Conta {account['id']} desabilitada. Motivo: {reason}")

    def get_stats(self) -> dict:
        """Retorna estatísticas do pool."""
        return {
            "total_accounts": len(self.accounts),
            "active_accounts": len(self.get_active_accounts()),
            "disabled_accounts": list(self.disabled_accounts.keys()),
            "total_requests_served": self.request_count,
            "accounts": [
                {
                    "id": acc["id"],
                    "requests_served": acc["requests_served"],
                    "errors": acc["errors"],
                    "active": acc["id"] not in self.disabled_accounts,
                }
                for acc in self.accounts
            ],
        }


# ─── Instância global do pool (State Persistence) ─────────────────────────────
pool = AccountPool()


# ─── Exceção customizada ──────────────────────────────────────────────────────

class AccountExhaustedException(Exception):
    """Conta sem crédito, rate-limited ou bloqueada."""
    pass


# ─── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
async def health_check():
    """Health check para o Render.com (sem autenticação)."""
    return {"status": "healthy", "pool": pool.get_stats()}


@app.get("/stats", dependencies=[Depends(verify_api_key)])
async def get_stats():
    """Retorna estatísticas detalhadas do pool."""
    return pool.get_stats()


# ─── Proxy com Streaming e Failover ───────────────────────────────────────────

@app.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH"],
    dependencies=[Depends(verify_api_key)],
)
async def proxy_request(request: Request, path: str):
    """
    Proxy reverso com Load Balancing e Streaming.
    Faz Round Robin entre contas e failover automático em caso de erro.
    """
    body = await request.body()
    last_error = None
    tried_accounts: set = set()
    max_attempts = min(MAX_RETRIES, len(pool.accounts)) if pool.accounts else 1

    for attempt in range(max_attempts):
        account = pool.get_next_account()
        if account is None:
            raise HTTPException(
                status_code=503,
                detail="Nenhuma conta Kiro disponível. Todas desabilitadas.",
            )

        if account["id"] in tried_accounts:
            continue
        tried_accounts.add(account["id"])

        logger.info(
            f"[Attempt {attempt+1}/{max_attempts}] {account['id']} | "
            f"/{path} | {request.method}"
        )

        try:
            return await _forward_with_streaming(request, path, body, account)
        except AccountExhaustedException as e:
            last_error = str(e)
            pool.disable_account(account, reason=str(e))
            logger.warning(f"{account['id']} exaurida: {e}. Próxima...")
            continue
        except httpx.TimeoutException:
            last_error = "Timeout upstream"
            logger.warning(f"Timeout em {account['id']}. Próxima...")
            continue
        except Exception as e:
            last_error = str(e)
            logger.error(f"Erro com {account['id']}: {e}")
            continue

    raise HTTPException(
        status_code=502,
        detail=f"Todas as tentativas falharam. Último erro: {last_error}",
    )


async def _forward_with_streaming(
    request: Request, path: str, body: bytes, account: dict
):
    """
    Encaminha requisição com streaming real (SSE-compatible).
    Detecta erros de conta para failover automático.
    """
    # Monta headers (remove hop-by-hop e injeta token)
    headers = {}
    skip_headers = {"host", "x-api-key", "content-length", "transfer-encoding"}
    for key, value in request.headers.items():
        if key.lower() not in skip_headers:
            headers[key] = value
    headers["Authorization"] = f"Bearer {account['token']}"

    url = f"{KIRO_BASE_URL}/{path}"
    query_params = dict(request.query_params)

    client = httpx.AsyncClient(timeout=httpx.Timeout(REQUEST_TIMEOUT, connect=30.0))

    try:
        req = client.build_request(
            method=request.method,
            url=url,
            content=body if body else None,
            headers=headers,
            params=query_params if query_params else None,
        )
        response = await client.send(req, stream=True)

        # Verifica erros de conta ANTES do streaming
        if response.status_code in (401, 403, 429):
            error_body = await response.aread()
            await response.aclose()
            await client.aclose()
            raise AccountExhaustedException(
                f"HTTP {response.status_code}: "
                f"{error_body.decode('utf-8', errors='replace')[:200]}"
            )

        if response.status_code >= 500:
            error_body = await response.aread()
            await response.aclose()
            await client.aclose()
            raise AccountExhaustedException(
                f"Upstream HTTP {response.status_code}: "
                f"{error_body.decode('utf-8', errors='replace')[:200]}"
            )

        # Generator de streaming
        async def stream_generator():
            try:
                async for chunk in response.aiter_bytes():
                    yield chunk
            finally:
                await response.aclose()
                await client.aclose()

        # Headers de resposta (filtra hop-by-hop)
        response_headers = {}
        hop_by_hop = {"connection", "keep-alive", "transfer-encoding", "upgrade"}
        for key, value in response.headers.items():
            if key.lower() not in hop_by_hop:
                response_headers[key] = value
        response_headers["X-Gateway-Account"] = account["id"]

        return StreamingResponse(
            content=stream_generator(),
            status_code=response.status_code,
            headers=response_headers,
            media_type=response.headers.get("content-type", "application/json"),
        )

    except (AccountExhaustedException, httpx.TimeoutException):
        raise
    except Exception as e:
        await client.aclose()
        raise Exception(f"Erro upstream: {e}")


# ─── Startup ──────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup_event():
    logger.info("=" * 60)
    logger.info("  Kiro API Gateway iniciado!")
    logger.info(f"   Contas no pool: {len(pool.accounts)}")
    logger.info(f"   Base URL: {KIRO_BASE_URL}")
    logger.info(f"   Max Retries: {MAX_RETRIES}")
    logger.info(f"   Timeout: {REQUEST_TIMEOUT}s")
    logger.info(f"   Auth: {'ATIVADA' if GATEWAY_API_KEY else 'DESATIVADA'}")
    logger.info("=" * 60)
