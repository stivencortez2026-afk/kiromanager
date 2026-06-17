"""
Kiro API Gateway v3 - Multi-Account Pool com Admin Panel
=========================================================
- Painel web para adicionar/remover contas em tempo real
- Persistência em arquivo JSON (sobrevive restart)
- Round Robin, auto-refresh, failover, streaming OpenAI
"""

import os
import json
import time
import asyncio
import hashlib
import uuid
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Any

import httpx
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import StreamingResponse, JSONResponse, HTMLResponse
from fastapi.security import APIKeyHeader

# ─── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("kiro-gateway")

# ─── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="Kiro Gateway", version="3.0.0")

# ─── Config ────────────────────────────────────────────────────────────────────
GATEWAY_API_KEY = os.getenv("GATEWAY_API_KEY", "admin123")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", GATEWAY_API_KEY)
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "300"))
ACCOUNT_COOLDOWN = int(os.getenv("ACCOUNT_COOLDOWN_SECONDS", "300"))
REGION = os.getenv("KIRO_REGION", "us-east-1")
DATA_DIR = os.getenv("DATA_DIR", "./data")

# URLs Kiro
KIRO_REFRESH_URL = f"https://prod.{REGION}.auth.desktop.kiro.dev/refreshToken"
KIRO_API_HOST = f"https://codewhisperer.{REGION}.amazonaws.com"

# ─── Security ──────────────────────────────────────────────────────────────────
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(api_key: Optional[str] = Depends(api_key_header)):
    if not GATEWAY_API_KEY:
        return True
    # Aceita via header X-API-Key ou Authorization Bearer
    if api_key == GATEWAY_API_KEY:
        return True
    raise HTTPException(status_code=401, detail="X-API-Key inválida")


def get_fingerprint() -> str:
    mid = os.getenv("MACHINE_ID", str(uuid.uuid4()))
    return hashlib.sha256(mid.encode()).hexdigest()[:32]


FINGERPRINT = get_fingerprint()


# ─── Persistência JSON ─────────────────────────────────────────────────────────

ACCOUNTS_FILE = os.path.join(DATA_DIR, "accounts.json")


def _ensure_data_dir():
    Path(DATA_DIR).mkdir(parents=True, exist_ok=True)


def load_accounts_from_disk() -> List[dict]:
    """Carrega contas do arquivo JSON."""
    _ensure_data_dir()
    if not os.path.exists(ACCOUNTS_FILE):
        return []
    try:
        with open(ACCOUNTS_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return []


def save_accounts_to_disk(accounts_data: List[dict]):
    """Salva contas no arquivo JSON."""
    _ensure_data_dir()
    with open(ACCOUNTS_FILE, "w") as f:
        json.dump(accounts_data, f, indent=2)


# ─── Kiro Account Class ───────────────────────────────────────────────────────

class KiroAccount:
    """Conta Kiro individual com auto-refresh."""

    def __init__(self, account_id: str, refresh_token: str, label: str = ""):
        self.id = account_id
        self.label = label or account_id
        self.refresh_token = refresh_token
        self.access_token: Optional[str] = None
        self.expires_at: Optional[datetime] = None
        self.profile_arn: str = ""
        self.requests_served: int = 0
        self.errors: int = 0
        self.last_used: Optional[float] = None
        self.disabled: bool = False
        self.disabled_at: Optional[float] = None
        self.last_error: str = ""
        self._lock = asyncio.Lock()

    def is_token_valid(self) -> bool:
        if not self.access_token or not self.expires_at:
            return False
        now = datetime.now(timezone.utc)
        return now < (self.expires_at - timedelta(minutes=10))

    async def get_access_token(self) -> str:
        async with self._lock:
            if self.is_token_valid():
                return self.access_token
            await self._refresh()
            return self.access_token

    async def _refresh(self):
        logger.info(f"[{self.id}] Refreshing token...")
        headers = {
            "Content-Type": "application/json",
            "User-Agent": f"KiroIDE-0.7.45-{FINGERPRINT}",
        }
        payload = {"refreshToken": self.refresh_token}
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(KIRO_REFRESH_URL, json=payload, headers=headers)
            if resp.status_code != 200:
                self.last_error = f"Refresh HTTP {resp.status_code}: {resp.text[:100]}"
                raise Exception(self.last_error)
            data = resp.json()

        new_access = data.get("accessToken")
        if not new_access:
            self.last_error = "Refresh não retornou accessToken"
            raise Exception(self.last_error)
        self.access_token = new_access
        if data.get("refreshToken"):
            self.refresh_token = data["refreshToken"]
        if data.get("profileArn"):
            self.profile_arn = data["profileArn"]
        expires_in = data.get("expiresIn", 3600)
        self.expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in - 60)
        self.last_error = ""
        logger.info(f"[{self.id}] Token OK, expira: {self.expires_at.isoformat()}")

    async def force_refresh(self):
        async with self._lock:
            await self._refresh()
            return self.access_token

    def to_dict(self) -> dict:
        """Serializa para persistência."""
        return {
            "id": self.id,
            "label": self.label,
            "refresh_token": self.refresh_token,
        }

    def status_dict(self) -> dict:
        """Status para o admin panel."""
        return {
            "id": self.id,
            "label": self.label,
            "active": not self.disabled,
            "token_valid": self.is_token_valid(),
            "requests": self.requests_served,
            "errors": self.errors,
            "last_error": self.last_error,
            "last_used": self.last_used,
        }


# ─── Account Pool ─────────────────────────────────────────────────────────────

class AccountPool:
    """Pool com persistência em disco."""

    def __init__(self):
        self.accounts: List[KiroAccount] = []
        self._cycle_index: int = 0
        self.request_count: int = 0
        self._load()

    def _load(self):
        """Carrega do disco + env vars."""
        # Do disco (admin panel)
        saved = load_accounts_from_disk()
        for acc_data in saved:
            self.accounts.append(KiroAccount(
                account_id=acc_data["id"],
                refresh_token=acc_data["refresh_token"],
                label=acc_data.get("label", ""),
            ))
        # De env vars (fallback inicial)
        if not self.accounts:
            tokens = os.getenv("KIRO_REFRESH_TOKENS", "")
            if tokens:
                for i, t in enumerate(t.strip() for t in tokens.split(",") if t.strip()):
                    self.accounts.append(KiroAccount(f"env_{i+1}", t, f"Env Account {i+1}"))
                self._save()
        if self.accounts:
            logger.info(f"Pool: {len(self.accounts)} conta(s) carregadas")
        else:
            logger.warning("Pool vazio! Adicione contas pelo painel admin.")

    def _save(self):
        """Persiste no disco."""
        data = [acc.to_dict() for acc in self.accounts]
        save_accounts_to_disk(data)

    def add_account(self, refresh_token: str, label: str = "") -> KiroAccount:
        """Adiciona conta via admin panel."""
        acc_id = f"account_{len(self.accounts) + 1}_{int(time.time()) % 10000}"
        acc = KiroAccount(acc_id, refresh_token, label or acc_id)
        self.accounts.append(acc)
        self._save()
        logger.info(f"Conta adicionada: {acc.id} ({acc.label})")
        return acc

    def remove_account(self, account_id: str) -> bool:
        """Remove conta via admin panel."""
        before = len(self.accounts)
        self.accounts = [a for a in self.accounts if a.id != account_id]
        if len(self.accounts) < before:
            self._save()
            logger.info(f"Conta removida: {account_id}")
            return True
        return False

    def get_next_account(self) -> Optional[KiroAccount]:
        self._check_cooldown()
        active = [a for a in self.accounts if not a.disabled]
        if not active:
            return None
        self._cycle_index = self._cycle_index % len(active)
        acc = active[self._cycle_index]
        self._cycle_index = (self._cycle_index + 1) % len(active)
        self.request_count += 1
        acc.requests_served += 1
        acc.last_used = time.time()
        return acc

    def disable_account(self, acc: KiroAccount, reason: str = ""):
        acc.errors += 1
        acc.disabled = True
        acc.disabled_at = time.time()
        acc.last_error = reason
        logger.warning(f"[{acc.id}] Disabled: {reason}")

    def _check_cooldown(self):
        now = time.time()
        for acc in self.accounts:
            if acc.disabled and acc.disabled_at and (now - acc.disabled_at > ACCOUNT_COOLDOWN):
                acc.disabled = False
                acc.disabled_at = None
                logger.info(f"[{acc.id}] Reativada")

    def get_stats(self) -> dict:
        return {
            "total": len(self.accounts),
            "active": len([a for a in self.accounts if not a.disabled]),
            "requests": self.request_count,
            "accounts": [a.status_dict() for a in self.accounts],
        }


# ─── Instância global ─────────────────────────────────────────────────────────
pool = AccountPool()


# ─── Exceções ──────────────────────────────────────────────────────────────────

class AccountExhaustedException(Exception):
    pass

class TokenRefreshFailedException(Exception):
    pass


# ─── Admin Panel (HTML) ────────────────────────────────────────────────────────

ADMIN_HTML = """<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Kiro Gateway - Admin</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, sans-serif; background: #0f0f0f; color: #e0e0e0; padding: 20px; }
.container { max-width: 900px; margin: 0 auto; }
h1 { color: #4fc3f7; margin-bottom: 10px; }
.subtitle { color: #888; margin-bottom: 30px; }
.card { background: #1a1a1a; border-radius: 12px; padding: 20px; margin-bottom: 20px; border: 1px solid #333; }
.card h2 { color: #4fc3f7; margin-bottom: 15px; font-size: 1.1em; }
.stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 15px; margin-bottom: 20px; }
.stat { background: #222; border-radius: 8px; padding: 15px; text-align: center; }
.stat .number { font-size: 2em; font-weight: bold; color: #4fc3f7; }
.stat .label { color: #888; font-size: 0.85em; margin-top: 5px; }
table { width: 100%; border-collapse: collapse; }
th, td { padding: 10px 12px; text-align: left; border-bottom: 1px solid #333; }
th { color: #888; font-size: 0.85em; text-transform: uppercase; }
.badge { padding: 3px 8px; border-radius: 4px; font-size: 0.8em; }
.badge-green { background: #1b5e20; color: #a5d6a7; }
.badge-red { background: #b71c1c; color: #ef9a9a; }
input, button { padding: 10px 15px; border-radius: 8px; border: 1px solid #444; background: #222; color: #e0e0e0; font-size: 0.95em; }
input { width: 100%; margin-bottom: 10px; }
input:focus { outline: none; border-color: #4fc3f7; }
button { cursor: pointer; background: #4fc3f7; color: #000; font-weight: bold; border: none; }
button:hover { background: #81d4fa; }
button.danger { background: #ef5350; color: #fff; }
button.danger:hover { background: #f44336; }
.form-row { display: flex; gap: 10px; align-items: end; }
.form-row input { flex: 1; }
#message { padding: 10px; border-radius: 8px; margin-bottom: 15px; display: none; }
.msg-ok { background: #1b5e20; color: #a5d6a7; display: block !important; }
.msg-err { background: #b71c1c; color: #ef9a9a; display: block !important; }
</style>
</head>
<body>
<div class="container">
<h1>Kiro Gateway Admin</h1>
<p class="subtitle">Gerencie suas contas Kiro em tempo real</p>
<div id="message"></div>
"""

ADMIN_HTML2 = """
<div class="card">
<h2>Adicionar Conta</h2>
<div class="form-row">
<input type="text" id="label" placeholder="Nome da conta (ex: Gmail 1)">
<input type="text" id="token" placeholder="Refresh Token da conta Kiro">
<button onclick="addAccount()">Adicionar</button>
</div>
</div>
<div class="card">
<h2>Pool de Contas</h2>
<div class="stats" id="stats"></div>
<table>
<thead><tr><th>Nome</th><th>Status</th><th>Requests</th><th>Erros</th><th>Último Erro</th><th></th></tr></thead>
<tbody id="accounts"></tbody>
</table>
</div>
<div class="card">
<h2>Como usar</h2>
<p style="color:#aaa; line-height:1.8;">
<strong>Base URL:</strong> <code style="color:#4fc3f7;">ESTE_DOMINIO/v1</code><br>
<strong>API Key:</strong> Sua GATEWAY_API_KEY (header X-API-Key ou Authorization Bearer)<br>
<strong>Endpoint:</strong> POST /v1/chat/completions (formato OpenAI)<br>
<strong>Modelos:</strong> auto, claude-sonnet-4, claude-sonnet-4.5, claude-haiku-4.5
</p>
</div>
</div>
<script>
const API_KEY = prompt("Digite a senha admin (GATEWAY_API_KEY):", "");
const H = {"X-API-Key": API_KEY, "Content-Type": "application/json"};

async function loadData() {
  try {
    const r = await fetch("/admin/api/stats", {headers: H});
    if (r.status === 401) { showMsg("Senha incorreta!", true); return; }
    const d = await r.json();
    document.getElementById("stats").innerHTML = `
      <div class="stat"><div class="number">${d.total}</div><div class="label">Total</div></div>
      <div class="stat"><div class="number">${d.active}</div><div class="label">Ativas</div></div>
      <div class="stat"><div class="number">${d.requests}</div><div class="label">Requests</div></div>
    `;
    let rows = "";
    for (const a of d.accounts) {
      const badge = a.active ? '<span class="badge badge-green">Ativa</span>' : '<span class="badge badge-red">Inativa</span>';
      rows += `<tr>
        <td>${a.label}</td><td>${badge}</td><td>${a.requests}</td><td>${a.errors}</td>
        <td style="color:#ef5350;font-size:0.8em;">${a.last_error||"-"}</td>
        <td><button class="danger" onclick="removeAccount('${a.id}')">X</button></td>
      </tr>`;
    }
    document.getElementById("accounts").innerHTML = rows || "<tr><td colspan=6 style='color:#888'>Nenhuma conta. Adicione acima.</td></tr>";
  } catch(e) { showMsg("Erro: " + e.message, true); }
}

async function addAccount() {
  const label = document.getElementById("label").value.trim();
  const token = document.getElementById("token").value.trim();
  if (!token) { showMsg("Cole o refresh token!", true); return; }
  const r = await fetch("/admin/api/accounts", {method:"POST", headers:H, body: JSON.stringify({label, refresh_token: token})});
  if (r.ok) { showMsg("Conta adicionada!"); document.getElementById("token").value=""; document.getElementById("label").value=""; loadData(); }
  else { const e = await r.json(); showMsg(e.detail || "Erro", true); }
}

async function removeAccount(id) {
  if (!confirm("Remover esta conta?")) return;
  const r = await fetch("/admin/api/accounts/" + id, {method:"DELETE", headers:H});
  if (r.ok) { showMsg("Removida!"); loadData(); }
  else { showMsg("Erro ao remover", true); }
}

function showMsg(t, err) {
  const m = document.getElementById("message");
  m.textContent = t; m.className = err ? "msg-err" : "msg-ok";
  setTimeout(() => { m.style.display = "none"; m.className = ""; }, 4000);
}

loadData(); setInterval(loadData, 10000);
</script>
</body></html>"""


# ─── Admin API Endpoints ───────────────────────────────────────────────────────

@app.get("/admin", response_class=HTMLResponse)
async def admin_panel():
    """Painel admin web."""
    return HTMLResponse(ADMIN_HTML + ADMIN_HTML2)


@app.get("/admin/api/stats", dependencies=[Depends(verify_api_key)])
async def admin_stats():
    return pool.get_stats()


@app.post("/admin/api/accounts", dependencies=[Depends(verify_api_key)])
async def admin_add_account(request: Request):
    """Adiciona conta via admin."""
    body = await request.json()
    refresh_token = body.get("refresh_token", "").strip()
    label = body.get("label", "").strip()
    if not refresh_token:
        raise HTTPException(400, "refresh_token é obrigatório")
    # Testa o token antes de adicionar
    acc = pool.add_account(refresh_token, label)
    try:
        await acc.get_access_token()
        return {"status": "ok", "id": acc.id, "message": "Conta adicionada e token validado!"}
    except Exception as e:
        # Mantém a conta mas avisa que o token pode estar ruim
        return {"status": "warning", "id": acc.id, "message": f"Conta adicionada mas refresh falhou: {e}"}


@app.delete("/admin/api/accounts/{account_id}", dependencies=[Depends(verify_api_key)])
async def admin_remove_account(account_id: str):
    """Remove conta via admin."""
    if pool.remove_account(account_id):
        return {"status": "ok"}
    raise HTTPException(404, "Conta não encontrada")


# ─── Health & Models ───────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "healthy", "accounts": len(pool.accounts), "active": len([a for a in pool.accounts if not a.disabled])}


@app.get("/v1/models", dependencies=[Depends(verify_api_key)])
async def list_models():
    models = [
        {"id": "auto", "object": "model", "owned_by": "kiro"},
        {"id": "claude-sonnet-4", "object": "model", "owned_by": "kiro"},
        {"id": "claude-sonnet-4.5", "object": "model", "owned_by": "kiro"},
        {"id": "claude-haiku-4.5", "object": "model", "owned_by": "kiro"},
        {"id": "claude-opus-4.5", "object": "model", "owned_by": "kiro"},
        {"id": "claude-3.7-sonnet", "object": "model", "owned_by": "kiro"},
    ]
    return {"object": "list", "data": models}


# ─── Chat Completions (OpenAI format) ─────────────────────────────────────────

@app.post("/v1/chat/completions", dependencies=[Depends(verify_api_key)])
async def chat_completions(request: Request):
    """Endpoint principal - formato OpenAI com streaming."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, "JSON inválido")

    messages = body.get("messages", [])
    model = body.get("model", "auto")
    stream = body.get("stream", False)

    if not messages:
        raise HTTPException(400, "'messages' obrigatório")

    if not pool.accounts:
        raise HTTPException(503, "Nenhuma conta configurada. Acesse /admin para adicionar.")

    last_error = None
    tried: set = set()
    max_attempts = min(MAX_RETRIES, len(pool.accounts))

    for attempt in range(max_attempts):
        account = pool.get_next_account()
        if not account:
            raise HTTPException(503, "Nenhuma conta ativa disponível")
        if account.id in tried:
            continue
        tried.add(account.id)

        logger.info(f"[Attempt {attempt+1}] {account.id} | model={model}")

        try:
            access_token = await account.get_access_token()
            url, headers, payload = _build_kiro_request(messages, model, access_token, account)

            if stream:
                return await _do_stream(url, headers, payload, account)
            else:
                return await _do_normal(url, headers, payload, account)

        except AccountExhaustedException as e:
            last_error = str(e)
            pool.disable_account(account, str(e))
            continue
        except TokenRefreshFailedException as e:
            last_error = str(e)
            pool.disable_account(account, str(e))
            continue
        except httpx.TimeoutException:
            last_error = "Timeout"
            continue
        except Exception as e:
            last_error = str(e)
            logger.error(f"[{account.id}] {e}")
            continue

    raise HTTPException(502, f"Todas tentativas falharam: {last_error}")


# ─── Build Kiro Request ────────────────────────────────────────────────────────

def _build_kiro_request(messages: List[dict], model: str, access_token: str, account: KiroAccount):
    """Monta request no formato Kiro API."""
    # Converte mensagens OpenAI → Kiro
    kiro_msgs = []
    for msg in messages:
        role = msg.get("role", "user")
        content = msg.get("content", "")
        if isinstance(content, list):
            # Multimodal - extrai texto
            content = " ".join(p.get("text", "") for p in content if p.get("type") == "text")
        if role == "system":
            kiro_msgs.append({"role": "user", "content": [{"text": f"[System]: {content}"}]})
        elif role == "user":
            kiro_msgs.append({"role": "user", "content": [{"text": content}]})
        elif role == "assistant":
            kiro_msgs.append({"role": "assistant", "content": [{"text": content}]})

    payload = {
        "conversationState": {
            "chatTriggerType": "MANUAL",
            "currentMessage": {
                "userInputMessage": {
                    "content": kiro_msgs[-1]["content"] if kiro_msgs else [{"text": ""}],
                    "userIntent": "CHAT",
                },
            },
            "history": kiro_msgs[:-1] if len(kiro_msgs) > 1 else [],
        },
    }
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


# ─── Streaming ─────────────────────────────────────────────────────────────────

async def _do_stream(url: str, headers: dict, payload: dict, account: KiroAccount):
    """Streaming SSE no formato OpenAI."""

    async def generate():
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            async with client.stream("POST", url, json=payload, headers=headers) as resp:
                if resp.status_code in (401, 403):
                    raise AccountExhaustedException(f"HTTP {resp.status_code}")
                if resp.status_code == 429:
                    raise AccountExhaustedException("Rate limited (429)")
                if resp.status_code >= 400:
                    body = await resp.aread()
                    raise AccountExhaustedException(f"HTTP {resp.status_code}: {body.decode()[:200]}")

                async for line in resp.aiter_lines():
                    line = line.strip()
                    if not line:
                        continue
                    content = _parse_kiro_event(line)
                    if content:
                        chunk = {
                            "id": f"chatcmpl-{int(time.time())}",
                            "object": "chat.completion.chunk",
                            "created": int(time.time()),
                            "model": "kiro",
                            "choices": [{"index": 0, "delta": {"content": content}, "finish_reason": None}],
                        }
                        yield f"data: {json.dumps(chunk)}\n\n"

        # Final
        final = {
            "id": f"chatcmpl-{int(time.time())}",
            "object": "chat.completion.chunk",
            "created": int(time.time()),
            "model": "kiro",
            "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
        }
        yield f"data: {json.dumps(final)}\n\n"
        yield "data: [DONE]\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "Connection": "keep-alive"})


# ─── Normal Response ───────────────────────────────────────────────────────────

async def _do_normal(url: str, headers: dict, payload: dict, account: KiroAccount):
    """Request normal, retorna formato OpenAI."""
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        resp = await client.post(url, json=payload, headers=headers)
        if resp.status_code in (401, 403):
            raise AccountExhaustedException(f"HTTP {resp.status_code}")
        if resp.status_code == 429:
            raise AccountExhaustedException("Rate limited")
        if resp.status_code >= 400:
            raise AccountExhaustedException(f"HTTP {resp.status_code}: {resp.text[:200]}")

    content = _extract_full_response(resp.text)
    return JSONResponse({
        "id": f"chatcmpl-{int(time.time())}",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": "kiro",
        "choices": [{"index": 0, "message": {"role": "assistant", "content": content}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
    })


# ─── Parsers Kiro ──────────────────────────────────────────────────────────────

def _parse_kiro_event(line: str) -> Optional[str]:
    """Extrai texto de evento streaming Kiro."""
    try:
        data = json.loads(line)
        # Vários formatos possíveis da API Kiro
        if "assistantResponseEvent" in data:
            return data["assistantResponseEvent"].get("content")
        if "contentBlockDelta" in data:
            return data["contentBlockDelta"].get("delta", {}).get("text")
        if "messageStream" in data:
            ms = data["messageStream"]
            if "contentBlockDelta" in ms:
                return ms["contentBlockDelta"].get("delta", {}).get("text")
            if "assistantResponseEvent" in ms:
                return ms["assistantResponseEvent"].get("content")
        if "text" in data:
            return data["text"]
    except (json.JSONDecodeError, KeyError, TypeError):
        if line and not line.startswith(("{", ":")):
            return line
    return None


def _extract_full_response(text: str) -> str:
    """Extrai conteúdo completo de resposta não-streaming."""
    parts = []
    for line in text.split("\n"):
        c = _parse_kiro_event(line.strip())
        if c:
            parts.append(c)
    return "".join(parts) if parts else text


# ─── Fallback Proxy ────────────────────────────────────────────────────────────

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"],
               dependencies=[Depends(verify_api_key)])
async def proxy_fallback(request: Request, path: str):
    """Proxy genérico para outros endpoints."""
    body = await request.body()
    account = pool.get_next_account()
    if not account:
        raise HTTPException(503, "Sem contas ativas")
    try:
        token = await account.get_access_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        url = f"{KIRO_API_HOST}/{path}"
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            resp = await client.request(request.method, url, content=body or None, headers=headers)
        return StreamingResponse(iter([resp.content]), status_code=resp.status_code,
                                 media_type=resp.headers.get("content-type", "application/json"))
    except Exception as e:
        raise HTTPException(502, str(e))


# ─── Startup ──────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    logger.info("=" * 50)
    logger.info("  Kiro Gateway v3.0 - Admin Panel Edition")
    logger.info(f"  Contas: {len(pool.accounts)}")
    logger.info(f"  Region: {REGION}")
    logger.info(f"  Admin: /admin")
    logger.info("=" * 50)
    # Testa tokens no startup
    for acc in pool.accounts:
        try:
            await acc.get_access_token()
            logger.info(f"  [{acc.id}] OK")
        except Exception as e:
            logger.warning(f"  [{acc.id}] Refresh falhou: {e}")
