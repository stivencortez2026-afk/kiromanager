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

# URLs Kiro
KIRO_REFRESH_URL = f"https://prod.{REGION}.auth.desktop.kiro.dev/refreshToken"
KIRO_API_HOST = f"https://codewhisperer.{REGION}.amazonaws.com"

# ─── Security ──────────────────────────────────────────────────────────────────
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(api_key: Optional[str] = Depends(api_key_header)):
    if not GATEWAY_API_KEY:
        return True
    # Admin master key
    if api_key == GATEWAY_API_KEY:
        return True
    # Generated API keys
    if api_key and key_manager.validate_key(api_key):
        return True
    raise HTTPException(status_code=401, detail="API Key inválida ou desativada")


async def verify_admin_key(api_key: Optional[str] = Depends(api_key_header)):
    """Só aceita a master key (admin)."""
    if not GATEWAY_API_KEY:
        return True
    if api_key == GATEWAY_API_KEY:
        return True
    raise HTTPException(status_code=401, detail="Acesso admin requer GATEWAY_API_KEY")


def get_fingerprint() -> str:
    mid = os.getenv("MACHINE_ID", str(uuid.uuid4()))
    return hashlib.sha256(mid.encode()).hexdigest()[:32]


FINGERPRINT = get_fingerprint()


# ─── Persistência Redis (Upstash) ──────────────────────────────────────────────

import redis

REDIS_URL = os.getenv("REDIS_URL", "")

_redis_client = None


def get_redis():
    global _redis_client
    if _redis_client is None and REDIS_URL:
        _redis_client = redis.from_url(REDIS_URL, decode_responses=True, ssl_cert_reqs=None)
    return _redis_client


def load_accounts_from_disk() -> List[dict]:
    r = get_redis()
    if not r:
        return []
    try:
        data = r.get("kiro:accounts")
        return json.loads(data) if data else []
    except Exception:
        return []


def save_accounts_to_disk(accounts_data: List[dict]):
    r = get_redis()
    if not r:
        return
    r.set("kiro:accounts", json.dumps(accounts_data))


def load_api_keys_from_disk() -> List[dict]:
    r = get_redis()
    if not r:
        return []
    try:
        data = r.get("kiro:api_keys")
        return json.loads(data) if data else []
    except Exception:
        return []


def save_api_keys_to_disk(keys_data: List[dict]):
    r = get_redis()
    if not r:
        return
    r.set("kiro:api_keys", json.dumps(keys_data))


# ─── API Key Manager ──────────────────────────────────────────────────────────

class ApiKeyManager:
    """Gerencia API keys geradas pelo admin."""

    def __init__(self):
        self.keys: List[dict] = load_api_keys_from_disk()

    def _save(self):
        save_api_keys_to_disk(self.keys)

    def generate_key(self, name: str = "") -> dict:
        """Gera uma nova API key."""
        key = f"sk-kiro-{uuid.uuid4().hex[:24]}"
        entry = {
            "key": key,
            "name": name or f"Key {len(self.keys) + 1}",
            "active": True,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "last_used": None,
            "requests": 0,
        }
        self.keys.append(entry)
        self._save()
        return entry

    def validate_key(self, key: str) -> bool:
        """Verifica se a key é válida e está ativa."""
        for entry in self.keys:
            if entry["key"] == key and entry["active"]:
                entry["last_used"] = datetime.now(timezone.utc).isoformat()
                entry["requests"] += 1
                self._save()
                return True
        return False

    def toggle_key(self, key: str) -> Optional[dict]:
        """Ativa/desativa uma key."""
        for entry in self.keys:
            if entry["key"] == key:
                entry["active"] = not entry["active"]
                self._save()
                return entry
        return None

    def delete_key(self, key: str) -> bool:
        """Remove uma key."""
        before = len(self.keys)
        self.keys = [k for k in self.keys if k["key"] != key]
        if len(self.keys) < before:
            self._save()
            return True
        return False

    def list_keys(self) -> List[dict]:
        return self.keys


key_manager = ApiKeyManager()


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
            "credits": None,
        }

    async def check_credits(self) -> dict:
        """Consulta créditos da conta via getUsageLimits."""
        try:
            access_token = await self.get_access_token()
            url = f"https://q.{REGION}.amazonaws.com/getUsageLimits"
            params = {
                "origin": "AI_EDITOR",
                "resourceType": "AGENTIC_REQUEST",
            }
            if self.profile_arn:
                params["profileArn"] = self.profile_arn

            headers = {
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json",
                "User-Agent": f"KiroIDE-0.7.45-{FINGERPRINT}",
            }

            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(url, params=params, headers=headers)
                if resp.status_code != 200:
                    return {"error": f"HTTP {resp.status_code}: {resp.text[:100]}"}
                data = resp.json()

            # Extrai info de créditos
            usage_list = data.get("usageBreakdownList", [])
            result = {"raw": data, "credits": []}
            for item in usage_list:
                credit_info = {
                    "type": item.get("type", "UNKNOWN"),
                    "used": item.get("currentUsage", 0),
                    "limit": item.get("usageLimit", 0),
                    "reset_date": item.get("resetDate", ""),
                }
                # Free trial info
                if "freeTrialUsage" in item:
                    ft = item["freeTrialUsage"]
                    credit_info["free_trial"] = {
                        "used": ft.get("currentUsage", 0),
                        "limit": ft.get("usageLimit", 0),
                        "expires": ft.get("expiryDate", ""),
                        "days_remaining": ft.get("daysRemaining", 0),
                    }
                result["credits"].append(credit_info)

            # Subscription info
            sub = data.get("subscriptionInfo", {})
            if sub:
                result["plan"] = sub.get("subscriptionTitle", "Unknown")

            return result
        except Exception as e:
            return {"error": str(e)}


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
<p class="subtitle">Gerencie suas contas Kiro <button onclick="loadData()" style="margin-left:15px;font-size:0.85em;padding:6px 14px;">🔄 Atualizar</button></p>
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
<thead><tr><th>Nome</th><th>Status</th><th>Requests</th><th>Erros</th><th>Último Erro</th><th>Créditos</th><th></th></tr></thead>
<tbody id="accounts"></tbody>
</table>
</div>
<div class="card">
<h2>API Keys</h2>
<p style="color:#888;margin-bottom:15px;font-size:0.9em;">Gere chaves para distribuir a clientes. Cada key pode ser ativada/desativada individualmente.</p>
<div class="form-row" style="margin-bottom:15px;">
<input type="text" id="keyname" placeholder="Nome da key (ex: Claude Desktop, Notebook 1)">
<button onclick="generateKey()">Gerar Key</button>
</div>
<table>
<thead><tr><th>Nome</th><th>Key</th><th>Status</th><th>Requests</th><th>Última Uso</th><th></th><th></th></tr></thead>
<tbody id="apikeys"></tbody>
</table>
</div>
<div class="card">
<h2>Como usar</h2>
<p style="color:#aaa; line-height:1.8;">
<strong>Base URL:</strong> <code style="color:#4fc3f7;">ESTE_DOMINIO/v1</code><br>
<strong>API Key:</strong> Use uma key gerada acima (header X-API-Key ou Authorization Bearer)<br>
<strong>Endpoint:</strong> POST /v1/chat/completions (formato OpenAI)<br>
<strong>Modelos:</strong> auto, claude-sonnet-4, claude-sonnet-4.5, claude-haiku-4.5
</p>
</div>
</div>
<script>
const API_KEY = localStorage.getItem("kiro_admin_key") || prompt("Digite a senha admin (GATEWAY_API_KEY):", "");
if (API_KEY) localStorage.setItem("kiro_admin_key", API_KEY);
const H = {"X-API-Key": API_KEY, "Content-Type": "application/json"};

async function loadData() {
  try {
    const r = await fetch("/admin/api/stats", {headers: H});
    if (r.status === 401) { localStorage.removeItem("kiro_admin_key"); showMsg("Senha incorreta! Recarregue a página.", true); return; }
    const d = await r.json();
    document.getElementById("stats").innerHTML = `
      <div class="stat"><div class="number">${d.total}</div><div class="label">Total Contas</div></div>
      <div class="stat"><div class="number">${d.active}</div><div class="label">Ativas</div></div>
      <div class="stat"><div class="number">${d.requests}</div><div class="label">Requests</div></div>
    `;
    let rows = "";
    for (const a of d.accounts) {
      const badge = a.active ? '<span class="badge badge-green">Ativa</span>' : '<span class="badge badge-red">Inativa</span>';
      rows += `<tr>
        <td>${a.label}</td><td>${badge}</td><td>${a.requests}</td><td>${a.errors}</td>
        <td style="color:#ef5350;font-size:0.8em;">${a.last_error||"-"}</td>
        <td><button style="font-size:0.8em;padding:5px 10px;" onclick="checkCredits('${a.id}')">Ver</button> <span id="credits-${a.id}" style="font-size:0.8em;"></span></td>
        <td><button class="danger" onclick="removeAccount('${a.id}')">X</button></td>
      </tr>`;
    }
    document.getElementById("accounts").innerHTML = rows || "<tr><td colspan=7 style='color:#888'>Nenhuma conta. Adicione acima.</td></tr>";

    // API Keys
    let keyRows = "";
    for (const k of (d.api_keys || [])) {
      const badge = k.active ? '<span class="badge badge-green">Ativa</span>' : '<span class="badge badge-red">Inativa</span>';
      const toggleLabel = k.active ? "Desativar" : "Ativar";
      const lastUsed = k.last_used ? new Date(k.last_used).toLocaleString() : "Nunca";
      keyRows += `<tr>
        <td>${k.name}</td>
        <td><code style="font-size:0.75em;color:#4fc3f7;cursor:pointer;" onclick="copyKey('${k.key}')">${k.key.substring(0,20)}... 📋</code></td>
        <td>${badge}</td><td>${k.requests}</td><td style="font-size:0.8em;">${lastUsed}</td>
        <td><button style="font-size:0.8em;padding:5px 10px;" onclick="toggleKey('${k.key}')">${toggleLabel}</button></td>
        <td><button class="danger" style="font-size:0.8em;padding:5px 10px;" onclick="deleteKey('${k.key}')">X</button></td>
      </tr>`;
    }
    document.getElementById("apikeys").innerHTML = keyRows || "<tr><td colspan=7 style='color:#888'>Nenhuma key gerada.</td></tr>";
  } catch(e) { showMsg("Erro: " + e.message, true); }
}

function copyKey(key) {
  navigator.clipboard.writeText(key);
  showMsg("Key copiada!");
}

async function generateKey() {
  const name = document.getElementById("keyname").value.trim();
  const r = await fetch("/admin/api/keys", {method:"POST", headers:H, body: JSON.stringify({name})});
  if (r.ok) {
    const d = await r.json();
    showMsg("Key gerada: " + d.key.key);
    document.getElementById("keyname").value = "";
    loadData();
  } else { showMsg("Erro ao gerar key", true); }
}

async function toggleKey(key) {
  await fetch("/admin/api/keys/" + encodeURIComponent(key) + "/toggle", {method:"PUT", headers:H});
  loadData();
}

async function deleteKey(key) {
  if (!confirm("Deletar esta key?")) return;
  await fetch("/admin/api/keys/" + encodeURIComponent(key), {method:"DELETE", headers:H});
  loadData();
}

async function checkCredits(id) {
  const el = document.getElementById("credits-" + id);
  el.textContent = "Carregando...";
  el.style.color = "#888";
  try {
    const r = await fetch("/admin/api/accounts/" + id + "/credits", {headers: H});
    const d = await r.json();
    if (d.error) { el.textContent = d.error; el.style.color = "#ef5350"; return; }
    if (d.credits && d.credits.length > 0) {
      const c = d.credits[0];
      let txt = c.used + "/" + c.limit;
      if (c.free_trial) { txt += " (Trial: " + c.free_trial.used.toFixed(1) + "/" + c.free_trial.limit + ")"; }
      if (d.plan) { txt += " [" + d.plan + "]"; }
      el.textContent = txt;
      el.style.color = "#4fc3f7";
    } else {
      el.textContent = "Sem dados";
      el.style.color = "#888";
    }
  } catch(e) { el.textContent = "Erro"; el.style.color = "#ef5350"; }
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

loadData();
</script>
</body></html>"""


# ─── Admin API Endpoints ───────────────────────────────────────────────────────

@app.get("/admin", response_class=HTMLResponse)
async def admin_panel():
    """Painel admin web."""
    return HTMLResponse(ADMIN_HTML + ADMIN_HTML2)


@app.get("/admin/api/stats", dependencies=[Depends(verify_admin_key)])
async def admin_stats():
    return {**pool.get_stats(), "api_keys": key_manager.list_keys()}


@app.post("/admin/api/accounts", dependencies=[Depends(verify_admin_key)])
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


@app.delete("/admin/api/accounts/{account_id}", dependencies=[Depends(verify_admin_key)])
async def admin_remove_account(account_id: str):
    """Remove conta via admin."""
    if pool.remove_account(account_id):
        return {"status": "ok"}
    raise HTTPException(404, "Conta não encontrada")


@app.post("/admin/api/keys", dependencies=[Depends(verify_admin_key)])
async def admin_generate_key(request: Request):
    """Gera nova API key."""
    body = await request.json()
    name = body.get("name", "").strip()
    entry = key_manager.generate_key(name)
    return {"status": "ok", "key": entry}


@app.put("/admin/api/keys/{key}/toggle", dependencies=[Depends(verify_admin_key)])
async def admin_toggle_key(key: str):
    """Ativa/desativa uma API key."""
    entry = key_manager.toggle_key(key)
    if entry:
        return {"status": "ok", "key": entry}
    raise HTTPException(404, "Key não encontrada")


@app.delete("/admin/api/keys/{key}", dependencies=[Depends(verify_admin_key)])
async def admin_delete_key(key: str):
    """Deleta uma API key."""
    if key_manager.delete_key(key):
        return {"status": "ok"}
    raise HTTPException(404, "Key não encontrada")


@app.get("/admin/api/accounts/{account_id}/credits", dependencies=[Depends(verify_admin_key)])
async def admin_check_credits(account_id: str):
    """Consulta créditos de uma conta."""
    for acc in pool.accounts:
        if acc.id == account_id:
            result = await acc.check_credits()
            return result
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


# ─── /v1/messages (Anthropic format) ──────────────────────────────────────────

@app.post("/v1/messages", dependencies=[Depends(verify_api_key)])
async def anthropic_messages(request: Request):
    """Endpoint Anthropic-compatible (/v1/messages) para Claude Desktop."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, "JSON inválido")

    messages = body.get("messages", [])
    model = body.get("model", "auto")
    stream = body.get("stream", False)
    system_msg = body.get("system", "")

    if not messages:
        raise HTTPException(400, "'messages' obrigatório")

    # Prepend system message se existir
    if system_msg:
        if isinstance(system_msg, list):
            system_msg = " ".join(b.get("text", "") for b in system_msg if b.get("type") == "text")
        messages = [{"role": "user", "content": f"[System]: {system_msg}"}] + messages

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

        logger.info(f"[Anthropic Attempt {attempt+1}] {account.id} | model={model}")

        try:
            access_token = await account.get_access_token()
            url, headers, payload = _build_kiro_request(messages, model, access_token, account)

            if stream:
                return await _do_stream_anthropic(url, headers, payload, account, model)
            else:
                return await _do_normal_anthropic(url, headers, payload, account, model)

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
    """Monta request no formato Kiro API (generateAssistantResponse)."""
    # Converte mensagens para formato Kiro
    kiro_history = []
    system_text = ""

    for msg in messages:
        role = msg.get("role", "user")
        content = msg.get("content", "")
        if isinstance(content, list):
            content = " ".join(p.get("text", "") for p in content if p.get("type") == "text")
        if not content:
            continue

        if role == "system":
            system_text += content + "\n"
        elif role == "user":
            kiro_history.append({
                "userInputMessage": {
                    "content": content,
                    "userIntent": "CHAT",
                }
            })
        elif role == "assistant":
            kiro_history.append({
                "assistantResponseMessage": {
                    "content": content,
                }
            })

    # A última mensagem do user é o currentMessage
    current_message = None
    history = []

    for i, entry in enumerate(kiro_history):
        if i == len(kiro_history) - 1 and "userInputMessage" in entry:
            current_message = entry
        else:
            history.append(entry)

    if not current_message:
        # Fallback: pega a última mensagem como current
        last_content = messages[-1].get("content", "Olá") if messages else "Olá"
        if isinstance(last_content, list):
            last_content = " ".join(p.get("text", "") for p in last_content if p.get("type") == "text")
        current_message = {
            "userInputMessage": {
                "content": last_content,
                "userIntent": "CHAT",
            }
        }

    # Se tem system prompt, injeta no content do current message
    if system_text:
        original = current_message["userInputMessage"]["content"]
        current_message["userInputMessage"]["content"] = f"[System Instructions]: {system_text.strip()}\n\n[User Message]: {original}"

    payload = {
        "conversationState": {
            "chatTriggerType": "MANUAL",
            "currentMessage": current_message,
            "history": history,
        },
    }

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


# ─── Anthropic Streaming ───────────────────────────────────────────────────────

async def _do_stream_anthropic(url: str, headers: dict, payload: dict, account: KiroAccount, model: str):
    """Streaming SSE no formato Anthropic."""
    msg_id = f"msg_{uuid.uuid4().hex[:24]}"

    async def generate():
        # Evento de início
        yield f"event: message_start\ndata: {json.dumps({'type':'message_start','message':{'id':msg_id,'type':'message','role':'assistant','content':[],'model':model,'stop_reason':None,'stop_sequence':None,'usage':{'input_tokens':0,'output_tokens':0}}})}\n\n"
        yield f"event: content_block_start\ndata: {json.dumps({'type':'content_block_start','index':0,'content_block':{'type':'text','text':''}})}\n\n"

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
                        delta_event = {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": content}}
                        yield f"event: content_block_delta\ndata: {json.dumps(delta_event)}\n\n"

        # Eventos de fim
        yield f"event: content_block_stop\ndata: {json.dumps({'type':'content_block_stop','index':0})}\n\n"
        yield f"event: message_delta\ndata: {json.dumps({'type':'message_delta','delta':{'stop_reason':'end_turn','stop_sequence':None},'usage':{'output_tokens':0}})}\n\n"
        yield f"event: message_stop\ndata: {json.dumps({'type':'message_stop'})}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "Connection": "keep-alive"})


async def _do_normal_anthropic(url: str, headers: dict, payload: dict, account: KiroAccount, model: str):
    """Resposta normal no formato Anthropic."""
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
        "id": f"msg_{uuid.uuid4().hex[:24]}",
        "type": "message",
        "role": "assistant",
        "content": [{"type": "text", "text": content}],
        "model": model,
        "stop_reason": "end_turn",
        "stop_sequence": None,
        "usage": {"input_tokens": 0, "output_tokens": 0},
    })


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
