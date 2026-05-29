"""
GenNet — Level 2
================
Federated network coordinator with PERSISTENT storage (PostgreSQL on Neon).

Storage:
  PostgreSQL via DATABASE_URL env var (Neon, etc.).
  Single JSONB row holds the full state dict. Simple and reliable for this scale.
  Falls back to a local JSON file ONLY if DATABASE_URL is not set (dev only).
"""

import os
import json
import secrets
import hashlib
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
import uvicorn


# ── Configuration ──────────────────────────────────────────────────────────
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "change-me-local-only")
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
LOCAL_FILE = Path(os.environ.get("DATA_FILE", "gennet_state.json"))
state_lock = threading.Lock()


# ── Storage backend ───────────────────────────────────────────────────────
USE_DB = bool(DATABASE_URL)

if USE_DB:
    import psycopg
    from psycopg.types.json import Jsonb

    def _normalize_db_url(url: str) -> str:
        if url.startswith("postgres://"):
            url = "postgresql://" + url[len("postgres://"):]
        return url

    _DB_URL = _normalize_db_url(DATABASE_URL)

    def _init_db():
        with psycopg.connect(_DB_URL, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS gennet_state (
                        id   INTEGER PRIMARY KEY,
                        data JSONB NOT NULL
                    )
                """)
                cur.execute("""
                    INSERT INTO gennet_state (id, data)
                    VALUES (1, %s)
                    ON CONFLICT (id) DO NOTHING
                """, (Jsonb({"pending": [], "approved": [], "rejected": []}),))

    _init_db()

    def load_state() -> dict:
        with psycopg.connect(_DB_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT data FROM gennet_state WHERE id = 1")
                row = cur.fetchone()
                if row is None:
                    return {"pending": [], "approved": [], "rejected": []}
                return row[0]

    def save_state(state: dict) -> None:
        with state_lock:
            with psycopg.connect(_DB_URL, autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE gennet_state SET data = %s WHERE id = 1",
                        (Jsonb(state),),
                    )

else:
    def load_state() -> dict:
        if not LOCAL_FILE.exists():
            return {"pending": [], "approved": [], "rejected": []}
        try:
            with LOCAL_FILE.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {"pending": [], "approved": [], "rejected": []}

    def save_state(state: dict) -> None:
        with state_lock:
            tmp = LOCAL_FILE.with_suffix(".tmp")
            with tmp.open("w", encoding="utf-8") as f:
                json.dump(state, f, ensure_ascii=False, indent=2)
            tmp.replace(LOCAL_FILE)


# ── Helpers ───────────────────────────────────────────────────────────────
def now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def make_personal_code() -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "HOSP-" + "".join(secrets.choice(alphabet) for _ in range(4))


def hash_password(password: str, salt: str) -> str:
    return hashlib.sha256((salt + ":" + password).encode("utf-8")).hexdigest()


def find_hospital_by_id(state: dict, hid: str) -> Optional[dict]:
    return next((h for h in state["approved"] if h.get("id") == hid), None)


# ── App ────────────────────────────────────────────────────────────────────
app = FastAPI(title="GenNet — Level 2", version="0.2.0")
BASE_DIR = Path(__file__).parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

ADMIN_COOKIE = "gennet_admin"
HOSPITAL_COOKIE = "gennet_hospital"


def is_admin(request: Request) -> bool:
    return request.cookies.get(ADMIN_COOKIE) == ADMIN_PASSWORD


def is_in_hospital(request: Request, hid: str) -> bool:
    return request.cookies.get(HOSPITAL_COOKIE, "") == hid


# ── PUBLIC ─────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    state = load_state()
    return templates.TemplateResponse("home.html", {
        "request": request,
        "approved": state["approved"],
        "n_approved": len(state["approved"]),
    })


@app.get("/join", response_class=HTMLResponse)
async def join_form(request: Request, sent: Optional[str] = None):
    return templates.TemplateResponse("join.html", {
        "request": request, "sent": sent,
    })


@app.post("/join")
async def join_submit(hospital: str = Form(...), doctor: str = Form(...),
                      city: str = Form(""), email: str = Form("")):
    hospital = hospital.strip()[:120]
    doctor = doctor.strip()[:120]
    city = city.strip()[:80]
    email = email.strip()[:120]
    if not hospital or not doctor:
        return RedirectResponse(url="/join?sent=error", status_code=303)
    state = load_state()
    state["pending"].append({
        "id": secrets.token_hex(6),
        "hospital": hospital, "doctor": doctor, "city": city, "email": email,
        "requested_at": now_iso(),
    })
    save_state(state)
    return RedirectResponse(url="/join?sent=ok", status_code=303)


# ── ADMIN ──────────────────────────────────────────────────────────────────
@app.get("/admin/login", response_class=HTMLResponse)
async def admin_login_form(request: Request, error: Optional[str] = None):
    return templates.TemplateResponse("admin_login.html", {"request": request, "error": error})


@app.post("/admin/login")
async def admin_login_submit(password: str = Form(...)):
    if password != ADMIN_PASSWORD:
        return RedirectResponse(url="/admin/login?error=1", status_code=303)
    resp = RedirectResponse(url="/admin", status_code=303)
    resp.set_cookie(ADMIN_COOKIE, ADMIN_PASSWORD, max_age=7 * 24 * 3600, httponly=True, samesite="lax")
    return resp


@app.get("/admin/logout")
async def admin_logout():
    resp = RedirectResponse(url="/", status_code=303)
    resp.delete_cookie(ADMIN_COOKIE)
    return resp


@app.get("/admin", response_class=HTMLResponse)
async def admin_panel(request: Request):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)
    state = load_state()
    return templates.TemplateResponse("admin.html", {
        "request": request,
        "pending": state["pending"],
        "approved": state["approved"],
        "rejected": state["rejected"],
    })


@app.post("/admin/approve/{req_id}")
async def admin_approve(req_id: str, request: Request):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)
    state = load_state()
    item = next((x for x in state["pending"] if x["id"] == req_id), None)
    if item is None:
        return RedirectResponse(url="/admin", status_code=303)
    state["pending"] = [x for x in state["pending"] if x["id"] != req_id]
    item["approved_at"] = now_iso()
    item["personal_code"] = make_personal_code()
    item["password_hash"] = None
    item["activated"] = False
    state["approved"].append(item)
    save_state(state)
    return RedirectResponse(url="/admin", status_code=303)


@app.post("/admin/reject/{req_id}")
async def admin_reject(req_id: str, request: Request):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)
    state = load_state()
    item = next((x for x in state["pending"] if x["id"] == req_id), None)
    if item is None:
        return RedirectResponse(url="/admin", status_code=303)
    state["pending"] = [x for x in state["pending"] if x["id"] != req_id]
    item["rejected_at"] = now_iso()
    state["rejected"].append(item)
    save_state(state)
    return RedirectResponse(url="/admin", status_code=303)


@app.post("/admin/remove/{req_id}")
async def admin_remove(req_id: str, request: Request):
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)
    state = load_state()
    state["approved"] = [x for x in state["approved"] if x["id"] != req_id]
    save_state(state)
    return RedirectResponse(url="/admin", status_code=303)


# ── HOSPITAL ACCESS ───────────────────────────────────────────────────────
@app.get("/hospital/{hid}", response_class=HTMLResponse)
async def hospital_gate(hid: str, request: Request):
    state = load_state()
    h = find_hospital_by_id(state, hid)
    if h is None:
        return RedirectResponse(url="/", status_code=303)
    if not h.get("activated"):
        return templates.TemplateResponse("hospital_activate.html", {
            "request": request, "hospital": h, "error": None,
        })
    if is_in_hospital(request, hid):
        return templates.TemplateResponse("hospital_workspace.html", {
            "request": request, "hospital": h,
        })
    return templates.TemplateResponse("hospital_login.html", {
        "request": request, "hospital": h, "error": None,
    })


@app.post("/hospital/{hid}/activate")
async def hospital_activate(hid: str, request: Request,
                            code: str = Form(...), password: str = Form(...),
                            password2: str = Form(...)):
    state = load_state()
    h = find_hospital_by_id(state, hid)
    if h is None:
        return RedirectResponse(url="/", status_code=303)
    if h.get("activated"):
        return RedirectResponse(url=f"/hospital/{hid}", status_code=303)
    if (code or "").strip().upper() != h.get("personal_code", "").upper():
        return templates.TemplateResponse("hospital_activate.html", {
            "request": request, "hospital": h,
            "error": "That activation code doesn't match this hospital.",
        })
    if len(password) < 6:
        return templates.TemplateResponse("hospital_activate.html", {
            "request": request, "hospital": h,
            "error": "Password must be at least 6 characters.",
        })
    if password != password2:
        return templates.TemplateResponse("hospital_activate.html", {
            "request": request, "hospital": h,
            "error": "The two passwords don't match.",
        })
    salt = secrets.token_hex(8)
    h["password_salt"] = salt
    h["password_hash"] = hash_password(password, salt)
    h["activated"] = True
    h["activated_at"] = now_iso()
    save_state(state)
    resp = RedirectResponse(url=f"/hospital/{hid}", status_code=303)
    resp.set_cookie(HOSPITAL_COOKIE, hid, max_age=7 * 24 * 3600, httponly=True, samesite="lax")
    return resp


@app.post("/hospital/{hid}/login")
async def hospital_login(hid: str, request: Request, password: str = Form(...)):
    state = load_state()
    h = find_hospital_by_id(state, hid)
    if h is None:
        return RedirectResponse(url="/", status_code=303)
    if not h.get("activated"):
        return RedirectResponse(url=f"/hospital/{hid}", status_code=303)
    salt = h.get("password_salt", "")
    if hash_password(password, salt) != h.get("password_hash"):
        return templates.TemplateResponse("hospital_login.html", {
            "request": request, "hospital": h, "error": "Incorrect password.",
        })
    resp = RedirectResponse(url=f"/hospital/{hid}", status_code=303)
    resp.set_cookie(HOSPITAL_COOKIE, hid, max_age=7 * 24 * 3600, httponly=True, samesite="lax")
    return resp


@app.get("/hospital/{hid}/logout")
async def hospital_logout(hid: str):
    resp = RedirectResponse(url="/", status_code=303)
    resp.delete_cookie(HOSPITAL_COOKIE)
    return resp


# ── API ────────────────────────────────────────────────────────────────────
@app.get("/api/hospitals")
async def api_hospitals():
    state = load_state()
    return JSONResponse({
        "count": len(state["approved"]),
        "hospitals": [
            {
                "id": h["id"], "hospital": h["hospital"], "doctor": h["doctor"],
                "city": h.get("city", ""), "approved_at": h.get("approved_at", ""),
                "activated": h.get("activated", False),
            }
            for h in state["approved"]
        ],
    })


@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "service": "GenNet Coordinator",
        "version": "0.2.0",
        "storage": "postgres" if USE_DB else "json-file",
    }


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)
