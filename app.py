"""
GenNet — Level 2 Skeleton
==========================
A minimal but real federated network coordinator.

What it does:
  1. Doctors submit a join request (hospital + doctor name).
  2. The admin (Rafael) approves or rejects from a private panel.
  3. Approved hospitals appear on a public global map.

What it does NOT do yet (coming in next sessions):
  - Patient data entry (Viewer integration)
  - Variant aggregation
  - Ensembl enrichment

Storage: simple JSON file on disk. Good enough for the demo.
For production, swap for PostgreSQL — same logic.
"""

import os
import json
import secrets
import hashlib
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Request, Form, HTTPException, Depends, Cookie
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import uvicorn


# ── Configuration ──────────────────────────────────────────────────────────
# Admin password — read from environment variable. NEVER hard-code passwords.
# On Render we'll set this in the dashboard. For local dev there's a fallback.
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "change-me-local-only")

# Where to store the state. /tmp persists during one container run; for a real
# demo we use a local file. Render's free tier doesn't have persistent disk,
# so state resets on redeploy — fine for a skeleton.
DATA_FILE = Path(os.environ.get("DATA_FILE", "gennet_state.json"))

# Lock to prevent two requests writing the file at the same time
state_lock = threading.Lock()


# ── State management ──────────────────────────────────────────────────────
def load_state() -> dict:
    """Read the JSON file. If it doesn't exist, return a fresh empty state."""
    if not DATA_FILE.exists():
        return {"pending": [], "approved": [], "rejected": []}
    try:
        with DATA_FILE.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"pending": [], "approved": [], "rejected": []}


def save_state(state: dict) -> None:
    """Write the state to disk, atomically."""
    with state_lock:
        tmp = DATA_FILE.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        tmp.replace(DATA_FILE)


def now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def make_personal_code() -> str:
    """Generate a code like HOSP-A7K9 for a newly approved hospital."""
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"  # no confusing chars
    return "HOSP-" + "".join(secrets.choice(alphabet) for _ in range(4))


def hash_password(password: str, salt: str) -> str:
    """Hash a password with a salt. We never store plaintext passwords."""
    return hashlib.sha256((salt + ":" + password).encode("utf-8")).hexdigest()


def find_hospital_by_code(state: dict, code: str) -> Optional[dict]:
    code = (code or "").strip().upper()
    return next((h for h in state["approved"] if h.get("personal_code", "").upper() == code), None)


def find_hospital_by_id(state: dict, hid: str) -> Optional[dict]:
    return next((h for h in state["approved"] if h.get("id") == hid), None)


# ── App setup ──────────────────────────────────────────────────────────────
app = FastAPI(title="GenNet — Level 2 Skeleton", version="0.1.0")

BASE_DIR = Path(__file__).parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


# ── Admin authentication (cookie-based, simple) ───────────────────────────
ADMIN_COOKIE = "gennet_admin"


def is_admin(request: Request) -> bool:
    cookie = request.cookies.get(ADMIN_COOKIE)
    return cookie == ADMIN_PASSWORD


def require_admin(request: Request):
    if not is_admin(request):
        raise HTTPException(status_code=303, headers={"Location": "/admin/login"})


# ─────────────────────────────────────────────────────────────────────────
# PUBLIC PAGES
# ─────────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Landing page — explains the project and shows the global map."""
    state = load_state()
    return templates.TemplateResponse("home.html", {
        "request": request,
        "approved": state["approved"],
        "n_approved": len(state["approved"]),
    })


@app.get("/join", response_class=HTMLResponse)
async def join_form(request: Request, sent: Optional[str] = None):
    """Form for a doctor to request joining the network."""
    return templates.TemplateResponse("join.html", {
        "request": request,
        "sent": sent,
    })


@app.post("/join")
async def join_submit(
    hospital: str = Form(...),
    doctor: str = Form(...),
    city: str = Form(""),
    email: str = Form(""),
):
    """Receive a join request and queue it for admin approval."""
    hospital = hospital.strip()[:120]
    doctor = doctor.strip()[:120]
    city = city.strip()[:80]
    email = email.strip()[:120]

    if not hospital or not doctor:
        return RedirectResponse(url="/join?sent=error", status_code=303)

    state = load_state()
    state["pending"].append({
        "id": secrets.token_hex(6),
        "hospital": hospital,
        "doctor": doctor,
        "city": city,
        "email": email,
        "requested_at": now_iso(),
    })
    save_state(state)
    return RedirectResponse(url="/join?sent=ok", status_code=303)


# ─────────────────────────────────────────────────────────────────────────
# ADMIN PAGES (password-protected)
# ─────────────────────────────────────────────────────────────────────────

@app.get("/admin/login", response_class=HTMLResponse)
async def admin_login_form(request: Request, error: Optional[str] = None):
    return templates.TemplateResponse("admin_login.html", {
        "request": request,
        "error": error,
    })


@app.post("/admin/login")
async def admin_login_submit(password: str = Form(...)):
    if password != ADMIN_PASSWORD:
        return RedirectResponse(url="/admin/login?error=1", status_code=303)
    resp = RedirectResponse(url="/admin", status_code=303)
    # Cookie lasts ~7 days
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
    item["password_hash"] = None   # set by the doctor on first activation
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
    """Remove an approved hospital from the network."""
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)
    state = load_state()
    state["approved"] = [x for x in state["approved"] if x["id"] != req_id]
    save_state(state)
    return RedirectResponse(url="/admin", status_code=303)


# ─────────────────────────────────────────────────────────────────────────
# HOSPITAL ACCESS (each doctor enters their own hospital)
# ─────────────────────────────────────────────────────────────────────────
HOSPITAL_COOKIE = "gennet_hospital"   # stores the hospital id the doctor is logged into


def is_in_hospital(request: Request, hid: str) -> bool:
    """Check the doctor has a valid session cookie for THIS hospital."""
    token = request.cookies.get(HOSPITAL_COOKIE, "")
    return token == hid


@app.get("/hospital/{hid}", response_class=HTMLResponse)
async def hospital_gate(hid: str, request: Request):
    """
    Entry point when someone clicks a hospital on the map.
    - If not activated yet → show activation page (enter code, create password).
    - If activated but not logged in → show login page.
    - If logged in → show the hospital workspace.
    """
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
    """First-time activation: verify the HOSP-XXXX code and set a password."""
    state = load_state()
    h = find_hospital_by_id(state, hid)
    if h is None:
        return RedirectResponse(url="/", status_code=303)

    if h.get("activated"):
        return RedirectResponse(url=f"/hospital/{hid}", status_code=303)

    # Validate the activation code matches THIS hospital
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


# ─────────────────────────────────────────────────────────────────────────
# JSON API (for the map to refresh live)
# ─────────────────────────────────────────────────────────────────────────

@app.get("/api/hospitals")
async def api_hospitals():
    """Public list of approved hospitals (no emails, no codes — public-safe)."""
    state = load_state()
    return JSONResponse({
        "count": len(state["approved"]),
        "hospitals": [
            {
                "id": h["id"],
                "hospital": h["hospital"],
                "doctor": h["doctor"],
                "city": h.get("city", ""),
                "approved_at": h.get("approved_at", ""),
                "activated": h.get("activated", False),
            }
            for h in state["approved"]
        ],
    })


@app.get("/api/health")
async def health():
    return {"status": "ok", "service": "GenNet Coordinator", "version": "0.1.0"}


# ─────────────────────────────────────────────────────────────────────────
# Entry point — Render runs this via the start command we configure
# ─────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)
