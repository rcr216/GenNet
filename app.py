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
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS gennet_beacon (
                        id              SERIAL PRIMARY KEY,
                        hospital_id     TEXT NOT NULL,
                        patient_local_id TEXT NOT NULL,
                        variant_cdna    TEXT,
                        variant_protein TEXT,
                        effect_type     TEXT,
                        exon            TEXT,
                        codon           INTEGER,
                        protein_domain  TEXT,
                        phenotype_tag   TEXT,
                        drug_response   TEXT,
                        submitted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        UNIQUE (hospital_id, patient_local_id)
                    )
                """)
                # Idempotent ALTER for existing tables (in case the database has the old schema)
                cur.execute("ALTER TABLE gennet_beacon ADD COLUMN IF NOT EXISTS codon INTEGER")
                cur.execute("ALTER TABLE gennet_beacon ADD COLUMN IF NOT EXISTS protein_domain TEXT")
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_beacon_hospital ON gennet_beacon(hospital_id);
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_beacon_variant ON gennet_beacon(variant_cdna);
                """)

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

    def beacon_upsert(hospital_id: str, payload: dict) -> dict:
        """Insert or update a beacon aggregate for one patient."""
        with psycopg.connect(_DB_URL, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO gennet_beacon
                        (hospital_id, patient_local_id, variant_cdna, variant_protein,
                         effect_type, exon, codon, protein_domain, phenotype_tag, drug_response)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (hospital_id, patient_local_id) DO UPDATE SET
                        variant_cdna    = EXCLUDED.variant_cdna,
                        variant_protein = EXCLUDED.variant_protein,
                        effect_type     = EXCLUDED.effect_type,
                        exon            = EXCLUDED.exon,
                        codon           = EXCLUDED.codon,
                        protein_domain  = EXCLUDED.protein_domain,
                        phenotype_tag   = EXCLUDED.phenotype_tag,
                        drug_response   = EXCLUDED.drug_response,
                        submitted_at    = NOW()
                    RETURNING id, submitted_at
                """, (
                    hospital_id,
                    payload.get("patient_local_id", ""),
                    payload.get("variant_cdna"),
                    payload.get("variant_protein"),
                    payload.get("effect_type"),
                    payload.get("exon"),
                    payload.get("codon"),
                    payload.get("protein_domain"),
                    payload.get("phenotype_tag"),
                    payload.get("drug_response"),
                ))
                row = cur.fetchone()
                return {"id": row[0], "submitted_at": row[1].isoformat()}

    def beacon_delete(hospital_id: str, patient_local_id: str) -> bool:
        with psycopg.connect(_DB_URL, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM gennet_beacon WHERE hospital_id=%s AND patient_local_id=%s",
                    (hospital_id, patient_local_id),
                )
                return cur.rowcount > 0

    def beacon_stats() -> dict:
        """Aggregate stats across all hospitals — what the coordinator can see."""
        with psycopg.connect(_DB_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM gennet_beacon")
                total = cur.fetchone()[0]
                cur.execute("SELECT COUNT(DISTINCT hospital_id) FROM gennet_beacon")
                hosp = cur.fetchone()[0]
                cur.execute("SELECT COUNT(DISTINCT variant_cdna) FROM gennet_beacon WHERE variant_cdna IS NOT NULL")
                uniq_vars = cur.fetchone()[0]
                cur.execute("""
                    SELECT variant_cdna, COUNT(*) c, COUNT(DISTINCT hospital_id) h
                    FROM gennet_beacon WHERE variant_cdna IS NOT NULL
                    GROUP BY variant_cdna ORDER BY c DESC LIMIT 20
                """)
                by_variant = [{"variant": r[0], "count": r[1], "hospitals": r[2]} for r in cur.fetchall()]
                cur.execute("""
                    SELECT effect_type, COUNT(*) FROM gennet_beacon
                    WHERE effect_type IS NOT NULL GROUP BY effect_type ORDER BY 2 DESC
                """)
                by_effect = [{"effect": r[0], "count": r[1]} for r in cur.fetchall()]
                cur.execute("""
                    SELECT drug_response, COUNT(*) FROM gennet_beacon
                    WHERE drug_response IS NOT NULL GROUP BY drug_response ORDER BY 2 DESC
                """)
                by_drug = [{"drug": r[0], "count": r[1]} for r in cur.fetchall()]
                return {
                    "total_aggregates": total,
                    "hospitals_contributing": hosp,
                    "unique_variants": uniq_vars,
                    "by_variant": by_variant,
                    "by_effect": by_effect,
                    "by_drug_response": by_drug,
                }

    def beacon_purge_by_hospital(hospital_id: str) -> int:
        """Remove ALL aggregates from a hospital — used when admin removes the hospital."""
        with psycopg.connect(_DB_URL, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM gennet_beacon WHERE hospital_id = %s", (hospital_id,))
                return cur.rowcount

    def beacon_purge_orphans(valid_hospital_ids) -> int:
        """Remove aggregates whose hospital_id is not in the approved list."""
        ids = list(valid_hospital_ids)
        with psycopg.connect(_DB_URL, autocommit=True) as conn:
            with conn.cursor() as cur:
                if not ids:
                    cur.execute("DELETE FROM gennet_beacon")
                else:
                    cur.execute("DELETE FROM gennet_beacon WHERE hospital_id <> ALL(%s)", (ids,))
                return cur.rowcount

    def beacon_wipe_all() -> int:
        with psycopg.connect(_DB_URL, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM gennet_beacon")
                n = cur.fetchone()[0]
                cur.execute("DELETE FROM gennet_beacon")
                return n

    def beacon_match(query_hospital_id: str, variant_cdna: str = None,
                     effect_type: str = None, exon: str = None,
                     codon: int = None, protein_domain: str = None,
                     phenotype_tag: str = None) -> dict:
        """Find coincidences in OTHER hospitals for a given query.
        Returns matches by levels. Never returns patient identities, only counts and hospital names.
        """
        levels = {"exact_variant": [], "same_codon": [], "same_exon": [], "same_domain": [], "same_effect": [], "same_phenotype": []}
        with psycopg.connect(_DB_URL) as conn:
            with conn.cursor() as cur:
                if variant_cdna:
                    cur.execute("""
                        SELECT b.hospital_id, COUNT(*) c,
                               array_agg(DISTINCT b.drug_response) FILTER (WHERE b.drug_response IS NOT NULL),
                               array_agg(DISTINCT b.phenotype_tag) FILTER (WHERE b.phenotype_tag IS NOT NULL)
                        FROM gennet_beacon b
                        WHERE b.variant_cdna = %s AND b.hospital_id <> %s
                        GROUP BY b.hospital_id ORDER BY c DESC
                    """, (variant_cdna, query_hospital_id))
                    levels["exact_variant"] = [
                        {"hospital_id": r[0], "count": r[1],
                         "drug_responses": list(r[2] or []), "phenotypes": list(r[3] or [])}
                        for r in cur.fetchall()
                    ]
                if exon:
                    cur.execute("""
                        SELECT b.hospital_id, COUNT(*) c
                        FROM gennet_beacon b
                        WHERE b.exon = %s AND b.hospital_id <> %s
                          AND (b.variant_cdna IS NULL OR b.variant_cdna <> COALESCE(%s,''))
                        GROUP BY b.hospital_id ORDER BY c DESC
                    """, (exon, query_hospital_id, variant_cdna))
                    levels["same_exon"] = [{"hospital_id": r[0], "count": r[1]} for r in cur.fetchall()]
                if effect_type:
                    cur.execute("""
                        SELECT b.hospital_id, COUNT(*) c
                        FROM gennet_beacon b
                        WHERE b.effect_type = %s AND b.hospital_id <> %s
                        GROUP BY b.hospital_id ORDER BY c DESC
                    """, (effect_type, query_hospital_id))
                    levels["same_effect"] = [{"hospital_id": r[0], "count": r[1]} for r in cur.fetchall()]
                if codon is not None:
                    cur.execute("""
                        SELECT b.hospital_id, COUNT(*) c
                        FROM gennet_beacon b
                        WHERE b.codon = %s AND b.hospital_id <> %s
                          AND (b.variant_cdna IS NULL OR b.variant_cdna <> COALESCE(%s,''))
                        GROUP BY b.hospital_id ORDER BY c DESC
                    """, (codon, query_hospital_id, variant_cdna))
                    levels["same_codon"] = [{"hospital_id": r[0], "count": r[1]} for r in cur.fetchall()]
                if protein_domain:
                    cur.execute("""
                        SELECT b.hospital_id, COUNT(*) c
                        FROM gennet_beacon b
                        WHERE b.protein_domain = %s AND b.hospital_id <> %s
                          AND (b.variant_cdna IS NULL OR b.variant_cdna <> COALESCE(%s,''))
                        GROUP BY b.hospital_id ORDER BY c DESC
                    """, (protein_domain, query_hospital_id, variant_cdna))
                    levels["same_domain"] = [{"hospital_id": r[0], "count": r[1]} for r in cur.fetchall()]
                if phenotype_tag:
                    cur.execute("""
                        SELECT b.hospital_id, COUNT(*) c
                        FROM gennet_beacon b
                        WHERE b.phenotype_tag = %s AND b.hospital_id <> %s
                        GROUP BY b.hospital_id ORDER BY c DESC
                    """, (phenotype_tag, query_hospital_id))
                    levels["same_phenotype"] = [{"hospital_id": r[0], "count": r[1]} for r in cur.fetchall()]
        return levels

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

    BEACON_FILE = Path(os.environ.get("BEACON_FILE", "gennet_beacon.json"))

    def _load_beacon_list() -> list:
        if not BEACON_FILE.exists():
            return []
        try:
            with BEACON_FILE.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []

    def _save_beacon_list(items: list) -> None:
        tmp = BEACON_FILE.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=2)
        tmp.replace(BEACON_FILE)

    def beacon_upsert(hospital_id: str, payload: dict) -> dict:
        with state_lock:
            items = _load_beacon_list()
            pid = payload.get("patient_local_id", "")
            items = [x for x in items if not (x["hospital_id"] == hospital_id and x["patient_local_id"] == pid)]
            rec = {
                "id": len(items) + 1,
                "hospital_id": hospital_id,
                "patient_local_id": pid,
                "variant_cdna": payload.get("variant_cdna"),
                "variant_protein": payload.get("variant_protein"),
                "effect_type": payload.get("effect_type"),
                "exon": payload.get("exon"),
                "codon": payload.get("codon"),
                "protein_domain": payload.get("protein_domain"),
                "phenotype_tag": payload.get("phenotype_tag"),
                "drug_response": payload.get("drug_response"),
                "submitted_at": now_iso(),
            }
            items.append(rec)
            _save_beacon_list(items)
            return {"id": rec["id"], "submitted_at": rec["submitted_at"]}

    def beacon_delete(hospital_id: str, patient_local_id: str) -> bool:
        with state_lock:
            items = _load_beacon_list()
            new = [x for x in items if not (x["hospital_id"] == hospital_id and x["patient_local_id"] == patient_local_id)]
            if len(new) == len(items):
                return False
            _save_beacon_list(new)
            return True

    def beacon_stats() -> dict:
        items = _load_beacon_list()
        hosp = {x["hospital_id"] for x in items}
        uniq = {x["variant_cdna"] for x in items if x.get("variant_cdna")}
        from collections import Counter
        by_v = Counter()
        by_v_h = {}
        for x in items:
            v = x.get("variant_cdna")
            if v:
                by_v[v] += 1
                by_v_h.setdefault(v, set()).add(x["hospital_id"])
        by_eff = Counter(x.get("effect_type") for x in items if x.get("effect_type"))
        by_drug = Counter(x.get("drug_response") for x in items if x.get("drug_response"))
        return {
            "total_aggregates": len(items),
            "hospitals_contributing": len(hosp),
            "unique_variants": len(uniq),
            "by_variant": [{"variant": v, "count": c, "hospitals": len(by_v_h[v])}
                           for v, c in by_v.most_common(20)],
            "by_effect": [{"effect": e, "count": c} for e, c in by_eff.most_common()],
            "by_drug_response": [{"drug": d, "count": c} for d, c in by_drug.most_common()],
        }

    def beacon_purge_by_hospital(hospital_id: str) -> int:
        with state_lock:
            items = _load_beacon_list()
            new = [x for x in items if x["hospital_id"] != hospital_id]
            removed = len(items) - len(new)
            _save_beacon_list(new)
            return removed

    def beacon_purge_orphans(valid_hospital_ids) -> int:
        valid = set(valid_hospital_ids)
        with state_lock:
            items = _load_beacon_list()
            new = [x for x in items if x["hospital_id"] in valid]
            removed = len(items) - len(new)
            _save_beacon_list(new)
            return removed

    def beacon_wipe_all() -> int:
        with state_lock:
            items = _load_beacon_list()
            n = len(items)
            _save_beacon_list([])
            return n

    def beacon_match(query_hospital_id: str, variant_cdna: str = None,
                     effect_type: str = None, exon: str = None,
                     codon: int = None, protein_domain: str = None,
                     phenotype_tag: str = None) -> dict:
        items = _load_beacon_list()
        others = [x for x in items if x["hospital_id"] != query_hospital_id]
        from collections import defaultdict, Counter
        levels = {"exact_variant": [], "same_codon": [], "same_exon": [], "same_domain": [], "same_effect": [], "same_phenotype": []}

        if variant_cdna:
            byh = defaultdict(lambda: {"count": 0, "drug_responses": set(), "phenotypes": set()})
            for x in others:
                if x.get("variant_cdna") == variant_cdna:
                    byh[x["hospital_id"]]["count"] += 1
                    if x.get("drug_response"): byh[x["hospital_id"]]["drug_responses"].add(x["drug_response"])
                    if x.get("phenotype_tag"): byh[x["hospital_id"]]["phenotypes"].add(x["phenotype_tag"])
            levels["exact_variant"] = sorted(
                [{"hospital_id": h, "count": d["count"],
                  "drug_responses": list(d["drug_responses"]), "phenotypes": list(d["phenotypes"])}
                 for h, d in byh.items()],
                key=lambda r: -r["count"])

        if exon:
            byh = Counter(x["hospital_id"] for x in others
                          if x.get("exon") == exon
                          and (not variant_cdna or x.get("variant_cdna") != variant_cdna))
            levels["same_exon"] = [{"hospital_id": h, "count": c} for h, c in byh.most_common()]

        if codon is not None:
            byh = Counter(x["hospital_id"] for x in others
                          if x.get("codon") == codon
                          and (not variant_cdna or x.get("variant_cdna") != variant_cdna))
            levels["same_codon"] = [{"hospital_id": h, "count": c} for h, c in byh.most_common()]

        if protein_domain:
            byh = Counter(x["hospital_id"] for x in others
                          if x.get("protein_domain") == protein_domain
                          and (not variant_cdna or x.get("variant_cdna") != variant_cdna))
            levels["same_domain"] = [{"hospital_id": h, "count": c} for h, c in byh.most_common()]

        if effect_type:
            byh = Counter(x["hospital_id"] for x in others if x.get("effect_type") == effect_type)
            levels["same_effect"] = [{"hospital_id": h, "count": c} for h, c in byh.most_common()]

        if phenotype_tag:
            byh = Counter(x["hospital_id"] for x in others if x.get("phenotype_tag") == phenotype_tag)
            levels["same_phenotype"] = [{"hospital_id": h, "count": c} for h, c in byh.most_common()]

        return levels


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
app = FastAPI(title="GenNet — Level 2", version="0.4.3")
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
    # Also purge any aggregates this hospital had — data integrity rule
    try:
        beacon_purge_by_hospital(req_id)
    except Exception:
        pass
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


@app.get("/hospital/{hid}/simulator", response_class=HTMLResponse)
async def hospital_simulator(hid: str, request: Request):
    """Personalized simulator for the doctor — only their hospital + GenNet."""
    state = load_state()
    h = find_hospital_by_id(state, hid)
    if h is None:
        return RedirectResponse(url="/", status_code=303)
    if not h.get("activated"):
        return RedirectResponse(url=f"/hospital/{hid}", status_code=303)
    if not is_in_hospital(request, hid):
        return RedirectResponse(url=f"/hospital/{hid}", status_code=303)
    return templates.TemplateResponse("hospital_simulator.html", {
        "request": request, "hospital": h,
    })


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
        "version": "0.4.3",
        "storage": "postgres" if USE_DB else "json-file",
    }


@app.post("/api/beacon/submit")
async def api_beacon_submit(request: Request):
    """Receive an aggregate from a doctor's simulator.
    Only the doctor logged into that hospital can submit beacons for it.
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid_json"}, status_code=400)
    hid = body.get("hospital_id", "")
    if not is_in_hospital(request, hid):
        return JSONResponse({"ok": False, "error": "not_logged_in_to_hospital"}, status_code=403)
    state = load_state()
    if find_hospital_by_id(state, hid) is None:
        return JSONResponse({"ok": False, "error": "unknown_hospital"}, status_code=404)
    codon_val = body.get("codon")
    try:
        codon_val = int(codon_val) if codon_val is not None and str(codon_val).strip() != "" else None
    except Exception:
        codon_val = None
    payload = {
        "patient_local_id": str(body.get("patient_local_id", "")).strip()[:64],
        "variant_cdna":     (body.get("variant_cdna") or "").strip()[:200] or None,
        "variant_protein":  (body.get("variant_protein") or "").strip()[:200] or None,
        "effect_type":      (body.get("effect_type") or "").strip()[:60] or None,
        "exon":             (body.get("exon") or "").strip()[:20] or None,
        "codon":            codon_val,
        "protein_domain":   (body.get("protein_domain") or "").strip()[:80] or None,
        "phenotype_tag":    (body.get("phenotype_tag") or "").strip()[:80] or None,
        "drug_response":    (body.get("drug_response") or "").strip()[:80] or None,
    }
    if not payload["patient_local_id"]:
        return JSONResponse({"ok": False, "error": "missing_patient_local_id"}, status_code=400)
    result = beacon_upsert(hid, payload)
    return JSONResponse({"ok": True, "aggregate_id": result["id"], "submitted_at": result["submitted_at"]})


@app.post("/api/beacon/delete")
async def api_beacon_delete(request: Request):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid_json"}, status_code=400)
    hid = body.get("hospital_id", "")
    pid = str(body.get("patient_local_id", "")).strip()
    if not is_in_hospital(request, hid):
        return JSONResponse({"ok": False, "error": "not_logged_in_to_hospital"}, status_code=403)
    if not pid:
        return JSONResponse({"ok": False, "error": "missing_patient_local_id"}, status_code=400)
    deleted = beacon_delete(hid, pid)
    return JSONResponse({"ok": True, "deleted": deleted})


@app.post("/admin/purge-orphans")
async def admin_purge_orphans(request: Request):
    """Remove aggregates whose hospital no longer exists. Admin only."""
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)
    state = load_state()
    valid = [h["id"] for h in state["approved"]]
    removed = beacon_purge_orphans(valid)
    return RedirectResponse(url=f"/admin?purged={removed}", status_code=303)


@app.post("/admin/wipe-aggregates")
async def admin_wipe_aggregates(request: Request):
    """Delete ALL aggregates. Useful when resetting the demo after testing.
    Does NOT delete hospitals — only their federated aggregates.
    """
    if not is_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)
    n = beacon_wipe_all()
    return RedirectResponse(url=f"/admin?wiped={n}", status_code=303)


@app.get("/api/beacon/stats")
async def api_beacon_stats():
    """Public stats — what the coordinator can show. No patient identities."""
    return JSONResponse(beacon_stats())


@app.post("/api/beacon/match")
async def api_beacon_match(request: Request):
    """Find coincidences in the federated network for a given query.
    Only a doctor logged into a hospital can query (so we know who 'we' are
    and exclude their own hospital from results).
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid_json"}, status_code=400)
    hid = body.get("hospital_id", "")
    if not is_in_hospital(request, hid):
        return JSONResponse({"ok": False, "error": "not_logged_in_to_hospital"}, status_code=403)
    codon_val = body.get("codon")
    try:
        codon_val = int(codon_val) if codon_val is not None and str(codon_val).strip() != "" else None
    except Exception:
        codon_val = None
    raw = beacon_match(
        query_hospital_id=hid,
        variant_cdna=(body.get("variant_cdna") or "").strip() or None,
        effect_type=(body.get("effect_type") or "").strip() or None,
        exon=(body.get("exon") or "").strip() or None,
        codon=codon_val,
        protein_domain=(body.get("protein_domain") or "").strip() or None,
        phenotype_tag=(body.get("phenotype_tag") or "").strip() or None,
    )
    # Enrich with hospital display names (no patient identities here)
    state = load_state()
    name_map = {h["id"]: {"hospital": h["hospital"], "city": h.get("city", "")}
                for h in state["approved"]}
    def enrich(items):
        out = []
        for r in items:
            meta = name_map.get(r["hospital_id"], {"hospital": "Unknown hospital", "city": ""})
            out.append({**r, "hospital_name": meta["hospital"], "hospital_city": meta["city"]})
        return out
    return JSONResponse({
        "ok": True,
        "query_hospital_id": hid,
        "exact_variant":  enrich(raw["exact_variant"]),
        "same_codon":     enrich(raw.get("same_codon", [])),
        "same_exon":      enrich(raw["same_exon"]),
        "same_domain":    enrich(raw.get("same_domain", [])),
        "same_effect":    enrich(raw["same_effect"]),
        "same_phenotype": enrich(raw["same_phenotype"]),
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)
