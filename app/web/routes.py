"""Web UI routes (server-rendered templates + htmx partials)."""
from __future__ import annotations

import hashlib
import hmac
import os
import re
import secrets
import sqlite3
import sys
from pathlib import Path
from typing import Any, cast
import datetime

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.templating import Jinja2Templates
import httpx

from .. import __version__
from ..config import (MAX_PAYLOAD_BYTES, POLL_INTERVAL_MIN, IPAWS_EVENT_TYPES,
                      GITHUB_LATEST_RELEASE_API, GITHUB_RELEASES_URL)
from ..serial_discovery import list_all_ports


def _template_dir() -> Path:
    """Locate the templates both in a normal run and inside a PyInstaller bundle."""
    if getattr(sys, "frozen", False):  # packaged (Windows .exe / onedir)
        base = Path(getattr(sys, "_MEIPASS", Path(sys.executable).parent))
        return base / "app" / "web" / "templates"
    return Path(__file__).parent / "templates"


_CSRF_COOKIE = "mesh_wx_csrf"
_CSRF_SECRET = secrets.token_bytes(32)
_CHANNEL_ADMIN_AUTH = HTTPBasic(auto_error=False)


def _new_csrf_token() -> str:
    nonce = secrets.token_urlsafe(24)
    signature = hmac.new(_CSRF_SECRET, nonce.encode(), hashlib.sha256).hexdigest()
    return nonce + "." + signature


def _valid_csrf_token(token: str) -> bool:
    try:
        nonce, signature = token.rsplit(".", 1)
    except ValueError:
        return False
    expected = hmac.new(_CSRF_SECRET, nonce.encode(), hashlib.sha256).hexdigest()
    return bool(nonce) and hmac.compare_digest(signature, expected)


async def _require_csrf(request: Request) -> None:
    if request.method not in {"POST", "PUT", "PATCH", "DELETE"}:
        return
    cookie = request.cookies.get(_CSRF_COOKIE, "")
    submitted = request.headers.get("X-CSRF-Token", "")
    if not submitted:
        form = await request.form()
        submitted = str(form.get("csrf_token", ""))
    if not (_valid_csrf_token(cookie) and hmac.compare_digest(cookie, submitted)):
        raise HTTPException(status_code=403, detail="CSRF validation failed")


def _require_channel_admin(
    credentials: HTTPBasicCredentials | None = Depends(_CHANNEL_ADMIN_AUTH),
) -> None:
    password = os.environ.get("MESHWX_ADMIN_PASSWORD", "")
    if not password:
        raise HTTPException(
            status_code=503,
            detail=("Channel administration is disabled until "
                    "MESHWX_ADMIN_PASSWORD is configured."),
        )
    supplied_username = credentials.username if credentials else ""
    supplied_password = credentials.password if credentials else ""
    username_ok = hmac.compare_digest(supplied_username.encode(), b"admin")
    password_ok = hmac.compare_digest(supplied_password.encode(), password.encode())
    if not (username_ok and password_ok):
        raise HTTPException(
            status_code=401,
            detail="Administrator authentication required.",
            headers={"WWW-Authenticate": 'Basic realm="WXDispatch channel administration"'},
        )


router = APIRouter(dependencies=[Depends(_require_csrf)])
TEMPLATES = Jinja2Templates(directory=str(_template_dir()))

from ..formatter import fmt_local
TEMPLATES.env.filters["localtime"] = fmt_local


# History/dashboard status labels describe the FILTER decision, not transmission
# (whether a message actually went out lives in the Transmit Log). So "sent"
# becomes "pass" (the alert passed your rules); the rest already describe the
# alert, not a send.
DISP_LABELS = {
    "sent": "pass",
    "filtered": "filtered",
    "update": "update",
    "cancelled": "cancelled",
    "duplicate": "duplicate",
    "no_route": "no route",
    "queued": "queued",
    "accepted": "accepted",
    "partial": "partial",
    "failed": "failed",
    "dry_run": "dry run",
    "cleared": "cleared",
}

_HISTORY_DISPOSITIONS = [
    "no_route", "queued", "accepted", "partial", "failed", "dry_run", "cleared",
    "cancelled", "sent", "update", "filtered", "duplicate",
]
_ACCEPTED_HISTORY_DISPOSITIONS = {
    "accepted", "partial", "cleared", "cancelled", "sent", "update",
}
_ROUTING_INACTIVE_MESSAGE = (
    "Automated delivery is inactive. Create and enable a destination and routing rule "
    "before going LIVE."
)


def render(request: Request, name: str, **ctx):
    try:
        tz = request.app.state.db.get_setting("display_timezone", "")
    except Exception:
        tz = ""   # fall back to this machine's local time
    token = request.cookies.get(_CSRF_COOKIE, "")
    if not _valid_csrf_token(token):
        token = _new_csrf_token()
    response = TEMPLATES.TemplateResponse(
        request, name,
        {"max_bytes": MAX_PAYLOAD_BYTES, "tz": tz, "disp_label": DISP_LABELS,
         "version": __version__, "csrf_token": token, **ctx},
    )
    response.set_cookie(_CSRF_COOKIE, token, httponly=True, samesite="strict", path="/")
    return response


def _db(request: Request):
    return request.app.state.db


def _tx(request: Request):
    return request.app.state.tx


def _poller(request: Request):
    return request.app.state.poller


def _routing_ready(db) -> bool:
    list_routes = getattr(db, "list_routes", None)
    if list_routes is None:
        return False
    return any(
        bool(rule.get("enabled"))
        and any(bool(destination.get("enabled")) for destination in rule.get("destinations", []))
        for rule in list_routes()
    )


def _status_flag(request: Request, name: str) -> bool:
    return any(s["connected"] for s in _tx(request).status() if s["name"] == name)


def _status_ctx(request: Request) -> dict:
    db, tx, poller = _db(request), _tx(request), _poller(request)
    return {
        "last_poll_time": poller.status.last_poll_time,
        "last_poll_result": poller.status.last_poll_result,
        "port": tx.port or "(none)",
        "connected": tx.connected,
        "tx_error": tx.last_error,
        "dry_run": bool(db.get_setting("dry_run", True)),
        "queue_depth": tx.queue_depth,
        "uptime": poller.status.uptime_seconds,
        "events": db.recent_events(10),
    }



def _spark(counts):
    if not counts or max(counts) == 0:
        return "", ""
    n, mx, W, H, pad = len(counts), max(counts), 720, 80, 8
    pts = []
    for i, c in enumerate(counts):
        x = 0 if n == 1 else i * (W / (n - 1))
        y = H - pad - (c / mx) * (H - 2 * pad)
        pts.append((x, y))
    line = "M" + " L".join("%.0f,%.0f" % (x, y) for x, y in pts)
    return line, line + " L%d,%d L0,%d Z" % (W, H, H)


def _dash_ctx(request) -> dict:
    db, tx, poller = _db(request), _tx(request), _poller(request)
    tz = db.get_setting("display_timezone", "")
    zones = [z.strip() for z in (db.get_setting("zones", "") or "").split(",") if z.strip()]
    forecast = [z for z in zones if len(z) > 2 and z[2] == "Z"]
    county = [z for z in zones if len(z) > 2 and z[2] == "C"]
    port = tx.port or ""
    device = "Heltec V3" if ("CP210" in port or "Silicon_Labs" in port) else (port.split("/")[-1] if port else "(none)")
    up = poller.status.uptime_seconds
    dd, rem = divmod(up, 86400); hh, rem = divmod(rem, 3600); mm = rem // 60
    uptime_str = ("%dd %dh" % (dd, hh)) if dd else ("%dh %dm" % (hh, mm))
    now = datetime.datetime.now(datetime.timezone.utc)
    def _age(ts):
        try:
            return (now - datetime.datetime.fromisoformat(ts)).total_seconds() / 86400.0
        except Exception:
            return 999
    rows = db.query_history(limit=1000)
    accepted = [r for r in rows if r["disposition"] in _ACCEPTED_HISTORY_DISPOSITIONS]
    sent_7d = sum(1 for r in accepted if _age(r["ts"]) < 7)
    sent_today = sum(1 for r in accepted if _age(r["ts"]) < 1)
    buckets = [0] * 7
    for r in accepted:
        a = _age(r["ts"])
        if 0 <= a < 7:
            buckets[6 - int(a)] += 1
    spark_line, spark_fill = _spark(buckets)
    include = list(db.get_setting("filter_include_exact", []) or [])
    if db.get_setting("filter_include_suffix", []):
        include = ["All Warnings"] + include
    recent = [{
        "event": r["event"], "area": r["area"], "disposition": r["disposition"],
        "detail": r["detail"], "text": r["transmitted_text"], "when": fmt_local(r["ts"], tz),
    } for r in db.query_history(limit=6)]
    ltx = db.query_transmit_log(limit=1)
    last_tx = "-"
    if ltx:
        # ASCII only: this string is auto-escaped through {{ }}, so a non-ASCII
        # separator could mojibake depending on charset. Keep it plain.
        last_tx = ("OK - " if ltx[0]["success"] else "failed - ") + fmt_local(ltx[0]["ts"], tz)

    # ---- health watchdog: catch SILENT failures (looks live, delivers nothing) --
    st = poller.status
    interval = int(db.get_setting("poll_interval", 120))
    dry = bool(db.get_setting("dry_run", True))
    def _age_s(ts):
        try:
            return (now - datetime.datetime.fromisoformat(ts)).total_seconds()
        except Exception:
            return None
    problems = []          # (level, message); level in {"critical","warn"}
    if not _routing_ready(db):
        problems.append(("critical", _ROUTING_INACTIVE_MESSAGE))
    # 1. Are we still reaching NWS? A stale success time = we are blind to alerts.
    succ_age = _age_s(st.last_poll_success_time)
    stale_after = max(interval * 3, 360)
    if succ_age is None:
        problems.append(("warn", "No successful NWS poll yet."))
    elif succ_age > stale_after:
        problems.append(("critical",
            "Not reaching NWS: last good poll %d min ago. Not receiving alerts." % (succ_age // 60)))
    if st.last_poll_result.startswith("error"):
        problems.append(("warn", "Last NWS poll errored: %s" % st.last_poll_result[7:][:80]))
    # 2. Radios: any enabled radio offline means alerts may not go out.
    radios = tx.status()
    on = [r for r in radios if r["enabled"]]
    off = [r for r in on if not r["connected"]]
    if on and len(off) == len(on):
        problems.append(("critical", "All radios offline. Alerts cannot be broadcast."))
    elif off:
        problems.append(("warn", "%s offline." % ", ".join(r["label"] for r in off)))
    if not on:
        problems.append(("critical", "No radios enabled. Nothing will broadcast."))
    # 3. A recent alert that failed on every radio (verified-transmit feedback).
    bf_age = _age_s(st.last_broadcast_failure)
    if bf_age is not None and bf_age < 1800:
        problems.append(("critical",
            "A broadcast FAILED %d min ago and is being retried: %s"
            % (bf_age // 60, (st.last_broadcast_failure_text or "")[:60])))
    # 4. Backed-up queue.
    if tx.queue_depth > 5:
        problems.append(("warn", "Transmit queue backed up (%d waiting)." % tx.queue_depth))
    # 5. System clock skew vs NWS: makes alert "until" times wrong.
    skew = getattr(st, "clock_skew_seconds", None)
    if skew is not None and abs(skew) > 120:
        problems.append(("warn",
            "System clock is off by %d s vs NWS. Alert times may be wrong; check the VM clock."
            % int(abs(skew))))
    health_level = ("critical" if any(l == "critical" for l, _ in problems)
                    else "warn" if problems else "ok")

    return {
        "health_level": health_level,
        "health_problems": [m for _, m in problems],
        "health_paused": dry,
        "dry_run": bool(db.get_setting("dry_run", True)),
        "connected": tx.connected, "device": device, "tx_error": tx.last_error,
        "channel_index": int(db.get_setting("channel_index", 0)),
        "zone_count": len(zones), "forecast_count": len(forecast), "county_count": len(county),
        "poll_interval": int(db.get_setting("poll_interval", 120)), "queue_depth": tx.queue_depth,
        "last_poll_local": fmt_local(poller.status.last_poll_time, tz) if poller.status.last_poll_time else "-",
        "uptime_str": uptime_str, "sent_7d": sent_7d, "sent_today": sent_today,
        "spark_line": spark_line, "spark_fill": spark_fill,
        "include": include, "recent": recent, "last_tx": last_tx,
        "transports": tx.status(),
    }


# ---- liveness probe ----------------------------------------------------
@router.get("/healthz", response_class=PlainTextResponse)
async def healthz(request: Request):
    # If this responds, the event loop is servicing requests (i.e. not wedged).
    # Used by the Docker HEALTHCHECK; deliberately does no DB/radio work so it
    # is a pure liveness signal, not a readiness check.
    return PlainTextResponse("ok")


# ---- dashboard ---------------------------------------------------------
@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return render(request, "dashboard.html", **_dash_ctx(request))


@router.get("/partials/status", response_class=HTMLResponse)
async def status_partial(request: Request):
    return render(request, "_dash_top.html", **_dash_ctx(request))


@router.get("/partials/dashboard", response_class=HTMLResponse)
async def dashboard_cols_partial(request: Request):
    # The recent-alerts list + radios + broadcasting columns, for live polling.
    return render(request, "_dash_cols.html", **_dash_ctx(request))


@router.get("/partials/ipaws", response_class=HTMLResponse)
async def ipaws_partial(request: Request):
    # IPAWS (FEMA) dashboard panel -- separate from the weather pipeline.
    db = _db(request)
    st = getattr(request.app.state, "ipaws", None)
    return render(request, "_ipaws_panel.html", ipaws_rows=db.query_ipaws(30),
                  ipaws_status=(st.status if st else None))


@router.get("/ipaws", response_class=HTMLResponse)
async def ipaws_history(request: Request):
    # Full IPAWS history page (mirrors the NOAA History page).
    db = _db(request)
    st = getattr(request.app.state, "ipaws", None)
    return render(request, "ipaws_history.html", ipaws_rows=db.query_ipaws(200),
                  ipaws_status=(st.status if st else None))


@router.get("/partials/ipaws-history", response_class=HTMLResponse)
async def ipaws_history_partial(request: Request):
    return render(request, "_ipaws_rows.html", ipaws_rows=_db(request).query_ipaws(200))


@router.post("/dry-run/toggle", response_class=HTMLResponse)
async def toggle_dry_run(request: Request):
    db = _db(request)
    currently_dry = bool(db.get_setting("dry_run", True))
    if currently_dry and not _routing_ready(db):
        response = render(
            request, "_dash_top.html", **_dash_ctx(request),
            live_error="Cannot go LIVE. " + _ROUTING_INACTIVE_MESSAGE,
        )
        response.status_code = 409
        return response
    db.set_setting("dry_run", not currently_dry)
    db.add_event("INFO", f"dry-run set to {db.get_setting('dry_run')}")
    return render(request, "_dash_top.html", **_dash_ctx(request))


# ---- history -----------------------------------------------------------
@router.get("/history", response_class=HTMLResponse)
async def history(request: Request, disposition: str = "", date_from: str = "",
                  date_to: str = ""):
    rows = _db(request).query_history(
        disposition=disposition or None,
        date_from=date_from or None,
        date_to=date_to or None,
    )
    return render(
        request, "history.html", rows=rows, disposition=disposition,
        date_from=date_from, date_to=date_to,
        dispositions=_HISTORY_DISPOSITIONS,
    )


@router.get("/partials/history", response_class=HTMLResponse)
async def history_partial(request: Request, disposition: str = "", date_from: str = "",
                          date_to: str = ""):
    # Just the rows, honoring the same filters, for live polling.
    rows = _db(request).query_history(
        disposition=disposition or None,
        date_from=date_from or None,
        date_to=date_to or None,
    )
    return render(request, "_history_rows.html", rows=rows)


# ---- transmit log ------------------------------------------------------
@router.get("/transmit-log", response_class=HTMLResponse)
async def transmit_log(request: Request):
    rows = _db(request).query_transmit_log()
    return render(request, "transmit_log.html", rows=rows)


@router.get("/partials/transmit-log", response_class=HTMLResponse)
async def transmit_log_partial(request: Request):
    return render(request, "_transmit_rows.html", rows=_db(request).query_transmit_log())


@router.post("/transmit-log/resend/{entry_id}", response_class=HTMLResponse)
async def resend_log(request: Request, entry_id: int):
    db, tx = _db(request), _tx(request)
    row = db.get_transmit_log(entry_id)
    if row is not None and row["transport"]:
        await tx.resend(row["transport"], row["text"] or "", int(row["channel"]))
    return render(request, "_transmit_rows.html", rows=db.query_transmit_log())



_STATES = [
    ("AL","Alabama"),("AK","Alaska"),("AZ","Arizona"),("AR","Arkansas"),("CA","California"),
    ("CO","Colorado"),("CT","Connecticut"),("DE","Delaware"),("DC","District of Columbia"),
    ("FL","Florida"),("GA","Georgia"),("HI","Hawaii"),("ID","Idaho"),("IL","Illinois"),
    ("IN","Indiana"),("IA","Iowa"),("KS","Kansas"),("KY","Kentucky"),("LA","Louisiana"),
    ("ME","Maine"),("MD","Maryland"),("MA","Massachusetts"),("MI","Michigan"),("MN","Minnesota"),
    ("MS","Mississippi"),("MO","Missouri"),("MT","Montana"),("NE","Nebraska"),("NV","Nevada"),
    ("NH","New Hampshire"),("NJ","New Jersey"),("NM","New Mexico"),("NY","New York"),
    ("NC","North Carolina"),("ND","North Dakota"),("OH","Ohio"),("OK","Oklahoma"),("OR","Oregon"),
    ("PA","Pennsylvania"),("RI","Rhode Island"),("SC","South Carolina"),("SD","South Dakota"),
    ("TN","Tennessee"),("TX","Texas"),("UT","Utah"),("VT","Vermont"),("VA","Virginia"),
    ("WA","Washington"),("WV","West Virginia"),("WI","Wisconsin"),("WY","Wyoming"),
    ("PR","Puerto Rico"),("VI","U.S. Virgin Islands"),("GU","Guam"),
]

# Timezones offered in Settings. WXDispatch serves the US NWS, so this is the US /
# territories set plus an "Automatic" option (blank = the device's local zone).
_TIMEZONES = [
    ("", "Automatic (this device's time zone)"),
    ("America/New_York", "Eastern (New York)"),
    ("America/Chicago", "Central (Chicago)"),
    ("America/Denver", "Mountain (Denver)"),
    ("America/Phoenix", "Mountain, no DST (Arizona)"),
    ("America/Los_Angeles", "Pacific (Los Angeles)"),
    ("America/Anchorage", "Alaska"),
    ("America/Adak", "Hawaii-Aleutian (Adak)"),
    ("Pacific/Honolulu", "Hawaii (Honolulu)"),
    ("America/Puerto_Rico", "Atlantic (Puerto Rico, U.S. Virgin Islands)"),
    ("Pacific/Guam", "Chamorro (Guam, Northern Marianas)"),
    ("Pacific/Pago_Pago", "Samoa (American Samoa)"),
    ("UTC", "UTC"),
]

_EVENT_GROUPS = {
    "Warnings": ["Tornado Warning","Severe Thunderstorm Warning","Flash Flood Warning",
        "Flood Warning","Hurricane Warning","Tropical Storm Warning","Storm Surge Warning",
        "Winter Storm Warning","Ice Storm Warning","Blizzard Warning","High Wind Warning",
        "Extreme Heat Warning","Excessive Heat Warning","Red Flag Warning","Dust Storm Warning",
        "Freeze Warning"],
    "Watches": ["Tornado Watch","Severe Thunderstorm Watch","Flash Flood Watch","Flood Watch",
        "Hurricane Watch","Tropical Storm Watch","Winter Storm Watch","High Wind Watch",
        "Fire Weather Watch"],
    "Advisories": ["Special Weather Statement","Severe Weather Statement","Heat Advisory",
        "Wind Advisory","Winter Weather Advisory","Flood Advisory","Dense Fog Advisory",
        "Frost Advisory","Air Quality Alert","Coastal Flood Advisory","Rip Current Statement"],
}


async def _fetch_counties(state: str, contact: str):
    ua = "WXDispatch/%s (%s)" % (__version__, contact) if contact \
        else "WXDispatch/%s" % __version__
    url = "https://api.weather.gov/zones?area=%s&type=county" % state
    async with httpx.AsyncClient(timeout=20,
            headers={"User-Agent": ua, "Accept": "application/geo+json"}) as c:
        r = await c.get(url); r.raise_for_status()
        feats = r.json().get("features", [])
    out = [(f["properties"]["id"], f["properties"]["name"]) for f in feats]
    return sorted(out, key=lambda x: x[1])


@router.get("/settings/counties", response_class=HTMLResponse)
async def settings_counties(request: Request, state: str = ""):
    db = _db(request)
    selected = set(z.strip() for z in (db.get_setting("zones", "") or "").split(",") if z.strip())
    counties, err = [], ""
    if state:
        try:
            counties = await _fetch_counties(state, db.get_setting("nws_contact", ""))
        except Exception as exc:
            err = "Could not load counties for %s (%s). Check your internet/NWS contact and retry." % (state, exc)
    return render(request, "_counties.html", counties=counties, selected=selected, state=state, err=err)


# ---- settings ----------------------------------------------------------
def _parse_version(v: str) -> tuple:
    """Leading numeric dotted parts of a version tag as an int tuple, e.g.
    'v1.2.10' -> (1, 2, 10). Stops at the first non-numeric chunk so a
    pre-release suffix ('1.2.0-rc1') compares on its numeric core (1, 2, 0)."""
    core = re.split(r"[-+ ]", (v or "").strip().lstrip("vV"), maxsplit=1)[0]
    parts = []
    for chunk in core.split("."):
        if chunk.isdigit():
            parts.append(int(chunk))
        else:
            break
    return tuple(parts)


def _is_newer(latest: str, current: str) -> bool:
    lt, cur = _parse_version(latest), _parse_version(current)
    if not lt:
        return False
    n = max(len(lt), len(cur))
    return lt + (0,) * (n - len(lt)) > cur + (0,) * (n - len(cur))


@router.get("/settings/check-updates", response_class=HTMLResponse)
async def check_updates(request: Request):
    """Compare the running version against the latest GitHub release. Server-side
    fetch (no CORS/CSP issues), read-only, gracefully degrades when offline."""
    ctx = {"current": __version__, "latest": None,
           "url": GITHUB_RELEASES_URL, "state": "error", "detail": ""}
    try:
        async with httpx.AsyncClient(
                timeout=10,
                headers={"User-Agent": "WXDispatch-update-check",
                         "Accept": "application/vnd.github+json"}) as client:
            r = await client.get(GITHUB_LATEST_RELEASE_API)
        if r.status_code == 200:
            data = r.json()
            ctx["latest"] = (data.get("tag_name") or "").lstrip("vV")
            ctx["url"] = data.get("html_url") or GITHUB_RELEASES_URL
            ctx["state"] = "update" if _is_newer(ctx["latest"], __version__) else "current"
        elif r.status_code == 404:
            ctx["state"] = "current"       # repo has no published releases yet
            ctx["detail"] = "No releases published yet."
        else:
            ctx["detail"] = "GitHub returned HTTP %d." % r.status_code
    except Exception:
        ctx["detail"] = "Could not reach GitHub. Check this machine's connection."
    return render(request, "_update_check.html", **ctx)


@router.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request):
    db = _db(request)
    s = db.all_settings()
    zones = [z.strip() for z in (s.get("zones", "") or "").split(",") if z.strip()]
    counties_sel = [z for z in zones if len(z) > 2 and z[2] == "C"]
    extra = [z for z in zones if not (len(z) > 2 and z[2] == "C")]
    current_state = counties_sel[0][:2] if counties_sel else ""
    county_list = []
    if current_state:
        try:
            county_list = await _fetch_counties(current_state, s.get("nws_contact", ""))
        except Exception:
            county_list = []
    return render(
        request, "settings.html", s=s, min_interval=POLL_INTERVAL_MIN, ports=None,
        states=_STATES, current_state=current_state, state=current_state,
        timezones=_TIMEZONES, tz_current=(s.get("display_timezone", "") or ""),
        tz_known={v for v, _ in _TIMEZONES},
        counties=county_list, selected=set(counties_sel), extra_zones=", ".join(extra),
        event_groups=_EVENT_GROUPS,
        routing_ready=_routing_ready(db), routing_inactive_message=_ROUTING_INACTIVE_MESSAGE,
        selected_events=set(s.get("filter_include_exact", []) or []),
        all_warnings=bool(s.get("filter_include_suffix", []) or []), err="",
        ipaws_enabled=bool(s.get("ipaws_enabled", True)),
        ipaws_send_tests=bool(s.get("ipaws_send_tests", False)),
        ipaws_event_types=IPAWS_EVENT_TYPES,
        ipaws_selected=set(s.get("ipaws_events", None) if s.get("ipaws_events", None) is not None
                           else [k for k, _l, _kw in IPAWS_EVENT_TYPES]),
        mt_enabled=bool(s.get("meshtastic_enabled", True)),
        mt_conn=s.get("meshtastic_conn", "serial") or "serial",
        mt_host=s.get("meshtastic_host", "") or "",
        mt_channel=int(s.get("channel_index", 0) or 0),
        mt_port=s.get("serial_port", "") or "",
        mc_enabled=bool(s.get("meshcore_enabled", False)),
        mc_conn=s.get("meshcore_conn", "serial") or "serial",
        mc_port=s.get("meshcore_port", "") or "",
        mc_host=s.get("meshcore_host", "") or "",
        mc_channel=int(s.get("meshcore_channel", 0) or 0),
        mt_test=int(s.get("meshtastic_test_channel", 1) or 1),
        mc_test=int(s.get("meshcore_test_channel", 1) or 1),
        mt_channels=s.get("meshtastic_channels", []) or [],
        mc_channels=s.get("meshcore_channels", []) or [],
        mt_connected=_status_flag(request, "meshtastic"),
        mc_connected=_status_flag(request, "meshcore"),
        mt_model=s.get("meshtastic_model", "") or "",
        mc_model=s.get("meshcore_model", "") or "",
    )


@router.post("/settings", response_class=HTMLResponse)
async def save_settings(
    request: Request,
    poll_interval: int = Form(...),
    nws_contact: str = Form(...),
    channel_index: int = Form(0),
    display_timezone: str = Form(""),   # blank = Automatic (device local time)
    serial_port: str = Form(""),
    state: str = Form(""),
    counties: list[str] = Form(default=[]),
    extra_zones: str = Form(""),
    events: list[str] = Form(default=[]),
    all_warnings: str = Form(""),
    ipaws_enabled: str = Form(""),
    ipaws_send_tests: str = Form(""),
    ipaws_events: list[str] = Form(default=[]),
    meshtastic_enabled: str = Form(""),
    meshtastic_conn: str = Form("serial"),
    meshtastic_host: str = Form(""),
    meshcore_enabled: str = Form(""),
    meshcore_conn: str = Form("serial"),
    meshcore_port: str = Form(""),
    meshcore_host: str = Form(""),
    meshcore_channel: int = Form(0),
    meshtastic_test_channel: int = Form(1),
    meshcore_test_channel: int = Form(1),
):
    def _rep(v):
        try:
            return max(1, min(5, int(v)))
        except (TypeError, ValueError):
            return 2
    db, tx, poller = _db(request), _tx(request), _poller(request)
    interval = max(POLL_INTERVAL_MIN, int(poll_interval))

    zone_list = [c.strip() for c in counties if c.strip()]
    zone_list += [z.strip() for z in extra_zones.split(",") if z.strip()]
    db.set_setting("zones", ",".join(zone_list))
    db.set_setting("poll_interval", interval)
    db.set_setting("nws_contact", nws_contact.strip())
    db.set_setting("channel_index", int(channel_index))
    db.set_setting("display_timezone", display_timezone.strip())
    db.set_setting("filter_include_exact", [e for e in events if e])
    db.set_setting("filter_include_suffix", ["Warning"] if all_warnings else [])
    db.set_setting("filter_exclude_exact", [])

    # Radios - Meshtastic + MeshCore, each independently enabled.
    db.set_setting("ipaws_enabled", bool(ipaws_enabled))
    db.set_setting("ipaws_send_tests", bool(ipaws_send_tests))
    db.set_setting("ipaws_events", [e for e in ipaws_events if e])
    db.set_setting("meshtastic_enabled", bool(meshtastic_enabled))
    db.set_setting("meshtastic_conn", (meshtastic_conn or "serial").strip())
    db.set_setting("meshtastic_host", meshtastic_host.strip())
    db.set_setting("serial_port", serial_port.strip())
    db.set_setting("meshcore_enabled", bool(meshcore_enabled))
    db.set_setting("meshcore_conn", (meshcore_conn or "serial").strip())
    db.set_setting("meshcore_port", meshcore_port.strip())
    db.set_setting("meshcore_host", meshcore_host.strip())
    db.set_setting("meshcore_channel", int(meshcore_channel))
    db.set_setting("meshtastic_test_channel", int(meshtastic_test_channel))
    db.set_setting("meshcore_test_channel", int(meshcore_test_channel))

    # Rebuild transports from the new settings and (re)connect the enabled ones.
    await tx.reconfigure()

    poller.poke()
    db.add_event("INFO", "settings saved")
    return RedirectResponse("/settings", status_code=303)


@router.post("/settings/scan", response_class=HTMLResponse)
async def scan_ports(request: Request, target: str = Form("serial_port")):
    ports = list_all_ports()
    # only allow the two known field ids to be scripted into the page
    field = target if target in ("serial_port", "meshcore_port") else "serial_port"
    return render(request, "_ports.html", ports=ports, target=field)


_RADIO_FIELDS = {
    "meshtastic": ("meshtastic_conn", "serial_port", "meshtastic_host",
                   "channel_index", "meshtastic_test_channel"),
    "meshcore":   ("meshcore_conn", "meshcore_port", "meshcore_host",
                   "meshcore_channel", "meshcore_test_channel"),
}


_INCLUDE_SEL = {
    "meshtastic": "[name='meshtastic_conn'],[name='serial_port'],[name='meshtastic_host']",
    "meshcore": "[name='meshcore_conn'],[name='meshcore_port'],[name='meshcore_host']",
}


def _channel_ctx(db, tx, name, channels=None, model=None, error="", connected=None):
    conn_f, port_f, host_f, live_f, test_f = _RADIO_FIELDS[name]
    if channels is None:  # fall back to the last channels we read from this radio
        channels = db.get_setting(name + "_channels", []) or []
    if model is None:
        model = db.get_setting(name + "_model", "") or ""
    if connected is None:  # reflect the live transport's real state
        connected = any(s["connected"] for s in tx.status() if s["name"] == name)
    return dict(
        radio=name, channels=channels, error=error, connected=connected, model=model,
        include_sel=_INCLUDE_SEL[name],
        live_field=live_f, test_field=test_f,
        live_val=int(db.get_setting(live_f, 0) or 0),
        test_val=int(db.get_setting(test_f, 1) or 1),
    )


@router.post("/settings/channels/{name}", response_class=HTMLResponse)
async def load_channels(request: Request, name: str):
    if name not in _RADIO_FIELDS:
        return PlainTextResponse("unknown radio", status_code=404)
    conn_f, port_f, host_f, _, _ = _RADIO_FIELDS[name]
    form = await request.form()
    conn = form.get(conn_f, "serial")
    port = form.get(port_f, "")
    host = form.get(host_f, "")
    db, tx = _db(request), _tx(request)
    channels, model, error = await tx.load_channels(name, conn, port, host)
    if channels is not None:  # success: cache names + model so they persist across reload/save
        db.set_setting(name + "_channels", channels)
        db.set_setting(name + "_model", model)
        ctx = _channel_ctx(db, tx, name, channels=channels, model=model,
                           error="", connected=True)
    else:  # failed read of THIS device: show the error + saved values, never a stale
        # cached list from a different radio, and don't claim connected.
        ctx = _channel_ctx(db, tx, name, channels=[], model="",
                           error=error or "could not read this radio", connected=False)
    return render(request, "_radio_channels.html", **ctx)


# ---- manual send -------------------------------------------------------
@router.get("/manual", response_class=HTMLResponse)
async def manual_page(request: Request):
    return render(request, "manual_send.html")


@router.post("/manual/send", response_class=HTMLResponse)
async def manual_send(request: Request, text: str = Form(...)):
    tx = _tx(request)
    text = text[:MAX_PAYLOAD_BYTES] if len(text.encode()) > MAX_PAYLOAD_BYTES else text
    # Enforce byte cap defensively (multibyte-safe).
    while len(text.encode()) > MAX_PAYLOAD_BYTES:
        text = text[:-1]
    ok = await tx.send_manual(text)   # goes on each radio's LIVE channel
    msg = ("accepted by configured radio interfaces; over-air delivery is not confirmed"
           if ok else f"failed: {tx.last_error}")
    return render(request, "_manual_result.html", ok=ok, message=msg,
                  text=text, bytes=len(text.encode()))


# ---- troubleshoot ------------------------------------------------------
@router.get("/troubleshoot", response_class=HTMLResponse)
async def troubleshoot(request: Request):
    return render(request, "troubleshoot.html",
                  errors=_db(request).recent_errors(),
                  transports=_tx(request).status())


@router.post("/troubleshoot/clear-errors", response_class=HTMLResponse)
async def clear_errors(request: Request):
    _db(request).clear_errors()
    return render(request, "_errors.html", errors=[])


@router.post("/troubleshoot/test", response_class=HTMLResponse)
async def send_test(request: Request):
    tx = _tx(request)
    text = "[WX] WXDispatch test message"
    ok = await tx.send_test(text)   # goes on each radio's TEST channel
    return render(
        request, "_manual_result.html", ok=ok,
        message=("accepted by configured radio interfaces; over-air delivery is not confirmed"
                 if ok else f"failed: {tx.last_error}"),
        text=text, bytes=len(text.encode()),
    )


@router.post("/troubleshoot/test/{name}", response_class=HTMLResponse)
async def send_test_one(request: Request, name: str):
    tx = _tx(request)
    label = {t["name"]: t["label"] for t in tx.status()}.get(name, name)
    text = "[WX] WXDispatch test via %s" % label
    ok, err = await tx.send_to(name, text)
    return render(
        request, "_manual_result.html", ok=ok,
        message=(
            ("accepted by MeshCore companion; over-air delivery is not confirmed"
             if name == "meshcore" else
             "accepted by Meshtastic node; over-air delivery is not confirmed")
            if ok else f"{label} failed: {err}"
        ),
        text=text, bytes=len(text.encode()),
    )


@router.get("/troubleshoot/raw", response_class=PlainTextResponse)
async def raw_response(request: Request):
    raw = _poller(request).status.last_raw_response
    return raw or "(no NWS response captured yet)"


def _split_lines(value: str) -> list[str]:
    """Parse a textarea/newline- or comma-separated list into clean items."""
    parts: list[str] = []
    for chunk in value.replace(",", "\n").splitlines():
        item = chunk.strip()
        if item:
            parts.append(item)
    return parts


# ---- OpenHop / MeshCore companion channel operations ------------------
def _channel_form_error(request: Request, message: str, status_code: int = 400):
    response = render(request, "_channel_operation.html", ok=False, message=message)
    response.status_code = status_code
    return response


def _channel_index(form) -> int | None:
    try:
        return int(str(form.get("index", "")))
    except ValueError:
        return None


def _safe_channel(result: dict) -> dict | None:
    channel = result.get("channel")
    if not isinstance(channel, dict):
        return None
    return {"index": channel.get("index"), "name": channel.get("name") or ""}


@router.post("/openhop/channels/inspect", response_class=HTMLResponse)
async def inspect_openhop_channel(request: Request):
    index = _channel_index(await request.form())
    if index is None:
        return _channel_form_error(request, "Channel index must be a whole number.")
    result = await _tx(request).inspect_meshcore_channel(index)
    if not result.get("ok"):
        return _channel_form_error(
            request, result.get("error") or "The companion channel could not be inspected.", 502,
        )
    return render(
        request, "_channel_operation.html", ok=True,
        message="Channel metadata read and verified by the companion.",
        channel=_safe_channel(result),
    )


@router.post("/openhop/channels/set", response_class=HTMLResponse)
async def set_openhop_channel(
    request: Request, _admin: None = Depends(_require_channel_admin),
):
    form = await request.form()
    index = _channel_index(form)
    if index is None:
        return _channel_form_error(request, "Channel index must be a whole number.")
    name = str(form.get("name", "")).strip()
    if not name:
        return _channel_form_error(request, "Channel name is required.")
    if len(name.encode("utf-8")) > 32:
        return _channel_form_error(request, "Channel name must be at most 32 UTF-8 bytes.")
    if not name.startswith("#"):
        return _channel_form_error(request, "Hash channel name must begin with #.")
    if len(name) == 1:
        return _channel_form_error(request, "Hash channel must include a name after #.")
    result = await _tx(request).set_meshcore_channel(index, name)
    if not result.get("ok"):
        return _channel_form_error(
            request, result.get("error") or "The companion did not accept the channel update.", 502,
        )
    return render(
        request, "_channel_operation.html", ok=True,
        message="Channel update accepted by MeshCore companion; over-air delivery is not confirmed.",
        channel=_safe_channel(result),
    )


def _channel_reference(db, index: int) -> str | None:
    """Return any durable reference; fail closed without the complete DB API."""
    channel_references = getattr(db, "channel_references", None)
    if not callable(channel_references):
        return "Channel references cannot be safely checked with this database version."
    try:
        references = cast(dict[str, Any], channel_references("meshcore", index))
    except Exception:
        return "Channel references cannot be safely checked right now."
    destinations = references.get("destinations", [])
    if destinations:
        return "Destination %s uses this channel." % destinations[0]["name"]
    snapshots = references.get("active_delivery_snapshots", [])
    if snapshots:
        return "An active delivery snapshot uses this channel."
    if references.get("is_referenced"):
        return "This channel has a durable reference and cannot be cleared."
    return None


@router.post("/openhop/channels/clear", response_class=HTMLResponse)
async def clear_openhop_channel(
    request: Request, _admin: None = Depends(_require_channel_admin),
):
    form = await request.form()
    index = _channel_index(form)
    if index is None:
        return _channel_form_error(request, "Channel index must be a whole number.")
    expected = "CLEAR CHANNEL %d" % index
    if str(form.get("confirmation", "")) != expected:
        return _channel_form_error(request, "Type %s to confirm this destructive operation." % expected)
    blocked = _channel_reference(_db(request), index)
    if blocked:
        return _channel_form_error(request, blocked, 409)
    result = await _tx(request).clear_meshcore_channel(index)
    if not result.get("ok"):
        return _channel_form_error(
            request, result.get("error") or "The companion did not accept the channel clear.", 502,
        )
    return render(
        request, "_channel_operation.html", ok=True,
        message="Channel clear accepted by MeshCore companion; over-air delivery is not confirmed.",
        channel=_safe_channel(result),
    )


def _routing_page_rows(db):
    list_destinations = getattr(db, "list_destinations", None)
    destinations = list(list_destinations()) if list_destinations else []
    list_routes = getattr(db, "list_routes", None)
    rules = list(list_routes()) if list_routes else []
    return destinations, rules


@router.get("/routing", response_class=HTMLResponse)
async def routing_page(request: Request):
    db = _db(request)
    destinations, rules = _routing_page_rows(db)
    settings = db.all_settings()
    return render(
        request, "routing.html", destinations=destinations, rules=rules,
        event_groups=_EVENT_GROUPS,
        meshcore_channels=settings.get("meshcore_channels", []) or [],
        meshcore_max_channels=int(settings.get("meshcore_max_channels", 8) or 8),
        channel_admin_enabled=bool(os.environ.get("MESHWX_ADMIN_PASSWORD", "")),
        routing_ready=_routing_ready(db), routing_inactive_message=_ROUTING_INACTIVE_MESSAGE,
    )


@router.get("/routing/destinations/{destination_id}/edit", response_class=HTMLResponse)
async def edit_routing_destination(request: Request, destination_id: int):
    destination = _db(request).get_destination(destination_id)
    if destination is None:
        raise HTTPException(status_code=404, detail="Destination not found")
    return render(request, "destination_edit.html", destination=destination)


def _destination_form_values(form):
    name = str(form.get("name", "")).strip()
    transport = str(form.get("transport", ""))
    try:
        channel = int(str(form.get("channel", "")))
    except ValueError:
        channel = -1
    if not name or len(name) > 80:
        return None, "Destination name is required and must be 80 characters or fewer."
    if transport not in {"meshcore", "meshtastic"}:
        return None, "Select a supported destination transport."
    if not 0 <= channel <= 255:
        return None, "Destination channel must be between 0 and 255."
    return (name, transport, channel, bool(form.get("enabled"))), ""


@router.post("/routing/destinations", response_class=HTMLResponse)
async def create_routing_destination(request: Request):
    values, error = _destination_form_values(await request.form())
    if error:
        return _channel_form_error(request, error)
    assert values is not None
    try:
        _db(request).create_destination(*values)
    except sqlite3.IntegrityError:
        return _channel_form_error(request, "That transport and channel already has a destination.", 409)
    except Exception:
        return _channel_form_error(request, "The destination could not be saved.", 500)
    return RedirectResponse("/routing", status_code=303)


@router.post("/routing/destinations/{destination_id}", response_class=HTMLResponse)
async def update_routing_destination(request: Request, destination_id: int):
    values, error = _destination_form_values(await request.form())
    if error:
        return _channel_form_error(request, error)
    assert values is not None
    try:
        updated = _db(request).update_destination(destination_id, *values)
    except sqlite3.IntegrityError:
        return _channel_form_error(request, "That transport and channel already has a destination.", 409)
    except Exception:
        return _channel_form_error(request, "The destination could not be saved.", 500)
    if not updated:
        raise HTTPException(status_code=404, detail="Destination not found")
    return RedirectResponse("/routing", status_code=303)


@router.post("/routing/destinations/{destination_id}/toggle", response_class=HTMLResponse)
async def toggle_routing_destination(request: Request, destination_id: int):
    if not _db(request).toggle_destination(destination_id):
        raise HTTPException(status_code=404, detail="Destination not found")
    return RedirectResponse("/routing", status_code=303)


@router.post("/routing/destinations/{destination_id}/delete", response_class=HTMLResponse)
async def delete_routing_destination(request: Request, destination_id: int):
    db = _db(request)
    if db.get_destination(destination_id) is None:
        raise HTTPException(status_code=404, detail="Destination not found")
    form = await request.form()
    expected = "DELETE DESTINATION %d" % destination_id
    if str(form.get("confirmation", "")) != expected:
        return _channel_form_error(request, "Type %s to confirm deletion." % expected)
    try:
        deleted = db.delete_destination(destination_id)
    except sqlite3.IntegrityError:
        return _channel_form_error(
            request, "This destination is in use and cannot be deleted.", 409,
        )
    if not deleted:
        raise HTTPException(status_code=404, detail="Destination not found")
    return RedirectResponse("/routing", status_code=303)


def _parse_route_counties(value: str) -> tuple[list[tuple[str, str]], str]:
    counties = []
    seen = set()
    for line in value.splitlines():
        if not line.strip():
            continue
        parts = [part.strip() for part in line.split("|", 1)]
        if len(parts) != 2 or not re.fullmatch(r"[A-Z]{2}C\d{3}", parts[0]) or not parts[1]:
            return [], "Each county must use 'zone_code | county_name' (for example TNC147 | Robertson County)."
        if len(parts[1]) > 100:
            return [], "County display names must be 100 characters or fewer."
        if parts[0] not in seen:
            counties.append((parts[0], parts[1]))
            seen.add(parts[0])
    if not counties:
        return [], "At least one county is required."
    return counties, ""


@router.get("/routing/rules/{rule_id}/edit", response_class=HTMLResponse)
async def edit_routing_rule(request: Request, rule_id: int):
    db = _db(request)
    rule = db.get_route(rule_id)
    if rule is None:
        raise HTTPException(status_code=404, detail="Routing rule not found")
    return render(
        request, "routing_rule_edit.html", rule=rule,
        destinations=db.list_destinations(), event_groups=_EVENT_GROUPS,
        selected_events=set(rule["events"]),
        selected_destinations={destination["id"] for destination in rule["destinations"]},
    )


@router.post("/routing/rules", response_class=HTMLResponse)
async def create_routing_rule(request: Request):
    form = await request.form()
    db = _db(request)
    name = str(form.get("name", "")).strip()
    try:
        priority = int(str(form.get("priority", "")))
        destination_ids = list(dict.fromkeys(int(str(value)) for value in form.getlist("destination_ids")))
    except ValueError:
        return _channel_form_error(request, "Priority and destination references must be whole numbers.")
    counties, county_error = _parse_route_counties(str(form.get("counties", "")))
    if not name or len(name) > 80:
        return _channel_form_error(request, "Rule name is required and must be 80 characters or fewer.")
    if not -10000 <= priority <= 10000:
        return _channel_form_error(request, "Rule priority must be between -10000 and 10000.")
    if county_error:
        return _channel_form_error(request, county_error)
    if not destination_ids or any(db.get_destination(value) is None for value in destination_ids):
        return _channel_form_error(request, "Select at least one valid destination.")
    allowed_events = {event for values in _EVENT_GROUPS.values() for event in values}
    events = list(dict.fromkeys(str(value) for value in form.getlist("events")))
    if any(event not in allowed_events for event in events):
        return _channel_form_error(request, "One or more selected event types is invalid.")
    all_warnings = bool(form.get("all_warnings"))
    if not all_warnings and not events:
        return _channel_form_error(request, "Select all warnings or at least one event type.")
    try:
        db.create_routing_rule(
            name, priority, bool(form.get("enabled")), all_warnings,
            counties, events, destination_ids,
        )
    except sqlite3.IntegrityError as exc:
        if "routing_rules.name" in str(exc) or "idx_routes_name_unique" in str(exc):
            return _channel_form_error(request, "A rule with that name already exists.", 409)
        return _channel_form_error(request, "The rule contains an invalid or duplicate reference.", 409)
    except Exception:
        return _channel_form_error(request, "The routing rule could not be saved.", 500)
    return RedirectResponse("/routing", status_code=303)


@router.post("/routing/rules/{rule_id}", response_class=HTMLResponse)
async def update_routing_rule(request: Request, rule_id: int):
    form = await request.form()
    db = _db(request)
    name = str(form.get("name", "")).strip()
    try:
        priority = int(str(form.get("priority", "")))
        destination_ids = list(dict.fromkeys(
            int(str(value)) for value in form.getlist("destination_ids")
        ))
    except ValueError:
        return _channel_form_error(request, "Priority and destination references must be whole numbers.")
    counties, county_error = _parse_route_counties(str(form.get("counties", "")))
    if not name or len(name) > 80:
        return _channel_form_error(request, "Rule name is required and must be 80 characters or fewer.")
    if not -10000 <= priority <= 10000:
        return _channel_form_error(request, "Rule priority must be between -10000 and 10000.")
    if county_error:
        return _channel_form_error(request, county_error)
    if not destination_ids or any(db.get_destination(value) is None for value in destination_ids):
        return _channel_form_error(request, "Select at least one valid destination.")
    allowed_events = {event for values in _EVENT_GROUPS.values() for event in values}
    events = list(dict.fromkeys(str(value) for value in form.getlist("events")))
    if any(event not in allowed_events for event in events):
        return _channel_form_error(request, "One or more selected event types is invalid.")
    all_warnings = bool(form.get("all_warnings"))
    if not all_warnings and not events:
        return _channel_form_error(request, "Select all warnings or at least one event type.")
    try:
        updated = db.update_routing_rule(
            rule_id, name, priority, bool(form.get("enabled")), all_warnings,
            counties, events, destination_ids,
        )
    except sqlite3.IntegrityError as exc:
        if "routing_rules.name" in str(exc) or "idx_routes_name_unique" in str(exc):
            return _channel_form_error(request, "A rule with that name already exists.", 409)
        return _channel_form_error(request, "The rule contains an invalid or duplicate reference.", 409)
    except Exception:
        return _channel_form_error(request, "The routing rule could not be saved.", 500)
    if not updated:
        raise HTTPException(status_code=404, detail="Routing rule not found")
    return RedirectResponse("/routing", status_code=303)


@router.post("/routing/rules/{rule_id}/toggle", response_class=HTMLResponse)
async def toggle_routing_rule(request: Request, rule_id: int):
    if not _db(request).toggle_route(rule_id):
        raise HTTPException(status_code=404, detail="Routing rule not found")
    return RedirectResponse("/routing", status_code=303)


@router.post("/routing/rules/{rule_id}/move/{direction}", response_class=HTMLResponse)
async def move_routing_rule(request: Request, rule_id: int, direction: str):
    if direction not in {"up", "down"}:
        raise HTTPException(status_code=400, detail="Invalid routing direction")
    if not _db(request).move_route(rule_id, direction):
        raise HTTPException(status_code=404, detail="Routing rule not found")
    return RedirectResponse("/routing", status_code=303)


@router.post("/routing/rules/{rule_id}/delete", response_class=HTMLResponse)
async def delete_routing_rule(request: Request, rule_id: int):
    db = _db(request)
    if db.get_route(rule_id) is None:
        raise HTTPException(status_code=404, detail="Routing rule not found")
    form = await request.form()
    expected = "DELETE RULE %d" % rule_id
    if str(form.get("confirmation", "")) != expected:
        return _channel_form_error(request, "Type %s to confirm deletion." % expected)
    if not db.delete_route(rule_id):
        raise HTTPException(status_code=404, detail="Routing rule not found")
    return RedirectResponse("/routing", status_code=303)

