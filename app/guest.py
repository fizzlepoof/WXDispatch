"""Password-protected, read-only WXDispatch guest service."""
from __future__ import annotations

import asyncio
import hmac
import json
import os
import sqlite3
import stat
from pathlib import Path
from typing import Annotated
from urllib.parse import quote

import httpx
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from . import __version__
from .web.routes import DISP_LABELS, TEMPLATES

_GUEST_AUTH = HTTPBasic(auto_error=False)
_PASSWORD_ENV = "MESH_WX_GUEST_PASSWORD_FILE"
_DATABASE_ENV = "MESH_WX_GUEST_DB"
_INTERNAL_MAP_URL = "http://127.0.0.1:8110/api/map-data"
_MAX_PASSWORD_FILE_BYTES = 1024
_MAX_MAP_RESPONSE_BYTES = 5 * 1024 * 1024
_HISTORY_LIMIT = 200
_DASHBOARD_HISTORY_LIMIT = 6
_DASHBOARD_MAP_TIMEOUT_SECONDS = 2.0


def _load_guest_password() -> str:
    raw_path = os.environ.get(_PASSWORD_ENV, "")
    if not raw_path:
        raise HTTPException(
            status_code=503, detail="Guest password file is not configured."
        )
    try:
        path = Path(raw_path)
        if not path.is_absolute():
            raise ValueError("path must be absolute")
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode):
                raise ValueError("password file must be regular")
            if stat.S_IMODE(info.st_mode) != 0o600:
                raise ValueError("password file permissions must be 0600")
            if info.st_uid != os.geteuid():
                raise ValueError("password file must be owned by the service user")
            if not 1 <= info.st_size <= _MAX_PASSWORD_FILE_BYTES:
                raise ValueError("invalid password file size")
            raw_password = os.read(descriptor, _MAX_PASSWORD_FILE_BYTES + 1)
        finally:
            os.close(descriptor)
        password = raw_password.decode("utf-8").rstrip("\r\n")
        if "\n" in password or "\r" in password or not 16 <= len(password) <= 256:
            raise ValueError("invalid password")
        return password
    except (OSError, UnicodeError, ValueError):
        raise HTTPException(
            status_code=503, detail="Guest password file is unavailable or insecure."
        ) from None


def _require_guest(
    credentials: Annotated[HTTPBasicCredentials | None, Depends(_GUEST_AUTH)],
) -> None:
    password = _load_guest_password()
    supplied_username = credentials.username if credentials else ""
    supplied_password = credentials.password if credentials else ""
    username_ok = hmac.compare_digest(supplied_username.encode(), b"guest")
    password_ok = hmac.compare_digest(supplied_password.encode(), password.encode())
    if not (username_ok and password_ok):
        raise HTTPException(
            status_code=401,
            detail="Guest authentication required.",
            headers={"WWW-Authenticate": 'Basic realm="WXDispatch guest view"'},
        )


def _secure(response):
    response.headers["Cache-Control"] = "no-store"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-Robots-Tag"] = "noindex, nofollow"
    return response


def _guest_database_path() -> Path:
    configured = os.environ.get(_DATABASE_ENV, "").strip()
    path = Path(configured) if configured else Path.cwd() / "data" / "mesh-wx.db"
    try:
        path = path.resolve(strict=True)
        if not path.is_file():
            raise ValueError("database is not a regular file")
    except (OSError, ValueError):
        raise HTTPException(
            status_code=503, detail="Guest history is temporarily unavailable."
        ) from None
    return path


def _read_guest_history(*, noaa_limit: int, ipaws_limit: int) -> dict:
    path = _guest_database_path()
    connection = None
    try:
        uri = f"file:{quote(str(path), safe='/')}?mode=ro"
        connection = sqlite3.connect(uri, uri=True, timeout=2.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        timezone_row = connection.execute(
            "SELECT value FROM settings WHERE key = 'display_timezone' LIMIT 1"
        ).fetchone()
        try:
            timezone_value = json.loads(timezone_row["value"]) if timezone_row else ""
            timezone = timezone_value if isinstance(timezone_value, str) else ""
        except (TypeError, ValueError):
            timezone = ""
        noaa = [dict(row) for row in connection.execute(
            """SELECT ts, event, area, transmitted_text
               FROM history ORDER BY id DESC LIMIT ?""",
            (max(0, min(noaa_limit, _HISTORY_LIMIT)),),
        ).fetchall()]
        ipaws = [dict(row) for row in connection.execute(
            """SELECT ts, event, area, headline, msg_type, status, sent, text
               FROM ipaws_log ORDER BY id DESC LIMIT ?""",
            (max(0, min(ipaws_limit, _HISTORY_LIMIT)),),
        ).fetchall()]
        return {"tz": timezone, "noaa_rows": noaa, "ipaws_rows": ipaws}
    except sqlite3.Error:
        raise HTTPException(
            status_code=503, detail="Guest history is temporarily unavailable."
        ) from None
    finally:
        if connection is not None:
            connection.close()


def _safe_text(value, limit: int = 4096) -> str:
    return str(value or "")[:limit]


def _safe_text_list(value, *, limit: int = 512) -> list[str]:
    if not isinstance(value, list):
        return []
    return [_safe_text(item, 128) for item in value[:limit]]


def _safe_geometry(value) -> dict | None:
    if not isinstance(value, dict):
        return None
    geometry_type = value.get("type")
    coordinates = value.get("coordinates")
    if geometry_type not in {"Polygon", "MultiPolygon"} or not isinstance(coordinates, list):
        return None
    return {"type": geometry_type, "coordinates": coordinates}


def _guest_map_payload(payload: dict) -> dict:
    counties = []
    for raw in payload.get("counties", [])[:512] if isinstance(payload.get("counties"), list) else []:
        if not isinstance(raw, dict) or not isinstance(raw.get("properties"), dict):
            continue
        geometry = _safe_geometry(raw.get("geometry"))
        if geometry is None:
            continue
        properties = raw["properties"]
        counties.append({
            "type": "Feature",
            "properties": {
                "code": _safe_text(properties.get("code"), 16),
                "name": _safe_text(properties.get("name"), 160),
                "watched": bool(properties.get("watched")),
            },
            "geometry": geometry,
        })

    alerts = []
    for raw in payload.get("alerts", [])[:512] if isinstance(payload.get("alerts"), list) else []:
        if not isinstance(raw, dict):
            continue
        alert = {
            "event": _safe_text(raw.get("event"), 256),
            "area": _safe_text(raw.get("area"), 4096),
            "headline": _safe_text(raw.get("headline"), 2048),
            "severity": _safe_text(raw.get("severity"), 32),
            "ends": _safe_text(raw.get("ends"), 128),
            "expires": _safe_text(raw.get("expires"), 128),
            "watched": bool(raw.get("watched")),
            "local_counties": _safe_text_list(raw.get("local_counties")),
            "local_zones": _safe_text_list(raw.get("local_zones")),
            "affected_zones": _safe_text_list(raw.get("affected_zones")),
        }
        geometry = _safe_geometry(raw.get("geometry"))
        if geometry is not None:
            alert["geometry"] = geometry
        alerts.append(alert)

    watched_count = sum(1 for alert in alerts if alert["watched"])
    zone_error_count = min(
        len(payload.get("zone_errors", [])) if isinstance(payload.get("zone_errors"), list) else 0,
        512,
    )
    alert_error_count = min(
        len(payload.get("alert_errors", [])) if isinstance(payload.get("alert_errors"), list) else 0,
        512,
    )
    return {
        "counties": counties,
        "county_count": sum(1 for county in counties if county["properties"]["watched"]),
        "alerts": alerts,
        "watched_alert_count": watched_count,
        "regional_alert_count": len(alerts) - watched_count,
        "last_poll_success": _safe_text(payload.get("last_poll_success"), 128),
        "stale": bool(payload.get("stale")),
        "zone_errors": [True] * zone_error_count,
        "alert_errors": [True] * alert_error_count,
        "error": (
            "Current NWS alert data is unavailable for the configured counties."
            if payload.get("error") else ""
        ),
    }


async def _fetch_map_payload(timeout: float = 20.0) -> dict:
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(timeout), follow_redirects=False, trust_env=False
        ) as client:
            upstream = await client.get(
                _INTERNAL_MAP_URL, headers={"Accept": "application/json"}
            )
        if upstream.status_code != 200 or len(upstream.content) > _MAX_MAP_RESPONSE_BYTES:
            raise ValueError("invalid upstream response")
        payload = upstream.json()
        if not isinstance(payload, dict):
            raise ValueError("invalid upstream payload")
        return _guest_map_payload(payload)
    except (httpx.HTTPError, ValueError):
        raise HTTPException(
            status_code=503,
            detail="Guest alert data is temporarily unavailable.",
        ) from None


def _render(request: Request, template: str, **context):
    response = TEMPLATES.TemplateResponse(
        request,
        template,
        {"version": __version__, "disp_label": DISP_LABELS, **context},
    )
    return _secure(response)


def create_guest_app() -> FastAPI:
    app = FastAPI(
        title="WXDispatch Guest View",
        version=__version__,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    @app.middleware("http")
    async def guest_security_headers(request: Request, call_next):
        return _secure(await call_next(request))

    protected = [Depends(_require_guest)]

    @app.get("/healthz", response_class=PlainTextResponse)
    async def healthz():
        return PlainTextResponse("ok")

    @app.get("/", response_class=HTMLResponse, dependencies=protected)
    async def guest_dashboard(request: Request):
        context = _read_guest_history(
            noaa_limit=_DASHBOARD_HISTORY_LIMIT,
            ipaws_limit=_DASHBOARD_HISTORY_LIMIT,
        )
        try:
            payload = await asyncio.wait_for(
                _fetch_map_payload(_DASHBOARD_MAP_TIMEOUT_SECONDS),
                timeout=_DASHBOARD_MAP_TIMEOUT_SECONDS + 0.1,
            )
            alerts = payload.get("alerts", [])
            if not isinstance(alerts, list):
                raise ValueError("invalid alert list")
            active_alerts = [
                dict(alert) for alert in alerts
                if isinstance(alert, dict) and bool(alert.get("watched"))
            ]
            alerts_unavailable = bool(payload.get("stale")) and not active_alerts
        except (HTTPException, asyncio.TimeoutError, ValueError):
            active_alerts = []
            alerts_unavailable = True
        return _render(
            request,
            "guest_dashboard.html",
            active_alerts=active_alerts,
            alerts_unavailable=alerts_unavailable,
            **context,
        )

    @app.get("/map", response_class=HTMLResponse, dependencies=protected)
    async def guest_map(request: Request):
        return _render(
            request,
            "map.html",
            guest_view=True,
            map_data_url="/api/map-data",
        )

    @app.get("/history", response_class=HTMLResponse, dependencies=protected)
    async def guest_noaa_history(request: Request):
        context = _read_guest_history(noaa_limit=_HISTORY_LIMIT, ipaws_limit=0)
        return _render(request, "guest_history.html", **context)

    @app.get("/ipaws", response_class=HTMLResponse, dependencies=protected)
    async def guest_ipaws_history(request: Request):
        context = _read_guest_history(noaa_limit=0, ipaws_limit=_HISTORY_LIMIT)
        return _render(request, "guest_ipaws_history.html", **context)

    @app.get(
        "/api/map-data",
        response_class=JSONResponse,
        dependencies=protected,
    )
    async def guest_map_data():
        return _secure(JSONResponse(await _fetch_map_payload()))

    return app


app = create_guest_app()


def main() -> None:
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8111, log_config=None)


if __name__ == "__main__":
    main()
