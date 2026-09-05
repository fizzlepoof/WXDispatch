"""Minimal password-protected, read-only WXDispatch guest service."""
from __future__ import annotations

import hmac
import os
import stat
from pathlib import Path
from typing import Annotated

import httpx
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from . import __version__
from .web.routes import TEMPLATES

_GUEST_AUTH = HTTPBasic(auto_error=False)
_PASSWORD_ENV = "MESH_WX_GUEST_PASSWORD_FILE"
_INTERNAL_MAP_URL = "http://127.0.0.1:8110/api/map-data"
_MAX_PASSWORD_FILE_BYTES = 1024
_MAX_MAP_RESPONSE_BYTES = 5 * 1024 * 1024


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

    @app.get("/healthz", response_class=PlainTextResponse)
    async def healthz():
        return PlainTextResponse("ok")

    @app.get("/", response_class=HTMLResponse, dependencies=[Depends(_require_guest)])
    async def guest_map(request: Request):
        response = TEMPLATES.TemplateResponse(
            request,
            "map.html",
            {
                "guest_view": True,
                "map_data_url": "/api/map-data",
                "version": __version__,
            },
        )
        return _secure(response)

    @app.get("/api/map-data", response_class=JSONResponse,
             dependencies=[Depends(_require_guest)])
    async def guest_map_data():
        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(20.0), follow_redirects=False, trust_env=False
            ) as client:
                upstream = await client.get(
                    _INTERNAL_MAP_URL, headers={"Accept": "application/json"}
                )
            if upstream.status_code != 200 or len(upstream.content) > _MAX_MAP_RESPONSE_BYTES:
                raise ValueError("invalid upstream response")
            payload = upstream.json()
            if not isinstance(payload, dict):
                raise ValueError("invalid upstream payload")
        except (httpx.HTTPError, ValueError):
            raise HTTPException(
                status_code=503,
                detail="Guest alert data is temporarily unavailable.",
            ) from None
        return _secure(JSONResponse(payload))

    return app


app = create_guest_app()


def main() -> None:
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8111, log_config=None)


if __name__ == "__main__":
    main()
