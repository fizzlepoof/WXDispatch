"""Password-protected, read-only guest alert map service."""
from __future__ import annotations

import base64
import os
from pathlib import Path

import httpx
from fastapi.testclient import TestClient
import pytest

from app.guest import create_guest_app


def guest_auth(password: str = "guest-password-long-enough") -> dict[str, str]:
    value = base64.b64encode(f"guest:{password}".encode()).decode()
    return {"Authorization": f"Basic {value}"}


@pytest.fixture
def password_file(tmp_path: Path) -> Path:
    path = tmp_path / "guest-password"
    path.write_text("guest-password-long-enough\n")
    path.chmod(0o600)
    return path


def test_guest_service_fails_closed_without_private_password_file(monkeypatch):
    monkeypatch.delenv("MESH_WX_GUEST_PASSWORD_FILE", raising=False)
    client = TestClient(create_guest_app())

    response = client.get("/")

    assert response.status_code == 503
    assert "password file is not configured" in response.text


def test_guest_service_rejects_missing_or_wrong_credentials(monkeypatch, password_file):
    monkeypatch.setenv("MESH_WX_GUEST_PASSWORD_FILE", str(password_file))
    client = TestClient(create_guest_app())

    missing = client.get("/")
    wrong = client.get("/", headers=guest_auth("wrong-password-long-enough"))

    assert missing.status_code == 401
    assert missing.headers["www-authenticate"] == 'Basic realm="WXDispatch guest view"'
    assert missing.headers["cache-control"] == "no-store"
    assert missing.headers["x-robots-tag"] == "noindex, nofollow"
    assert wrong.status_code == 401
    assert "guest-password-long-enough" not in wrong.text


def test_guest_service_rejects_symlink_or_overpermissive_password_file(
    monkeypatch, tmp_path: Path, password_file: Path,
):
    link = tmp_path / "guest-link"
    link.symlink_to(password_file)
    monkeypatch.setenv("MESH_WX_GUEST_PASSWORD_FILE", str(link))
    assert TestClient(create_guest_app()).get("/", headers=guest_auth()).status_code == 503

    password_file.chmod(0o640)
    monkeypatch.setenv("MESH_WX_GUEST_PASSWORD_FILE", str(password_file))
    assert TestClient(create_guest_app()).get("/", headers=guest_auth()).status_code == 503


def test_authenticated_guest_page_is_map_only(monkeypatch, password_file):
    monkeypatch.setenv("MESH_WX_GUEST_PASSWORD_FILE", str(password_file))
    client = TestClient(create_guest_app())

    response = client.get("/", headers=guest_auth())

    assert response.status_code == 200
    assert "WXDispatch Guest View" in response.text
    assert "Current regional alerts" in response.text
    assert 'fetch("/api/map-data"' in response.text
    for private_label in (
        "Dashboard", "NOAA History", "IPAWS History", "Transmit Log", "Routing",
        "Settings", "Manual Send", "Troubleshoot",
    ):
        assert private_label not in response.text
    assert "csrf-token" not in response.text


def test_guest_api_proxies_only_fixed_loopback_map_endpoint(
    monkeypatch, password_file, respx_mock,
):
    monkeypatch.setenv("MESH_WX_GUEST_PASSWORD_FILE", str(password_file))
    payload = {
        "counties": [], "county_count": 0, "alerts": [],
        "watched_alert_count": 0, "regional_alert_count": 0,
        "scope_areas": [], "last_poll_success": "", "poll_result": "ok",
        "stale": False, "zone_errors": [], "alert_errors": [], "error": "",
    }
    upstream = respx_mock.get("http://127.0.0.1:8110/api/map-data").mock(
        return_value=httpx.Response(200, json=payload)
    )
    client = TestClient(create_guest_app())

    response = client.get("/api/map-data", headers=guest_auth())

    assert response.status_code == 200
    assert response.json() == payload
    assert response.headers["cache-control"] == "no-store"
    assert upstream.called
    request = upstream.calls[0].request
    assert "authorization" not in request.headers
    assert request.url == "http://127.0.0.1:8110/api/map-data"


def test_guest_api_fails_closed_when_internal_map_is_unavailable(
    monkeypatch, password_file, respx_mock,
):
    monkeypatch.setenv("MESH_WX_GUEST_PASSWORD_FILE", str(password_file))
    respx_mock.get("http://127.0.0.1:8110/api/map-data").mock(
        side_effect=httpx.ConnectError("offline")
    )
    client = TestClient(create_guest_app())

    response = client.get("/api/map-data", headers=guest_auth())

    assert response.status_code == 503
    assert response.json() == {"detail": "Guest alert data is temporarily unavailable."}


def test_guest_service_documentation_keeps_password_out_of_service_metadata():
    readme = (Path(__file__).parents[1] / "README.md").read_text()
    normalized = " ".join(readme.split())

    assert "WXDispatch guest view" in normalized
    assert "mesh-wx-guest.service" in readme
    assert "/etc/mesh-wx/guest-password" in readme
    assert "username `guest`" in normalized
    assert "separate backend credential" in normalized
    assert "Pangolin shared password" in normalized
    assert "HTTPS" in readme
    assert "must not expose port `8110`" in normalized


def test_guest_service_has_no_write_admin_or_schema_routes(monkeypatch, password_file):
    monkeypatch.setenv("MESH_WX_GUEST_PASSWORD_FILE", str(password_file))
    app = create_guest_app()
    paths = {route.path for route in app.routes}
    methods = {method for route in app.routes for method in getattr(route, "methods", set())}

    assert paths == {"/", "/api/map-data", "/healthz"}
    assert methods <= {"GET", "HEAD"}
    client = TestClient(app)
    assert client.post("/api/map-data", headers=guest_auth()).status_code == 405
    assert client.get("/settings", headers=guest_auth()).status_code == 404
