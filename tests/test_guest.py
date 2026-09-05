"""Password-protected, read-only guest alert map service."""
from __future__ import annotations

import base64
import os
import sqlite3
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


@pytest.fixture
def guest_db(tmp_path: Path) -> Path:
    path = tmp_path / "guest.db"
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE history (
          id INTEGER PRIMARY KEY, ts TEXT NOT NULL, nws_id TEXT, event TEXT,
          area TEXT, disposition TEXT, transmitted_text TEXT, detail TEXT
        );
        CREATE TABLE ipaws_log (
          id INTEGER PRIMARY KEY, ts TEXT NOT NULL, identifier TEXT, sender TEXT,
          event TEXT, area TEXT, headline TEXT, msg_type TEXT, status TEXT,
          sent TEXT, text TEXT, transmitted INTEGER, error TEXT
        );
        INSERT INTO settings VALUES ('display_timezone', '"America/Chicago"');
        INSERT INTO history VALUES (
          1, '2026-09-05T20:00:00+00:00', 'nws-private-id',
          'Tornado Warning', 'Montgomery County', 'accepted',
          'Public NOAA message', 'radio Alpha /dev/ttyUSB0: permission denied'
        );
        INSERT INTO ipaws_log VALUES (
          1, '2026-09-05T20:01:00+00:00', 'ipaws-private-id',
          'sender@example.gov', 'AMBER Alert', 'Middle Tennessee', 'Headline',
          'Alert', 'Actual', '2026-09-05T20:00:00+00:00',
          'Public IPAWS message', 1, 'internal-error-must-not-leak'
        );
        """
    )
    connection.commit()
    connection.close()
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


def test_authenticated_guest_navigation_exposes_only_requested_read_only_pages(
    monkeypatch, password_file, guest_db,
):
    monkeypatch.setenv("MESH_WX_GUEST_PASSWORD_FILE", str(password_file))
    monkeypatch.setenv("MESH_WX_GUEST_DB", str(guest_db))
    client = TestClient(create_guest_app())

    for path, heading in (
        ("/", "Dashboard"),
        ("/map", "Local Alert Map"),
        ("/history", "Received NOAA Alerts"),
        ("/ipaws", "IPAWS Alerts"),
    ):
        response = client.get(path, headers=guest_auth())
        assert response.status_code == 200
        assert heading in response.text
        assert "Dashboard" in response.text
        assert "Local Alert Map" in response.text
        assert "NOAA History" in response.text
        assert "IPAWS History" in response.text
        assert "csrf-token" not in response.text
        for private_label in (
            "Transmit Log", "Routing", "Settings", "Manual Send", "Troubleshoot",
            "Pause", "Go live", "Resend",
        ):
            assert private_label not in response.text

    map_page = client.get("/map", headers=guest_auth())
    assert "Current regional alerts" in map_page.text
    assert 'fetch("/api/map-data"' in map_page.text


def test_guest_histories_are_bounded_sanitized_and_database_is_read_only(
    monkeypatch, password_file, guest_db,
):
    monkeypatch.setenv("MESH_WX_GUEST_PASSWORD_FILE", str(password_file))
    monkeypatch.setenv("MESH_WX_GUEST_DB", str(guest_db))
    before = guest_db.read_bytes()
    client = TestClient(create_guest_app())

    noaa = client.get("/history", headers=guest_auth())
    ipaws = client.get("/ipaws", headers=guest_auth())
    dashboard = client.get("/", headers=guest_auth())

    assert "Tornado Warning" in noaa.text
    assert "Public NOAA message" in noaa.text
    assert "Sep 5, 3:00 PM" in noaa.text
    assert 'class="status">accepted' not in noaa.text
    assert "nws-private-id" not in noaa.text
    assert "/dev/ttyUSB0" not in noaa.text
    assert "permission denied" not in noaa.text
    assert "AMBER Alert" in ipaws.text
    assert "Public IPAWS message" in ipaws.text
    assert "&middot; sent" not in ipaws.text
    assert "ipaws-private-id" not in ipaws.text
    assert "sender@example.gov" not in ipaws.text
    assert "internal-error-must-not-leak" not in ipaws.text
    assert "Recent NOAA alerts" in dashboard.text
    assert "Recent IPAWS alerts" in dashboard.text
    assert guest_db.read_bytes() == before


def test_guest_history_pages_fail_closed_when_database_is_unavailable(
    monkeypatch, password_file, tmp_path,
):
    monkeypatch.setenv("MESH_WX_GUEST_PASSWORD_FILE", str(password_file))
    monkeypatch.setenv("MESH_WX_GUEST_DB", str(tmp_path / "missing.db"))
    client = TestClient(create_guest_app())

    for path in ("/", "/history", "/ipaws"):
        response = client.get(path, headers=guest_auth())
        assert response.status_code == 503
        assert "Guest history is temporarily unavailable" in response.text
        assert str(tmp_path) not in response.text


def test_guest_api_proxies_only_fixed_loopback_map_endpoint(
    monkeypatch, password_file, respx_mock,
):
    monkeypatch.setenv("MESH_WX_GUEST_PASSWORD_FILE", str(password_file))
    payload = {
        "counties": [{
            "type": "Feature",
            "properties": {
                "code": "TNC125", "name": "Montgomery", "watched": True,
                "internal": "must-not-leak",
            },
            "geometry": {"type": "Polygon", "coordinates": []},
            "id": "county-private-id",
        }],
        "county_count": 1,
        "alerts": [{
            "id": "alert-private-id", "event": "Tornado Warning",
            "area": "Montgomery County", "headline": "Take shelter now",
            "severity": "Extreme", "ends": "2026-09-05T21:00:00+00:00",
            "expires": "2026-09-05T21:30:00+00:00", "watched": True,
            "local_counties": ["Montgomery County"], "local_zones": ["TNC125"],
            "affected_zones": ["TNC125"],
            "geometry": {"type": "Polygon", "coordinates": []},
            "internal": "must-not-leak",
        }],
        "watched_alert_count": 1, "regional_alert_count": 0,
        "scope_areas": ["TN"], "last_poll_success": "2026-09-05T20:00:00+00:00",
        "poll_result": "internal poll result", "stale": False,
        "zone_errors": ["TNC999"], "alert_errors": ["https://private.example/id"],
        "error": "", "unknown": "must-not-leak",
    }
    upstream = respx_mock.get("http://127.0.0.1:8110/api/map-data").mock(
        return_value=httpx.Response(200, json=payload)
    )
    client = TestClient(create_guest_app())

    response = client.get("/api/map-data", headers=guest_auth())

    assert response.status_code == 200
    safe = response.json()
    assert set(safe) == {
        "counties", "county_count", "alerts", "watched_alert_count",
        "regional_alert_count", "last_poll_success", "stale",
        "zone_errors", "alert_errors", "error",
    }
    assert safe["county_count"] == 1
    assert safe["counties"][0]["properties"] == {
        "code": "TNC125", "name": "Montgomery", "watched": True,
    }
    assert "id" not in safe["counties"][0]
    assert safe["alerts"][0]["event"] == "Tornado Warning"
    assert "id" not in safe["alerts"][0]
    assert "internal" not in safe["alerts"][0]
    assert safe["zone_errors"] == [True]
    assert safe["alert_errors"] == [True]
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
    assert "Pangolin header authentication" in normalized
    assert "extended compatibility mode" in normalized
    assert "HTTPS" in readme
    assert "must not expose port `8110`" in normalized


def test_guest_service_has_no_write_admin_or_schema_routes(monkeypatch, password_file):
    monkeypatch.setenv("MESH_WX_GUEST_PASSWORD_FILE", str(password_file))
    app = create_guest_app()
    paths = {route.path for route in app.routes}
    methods = {method for route in app.routes for method in getattr(route, "methods", set())}

    assert paths == {"/", "/map", "/history", "/ipaws", "/api/map-data", "/healthz"}
    assert methods <= {"GET", "HEAD"}
    client = TestClient(app)
    assert client.post("/api/map-data", headers=guest_auth()).status_code == 405
    assert client.get("/settings", headers=guest_auth()).status_code == 404
    assert client.get("/transmit-log", headers=guest_auth()).status_code == 404
    assert client.get("/routing", headers=guest_auth()).status_code == 404
