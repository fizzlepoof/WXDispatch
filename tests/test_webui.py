"""Server-rendered routing UI, channel guards, and CSRF coverage."""
from __future__ import annotations

import base64
from pathlib import Path
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from app.db import Database
from app.web.routes import router


class DummyTx:
    last_error = ""
    port = ""
    connected = False
    queue_depth = 0

    def __init__(self):
        self.channel_calls = []
        self.transports = []

    async def send_manual(self, text):
        return True

    async def send_test(self, text):
        return True

    async def send_to(self, name, text):
        return True, ""

    async def inspect_meshcore_channel(self, index):
        self.channel_calls.append(("inspect", index))
        return {"ok": True, "error": "", "channel": {"index": index, "name": "County WX"}}

    async def set_meshcore_channel(self, index, name):
        self.channel_calls.append(("set", index, name))
        return {"ok": True, "error": "", "channel": {"index": index, "name": name}}

    async def clear_meshcore_channel(self, index):
        self.channel_calls.append(("clear", index))
        return {"ok": True, "error": "", "channel": {"index": index, "name": ""}}

    def status(self):
        return self.transports


class DummyPoller:
    def __init__(self):
        self.status = SimpleNamespace(
            last_poll_time="",
            last_poll_result="ok",
            last_poll_success_time="",
            uptime_seconds=60,
            last_broadcast_failure="",
            last_broadcast_failure_text="",
            clock_skew_seconds=0,
            last_raw_response="",
        )

    def poke(self):
        pass


@pytest.fixture
def web(tmp_path, monkeypatch):
    monkeypatch.setenv("MESHWX_ADMIN_PASSWORD", "correct horse battery staple")
    db = Database(str(tmp_path / "web.db"))
    app = FastAPI()
    app.state.db = db
    app.state.tx = DummyTx()
    app.state.poller = DummyPoller()
    app.include_router(router)
    with TestClient(app) as client:
        yield client, db, app.state.tx
    db.close()


def csrf(client: TestClient) -> str:
    response = client.get("/manual")
    assert response.status_code == 200
    token = client.cookies.get("mesh_wx_csrf")
    assert token
    assert 'name="csrf_token"' in response.text
    return token


def admin_auth(password: str = "correct horse battery staple") -> dict[str, str]:
    value = base64.b64encode(f"admin:{password}".encode()).decode()
    return {"Authorization": f"Basic {value}"}


def test_readme_prominently_documents_routing_upgrade_auth_and_acceptance_limits():
    readme = (Path(__file__).parents[1] / "README.md").read_text()
    normalized = " ".join(readme.split())

    assert "Upgrades intentionally create no automatic destinations or routing rules" in normalized
    assert "create and enable a destination and routing rule before disabling dry-run" in normalized
    assert "MESHWX_ADMIN_PASSWORD" in readme
    assert "accepted by MeshCore companion" in normalized
    assert "accepted by Meshtastic node" in normalized
    assert "logged **sent** only when the radio really keyed up" not in normalized


def test_state_changes_require_matching_csrf_token(web):
    client, _db, _tx = web

    rejected = client.post("/manual/send", data={"text": "hello"})
    assert rejected.status_code == 403
    assert "CSRF validation failed" in rejected.text

    token = csrf(client)
    accepted = client.post(
        "/manual/send", data={"text": "hello", "csrf_token": token}
    )
    assert accepted.status_code == 200
    assert "accepted by configured radio interfaces" in accepted.text
    assert "over-air delivery is not confirmed" in accepted.text


@pytest.mark.parametrize("action", ["create", "update", "toggle", "delete"])
def test_every_destination_post_requires_csrf(web, action):
    client, db, _tx = web
    destination_id = db.create_destination("Existing", "meshcore", 1, False)
    path = "/routing/destinations"
    data = {"name": "Changed", "transport": "meshtastic", "channel": "2"}
    if action == "update":
        path += f"/{destination_id}"
    elif action in {"toggle", "delete"}:
        path += f"/{destination_id}/{action}"
        data = {}

    response = client.post(path, data=data)

    assert response.status_code == 403
    row = db.get_destination(destination_id)
    assert (row["name"], row["transport"], row["channel"], row["enabled"]) == (
        "Existing", "meshcore", 1, 0,
    )
    assert db_count(db, "destinations") == 1


@pytest.mark.parametrize(
    "mutation",
    [
        "destination_create", "destination_update", "destination_toggle", "destination_delete",
        "rule_create", "rule_update", "rule_toggle", "rule_move", "rule_delete",
        "channel_set", "channel_clear",
    ],
)
def test_every_routing_and_channel_mutation_rejects_tampered_csrf_without_state_change(
    web, mutation,
):
    client, db, tx = web
    destination_id = db.create_destination("Existing", "meshcore", 9, True)
    rule_id = db.create_routing_rule(
        "Existing rule", 10, True, True,
        [("TNC147", "Robertson County")], [], [destination_id],
    )
    other_rule_id = db.create_routing_rule(
        "Other rule", 20, True, True,
        [("TNC159", "Smith County")], [], [destination_id],
    )
    csrf(client)
    data: dict[str, object] = {"csrf_token": "tampered"}
    headers: dict[str, str] = {}
    paths = {
        "destination_create": "/routing/destinations",
        "destination_update": f"/routing/destinations/{destination_id}",
        "destination_toggle": f"/routing/destinations/{destination_id}/toggle",
        "destination_delete": f"/routing/destinations/{destination_id}/delete",
        "rule_create": "/routing/rules",
        "rule_update": f"/routing/rules/{rule_id}",
        "rule_toggle": f"/routing/rules/{rule_id}/toggle",
        "rule_move": f"/routing/rules/{other_rule_id}/move/up",
        "rule_delete": f"/routing/rules/{rule_id}/delete",
        "channel_set": "/openhop/channels/set",
        "channel_clear": "/openhop/channels/clear",
    }
    if mutation.startswith("destination_"):
        data.update(name="Changed", transport="meshtastic", channel="2")
        if mutation == "destination_delete":
            data["confirmation"] = f"DELETE DESTINATION {destination_id}"
    elif mutation.startswith("rule_"):
        data.update(
            name="Changed rule", priority="5", enabled="on", all_warnings="on",
            counties="TNC147 | Robertson County", destination_ids=str(destination_id),
        )
        if mutation == "rule_delete":
            data["confirmation"] = f"DELETE RULE {rule_id}"
    else:
        headers = admin_auth()
        data.update(
            index="9", name="Changed channel",
            secret="00112233445566778899aabbccddeeff",
            confirmation="CLEAR CHANNEL 9",
        )
    before = "\n".join(db._conn.iterdump())

    response = client.post(paths[mutation], data=data, headers=headers)

    assert response.status_code == 403
    assert "CSRF validation failed" in response.text
    assert "\n".join(db._conn.iterdump()) == before
    assert tx.channel_calls == []


def test_openhop_channel_set_is_hash_only_and_companion_worded(web):
    client, db, tx = web
    token = csrf(client)

    response = client.post(
        "/openhop/channels/set",
        headers=admin_auth(),
        data={
            "csrf_token": token,
            "index": "17",
            "name": "#robertson-wx",
        },
    )

    assert response.status_code == 200
    assert tx.channel_calls == [("set", 17, "#robertson-wx")]
    assert "accepted by MeshCore companion" in response.text
    assert "over-air delivery is not confirmed" in response.text
    assert "RF delivered" not in response.text
    assert "write-only secret" not in response.text.lower()
    assert "32 hex" not in response.text.lower()


def test_openhop_channel_set_rejects_non_hash_name(web):
    client, _db, tx = web
    token = csrf(client)

    response = client.post(
        "/openhop/channels/set",
        headers=admin_auth(),
        data={"csrf_token": token, "index": "17", "name": "robertson-wx"},
    )

    assert response.status_code == 400
    assert "must begin with #" in response.text
    assert tx.channel_calls == []


def test_openhop_channel_set_rejects_bare_hash_name(web):
    client, _db, tx = web
    token = csrf(client)

    response = client.post(
        "/openhop/channels/set",
        headers=admin_auth(),
        data={"csrf_token": token, "index": "17", "name": "#"},
    )

    assert response.status_code == 400
    assert "must include a name" in response.text
    assert tx.channel_calls == []


def test_openhop_channel_set_rejects_overlength_utf8_name(web):
    client, _db, tx = web
    token = csrf(client)

    response = client.post(
        "/openhop/channels/set",
        headers=admin_auth(),
        data={"csrf_token": token, "index": "17", "name": "#" + "é" * 16},
    )

    assert response.status_code == 400
    assert "32 UTF-8 bytes" in response.text
    assert tx.channel_calls == []


@pytest.mark.parametrize("path", ["/openhop/channels/set", "/openhop/channels/clear"])
def test_channel_mutations_require_admin_basic_auth(web, path):
    client, _db, tx = web
    token = csrf(client)
    data = {
        "csrf_token": token,
        "index": "9",
        "name": "County WX",
        "secret": "00112233445566778899aabbccddeeff",
        "confirmation": "CLEAR CHANNEL 9",
    }

    unauthenticated = client.post(path, data=data)
    wrong = client.post(path, data=data, headers=admin_auth("wrong password"))

    assert unauthenticated.status_code == 401
    assert unauthenticated.headers["www-authenticate"] == 'Basic realm="WXDispatch channel administration"'
    assert wrong.status_code == 401
    assert tx.channel_calls == []


@pytest.mark.parametrize("path", ["/openhop/channels/set", "/openhop/channels/clear"])
def test_channel_mutations_fail_closed_when_admin_password_is_unset(web, monkeypatch, path):
    client, _db, tx = web
    monkeypatch.delenv("MESHWX_ADMIN_PASSWORD")
    token = csrf(client)

    response = client.post(
        path,
        headers=admin_auth(),
        data={
            "csrf_token": token,
            "index": "9",
            "name": "County WX",
            "secret": "00112233445566778899aabbccddeeff",
            "confirmation": "CLEAR CHANNEL 9",
        },
    )

    assert response.status_code == 503
    assert "Channel administration is disabled until MESHWX_ADMIN_PASSWORD is configured" in response.text
    assert tx.channel_calls == []


def test_routing_page_reports_disabled_channel_administration_without_exposing_password(
    web, monkeypatch,
):
    client, _db, _tx = web
    password = "never-render-this-password"
    monkeypatch.delenv("MESHWX_ADMIN_PASSWORD")

    disabled = client.get("/routing")
    monkeypatch.setenv("MESHWX_ADMIN_PASSWORD", password)
    enabled = client.get("/routing")

    assert "Channel administration is disabled until MESHWX_ADMIN_PASSWORD is configured" in disabled.text
    assert password not in disabled.text
    assert password not in enabled.text
    assert "Channel administration enabled" in enabled.text


def test_dashboard_broadcast_counts_include_routed_accepted_and_partial_outcomes(web):
    client, db, _tx = web
    for index, disposition in enumerate(
        ["accepted", "partial", "cleared", "sent", "update", "queued", "failed", "dry_run"]
    ):
        db.add_history(f"alert-{index}", "Tornado Warning", "Robertson County", disposition)

    response = client.get("/")

    assert response.status_code == 200
    assert "Accepted &middot; 7d</div><div class=\"v\">5</div>" in response.text
    assert "Accepted today</div><div class=\"v\">5</div>" in response.text


def test_history_filter_lists_every_routed_and_legacy_status(web):
    client, _db, _tx = web

    response = client.get("/history")

    assert response.status_code == 200
    for status in (
        "no_route", "queued", "accepted", "partial", "failed", "dry_run", "cleared",
        "cancelled", "sent", "update",
    ):
        assert f'<option value="{status}"' in response.text


@pytest.mark.parametrize("path", ["/", "/settings", "/routing"])
def test_zero_routes_show_critical_automated_delivery_inactive_banner(web, path):
    client, db, _tx = web

    response = client.get(path)

    assert response.status_code == 200
    assert "Automated delivery is inactive" in response.text
    assert "Create and enable a destination and routing rule" in response.text
    assert db.list_destinations() == []
    assert db.list_routes() == []


def test_go_live_is_blocked_without_enabled_route_and_destination(web):
    client, db, _tx = web
    token = csrf(client)

    response = client.post("/dry-run/toggle", data={"csrf_token": token})

    assert response.status_code == 409
    assert "Cannot go LIVE" in response.text
    assert "Automated delivery is inactive" in response.text
    assert db.get_setting("dry_run", True) is True


def test_go_live_succeeds_with_enabled_route_and_destination(web):
    client, db, _tx = web
    destination_id = db.create_destination("Group", "meshcore", 17, True)
    db.create_routing_rule(
        "County warnings", 10, True, True,
        [("TNC147", "Robertson County")], [], [destination_id],
    )
    token = csrf(client)

    response = client.post("/dry-run/toggle", data={"csrf_token": token})

    assert response.status_code == 200
    assert "Broadcasting LIVE" in response.text
    assert db.get_setting("dry_run", True) is False


@pytest.mark.parametrize(
    ("name", "label", "expected"),
    [
        ("meshcore", "MeshCore", "accepted by MeshCore companion"),
        ("meshtastic", "Meshtastic", "accepted by Meshtastic node"),
    ],
)
def test_per_radio_test_response_reports_acceptance_not_rf_delivery(web, name, label, expected):
    client, _db, tx = web
    tx.transports = [{"name": name, "label": label, "enabled": True}]
    token = csrf(client)

    response = client.post(
        f"/troubleshoot/test/{name}", data={"csrf_token": token},
    )

    assert response.status_code == 200
    assert expected in response.text
    assert "over-air delivery is not confirmed" in response.text
    for false_claim in ("test sent", "transmitted", "keyed", "RF-delivered"):
        assert false_claim not in response.text


def test_all_radio_test_response_reports_acceptance_not_delivery(web):
    client, _db, _tx = web
    token = csrf(client)

    response = client.post("/troubleshoot/test", data={"csrf_token": token})

    assert response.status_code == 200
    assert "accepted by configured radio interfaces" in response.text
    assert "over-air delivery is not confirmed" in response.text
    assert "test sent" not in response.text


def test_openhop_channel_inspect_returns_only_safe_metadata(web):
    client, _db, tx = web
    token = csrf(client)

    response = client.post(
        "/openhop/channels/inspect", data={"csrf_token": token, "index": "9"}
    )

    assert response.status_code == 200
    assert tx.channel_calls == [("inspect", 9)]
    assert "Slot 9" in response.text
    assert "County WX" in response.text
    assert "secret" not in response.text.lower()


def test_openhop_clear_requires_exact_confirmation_before_reference_check(web):
    client, _db, tx = web
    token = csrf(client)

    response = client.post(
        "/openhop/channels/clear",
        headers=admin_auth(),
        data={"csrf_token": token, "index": "9", "confirmation": "clear"},
    )

    assert response.status_code == 400
    assert "Type CLEAR CHANNEL 9" in response.text
    assert tx.channel_calls == []


def test_openhop_clear_blocks_disabled_current_destination_reference(web):
    client, db, tx = web
    db.create_destination("Disabled group", "meshcore", 9, False)
    token = csrf(client)

    response = client.post(
        "/openhop/channels/clear",
        headers=admin_auth(),
        data={"csrf_token": token, "index": "9", "confirmation": "CLEAR CHANNEL 9"},
    )

    assert response.status_code == 409
    assert "Disabled group" in response.text
    assert "uses this channel" in response.text
    assert tx.channel_calls == []


def test_openhop_clear_blocks_active_immutable_delivery_snapshot(web):
    client, db, tx = web
    destination_id = db.create_destination("Moved group", "meshcore", 9, True)
    db.upsert_delivery_state(
        "root-alert", "last-alert", destination_id, "meshcore", 9,
        ["Robertson County"], "Tornado Warning", "Warning",
        "2099-01-01T00:00:00+00:00", "hash", "sent",
    )
    db.update_destination(destination_id, "Moved group", "meshcore", 10, True)
    token = csrf(client)

    response = client.post(
        "/openhop/channels/clear",
        headers=admin_auth(),
        data={"csrf_token": token, "index": "9", "confirmation": "CLEAR CHANNEL 9"},
    )

    assert response.status_code == 409
    assert "active delivery snapshot" in response.text
    assert tx.channel_calls == []


def test_openhop_clear_fails_closed_without_complete_reference_api(web):
    client, db, tx = web
    db.channel_references = None
    token = csrf(client)

    response = client.post(
        "/openhop/channels/clear",
        headers=admin_auth(),
        data={"csrf_token": token, "index": "9", "confirmation": "CLEAR CHANNEL 9"},
    )

    assert response.status_code == 409
    assert "cannot be safely checked" in response.text
    assert tx.channel_calls == []


def test_routing_page_has_empty_states_and_hash_only_channel_form(web):
    client, _db, _tx = web

    response = client.get("/routing")

    assert response.status_code == 200
    assert "No destinations configured" in response.text
    assert "No routing rules configured" in response.text
    assert 'name="secret"' not in response.text
    assert "WXDispatch creates hash channels only" in response.text
    assert "Anyone who knows the exact channel name can derive its key" in response.text
    assert 'placeholder="#robertson-wx"' in response.text
    assert db_count(_db, "routing_rules") == 0
    assert db_count(_db, "destinations") == 0


def test_routing_page_lists_reusable_destinations(web):
    client, db, _tx = web
    destination_id = db.create_destination("Robertson OpenHop", "meshcore", 17, False)

    response = client.get("/routing")

    assert response.status_code == 200
    assert "Robertson OpenHop" in response.text
    assert "meshcore" in response.text
    assert "disabled" in response.text
    assert f'/routing/destinations/{destination_id}/edit' in response.text
    assert f'/routing/destinations/{destination_id}/toggle' in response.text
    assert f'/routing/destinations/{destination_id}/delete' in response.text
    assert [row["id"] for row in db.list_destinations()] == [destination_id]


def db_count(db: Database, table: str) -> int:
    return int(db._conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0])


def test_destination_create_validates_and_never_creates_rule(web):
    client, db, _tx = web
    token = csrf(client)

    response = client.post(
        "/routing/destinations",
        data={
            "csrf_token": token,
            "name": "Robertson OpenHop",
            "transport": "meshcore",
            "channel": "17",
            "enabled": "on",
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert db_count(db, "destinations") == 1
    assert db_count(db, "routing_rules") == 0
    row = db._conn.execute("SELECT * FROM destinations").fetchone()
    assert dict(row) == {
        "id": row["id"],
        "name": "Robertson OpenHop",
        "transport": "meshcore",
        "channel": 17,
        "enabled": 1,
    }


@pytest.mark.parametrize(
    ("name", "transport", "channel", "message"),
    [
        ("   ", "meshcore", "1", "Destination name is required"),
        ("x" * 81, "meshcore", "1", "80 characters or fewer"),
        ("Storm group", "email", "1", "supported destination transport"),
        ("Storm group", "meshcore", "-1", "between 0 and 255"),
        ("Storm group", "meshcore", "256", "between 0 and 255"),
        ("Storm group", "meshcore", "not-a-number", "between 0 and 255"),
    ],
)
def test_destination_create_rejects_invalid_fields(web, name, transport, channel, message):
    client, db, _tx = web
    token = csrf(client)

    response = client.post(
        "/routing/destinations",
        data={
            "csrf_token": token,
            "name": name,
            "transport": transport,
            "channel": channel,
        },
    )

    assert response.status_code == 400
    assert message in response.text
    assert db.list_destinations() == []
    assert db_count(db, "routing_rules") == 0


def test_destination_names_may_repeat_when_endpoints_differ(web):
    client, db, _tx = web
    db.create_destination("Storm group", "meshcore", 1)
    token = csrf(client)

    response = client.post(
        "/routing/destinations",
        data={
            "csrf_token": token,
            "name": "Storm group",
            "transport": "meshcore",
            "channel": "2",
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert [row["name"] for row in db.list_destinations()] == ["Storm group", "Storm group"]


def test_destination_create_rejects_duplicate_transport_channel_endpoint(web):
    client, db, _tx = web
    existing_id = db.create_destination("Existing", "meshcore", 1)
    token = csrf(client)

    response = client.post(
        "/routing/destinations",
        data={
            "csrf_token": token,
            "name": "Duplicate endpoint",
            "transport": "meshcore",
            "channel": "1",
        },
    )

    assert response.status_code == 409
    assert "transport and channel already has a destination" in response.text
    assert [row["id"] for row in db.list_destinations()] == [existing_id]
    assert db_count(db, "routing_rules") == 0
    assert not db._conn.in_transaction


def test_destination_edit_form_loads_existing_values(web):
    client, db, _tx = web
    destination_id = db.create_destination("Robertson OpenHop", "meshcore", 17, False)

    response = client.get(f"/routing/destinations/{destination_id}/edit")

    assert response.status_code == 200
    assert 'value="Robertson OpenHop"' in response.text
    assert '<option value="meshcore" selected>' in response.text
    assert 'value="17"' in response.text
    assert 'name="enabled"' in response.text
    assert 'name="enabled" type="checkbox" checked' not in response.text
    assert 'name="csrf_token"' in response.text


def test_destination_update_changes_all_editable_fields_without_creating_rule(web):
    client, db, _tx = web
    destination_id = db.create_destination("Old name", "meshcore", 17, False)
    token = csrf(client)

    response = client.post(
        f"/routing/destinations/{destination_id}",
        data={
            "csrf_token": token,
            "name": "Storm group",
            "transport": "meshtastic",
            "channel": "3",
            "enabled": "on",
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert dict(db.get_destination(destination_id)) == {
        "id": destination_id,
        "name": "Storm group",
        "transport": "meshtastic",
        "channel": 3,
        "enabled": 1,
    }
    assert db_count(db, "routing_rules") == 0


def test_destination_update_rejects_duplicate_endpoint_and_preserves_record(web):
    client, db, _tx = web
    db.create_destination("Existing", "meshcore", 1)
    destination_id = db.create_destination("Edited", "meshtastic", 2, False)
    token = csrf(client)

    response = client.post(
        f"/routing/destinations/{destination_id}",
        data={
            "csrf_token": token,
            "name": "Conflicting",
            "transport": "meshcore",
            "channel": "1",
            "enabled": "on",
        },
    )

    assert response.status_code == 409
    assert "transport and channel already has a destination" in response.text
    row = db.get_destination(destination_id)
    assert (row["name"], row["transport"], row["channel"], row["enabled"]) == (
        "Edited", "meshtastic", 2, 0,
    )
    assert not db._conn.in_transaction


def test_destination_update_rejects_invalid_fields_and_preserves_record(web):
    client, db, _tx = web
    destination_id = db.create_destination("Existing", "meshcore", 1, True)
    token = csrf(client)

    response = client.post(
        f"/routing/destinations/{destination_id}",
        data={
            "csrf_token": token,
            "name": "Changed",
            "transport": "unsupported",
            "channel": "2000",
        },
    )

    assert response.status_code == 400
    assert "supported destination transport" in response.text
    row = db.get_destination(destination_id)
    assert (row["name"], row["transport"], row["channel"], row["enabled"]) == (
        "Existing", "meshcore", 1, 1,
    )


def test_destination_toggle_flips_enabled_without_creating_rule(web):
    client, db, _tx = web
    destination_id = db.create_destination("Storm group", "meshcore", 17, False)
    token = csrf(client)

    response = client.post(
        f"/routing/destinations/{destination_id}/toggle",
        data={"csrf_token": token},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert db.get_destination(destination_id)["enabled"] == 1
    assert db_count(db, "routing_rules") == 0


def test_destination_delete_removes_unreferenced_destination(web):
    client, db, _tx = web
    destination_id = db.create_destination("Storm group", "meshcore", 17)
    token = csrf(client)

    response = client.post(
        f"/routing/destinations/{destination_id}/delete",
        data={"csrf_token": token, "confirmation": f"DELETE DESTINATION {destination_id}"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert db.get_destination(destination_id) is None
    assert db_count(db, "routing_rules") == 0


def test_destination_delete_is_blocked_when_routing_rule_references_it(web):
    client, db, _tx = web
    destination_id = db.create_destination("Storm group", "meshcore", 17)
    rule_id = db.create_route("County warnings")
    db.replace_route_destinations(rule_id, [destination_id])
    token = csrf(client)

    response = client.post(
        f"/routing/destinations/{destination_id}/delete",
        data={"csrf_token": token, "confirmation": f"DELETE DESTINATION {destination_id}"},
    )

    assert response.status_code == 409
    assert "destination is in use" in response.text
    assert "FOREIGN KEY" not in response.text
    assert db.get_destination(destination_id) is not None
    assert db_count(db, "routing_rules") == 1
    assert not db._conn.in_transaction


@pytest.mark.parametrize("action", ["update", "toggle", "delete"])
def test_destination_mutations_reject_unknown_destination_ids(web, action):
    client, db, _tx = web
    token = csrf(client)
    path = "/routing/destinations/99999"
    data = {"csrf_token": token}
    if action == "update":
        data.update(name="Unknown", transport="meshcore", channel="1")
    else:
        path += f"/{action}"

    response = client.post(path, data=data)

    assert response.status_code == 404
    assert "Destination not found" in response.text
    assert db.list_destinations() == []


def test_destination_delete_requires_server_validated_confirmation(web):
    client, db, _tx = web
    destination_id = db.create_destination("Group", "meshcore", 17)
    page = client.get("/routing")
    token = client.cookies.get("mesh_wx_csrf")
    assert 'name="confirmation"' in page.text
    assert f"DELETE DESTINATION {destination_id}" in page.text

    missing = client.post(
        f"/routing/destinations/{destination_id}/delete", data={"csrf_token": token},
    )
    wrong = client.post(
        f"/routing/destinations/{destination_id}/delete",
        data={"csrf_token": token, "confirmation": "yes"},
    )

    assert missing.status_code == 400
    assert wrong.status_code == 400
    assert f"Type DELETE DESTINATION {destination_id}" in wrong.text
    assert db.get_destination(destination_id) is not None


def test_destination_edit_rejects_unknown_destination_id(web):
    client, db, _tx = web

    response = client.get("/routing/destinations/99999/edit")

    assert response.status_code == 404
    assert "Destination not found" in response.text
    assert db.list_destinations() == []


def test_manual_rule_create_round_trip_keeps_county_names_events_and_destinations(web):
    client, db, _tx = web
    destination_id = db.create_destination("Robertson OpenHop", "meshcore", 17)
    token = csrf(client)

    response = client.post(
        "/routing/rules",
        data={
            "csrf_token": token,
            "name": "Robertson severe weather",
            "priority": "20",
            "enabled": "on",
            "all_warnings": "on",
            "events": ["Tornado Watch"],
            "counties": "TNC147 | Robertson County\nTNC159 | Smith County",
            "destination_ids": [str(destination_id)],
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    rule = db._conn.execute("SELECT * FROM routing_rules").fetchone()
    assert (rule["name"], rule["priority"], rule["enabled"], rule["all_warnings"]) == (
        "Robertson severe weather", 20, 1, 1
    )
    assert [tuple(r) for r in db._conn.execute(
        "SELECT zone_code, county_name FROM route_counties ORDER BY zone_code"
    )] == [("TNC147", "Robertson County"), ("TNC159", "Smith County")]
    assert db._conn.execute("SELECT event FROM route_events").fetchone()[0] == "Tornado Watch"
    assert db._conn.execute("SELECT destination_id FROM route_destinations").fetchone()[0] == destination_id


def test_routing_page_lists_complete_rules_in_stable_priority_order(web):
    client, db, _tx = web
    first_destination = db.create_destination("OpenHop group", "meshcore", 17)
    second_destination = db.create_destination("Meshtastic group", "meshtastic", 3, False)
    later = db.create_route("Later all warnings", 40, False)
    db.replace_route_counties(later, [("TNC159", "Smith County")])
    db.replace_route_events(later, [], all_warnings=True)
    db.replace_route_destinations(later, [second_destination])
    earlier = db.create_route("Earlier tornado route", 10, True)
    db.replace_route_counties(
        earlier,
        [("TNC147", "Robertson County"), ("TNC021", "Cheatham County")],
    )
    db.replace_route_events(earlier, ["Tornado Warning", "Tornado Watch"])
    db.replace_route_destinations(earlier, [first_destination, second_destination])

    response = client.get("/routing")

    assert response.status_code == 200
    assert response.text.index("Earlier tornado route") < response.text.index("Later all warnings")
    for value in (
        "10", "enabled", "TNC147", "Robertson County", "TNC021",
        "Cheatham County", "Tornado Warning", "Tornado Watch", "OpenHop group",
        "Meshtastic group", "40", "disabled", "TNC159", "Smith County",
        "All warnings",
    ):
        assert value in response.text
    assert f'/routing/rules/{earlier}/edit' in response.text
    assert f'/routing/rules/{later}/edit' in response.text


def test_rule_create_rejects_duplicate_name_without_partial_rows(web):
    client, db, _tx = web
    destination_id = db.create_destination("OpenHop group", "meshcore", 17)
    existing_id = db.create_route("County warnings", 10)
    db.replace_route_counties(existing_id, [("TNC147", "Robertson County")])
    db.replace_route_events(existing_id, [], all_warnings=True)
    db.replace_route_destinations(existing_id, [destination_id])
    token = csrf(client)

    response = client.post(
        "/routing/rules",
        data={
            "csrf_token": token,
            "name": "County warnings",
            "priority": "20",
            "all_warnings": "on",
            "counties": "TNC159 | Smith County",
            "destination_ids": str(destination_id),
        },
    )

    assert response.status_code == 409
    assert "name already exists" in response.text
    assert [rule["id"] for rule in db.list_routes()] == [existing_id]
    assert db_count(db, "route_counties") == 1
    assert db_count(db, "route_destinations") == 1
    assert not db._conn.in_transaction


def test_rule_edit_form_is_prefilled_with_all_associations(web):
    client, db, _tx = web
    selected_destination = db.create_destination("Selected group", "meshcore", 17)
    other_destination = db.create_destination("Other group", "meshtastic", 3)
    rule_id = db.create_route("County warnings", 25, False)
    db.replace_route_counties(
        rule_id,
        [("TNC147", "Robertson County"), ("TNC159", "Smith County")],
    )
    db.replace_route_events(rule_id, ["Tornado Warning"])
    db.replace_route_destinations(rule_id, [selected_destination])

    response = client.get(f"/routing/rules/{rule_id}/edit")

    assert response.status_code == 200
    assert 'value="County warnings"' in response.text
    assert 'value="25"' in response.text
    assert "TNC147 | Robertson County" in response.text
    assert "TNC159 | Smith County" in response.text
    assert f'name="destination_ids" value="{selected_destination}" checked' in response.text
    assert f'name="destination_ids" value="{other_destination}" checked' not in response.text
    assert 'name="events" value="Tornado Warning" checked' in response.text
    assert 'name="all_warnings"' in response.text
    assert 'name="all_warnings" type="checkbox" checked' not in response.text
    assert 'name="enabled" type="checkbox" checked' not in response.text
    assert 'name="csrf_token"' in response.text


def test_rule_update_changes_every_field_and_deduplicates_associations(web):
    client, db, _tx = web
    first_destination = db.create_destination("First group", "meshcore", 17)
    second_destination = db.create_destination("Second group", "meshtastic", 3)
    rule_id = db.create_route("Old rule", 25, False)
    db.replace_route_counties(rule_id, [("TNC147", "Robertson County")])
    db.replace_route_events(rule_id, ["Tornado Warning"])
    db.replace_route_destinations(rule_id, [first_destination])
    token = csrf(client)

    response = client.post(
        f"/routing/rules/{rule_id}",
        data={
            "csrf_token": token,
            "name": "Updated route",
            "priority": "7",
            "enabled": "on",
            "all_warnings": "on",
            "events": ["Heat Advisory", "Heat Advisory"],
            "counties": (
                "TNC159 | Smith County\nTNC021 | Cheatham County\n"
                "TNC159 | Smith County"
            ),
            "destination_ids": [
                str(second_destination), str(second_destination), str(first_destination),
            ],
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    rule = db.get_route(rule_id)
    assert rule is not None
    assert (rule["name"], rule["priority"], rule["enabled"], rule["all_warnings"]) == (
        "Updated route", 7, True, True,
    )
    assert [(county["zone_code"], county["county_name"]) for county in rule["counties"]] == [
        ("TNC021", "Cheatham County"), ("TNC159", "Smith County"),
    ]
    assert rule["events"] == ["Heat Advisory"]
    assert [destination["id"] for destination in rule["destinations"]] == [
        first_destination, second_destination,
    ]
    assert not db._conn.in_transaction


def test_rule_toggle_is_exposed_and_flips_only_enabled_state(web):
    client, db, _tx = web
    destination_id = db.create_destination("Group", "meshcore", 17)
    rule_id = db.create_routing_rule(
        "County warnings", 25, False, True,
        [("TNC147", "Robertson County")], [], [destination_id],
    )
    before = db.get_route(rule_id)

    page = client.get("/routing")
    assert f'/routing/rules/{rule_id}/toggle' in page.text
    token = client.cookies.get("mesh_wx_csrf")
    response = client.post(
        f"/routing/rules/{rule_id}/toggle",
        data={"csrf_token": token},
        follow_redirects=False,
    )

    assert response.status_code == 303
    after = db.get_route(rule_id)
    assert before is not None and after is not None
    assert after["enabled"] is True
    assert {key: value for key, value in after.items() if key != "enabled"} == {
        key: value for key, value in before.items() if key != "enabled"
    }


def test_rule_up_down_controls_persist_deterministic_stable_order(web):
    client, db, _tx = web
    destination_id = db.create_destination("Group", "meshcore", 17)
    first = db.create_routing_rule(
        "First", 50, True, True, [("TNC001", "One")], [], [destination_id],
    )
    second = db.create_routing_rule(
        "Second", 50, True, True, [("TNC003", "Two")], [], [destination_id],
    )
    third = db.create_routing_rule(
        "Third", 90, True, True, [("TNC005", "Three")], [], [destination_id],
    )

    page = client.get("/routing")
    token = client.cookies.get("mesh_wx_csrf")
    assert f'/routing/rules/{third}/move/up' in page.text
    assert f'/routing/rules/{first}/move/down' in page.text

    response = client.post(
        f"/routing/rules/{third}/move/up",
        data={"csrf_token": token},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert [(rule["id"], rule["priority"]) for rule in db.list_routes()] == [
        (first, 10), (third, 20), (second, 30),
    ]
    reopened = Database(db.path)
    try:
        assert [rule["id"] for rule in reopened.list_routes()] == [first, third, second]
    finally:
        reopened.close()


def test_rule_delete_cascades_associations_but_preserves_destination_and_snapshot(web):
    client, db, _tx = web
    destination_id = db.create_destination("Group", "meshcore", 17)
    rule_id = db.create_routing_rule(
        "County warnings", 25, True, False,
        [("TNC147", "Robertson County")], ["Tornado Warning"], [destination_id],
    )
    db.upsert_delivery_state(
        "root-alert", "last-alert", destination_id, "meshcore", 17,
        ["Robertson County"], "Tornado Warning", "Warning", "2099-01-01T00:00:00+00:00",
        "hash", "sent",
    )

    page = client.get("/routing")
    token = client.cookies.get("mesh_wx_csrf")
    assert f'/routing/rules/{rule_id}/delete' in page.text
    response = client.post(
        f"/routing/rules/{rule_id}/delete",
        data={"csrf_token": token, "confirmation": f"DELETE RULE {rule_id}"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert db.get_route(rule_id) is None
    assert db_count(db, "route_counties") == 0
    assert db_count(db, "route_events") == 0
    assert db_count(db, "route_destinations") == 0
    assert db.get_destination(destination_id) is not None
    snapshot = db.get_delivery_state("root-alert", destination_id)
    assert snapshot is not None
    assert snapshot["last_alert_id"] == "last-alert"
    assert not db._conn.in_transaction


def test_rule_delete_requires_server_validated_confirmation(web):
    client, db, _tx = web
    destination_id = db.create_destination("Group", "meshcore", 17)
    rule_id = db.create_routing_rule(
        "County warnings", 25, True, True,
        [("TNC147", "Robertson County")], [], [destination_id],
    )
    page = client.get("/routing")
    token = client.cookies.get("mesh_wx_csrf")
    assert f"DELETE RULE {rule_id}" in page.text

    response = client.post(
        f"/routing/rules/{rule_id}/delete",
        data={"csrf_token": token, "confirmation": "delete"},
    )

    assert response.status_code == 400
    assert f"Type DELETE RULE {rule_id}" in response.text
    assert db.get_route(rule_id) is not None
