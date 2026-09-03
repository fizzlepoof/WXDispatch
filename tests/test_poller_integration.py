"""End-to-end poller: feed captured NWS JSON through poll_once against a real
(in-memory) database and assert what gets recorded across polls.

Policy: EVERY distinct alert pulled from NOAA is logged to history exactly once
(deduped by nws_id -- we re-poll every cycle), recording its disposition and the
reason, whether or not it was broadcast. Re-polls of the same id add no new row."""
import pytest

from app.db import Database
from app.poller import WxPoller


class FakeTx:
    def __init__(self):
        self.sent = []

    def enqueue(self, text, channel):
        self.sent.append((text, channel))
        return True

    def enqueue_destination(self, text, transport, channel, destination_id,
                            on_result=None):
        self.sent.append((text, channel))
        if on_result is not None:
            on_result(True)
        return True


def _future(feat: dict) -> dict:
    props = feat["properties"]
    for key in ("effective", "expires", "ends", "onset"):
        if props.get(key):
            props[key] = props[key].replace("2024-", "2099-")
    return feat


def _collection(features):
    return {"features": [_future(f) for f in features]}


class FakeNWS:
    queue = []

    def __init__(self, *a, **k):
        pass

    async def fetch_active(self, zones, max_retries=4):
        data = FakeNWS.queue.pop(0)
        return data, "{}"


def _dispositions(db, nws_suffix):
    rows = db.query_history(limit=500)
    return [r["disposition"] for r in rows if nws_suffix in (r["nws_id"] or "")]


@pytest.fixture
def wired(monkeypatch, feature):
    import app.poller as poller_mod

    monkeypatch.setattr(poller_mod, "NWSClient", FakeNWS)
    db = Database(":memory:")
    _add_fixture_route(db)
    poller = WxPoller(db, FakeTx())
    return db, poller, feature


def _add_fixture_route(db):
    destination_id = db.create_destination("fixture channel", "meshtastic", 0)
    rule_id = db.create_route("fixture counties", 10, True)
    db.replace_route_counties(rule_id, [
        ("SCZ050", "Charleston"),
        ("SCZ040", "Richland"),
    ])
    db.replace_route_events(rule_id, ["Tornado Watch"], all_warnings=True)
    db.replace_route_destinations(rule_id, [destination_id])
    return destination_id


async def test_logs_all_alerts_once_dedupe_update_cancel(wired):
    db, poller, feature = wired
    ffw = feature("flash_flood_warning")

    # Poll 1: mix of includable + droppable alerts (dry-run ON by default).
    FakeNWS.queue = [_collection([
        feature("tornado_warning"),
        feature("tornado_watch"),
        feature("severe_tstorm_watch"),
        feature("lake_wind_advisory"),
        ffw,
        feature("special_weather_statement"),
    ])]
    await poller.poll_once()

    rows = db.query_history(limit=500)
    disp = [r["disposition"] for r in rows]
    assert disp.count("dry_run") == 3     # 2 tornado + flash flood warning
    assert disp.count("filtered") == 3    # tstorm watch, lake wind, SWS -- LOGGED with reason
    # every filtered row records WHY it didn't go out
    assert all(r["detail"] for r in rows if r["disposition"] == "filtered")

    # Poll 2: identical feed -> deduped by id. No new rows, no "duplicate" rows.
    n_before = len(db.query_history(limit=500))
    FakeNWS.queue = [_collection([feature("tornado_warning"), ffw])]
    await poller.poll_once()
    assert len(db.query_history(limit=500)) == n_before          # nothing re-logged
    assert _dispositions(db, "ffw400") == ["dry_run"]            # still one row
    assert "duplicate" not in [r["disposition"] for r in db.query_history(limit=500)]

    # Poll 3: official Update is simulated, not claimed as delivered.
    FakeNWS.queue = [_collection([feature("flash_flood_warning_update")])]
    await poller.poll_once()
    assert _dispositions(db, "ffw401") == ["dry_run"]

    # Poll 4: cancellation was never actually delivered in dry-run mode.
    FakeNWS.queue = [_collection([feature("flash_flood_warning_cancel")])]
    await poller.poll_once()
    assert _dispositions(db, "ffw402") == ["filtered"]

    assert poller._tx.sent == []  # dry-run: nothing actually transmitted


async def test_active_alert_logged_once_across_polls(wired):
    """An alert that stays active across many polls is logged exactly once
    (deduped by id), not once per poll -- here a filtered SWS under default rules."""
    db, poller, feature = wired
    sws = feature("special_weather_statement")
    FakeNWS.queue = [_collection([sws]), _collection([sws]), _collection([sws])]
    await poller.poll_once()
    await poller.poll_once()
    await poller.poll_once()
    filtered = [r for r in db.query_history(limit=500) if r["disposition"] == "filtered"]
    assert len(filtered) == 1
    assert filtered[0]["detail"]  # reason recorded


class ResultTx:
    """Simulates a broadcast whose real outcome (verified sent / failed on all
    radios) is reported back through on_result -- the feedback the poller needs
    so it does not mark a failed alert as delivered."""
    def __init__(self, ok):
        self.ok = ok
        self.calls = []

    def enqueue_destination(self, text, transport, channel, destination_id,
                            on_result=None):
        self.calls.append(text)
        if on_result is not None:
            on_result(self.ok)
        return True


async def test_failed_broadcast_retries_and_alarms(monkeypatch, feature):
    """A warning that fails on every radio must NOT be deduped (so it retries)
    and must raise an alarm -- never silently marked delivered."""
    import app.poller as poller_mod
    monkeypatch.setattr(poller_mod, "NWSClient", FakeNWS)
    db = Database(":memory:")
    db.set_setting("dry_run", False)
    _add_fixture_route(db)
    tx = ResultTx(ok=False)
    poller = WxPoller(db, tx)
    ffw = feature("flash_flood_warning")

    FakeNWS.queue = [_collection([ffw])]
    await poller.poll_once()
    assert len(tx.calls) == 1                      # it went to the radios
    assert poller.status.last_broadcast_failure is not None  # alarm state set
    levels = [e["level"] for e in db.recent_events(50)]
    assert "ALARM" in levels                       # loud, visible alarm raised

    FakeNWS.queue = [_collection([ffw])]
    await poller.poll_once()
    assert len(tx.calls) == 2, "a failed alert must be retried, not deduped away"


async def test_successful_broadcast_is_deduped(monkeypatch, feature):
    """A verified send is recorded so the same alert is not rebroadcast every poll."""
    import app.poller as poller_mod
    monkeypatch.setattr(poller_mod, "NWSClient", FakeNWS)
    db = Database(":memory:")
    db.set_setting("dry_run", False)
    _add_fixture_route(db)
    tx = ResultTx(ok=True)
    poller = WxPoller(db, tx)
    ffw = feature("flash_flood_warning")

    FakeNWS.queue = [_collection([ffw])]
    await poller.poll_once()
    FakeNWS.queue = [_collection([ffw])]
    await poller.poll_once()
    assert len(tx.calls) == 1, "a verified send must be deduped, not resent every poll"
    assert poller.status.last_broadcast_failure is None


async def test_poll_scope_unions_legacy_and_enabled_route_zones(monkeypatch):
    import app.poller as poller_mod

    requested = []

    class ScopeNWS:
        def __init__(self, *args, **kwargs):
            pass

        async def fetch_active(self, zones, max_retries=4):
            requested.append(zones)
            return {"features": []}, "{}"

    monkeypatch.setattr(poller_mod, "NWSClient", ScopeNWS)
    db = Database(":memory:")
    db.set_setting("zones", "SCZ050, TNC147")
    destination_id = db.create_destination("manual county", "meshcore", 4)
    enabled = db.create_route("enabled route", 10, True)
    db.replace_route_counties(enabled, [
        ("TNC147", "Robertson"),
        ("KYC201", "Robertson KY"),
    ])
    db.replace_route_destinations(enabled, [destination_id])
    disabled = db.create_route("disabled route", 20, False)
    db.replace_route_counties(disabled, [("WIC111", "Sauk")])
    db.replace_route_destinations(disabled, [destination_id])

    await WxPoller(db, FakeTx()).poll_once()

    assert requested == ["SCZ050,TNC147,KYC201"]


async def test_routed_poll_replays_multibyte_payload_at_195_byte_boundary(monkeypatch):
    import app.poller as poller_mod

    event = "é" * 200 + " Warning"
    feature = {
        "id": "utf8-alert",
        "properties": {
            "event": event,
            "headline": "multibyte replay",
            "areaDesc": "Robertson",
            "messageType": "Alert",
            "effective": "",
            "expires": "2099-01-01T00:00:00Z",
            "ends": "",
            "geocode": {"UGC": ["TNC147"]},
        },
    }

    class ReplayNWS:
        def __init__(self, *args, **kwargs):
            pass

        async def fetch_active(self, zones, max_retries=4):
            return {"features": [feature]}, "captured-noaa-json"

    monkeypatch.setattr(poller_mod, "NWSClient", ReplayNWS)
    db = Database(":memory:")
    db.set_setting("dry_run", False)
    destination_id = db.create_destination("utf8 route", "meshcore", 5)
    rule_id = db.create_route("utf8 replay", 10, True)
    db.replace_route_counties(rule_id, [("TNC147", "Robertson")])
    db.replace_route_events(rule_id, [], all_warnings=True)
    db.replace_route_destinations(rule_id, [destination_id])
    tx = FakeTx()

    await WxPoller(db, tx).poll_once()

    assert len(tx.sent) == 1
    payload, channel = tx.sent[0]
    assert channel == 5
    assert len(payload.encode("utf-8")) == 195
    assert payload.encode("utf-8").decode("utf-8") == payload
    attempt = db.query_delivery_attempts(alert_id="utf8-alert")[0]
    assert attempt["message_text"] == payload and attempt["state"] == "accepted"
