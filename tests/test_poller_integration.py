"""End-to-end poller: feed captured NWS JSON through poll_once against a real
(in-memory) database and assert what gets recorded across polls.

Policy: EVERY distinct alert pulled from NOAA is logged to history exactly once
(deduped by nws_id -- we re-poll every cycle), recording its disposition and the
reason, whether or not it was broadcast. Re-polls of the same id add no new row."""
import copy
import json

import pytest

from app.arbitration import ArbitrationOutcome, WeatherArbiter
from app.db import Database
from app.filters import FilterRules
from app.poller import WxPoller, _vtec_identity
from app.transmit import TransmitManager


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


def _identity_feature(**timestamps) -> dict:
    return {
        "properties": {
            **timestamps,
            "parameters": {
                "VTEC": [
                    "/O.NEW.KCHS.TO.W.0001.000000T0000Z-000000T0000Z/"
                ],
            },
        },
    }


def test_poller_vtec_identity_prefers_valid_sent_over_effective_year() -> None:
    identity = _vtec_identity(_identity_feature(
        sent="2026-01-01T00:01:00+00:00",
        effective="2025-12-31T23:59:00+00:00",
        onset="2024-12-31T23:59:00+00:00",
    ))

    assert identity == "urn:nws:vtec:2026:KCHS:TO:W:0001"


def test_poller_vtec_identity_falls_back_from_malformed_sent_to_effective() -> None:
    identity = _vtec_identity(_identity_feature(
        sent="not-a-timestamp",
        effective="2025-12-31T23:59:00+00:00",
        onset="2024-12-31T23:59:00+00:00",
    ))

    assert identity == "urn:nws:vtec:2025:KCHS:TO:W:0001"


def test_poller_vtec_identity_ignores_malformed_effective_when_sent_is_valid() -> None:
    identity = _vtec_identity(_identity_feature(
        sent="2026-01-01T00:01:00+00:00",
        effective="not-a-timestamp",
        onset="2024-12-31T23:59:00+00:00",
    ))

    assert identity == "urn:nws:vtec:2026:KCHS:TO:W:0001"


def test_poller_vtec_identity_falls_back_to_onset() -> None:
    identity = _vtec_identity(_identity_feature(
        sent="not-a-timestamp",
        effective="also-not-a-timestamp",
        onset="2024-12-31T23:59:00+00:00",
    ))

    assert identity == "urn:nws:vtec:2024:KCHS:TO:W:0001"


def test_poller_vtec_identity_rejects_all_invalid_timestamps() -> None:
    assert _vtec_identity(_identity_feature(
        sent="not-a-timestamp",
        effective="2026-01-01T00:01:00",
        onset=None,
    )) is None


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


class RecordingArbiter:
    def __init__(self, *, accepted=ArbitrationOutcome.HANDLED, followup=None):
        self.accepted = accepted
        self.followup = followup
        self.ingested = []
        self.followups = []
        self.delivered_followups = []
        self.released = 0

    async def ingest(self, feature, *, source):
        self.ingested.append((copy.deepcopy(feature), source))
        return self.accepted

    async def prepare_followup(self, feature):
        self.followups.append(copy.deepcopy(feature))
        return copy.deepcopy(self.followup)

    async def deliver_prepared_followup(self, feature):
        self.delivered_followups.append(copy.deepcopy(feature))
        return 1

    async def release_due(self):
        self.released += 1
        return 0


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


async def test_rest_poll_uses_arbiter_instead_of_immediate_processing(
    monkeypatch, feature,
):
    import app.poller as poller_mod
    monkeypatch.setattr(poller_mod, "NWSClient", FakeNWS)
    db = Database(":memory:")
    _add_fixture_route(db)
    arbiter = RecordingArbiter()
    poller = WxPoller(db, FakeTx(), arbiter=arbiter)
    warning = feature("tornado_warning")
    FakeNWS.queue = [_collection([warning])]

    await poller.poll_once()

    assert len(arbiter.ingested) == 1
    assert arbiter.ingested[0][1] == "rest"
    assert poller._tx.sent == []


@pytest.mark.parametrize("vtec", [
    ["not-vtec"],
    [
        "/O.NEW.KCHS.TO.W.0001.990520T2010Z-990520T2045Z/",
        "/O.NEW.KCHS.TO.W.0002.990520T2010Z-990520T2045Z/",
    ],
], ids=["malformed", "multiple"])
async def test_same_capable_invalid_rest_vtec_is_suppressed(monkeypatch, feature, vtec):
    import app.poller as poller_mod
    monkeypatch.setattr(poller_mod, "NWSClient", FakeNWS)
    db = Database(":memory:")
    db.set_setting("dry_run", False)
    _add_fixture_route(db)
    tx = FakeTx()
    poller = WxPoller(db, tx)
    arbiter = WeatherArbiter(db, poller.deliver_arbitrated)
    poller.set_arbiter(arbiter)
    warning = _future(feature("tornado_warning"))
    warning["properties"].setdefault("parameters", {})["VTEC"] = vtec
    FakeNWS.queue = [_collection([warning])]

    await poller.poll_once()

    assert tx.sent == []
    assert db.query_history(limit=10) == []


async def test_ambiguous_same_match_from_rest_poll_is_suppressed(monkeypatch, feature):
    import app.poller as poller_mod
    monkeypatch.setattr(poller_mod, "NWSClient", FakeNWS)
    db = Database(":memory:")
    db.set_setting("dry_run", False)
    _add_fixture_route(db)
    tx = FakeTx()
    poller = WxPoller(db, tx)
    arbiter = WeatherArbiter(db, poller.deliver_arbitrated)
    poller.set_arbiter(arbiter)
    warning = _future(feature("tornado_warning"))
    props = warning["properties"]
    props["status"] = "Actual"
    props["geocode"] = {"UGC": ["SCZ050"]}
    props["parameters"] = {
        "VTEC": ["/O.NEW.KCHS.TO.W.0001.990520T2010Z-990520T2045Z/"]
    }
    same = copy.deepcopy(warning)
    same["properties"]["parameters"] = {
        "SAMEOriginator": ["WXR"],
        "SAMEEventCode": ["TOR"],
        "SAMESender": ["KCHS/NWS"],
    }
    serialized = json.dumps(same)
    for canonical_id in ("same-a", "same-b"):
        db.create_arbitration_candidate(
            canonical_id=canonical_id,
            event="Tornado Warning",
            office="KCHS",
            issued_at=props["effective"],
            expires_at=props["expires"],
            deadline=props["expires"],
            same_feature=serialized,
        )
    FakeNWS.queue = [_collection([warning])]

    await poller.poll_once()

    assert tx.sent == []
    assert db.query_history(limit=10) == []


async def test_same_external_feature_uses_arbiter_and_nwws_initial_cannot_bypass_it():
    db = Database(":memory:")
    arbiter = RecordingArbiter()
    poller = WxPoller(db, FakeTx(), arbiter=arbiter)
    feature = {"id": "x", "properties": {"event": "Tornado Warning"}}

    assert await poller.ingest_feature(feature, source="noaa_sdr", shadow=False)
    assert await poller.ingest_feature(feature, source="nwws", shadow=False)

    assert [(item[1]) for item in arbiter.ingested] == ["same"]
    assert arbiter.followups == []
    assert poller._tx.sent == []


async def test_nwws_followup_supersedes_pending_rest_while_same_primary_is_active(
    feature,
):
    db = Database(":memory:")
    arbiter = RecordingArbiter()
    poller = WxPoller(db, FakeTx(), arbiter=arbiter)
    followup = _future(feature("tornado_warning"))
    followup["properties"]["status"] = "Actual"
    followup["properties"]["messageType"] = "Update"
    followup["properties"].setdefault("parameters", {})["VTEC"] = [
        "/O.CAN.KCHS.TO.W.0001.990520T2010Z-990520T2045Z/"
    ]

    assert await poller.ingest_feature(followup, source="nwws", shadow=False)

    assert [item["id"] for item in arbiter.followups] == [followup["id"]]
    assert arbiter.ingested == []
    assert poller._tx.sent == []


async def test_ineligible_rest_followups_use_legacy_update_and_cancel_chain(
    monkeypatch, feature,
):
    import app.poller as poller_mod

    monkeypatch.setattr(poller_mod, "NWSClient", FakeNWS)
    db = Database(":memory:")
    db.set_setting("dry_run", False)
    destination = _add_fixture_route(db)
    tx = ResultTx(ok=True)
    poller = WxPoller(db, tx)
    poller.set_arbiter(WeatherArbiter(db, poller.deliver_arbitrated))

    def ice_product(action, message_type, suffix):
        product = _future(feature("tornado_warning"))
        product["id"] = f"ice-{suffix}"
        props = product["properties"]
        props["id"] = product["id"]
        props["event"] = "Ice Storm Warning"
        props["headline"] = f"Ice Storm Warning {suffix}"
        props["messageType"] = message_type
        props["parameters"] = {
            "VTEC": [f"/O.{action}.KCHS.IS.W.0001.990520T2010Z-990520T2045Z/"]
        }
        return product

    FakeNWS.queue = [_collection([ice_product("NEW", "Alert", "new")])]
    await poller.poll_once()
    FakeNWS.queue = [_collection([ice_product("CON", "Update", "con")])]
    await poller.poll_once()
    FakeNWS.queue = [_collection([ice_product("CAN", "Cancel", "can")])]
    await poller.poll_once()

    assert len(tx.calls) == 3
    attempts = db.query_delivery_attempts(destination_id=destination)
    assert [row["disposition"] for row in reversed(attempts)] == [
        "sent", "update", "cancelled",
    ]
    assert db.list_arbitration_candidates() == []


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
    # tstorm watch, lake wind, and SWS are logged with their filter reason.
    assert disp.count("filtered") == 3
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
    filtered = [
        row for row in db.query_history(limit=500)
        if row["disposition"] == "filtered"
    ]
    assert len(filtered) == 1
    assert filtered[0]["detail"]  # reason recorded


async def test_external_feature_shadow_is_observed_without_db_or_radio(wired):
    db, poller, feature = wired
    accepted = await poller.ingest_feature(
        _future(feature("tornado_warning")), source="nwws", shadow=True
    )
    assert accepted is True
    assert poller._tx.sent == []
    assert db.query_history(limit=10) == []


async def test_malformed_rest_vtec_falls_back_to_original_identity(wired):
    _db, poller, feature = wired
    malformed = _future(feature("tornado_warning"))
    malformed["properties"]["parameters"] = {
        "VTEC": ["/O.NEW.KCHS.TO.W.0001.991332T2510Z-991332T2610Z/"],
    }
    assert await poller.ingest_feature(
        malformed, source="nwws", shadow=True
    ) is True


async def test_external_feature_uses_shared_dedupe_with_rest(wired):
    db, poller, feature = wired
    db.set_setting("dry_run", False)
    rest = _future(feature("tornado_warning"))
    rest["properties"]["parameters"] = {
        "VTEC": ["/O.NEW.KCHS.TO.W.0001.990520T2010Z-990520T2045Z/"]
    }
    nwws = copy.deepcopy(rest)
    nwws["id"] = "urn:nws:vtec:2099:KCHS:TO:W:0001"
    nwws["properties"]["parameters"]["VTECCorrelationKey"] = [nwws["id"]]
    await poller._process(
        rest, FilterRules.from_settings(db.all_settings()), "", 0, False
    )
    sent_after_rest = len(poller._tx.sent)
    assert db.query_history(limit=1)[0]["nws_id"] == rest["id"]
    assert await poller.ingest_feature(nwws, source="nwws", shadow=False) is True
    assert sent_after_rest == 1
    assert len(poller._tx.sent) == sent_after_rest
    restarted_tx = FakeTx()
    restarted = WxPoller(db, restarted_tx)
    assert await restarted.ingest_feature(nwws, source="nwws", shadow=False) is True
    assert restarted_tx.sent == []


async def test_external_feature_update_uses_shared_supersession_path(wired):
    db, poller, feature = wired
    db.set_setting("dry_run", False)
    initial = _future(feature("tornado_warning"))
    initial["properties"]["parameters"] = {
        "VTEC": ["/O.NEW.KCHS.TO.W.0001.990520T2010Z-990520T2045Z/"]
    }
    update = copy.deepcopy(initial)
    update["id"] = "urn:nwws:stream.22"
    update["properties"]["parameters"]["VTEC"] = [
        "/O.CON.KCHS.TO.W.0001.990520T2010Z-990520T2100Z/"
    ]
    update["properties"]["messageType"] = "Update"
    update["properties"]["headline"] += " updated"
    cancel = copy.deepcopy(update)
    cancel["id"] = "urn:nwws:stream.23"
    cancel["properties"]["parameters"]["VTEC"] = [
        "/O.CAN.KCHS.TO.W.0001.000000T0000Z-000000T0000Z/"
    ]
    cancel["properties"]["messageType"] = "Cancel"
    cancel["properties"]["headline"] += " cancelled"
    await poller._process(
        initial, FilterRules.from_settings(db.all_settings()), "", 0, False
    )
    await poller.ingest_feature(update, source="nwws", shadow=False)
    await poller.ingest_feature(cancel, source="nwws", shadow=False)
    assert len(poller._tx.sent) == 3


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


@pytest.mark.parametrize(
    ("action", "message_type"), [("EXT", "Update"), ("CAN", "Cancel")]
)
@pytest.mark.parametrize("initial_state", ["pending", "queued", "restarted"])
async def test_route_changing_followup_delivers_every_persisted_initial_recipient(
    feature, tmp_path, action, message_type, initial_state,
):
    path = tmp_path / f"route-change-{action.lower()}-{initial_state}.db"
    db = Database(str(path))
    db.set_setting("dry_run", False)
    original_destination = _add_fixture_route(db)
    new_destination = db.create_destination("Sullivan", "meshtastic", 6)
    rule = db.create_route("Sullivan tornadoes", 20, True)
    db.replace_route_counties(rule, [("TNC165", "Sullivan")])
    db.replace_route_events(rule, ["Tornado Warning"])
    db.replace_route_destinations(rule, [new_destination])

    class DestinationTx:
        def __init__(self):
            self.destination_ids = []

        def enqueue_destination(self, text, transport, channel, destination_id,
                                on_result=None):
            self.destination_ids.append(destination_id)
            if on_result is not None:
                on_result(True)
            return True

    tx = DestinationTx()
    poller = WxPoller(db, tx)
    arbiter = WeatherArbiter(db, poller.deliver_arbitrated, grace_seconds=45)
    poller.set_arbiter(arbiter)
    initial = _future(feature("tornado_warning"))
    initial["properties"]["status"] = "Actual"
    initial["properties"]["geocode"] = {"UGC": ["SCZ050"]}
    initial["properties"].setdefault("parameters", {})["VTEC"] = [
        "/O.NEW.KCHS.TO.W.0001.990520T2010Z-990520T2045Z/"
    ]
    assert await arbiter.ingest(initial, source="rest") is ArbitrationOutcome.HANDLED
    if initial_state == "queued":
        assert db.claim_arbitration_destinations(
            initial["id"], [original_destination], "rest"
        ) == [original_destination]
    elif initial_state == "restarted":
        db.close()
        db = Database(str(path))
        tx = DestinationTx()
        poller = WxPoller(db, tx)
        arbiter = WeatherArbiter(db, poller.deliver_arbitrated, grace_seconds=45)
        poller.set_arbiter(arbiter)

    followup = copy.deepcopy(initial)
    followup["id"] = f"route-change-{action.lower()}"
    followup["properties"]["id"] = followup["id"]
    followup["properties"]["messageType"] = message_type
    followup["properties"]["areaDesc"] = "Sullivan"
    followup["properties"]["geocode"]["UGC"] = ["TNC165"]
    followup["properties"]["parameters"]["VTEC"] = [
        f"/O.{action}.KCHS.TO.W.0001.990520T2010Z-990520T2115Z/"
    ]
    rebound = await arbiter.prepare_followup(followup)
    assert rebound is not None

    expected_destinations = (
        [original_destination, new_destination]
        if action == "EXT" else [original_destination]
    )
    assert await arbiter.deliver_prepared_followup(rebound) == len(expected_destinations)
    assert sorted(tx.destination_ids) == sorted(expected_destinations)
    rows = db._conn.execute(
        "SELECT destination_id,state FROM weather_arbitration_followups "
        "WHERE followup_id=? ORDER BY destination_id", (followup["id"],),
    ).fetchall()
    assert [tuple(row) for row in rows] == [
        (destination_id, "accepted") for destination_id in sorted(expected_destinations)
    ]


async def test_arbiter_delivery_is_destination_scoped_and_persists_acceptance(feature):
    db = Database(":memory:")
    db.set_setting("dry_run", False)
    destination = _add_fixture_route(db)
    other = db.create_destination("other channel", "meshtastic", 6)
    rule = db.create_route("other fixture route", 20, True)
    db.replace_route_counties(rule, [("SCZ050", "Charleston")])
    db.replace_route_events(rule, [], all_warnings=True)
    db.replace_route_destinations(rule, [other])
    tx = ResultTx(ok=True)
    poller = WxPoller(db, tx)
    warning = _future(feature("tornado_warning"))
    db.create_arbitration_candidate_with_destinations(
        canonical_id=warning["id"], event="Tornado Warning", office="KCHS",
        issued_at=warning["properties"]["effective"],
        expires_at=warning["properties"]["expires"],
        deadline=warning["properties"]["effective"],
        same_feature=json.dumps(warning), same_destination_ids=[destination],
    )
    assert db.claim_arbitration_destinations(
        warning["id"], [destination], "same"
    ) == [destination]

    await poller.deliver_arbitrated(
        warning, [destination], "same", warning["id"],
        lambda destination_id, accepted, error="": None,
    )

    assert len(tx.calls) == 1
    assert db._conn.execute(
        "SELECT state FROM weather_arbitration_destinations "
        "WHERE destination_id=?", (destination,),
    ).fetchone()[0] == "accepted"
    assert db.get_delivery_state(warning["id"], destination) is not None


async def test_disabled_route_permanently_terminates_due_rest_fallback(feature):
    db = Database(":memory:")
    db.set_setting("dry_run", False)
    destination = _add_fixture_route(db)
    tx = ResultTx(ok=True)
    poller = WxPoller(db, tx)
    arbiter = WeatherArbiter(db, poller.deliver_arbitrated, grace_seconds=0)
    warning = _future(feature("tornado_warning"))
    warning["properties"]["status"] = "Actual"
    warning["properties"].setdefault("parameters", {})["VTEC"] = [
        "/O.NEW.KCHS.TO.W.0001.990520T2010Z-990520T2045Z/"
    ]

    assert await arbiter.ingest(warning, source="rest") is ArbitrationOutcome.HANDLED
    assert db.toggle_route(db.list_routes()[0]["id"])

    assert await arbiter.release_due() == 1
    row = db._conn.execute(
        "SELECT state,source FROM weather_arbitration_destinations "
        "WHERE destination_id=?", (destination,),
    ).fetchone()
    assert tuple(row) == ("no_fallback", "")
    assert tx.calls == []
    assert await arbiter.release_due() == 0
    assert tx.calls == []


async def test_ext_routes_update_to_new_destination_without_releasing_stale_new(
    monkeypatch, feature,
):
    import app.poller as poller_mod

    monkeypatch.setattr(poller_mod, "NWSClient", FakeNWS)
    db = Database(":memory:")
    db.set_setting("dry_run", False)
    original_destination = db.create_destination("Charleston", "meshtastic", 1)
    original_rule = db.create_route("Charleston tornadoes", 10, True)
    db.replace_route_counties(original_rule, [("SCZ050", "Charleston")])
    db.replace_route_events(original_rule, ["Tornado Warning"])
    db.replace_route_destinations(original_rule, [original_destination])

    class LifecycleTx:
        def __init__(self):
            self.calls = []

        def enqueue_destination(self, text, transport, channel, destination_id,
                                on_result=None):
            candidate = db.list_arbitration_candidates()[0]
            self.calls.append((text, destination_id, candidate["phase"]))
            if on_result is not None:
                on_result(True)
            return True

    tx = LifecycleTx()
    poller = WxPoller(db, tx)
    arbiter = WeatherArbiter(db, poller.deliver_arbitrated)
    poller.set_arbiter(arbiter)
    initial = _future(feature("tornado_warning"))
    initial["properties"]["status"] = "Actual"
    initial["properties"]["geocode"] = {"UGC": ["SCZ050"]}
    initial["properties"].setdefault("parameters", {})["VTEC"] = [
        "/O.NEW.KCHS.TO.W.0001.990520T2010Z-990520T2045Z/"
    ]
    assert await arbiter.ingest(initial, source="rest") is ArbitrationOutcome.HANDLED

    new_destination = db.create_destination("Richland", "meshtastic", 2)
    new_rule = db.create_route("Richland tornadoes", 20, True)
    db.replace_route_counties(new_rule, [("SCZ040", "Richland")])
    db.replace_route_events(new_rule, ["Tornado Warning"])
    db.replace_route_destinations(new_rule, [new_destination])
    extension = copy.deepcopy(initial)
    extension["id"] = "rest-extension"
    extension["properties"]["id"] = "rest-extension"
    extension["properties"]["messageType"] = "Alert"
    extension["properties"]["areaDesc"] = "Richland"
    extension["properties"]["geocode"] = {"UGC": ["SCZ040"]}
    extension["properties"]["parameters"]["VTEC"] = [
        "/O.EXT.KCHS.TO.W.0001.990520T2010Z-990520T2115Z/"
    ]
    FakeNWS.queue = [_collection([extension])]

    await poller.poll_once()

    assert len(tx.calls) == 2
    assert [(destination_id, phase) for _text, destination_id, phase in tx.calls] == [
        (new_destination, "updated"), (original_destination, "updated")
    ]
    assert db.query_delivery_attempts(destination_id=new_destination)[0][
        "disposition"
    ] == "update"
    assert db.query_delivery_attempts(destination_id=original_destination)[0][
        "disposition"
    ] == "cleared"
    assert await arbiter.release_due() == 0
    assert len(tx.calls) == 2


@pytest.mark.parametrize("action", ["CON", "CAN", "EXP"])
async def test_stale_rest_followup_after_newer_ext_is_not_transmitted_or_persisted(
    monkeypatch, feature, action,
):
    import app.poller as poller_mod

    monkeypatch.setattr(poller_mod, "NWSClient", FakeNWS)
    db = Database(":memory:")
    db.set_setting("dry_run", False)
    _add_fixture_route(db)
    tx = ResultTx(ok=True)
    poller = WxPoller(db, tx)
    arbiter = WeatherArbiter(db, poller.deliver_arbitrated, grace_seconds=0)
    poller.set_arbiter(arbiter)
    initial = _future(feature("tornado_warning"))
    props = initial["properties"]
    props["status"] = "Actual"
    props["sent"] = "2099-05-21T00:10:00+00:00"
    props["geocode"] = {"UGC": ["SCZ050"]}
    props.setdefault("parameters", {})["VTEC"] = [
        "/O.NEW.KCHS.TO.W.0001.990520T2010Z-990520T2045Z/"
    ]
    assert await arbiter.ingest(initial, source="rest") is ArbitrationOutcome.HANDLED
    assert await arbiter.release_due() == 1

    extension = copy.deepcopy(initial)
    extension["id"] = "rest-extension-newer"
    extension["properties"]["id"] = extension["id"]
    extension["properties"]["messageType"] = "Update"
    extension["properties"]["sent"] = "2099-05-21T00:20:00+00:00"
    extension["properties"]["expires"] = "2099-05-21T02:00:00+00:00"
    extension["properties"]["ends"] = "2099-05-21T02:00:00+00:00"
    extension["properties"]["parameters"]["VTEC"] = [
        "/O.EXT.KCHS.TO.W.0001.990520T2010Z-990521T0200Z/"
    ]
    FakeNWS.queue = [_collection([extension])]
    await poller.poll_once()
    assert len(tx.calls) == 2

    stale = copy.deepcopy(extension)
    stale["id"] = f"rest-{action.lower()}-older"
    stale["properties"]["id"] = stale["id"]
    stale["properties"]["sent"] = "2099-05-21T00:15:00+00:00"
    stale["properties"]["expires"] = "2099-05-21T01:00:00+00:00"
    stale["properties"]["ends"] = "2099-05-21T01:00:00+00:00"
    stale["properties"]["parameters"]["VTEC"] = [
        f"/O.{action}.KCHS.TO.W.0001.990520T2010Z-990521T0100Z/"
    ]
    FakeNWS.queue = [_collection([stale])]

    await poller.poll_once()

    assert len(tx.calls) == 2
    candidate = db.list_arbitration_candidates()[0]
    persisted = json.loads(candidate["rest_feature"])
    assert persisted["id"] == "rest-extension-newer"
    assert persisted["properties"]["parameters"]["VTEC"][0].startswith("/O.EXT.")
    assert candidate["expires_at"] == "2099-05-21T02:00:00+00:00"


@pytest.mark.parametrize("action", ["CAN", "EXP"])
async def test_terminal_followup_only_targets_previously_notified_destination(
    monkeypatch, feature, action,
):
    import app.poller as poller_mod

    monkeypatch.setattr(poller_mod, "NWSClient", FakeNWS)
    db = Database(":memory:")
    db.set_setting("dry_run", False)
    notified = db.create_destination("Charleston", "meshtastic", 1)
    initial_rule = db.create_route("Charleston tornadoes", 10, True)
    db.replace_route_counties(initial_rule, [("SCZ050", "Charleston")])
    db.replace_route_events(initial_rule, ["Tornado Warning"])
    db.replace_route_destinations(initial_rule, [notified])

    class DestinationTx:
        def __init__(self):
            self.destination_ids = []

        def enqueue_destination(self, text, transport, channel, destination_id,
                                on_result=None):
            self.destination_ids.append(destination_id)
            if on_result is not None:
                on_result(True)
            return True

    tx = DestinationTx()
    poller = WxPoller(db, tx)
    arbiter = WeatherArbiter(db, poller.deliver_arbitrated, grace_seconds=0)
    poller.set_arbiter(arbiter)
    initial = _future(feature("tornado_warning"))
    initial["properties"]["status"] = "Actual"
    initial["properties"]["geocode"] = {"UGC": ["SCZ050"]}
    initial["properties"].setdefault("parameters", {})["VTEC"] = [
        "/O.NEW.KCHS.TO.W.0001.990520T2010Z-990520T2045Z/"
    ]
    assert await arbiter.ingest(initial, source="rest") is ArbitrationOutcome.HANDLED
    assert await arbiter.release_due() == 1

    never_notified = db.create_destination("Richland", "meshtastic", 2)
    terminal_rule = db.create_route("Richland tornadoes", 20, True)
    db.replace_route_counties(terminal_rule, [("SCZ040", "Richland")])
    db.replace_route_events(terminal_rule, ["Tornado Warning"])
    db.replace_route_destinations(terminal_rule, [never_notified])
    terminal = copy.deepcopy(initial)
    terminal["id"] = f"rest-{action.lower()}"
    terminal["properties"]["id"] = terminal["id"]
    terminal["properties"]["messageType"] = "Alert"
    terminal["properties"]["areaDesc"] = "Richland"
    terminal["properties"]["geocode"] = {"UGC": ["SCZ040"]}
    terminal["properties"]["parameters"]["VTEC"] = [
        f"/O.{action}.KCHS.TO.W.0001.990520T2010Z-990520T2115Z/"
    ]
    FakeNWS.queue = [_collection([terminal])]

    await poller.poll_once()

    assert tx.destination_ids == [notified, notified]
    assert db.query_delivery_attempts(destination_id=never_notified) == []
    terminal_attempt = db.query_delivery_attempts(destination_id=notified)[0]
    assert terminal_attempt["disposition"] == "cancelled"


@pytest.mark.parametrize("action", ["CAN", "EXP"])
async def test_failed_terminal_followup_retries_without_reopening_candidate(
    monkeypatch, feature, action,
):
    import app.poller as poller_mod

    monkeypatch.setattr(poller_mod, "NWSClient", FakeNWS)
    db = Database(":memory:")
    db.set_setting("dry_run", False)
    destination = _add_fixture_route(db)

    class RetryTx:
        def __init__(self):
            self.ok = True
            self.calls = []

        def enqueue_destination(self, text, transport, channel, destination_id,
                                on_result=None):
            self.calls.append((text, destination_id))
            if on_result is not None:
                on_result(self.ok, "radio offline" if not self.ok else "")
            return True

    tx = RetryTx()
    poller = WxPoller(db, tx)
    arbiter = WeatherArbiter(db, poller.deliver_arbitrated, grace_seconds=0)
    poller.set_arbiter(arbiter)
    initial = _future(feature("tornado_warning"))
    initial["properties"]["status"] = "Actual"
    initial["properties"].setdefault("parameters", {})["VTEC"] = [
        "/O.NEW.KCHS.TO.W.0001.990520T2010Z-990520T2045Z/"
    ]
    assert await arbiter.ingest(initial, source="rest") is ArbitrationOutcome.HANDLED
    assert await arbiter.release_due() == 1

    terminal = copy.deepcopy(initial)
    terminal["id"] = f"rest-{action.lower()}"
    terminal["properties"]["id"] = terminal["id"]
    terminal["properties"]["messageType"] = "Update"
    terminal["properties"]["parameters"]["VTEC"] = [
        f"/O.{action}.KCHS.TO.W.0001.990520T2010Z-990520T2115Z/"
    ]
    tx.ok = False
    FakeNWS.queue = [_collection([terminal])]
    await poller.poll_once()
    expected_phase = "cancelled" if action == "CAN" else "expired"
    assert db.get_arbitration_candidate(initial["id"])["phase"] == expected_phase
    assert len(tx.calls) == 2

    tx.ok = True
    FakeNWS.queue = [_collection([terminal])]
    await poller.poll_once()
    assert db.get_arbitration_candidate(initial["id"])["phase"] == expected_phase
    assert len(tx.calls) == 3

    FakeNWS.queue = [_collection([terminal])]
    await poller.poll_once()
    assert len(tx.calls) == 3
    terminal_attempts = [
        row for row in db.query_delivery_attempts(destination_id=destination)
        if row["disposition"] == "cancelled"
    ]
    assert [row["state"] for row in terminal_attempts] == ["accepted", "failed"]


async def test_obsolete_initial_callback_cannot_restore_delivery_after_followup(
    monkeypatch, feature,
):
    import app.poller as poller_mod

    monkeypatch.setattr(poller_mod, "NWSClient", FakeNWS)
    db = Database(":memory:")
    db.set_setting("dry_run", False)
    destination = _add_fixture_route(db)

    class HoldingTx:
        def __init__(self):
            self.callbacks = []

        def enqueue_destination(self, text, transport, channel, destination_id,
                                on_result=None):
            self.callbacks.append(on_result)
            return True

    tx = HoldingTx()
    poller = WxPoller(db, tx)
    arbiter = WeatherArbiter(db, poller.deliver_arbitrated, grace_seconds=0)
    poller.set_arbiter(arbiter)
    initial = _future(feature("tornado_warning"))
    initial["properties"]["status"] = "Actual"
    initial["properties"].setdefault("parameters", {})["VTEC"] = [
        "/O.NEW.KCHS.TO.W.0001.990520T2010Z-990520T2045Z/"
    ]
    assert await arbiter.ingest(initial, source="rest") is ArbitrationOutcome.HANDLED
    assert await arbiter.release_due() == 1
    assert len(tx.callbacks) == 1

    extension = copy.deepcopy(initial)
    extension["id"] = "rest-extension"
    extension["properties"]["id"] = "rest-extension"
    extension["properties"]["messageType"] = "Update"
    extension["properties"]["headline"] += " extended"
    extension["properties"]["parameters"]["VTEC"] = [
        "/O.EXT.KCHS.TO.W.0001.990520T2010Z-990520T2115Z/"
    ]
    FakeNWS.queue = [_collection([extension])]
    await poller.poll_once()
    assert len(tx.callbacks) == 2

    tx.callbacks[0](True)

    initial_attempt = next(
        row for row in db.query_delivery_attempts(alert_id=initial["id"])
        if row["disposition"] == "sent"
    )
    assert initial_attempt["state"] == "superseded"
    assert db.get_delivery_state(initial["id"], destination) is None
    row = db._conn.execute(
        "SELECT state,source FROM weather_arbitration_destinations "
        "WHERE destination_id=?", (destination,),
    ).fetchone()
    assert tuple(row) == ("superseded", "")

    tx.callbacks[1](True)
    current = db.get_delivery_state(initial["id"], destination)
    assert current["last_alert_id"] == initial["id"]
    assert current["disposition"] == "update"


async def test_arbitrated_acceptance_rolls_back_all_authoritative_state_on_failure(
    feature, tmp_path,
):
    path = tmp_path / "atomic-rollback.db"
    db = Database(str(path))
    db.set_setting("dry_run", False)
    destination = _add_fixture_route(db)
    tx = ResultTx(ok=True)
    poller = WxPoller(db, tx)
    arbiter = WeatherArbiter(db, poller.deliver_arbitrated, grace_seconds=0)
    poller.set_arbiter(arbiter)
    warning = _future(feature("tornado_warning"))
    warning["properties"]["status"] = "Actual"
    warning["properties"].setdefault("parameters", {})["VTEC"] = [
        "/O.NEW.KCHS.TO.W.0001.990520T2010Z-990520T2045Z/"
    ]

    def fail_between_arbitration_and_chain() -> None:
        raise RuntimeError("injected atomic acceptance failure")

    db._arbitrated_acceptance_test_hook = fail_between_arbitration_and_chain

    assert await arbiter.ingest(warning, source="rest") is ArbitrationOutcome.HANDLED
    assert await arbiter.release_due() == 1

    arbitration = db._conn.execute(
        "SELECT state,source FROM weather_arbitration_destinations "
        "WHERE destination_id=?", (destination,),
    ).fetchone()
    assert tuple(arbitration) == ("pending", "")
    attempt = db.query_delivery_attempts(destination_id=destination)[0]
    assert attempt["state"] == "failed"
    assert db.get_delivery_state(warning["id"], destination) is None

    db.close()
    restarted_db = Database(str(path))
    restarted_tx = ResultTx(ok=True)
    restarted_poller = WxPoller(restarted_db, restarted_tx)
    restarted_arbiter = WeatherArbiter(
        restarted_db, restarted_poller.deliver_arbitrated, grace_seconds=0
    )
    restarted_poller.set_arbiter(restarted_arbiter)

    assert await restarted_arbiter.release_due() == 1
    retried = restarted_db._conn.execute(
        "SELECT state,source FROM weather_arbitration_destinations "
        "WHERE destination_id=?", (destination,),
    ).fetchone()
    assert tuple(retried) == ("accepted", "rest")
    assert restarted_db.get_delivery_state(warning["id"], destination) is not None


async def test_arbitrated_acceptance_persists_all_authoritative_state_before_return(
    feature,
):
    db = Database(":memory:")
    db.set_setting("dry_run", False)
    _add_fixture_route(db)

    class InspectingTx:
        def __init__(self):
            self.snapshot = None

        def enqueue_destination(self, text, transport, channel, destination_id,
                                on_result=None):
            assert on_result is not None
            on_result(True)
            arbitration = db._conn.execute(
                "SELECT state,source FROM weather_arbitration_destinations "
                "WHERE destination_id=?", (destination_id,),
            ).fetchone()
            attempt = db.query_delivery_attempts(destination_id=destination_id)[0]
            chain = db.get_delivery_state(attempt["root_alert_id"], destination_id)
            self.snapshot = (arbitration, attempt, chain)
            return True

    tx = InspectingTx()
    poller = WxPoller(db, tx)
    arbiter = WeatherArbiter(db, poller.deliver_arbitrated, grace_seconds=0)
    poller.set_arbiter(arbiter)
    warning = _future(feature("tornado_warning"))
    warning["properties"]["status"] = "Actual"
    warning["properties"].setdefault("parameters", {})["VTEC"] = [
        "/O.NEW.KCHS.TO.W.0001.990520T2010Z-990520T2045Z/"
    ]

    assert await arbiter.ingest(warning, source="rest") is ArbitrationOutcome.HANDLED
    assert await arbiter.release_due() == 1

    assert tx.snapshot is not None
    arbitration, attempt, chain = tx.snapshot
    assert tuple(arbitration) == ("accepted", "rest")
    assert attempt["state"] == "accepted"
    assert chain is not None
    assert chain["root_alert_id"] == attempt["root_alert_id"] == warning["id"]
    assert chain["last_alert_id"] == attempt["alert_id"] == warning["id"]
    assert chain["matched_areas"] == attempt["matched_areas"]
    assert chain["event"] == attempt["event"]
    assert chain["headline"] == attempt["headline"]
    assert chain["expires"] == attempt["expires"]
    assert chain["msg_hash"] == attempt["msg_hash"]
    assert chain["disposition"] == attempt["disposition"] == "sent"


async def test_restart_reconciles_successful_transport_before_result_callback(
    feature, tmp_path,
):
    path = tmp_path / "transport-before-callback.db"
    db = Database(str(path))
    db.set_setting("dry_run", False)
    destination = _add_fixture_route(db)
    manager = TransmitManager(db)
    transport = manager._transports["meshtastic"]

    class AcceptedRadio:
        async def send_text(self, text, channel):
            return None

    transport.enabled = transport.connected = True
    transport.tx = AcceptedRadio()
    poller = WxPoller(db, manager)
    arbiter = WeatherArbiter(db, poller.deliver_arbitrated, grace_seconds=0)
    poller.set_arbiter(arbiter)
    warning = _future(feature("tornado_warning"))
    warning["properties"]["status"] = "Actual"
    warning["properties"].setdefault("parameters", {})["VTEC"] = [
        "/O.NEW.KCHS.TO.W.0001.990520T2010Z-990520T2045Z/"
    ]
    assert await arbiter.ingest(warning, source="rest") is ArbitrationOutcome.HANDLED
    assert await arbiter.release_due() == 1
    queued_item = manager._queue.popleft()

    assert await manager._transmit_item(queued_item) == (True, "")
    # Injected crash: the worker never invokes queued_item.on_result.
    db.close()

    reopened = Database(str(path))

    async def no_duplicate(*_args):
        raise AssertionError("durably successful transmission must not be retried")

    restarted = WeatherArbiter(reopened, no_duplicate, grace_seconds=0)
    assert await restarted.release_due() == 0
    arbitration = reopened._conn.execute(
        "SELECT state,source FROM weather_arbitration_destinations "
        "WHERE destination_id=?", (destination,),
    ).fetchone()
    assert tuple(arbitration) == ("accepted", "rest")
    attempt = reopened.query_delivery_attempts(destination_id=destination)[0]
    assert attempt["state"] == "accepted"
    assert reopened.get_delivery_state(warning["id"], destination) is not None


@pytest.mark.parametrize("reconcile_failure", [False, True])
async def test_successful_transport_acceptance_exception_reconciles_without_duplicate(
    feature, tmp_path, reconcile_failure,
):
    path = tmp_path / f"transport-acceptance-exception-{reconcile_failure}.db"
    db = Database(str(path))
    db.set_setting("dry_run", False)
    destination = _add_fixture_route(db)
    manager = TransmitManager(db)
    transport = manager._transports["meshtastic"]

    class AcceptedRadio:
        async def send_text(self, text, channel):
            return None

    transport.enabled = transport.connected = True
    transport.tx = AcceptedRadio()
    poller = WxPoller(db, manager)
    arbiter = WeatherArbiter(db, poller.deliver_arbitrated, grace_seconds=0)
    poller.set_arbiter(arbiter)
    warning = _future(feature("tornado_warning"))
    warning["properties"]["status"] = "Actual"
    warning["properties"].setdefault("parameters", {})["VTEC"] = [
        "/O.NEW.KCHS.TO.W.0001.990520T2010Z-990520T2045Z/"
    ]

    assert await arbiter.ingest(warning, source="rest") is ArbitrationOutcome.HANDLED
    assert await arbiter.release_due() == 1
    queued_item = manager._queue.popleft()
    assert await manager._transmit_item(queued_item) == (True, "")

    def fail_acceptance() -> None:
        raise RuntimeError("injected post-transport acceptance failure")

    db._arbitrated_acceptance_test_hook = fail_acceptance
    if reconcile_failure:
        def fail_reconciliation() -> int:
            raise RuntimeError("injected reconciliation failure")

        db.reconcile_successful_arbitration_transmits = fail_reconciliation
    manager._safe_result(queued_item.on_result, True, "")
    arbitration = db._conn.execute(
        "SELECT state,source FROM weather_arbitration_destinations "
        "WHERE destination_id=?", (destination,),
    ).fetchone()
    expected_state = "queued" if reconcile_failure else "accepted"
    assert tuple(arbitration) == (expected_state, "rest")
    assert db.query_delivery_attempts(destination_id=destination)[0][
        "state"
    ] == expected_state
    db.close()

    reopened = Database(str(path))

    async def no_duplicate(*_args):
        raise AssertionError("successful physical transport must not be retried")

    restarted = WeatherArbiter(reopened, no_duplicate, grace_seconds=0)
    assert await restarted.release_due() == 0
    assert reopened.get_delivery_state(warning["id"], destination) is not None


async def test_restarted_poller_discovers_atomic_acceptance_for_update(
    monkeypatch, feature, tmp_path,
):
    import app.poller as poller_mod

    monkeypatch.setattr(poller_mod, "NWSClient", FakeNWS)
    path = tmp_path / "accepted-restart.db"
    db = Database(str(path))
    db.set_setting("dry_run", False)
    destination = _add_fixture_route(db)
    initial_tx = ResultTx(ok=True)
    poller = WxPoller(db, initial_tx)
    arbiter = WeatherArbiter(db, poller.deliver_arbitrated, grace_seconds=0)
    poller.set_arbiter(arbiter)
    initial = _future(feature("tornado_warning"))
    initial["properties"]["status"] = "Actual"
    initial["properties"].setdefault("parameters", {})["VTEC"] = [
        "/O.NEW.KCHS.TO.W.0001.990520T2010Z-990520T2045Z/"
    ]
    assert await arbiter.ingest(initial, source="rest") is ArbitrationOutcome.HANDLED
    assert await arbiter.release_due() == 1
    db.close()

    restarted_db = Database(str(path))
    restarted_tx = ResultTx(ok=True)
    restarted = WxPoller(restarted_db, restarted_tx)
    restarted_arbiter = WeatherArbiter(
        restarted_db, restarted.deliver_arbitrated, grace_seconds=0
    )
    restarted.set_arbiter(restarted_arbiter)
    extension = copy.deepcopy(initial)
    extension["id"] = "rest-extension-after-restart"
    extension["properties"]["id"] = extension["id"]
    extension["properties"]["messageType"] = "Update"
    extension["properties"]["headline"] += " extended"
    extension["properties"]["parameters"]["VTEC"] = [
        "/O.EXT.KCHS.TO.W.0001.990520T2010Z-990520T2115Z/"
    ]
    FakeNWS.queue = [_collection([extension])]

    await restarted.poll_once()

    assert len(restarted_tx.calls) == 1
    chain = restarted_db.get_delivery_state(initial["id"], destination)
    assert chain is not None
    assert chain["root_alert_id"] == initial["id"]
    assert chain["last_alert_id"] == initial["id"]
    assert chain["disposition"] == "update"
    attempts = restarted_db.query_delivery_attempts(destination_id=destination)
    assert attempts[0]["state"] == "accepted"
    assert attempts[0]["disposition"] == "update"


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
    assert 193 <= len(payload.encode("utf-8")) <= 195
    assert payload.encode("utf-8").decode("utf-8") == payload
    attempt = db.query_delivery_attempts(alert_id="utf8-alert")[0]
    assert attempt["message_text"] == payload and attempt["state"] == "accepted"
