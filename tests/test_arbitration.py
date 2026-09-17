from __future__ import annotations

import asyncio
import copy
import inspect
import json
from datetime import UTC, datetime, timedelta

import pytest

from app.arbitration import (
    ArbitrationOutcome,
    WeatherArbiter,
    _rest_metadata,
    classify_rest_feature,
)
from app.db import Database
from app.main import _run_weather_arbitration


class Clock:
    def __init__(self) -> None:
        self.value = datetime(2026, 9, 6, 18, 31, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += timedelta(seconds=seconds)


class Delivery:
    def __init__(self, result: bool | None = True) -> None:
        self.result = result
        self.calls = []

    async def __call__(self, feature, destination_ids, source, canonical_id, on_result):
        self.calls.append((copy.deepcopy(feature), tuple(destination_ids), source, on_result))
        if self.result is not None:
            for destination_id in destination_ids:
                completed = on_result(
                    destination_id, self.result, "" if self.result else "failed"
                )
                if inspect.isawaitable(completed):
                    await completed


def configured_db(path=":memory:"):
    db = Database(path)
    destination = db.create_destination("Robertson", "meshcore", 2)
    rule = db.create_route("Robertson tornadoes", 10, True)
    db.replace_route_counties(rule, [("TNC147", "Robertson")])
    db.replace_route_events(rule, ["Tornado Warning"])
    db.replace_route_destinations(rule, [destination])
    return db, destination


def rest_feature(
    alert_id="rest-1", *, ugcs=None, message_type="Alert", action="NEW",
    number=1,
):
    return {
        "id": alert_id,
        "properties": {
            "event": "Tornado Warning",
            "headline": "Tornado Warning",
            "areaDesc": "Robertson",
            "status": "Actual",
            "messageType": message_type,
            "effective": "2026-09-06T18:30:00+00:00",
            "onset": "2026-09-06T18:30:00+00:00",
            "expires": "2026-09-06T19:00:00+00:00",
            "ends": "2026-09-06T19:00:00+00:00",
            "geocode": {"UGC": ugcs or ["TNC147"]},
            "parameters": {
                "VTEC": [f"/O.{action}.KOHX.TO.W.{number:04d}.260906T1830Z-260906T1900Z/"]
            },
            "references": [],
        },
    }


def test_rest_metadata_prefers_valid_sent_over_effective() -> None:
    product = rest_feature()
    product["properties"].update({
        "sent": "2027-02-01T00:00:00+00:00",
        "effective": "2026-02-01T00:00:00+00:00",
    })
    product["properties"]["parameters"]["VTEC"] = [
        "/O.NEW.KOHX.TO.W.0001.000000T0000Z-000000T0000Z/"
    ]

    metadata = _rest_metadata(product)

    assert metadata is not None
    assert metadata[1] == "urn:nws:vtec:2027:KOHX:TO:W:0001"
    assert metadata[2] == datetime(2027, 2, 1, tzinfo=UTC)


def test_rest_metadata_uses_onset_after_malformed_sent_and_absent_effective() -> None:
    product = rest_feature()
    product["properties"].update({
        "sent": "not-a-timestamp",
        "onset": "2026-09-06T18:32:00+00:00",
    })
    product["properties"].pop("effective")

    metadata = _rest_metadata(product)

    assert metadata is not None
    assert metadata[2] == datetime(2026, 9, 6, 18, 32, tzinfo=UTC)


def test_rest_metadata_uses_effective_after_malformed_sent() -> None:
    product = rest_feature()
    product["properties"].update({
        "sent": "not-a-timestamp",
        "effective": "2026-09-06T18:31:00+00:00",
    })

    metadata = _rest_metadata(product)

    assert metadata is not None
    assert metadata[2] == datetime(2026, 9, 6, 18, 31, tzinfo=UTC)


def test_rest_metadata_rejects_all_invalid_product_timestamps() -> None:
    product = rest_feature()
    product["properties"].update({
        "sent": "not-a-timestamp",
        "effective": "2026-09-06T18:30:00",
        "onset": 123,
    })

    assert _rest_metadata(product) is None


@pytest.mark.parametrize("vtec", [
    "junk-line\n/O.NEW.KOHX.TO.W.0001.260906T1830Z-260906T1900Z/",
    "/O.NEW.KOHX.TO.W.0001.260906T1830Z-260906T1900Z/\n/junk/",
    " /O.NEW.KOHX.TO.W.0001.260906T1830Z-260906T1900Z/ ",
    (
        "/O.NEW.KOHX.TO.W.0001.260906T1830Z-260906T1900Z/"
        "/O.CON.KOHX.TO.W.0001.260906T1830Z-260906T1900Z/"
    ),
])
def test_rest_metadata_rejects_non_exact_vtec_parameter(vtec: str) -> None:
    product = rest_feature()
    product["properties"]["parameters"]["VTEC"] = [vtec]

    assert classify_rest_feature(product) is ArbitrationOutcome.REJECTED


def test_rest_metadata_handles_exact_vtec_parameter() -> None:
    assert classify_rest_feature(rest_feature()) is ArbitrationOutcome.HANDLED


def test_rest_metadata_rejects_cap_event_that_mismatches_vtec_identity() -> None:
    product = rest_feature()
    product["properties"]["parameters"]["VTEC"] = [
        "/O.NEW.KOHX.SV.W.0001.260906T1830Z-260906T1900Z/"
    ]

    assert classify_rest_feature(product) is ArbitrationOutcome.REJECTED


def test_rest_metadata_accepts_cap_event_matching_vtec_identity() -> None:
    product = rest_feature()
    product["properties"]["parameters"]["VTEC"] = [
        "/O.NEW.KOHX.TO.W.0001.260906T1830Z-260906T1900Z/"
    ]

    assert classify_rest_feature(product) is ArbitrationOutcome.HANDLED


def same_feature(alert_id="same-1", *, ugcs=None, sender="KOHX/NWS", originator="WXR", code="TOR"):
    return {
        "id": alert_id,
        "properties": {
            "event": "Tornado Warning",
            "headline": "Weather radio Tornado Warning",
            "areaDesc": "TNC147",
            "status": "Actual",
            "category": "Met",
            "messageType": "Alert",
            "effective": "2026-09-06T18:30:00+00:00",
            "onset": "2026-09-06T18:30:00+00:00",
            "expires": "2026-09-06T19:00:00+00:00",
            "ends": "2026-09-06T19:00:00+00:00",
            "geocode": {"UGC": ugcs or ["TNC147"], "SAME": ["047147"]},
            "parameters": {
                "NWSSource": ["NOAA Weather Radio SAME"],
                "SAMEOriginator": [originator],
                "SAMEEventCode": [code],
                "SAMESender": [sender],
            },
            "references": [],
        },
    }


@pytest.mark.asyncio
async def test_rest_initial_is_held_until_grace_deadline():
    db, destination = configured_db()
    clock = Clock()
    delivery = Delivery()
    arbiter = WeatherArbiter(db, delivery, clock=clock, grace_seconds=45)

    assert await arbiter.ingest(rest_feature(), source="rest") is ArbitrationOutcome.HANDLED
    assert delivery.calls == []

    clock.advance(44.999)
    assert await arbiter.release_due() == 0
    assert delivery.calls == []

    clock.advance(0.001)
    assert await arbiter.release_due() == 1
    assert [(call[1], call[2]) for call in delivery.calls] == [((destination,), "rest")]


@pytest.mark.asyncio
async def test_due_rest_delivery_exception_is_retried_by_next_release():
    db, _destination = configured_db()
    clock = Clock()
    attempts = 0

    async def deliver(feature, destination_ids, source, canonical_id, on_result):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("delivery crashed before callback")
        for destination_id in destination_ids:
            on_result(destination_id, True, "")

    arbiter = WeatherArbiter(db, deliver, clock=clock)
    await arbiter.ingest(rest_feature(), source="rest")
    clock.advance(45)

    with pytest.raises(RuntimeError, match="delivery crashed before callback"):
        await arbiter.release_due()

    row = db._conn.execute(
        "SELECT state,source FROM weather_arbitration_destinations"
    ).fetchone()
    assert tuple(row) == ("pending", "")
    assert await arbiter.release_due() == 1
    assert attempts == 2
    row = db._conn.execute(
        "SELECT state,source FROM weather_arbitration_destinations"
    ).fetchone()
    assert tuple(row) == ("accepted", "rest")


@pytest.mark.asyncio
async def test_rest_first_same_within_grace_wins_immediately():
    db, destination = configured_db()
    clock = Clock()
    delivery = Delivery()
    arbiter = WeatherArbiter(db, delivery, clock=clock)

    await arbiter.ingest(rest_feature(), source="rest")
    clock.advance(10)
    assert await arbiter.ingest(same_feature(), source="same") is ArbitrationOutcome.HANDLED

    assert [(call[1], call[2]) for call in delivery.calls] == [((destination,), "same")]
    clock.advance(60)
    assert await arbiter.release_due() == 0
    assert len(delivery.calls) == 1


@pytest.mark.asyncio
async def test_same_delivery_exception_is_retried_by_replay_without_restart():
    db, destination = configured_db()
    attempts = 0

    async def deliver(feature, destination_ids, source, canonical_id, on_result):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("delivery crashed before callback")
        for destination_id in destination_ids:
            on_result(destination_id, True, "")

    arbiter = WeatherArbiter(db, deliver, clock=Clock())

    with pytest.raises(RuntimeError, match="delivery crashed before callback"):
        await arbiter.ingest(same_feature(), source="same")

    assert await arbiter.ingest(
        same_feature(), source="same"
    ) is ArbitrationOutcome.HANDLED
    assert attempts == 2
    row = db._conn.execute(
        "SELECT destination_id,state,source FROM weather_arbitration_destinations"
    ).fetchone()
    assert tuple(row) == (destination, "accepted", "same")


@pytest.mark.asyncio
async def test_same_ingest_repairs_candidate_missing_destination_rows():
    db, destination = configured_db()
    feature = same_feature()
    db.create_arbitration_candidate(
        canonical_id="same-1",
        event="Tornado Warning",
        office="KOHX",
        issued_at="2026-09-06T18:30:00+00:00",
        expires_at="2026-09-06T19:00:00+00:00",
        deadline="2026-09-06T18:31:45+00:00",
        same_feature=json.dumps(feature),
    )
    delivery = Delivery()
    arbiter = WeatherArbiter(db, delivery, clock=Clock())

    assert await arbiter.ingest(feature, source="same") is ArbitrationOutcome.HANDLED

    assert [(call[1], call[2]) for call in delivery.calls] == [
        ((destination,), "same")
    ]
    rows = db._conn.execute(
        """SELECT destination_id,state,source
           FROM weather_arbitration_destinations"""
    ).fetchall()
    assert [tuple(row) for row in rows] == [(destination, "accepted", "same")]


@pytest.mark.asyncio
async def test_same_first_rest_new_binds_without_duplicate_delivery():
    db, destination = configured_db()
    delivery = Delivery()
    arbiter = WeatherArbiter(db, delivery, clock=Clock())

    assert await arbiter.ingest(same_feature(), source="same") is ArbitrationOutcome.HANDLED
    assert [(call[1], call[2]) for call in delivery.calls] == [((destination,), "same")]

    assert await arbiter.ingest(rest_feature(), source="rest") is ArbitrationOutcome.HANDLED
    assert len(delivery.calls) == 1
    row = db._conn.execute(
        "SELECT canonical_id,vtec_key,rest_feature FROM weather_arbitration_candidates"
    ).fetchone()
    assert row["canonical_id"] == "same-1"
    assert row["vtec_key"] == "urn:nws:vtec:2026:KOHX:TO:W:0001"
    assert json.loads(row["rest_feature"])["id"] == "rest-1"


@pytest.mark.asyncio
async def test_rest_replay_repairs_partial_same_first_binding_by_vtec():
    db, destination = configured_db()
    clock = Clock()
    delivery = Delivery(result=None)
    arbiter = WeatherArbiter(db, delivery, clock=clock)

    assert await arbiter.ingest(same_feature(), source="same") is ArbitrationOutcome.HANDLED
    serialized = json.dumps(rest_feature(), sort_keys=True, separators=(",", ":"))
    db._conn.execute(
        """UPDATE weather_arbitration_candidates
           SET rest_feature = ?, vtec_key = ? WHERE canonical_id = ?""",
        (serialized, "urn:nws:vtec:2026:KOHX:TO:W:0001", "same-1"),
    )
    db._conn.execute("DELETE FROM weather_arbitration_destinations")
    db._conn.commit()

    assert await arbiter.ingest(rest_feature(), source="rest") is ArbitrationOutcome.HANDLED
    assert await arbiter.ingest(rest_feature(), source="rest") is ArbitrationOutcome.HANDLED
    rows = db._conn.execute(
        """SELECT canonical_id,destination_id,state,source
           FROM weather_arbitration_destinations"""
    ).fetchall()
    assert [tuple(row) for row in rows] == [("same-1", destination, "pending", "")]
    assert [call[2] for call in delivery.calls] == ["same"]

    clock.advance(45)
    delivery.result = True
    assert await arbiter.release_due() == 1
    assert [(call[0]["id"], call[1], call[2]) for call in delivery.calls] == [
        ("same-1", (destination,), "same"),
        ("rest-1", (destination,), "rest"),
    ]


@pytest.mark.asyncio
async def test_same_failure_releases_waiting_rest_immediately():
    db, destination = configured_db()
    delivery = Delivery(result=None)
    arbiter = WeatherArbiter(db, delivery, clock=Clock())

    assert await arbiter.ingest(same_feature(), source="same") is ArbitrationOutcome.HANDLED
    assert await arbiter.ingest(rest_feature(), source="rest") is ArbitrationOutcome.HANDLED
    assert [call[2] for call in delivery.calls] == ["same"]

    same_callback = delivery.calls[0][3]
    completed = same_callback(destination, False, "radio offline")
    if inspect.isawaitable(completed):
        await completed
    await arbiter.drain()

    assert [(call[1], call[2]) for call in delivery.calls] == [
        ((destination,), "same"),
        ((destination,), "rest"),
    ]


@pytest.mark.asyncio
async def test_raw_same_header_is_not_persisted():
    db, _destination = configured_db()
    feature = same_feature()
    feature["properties"]["parameters"]["SAMEHeader"] = [
        "ZCZC-WXR-TOR-047147+0030-2501830-KOHX/NWS-"
    ]
    arbiter = WeatherArbiter(db, Delivery(), clock=Clock())

    assert await arbiter.ingest(feature, source="same") is ArbitrationOutcome.HANDLED

    stored = db._conn.execute(
        "SELECT same_feature FROM weather_arbitration_candidates"
    ).fetchone()[0]
    assert "ZCZC-" not in stored
    assert "SAMEHeader" not in stored


def test_pruning_removes_candidates_at_least_48_hours_after_expiry():
    db, destination = configured_db()
    clock = Clock()
    expired_at_boundary = clock.value - timedelta(hours=48)
    retained = clock.value - timedelta(hours=48) + timedelta(microseconds=1)
    for canonical_id, expires in (
        ("prune-me", expired_at_boundary),
        ("keep-me", retained),
    ):
        db.create_arbitration_candidate(
            canonical_id=canonical_id,
            event="Tornado Warning",
            office="KOHX",
            issued_at=(expires - timedelta(minutes=30)).isoformat(),
            expires_at=expires.isoformat(),
            deadline=(expires - timedelta(minutes=29)).isoformat(),
        )
        db.add_arbitration_destinations(canonical_id, [destination])

    assert db.prune_arbitration_candidates(clock.value.isoformat()) == 1
    assert [row["canonical_id"] for row in db.list_arbitration_candidates()] == [
        "keep-me"
    ]
    assert db._conn.execute(
        "SELECT canonical_id FROM weather_arbitration_destinations"
    ).fetchall()[0][0] == "keep-me"


@pytest.mark.asyncio
async def test_release_due_prunes_expired_history_after_48_hours():
    db, _destination = configured_db()
    clock = Clock()
    expires = clock.value - timedelta(hours=48)
    db.create_arbitration_candidate(
        canonical_id="expired",
        event="Tornado Warning",
        office="KOHX",
        issued_at=(expires - timedelta(minutes=30)).isoformat(),
        expires_at=expires.isoformat(),
        deadline=(expires - timedelta(minutes=29)).isoformat(),
    )
    arbiter = WeatherArbiter(db, Delivery(), clock=clock)

    assert await arbiter.release_due() == 0
    assert db.list_arbitration_candidates() == []


@pytest.mark.asyncio
async def test_rest_timeout_then_late_same_is_suppressed():
    db, destination = configured_db()
    clock = Clock()
    delivery = Delivery()
    arbiter = WeatherArbiter(db, delivery, clock=clock)

    await arbiter.ingest(rest_feature(), source="rest")
    clock.advance(45)
    assert await arbiter.release_due() == 1
    assert await arbiter.ingest(same_feature(), source="same") is ArbitrationOutcome.HANDLED

    assert [(call[1], call[2]) for call in delivery.calls] == [
        ((destination,), "rest")
    ]


@pytest.mark.asyncio
async def test_queued_same_is_not_treated_as_accepted():
    db, _destination = configured_db()
    clock = Clock()
    delivery = Delivery(result=None)
    arbiter = WeatherArbiter(db, delivery, clock=clock)

    await arbiter.ingest(same_feature(), source="same")
    await arbiter.ingest(rest_feature(), source="rest")
    row = db._conn.execute(
        "SELECT state,source FROM weather_arbitration_destinations"
    ).fetchone()
    assert tuple(row) == ("queued", "same")

    clock.advance(45)
    assert await arbiter.release_due() == 0
    assert [call[2] for call in delivery.calls] == ["same"]


@pytest.mark.asyncio
async def test_acceptance_callback_persists_before_returning():
    db, destination = configured_db()
    delivery = Delivery(result=None)
    arbiter = WeatherArbiter(db, delivery, clock=Clock())
    await arbiter.ingest(same_feature(), source="same")

    result = delivery.calls[0][3](destination, True, "")

    assert not inspect.isawaitable(result)
    row = db._conn.execute(
        "SELECT state,source FROM weather_arbitration_destinations"
    ).fetchone()
    assert tuple(row) == ("accepted", "same")


@pytest.mark.asyncio
async def test_deadline_same_race_has_exactly_one_winner():
    db, _destination = configured_db()
    clock = Clock()
    delivery = Delivery(result=None)
    arbiter = WeatherArbiter(db, delivery, clock=clock)
    await arbiter.ingest(rest_feature(), source="rest")
    clock.advance(45)

    await __import__("asyncio").gather(
        arbiter.release_due(),
        arbiter.ingest(same_feature(), source="same"),
    )

    assert len(delivery.calls) == 1
    assert delivery.calls[0][2] in {"rest", "same"}


@pytest.mark.asyncio
async def test_ambiguous_same_match_leaves_each_rest_fallback_pending():
    db, destination = configured_db()
    clock = Clock()
    delivery = Delivery()
    arbiter = WeatherArbiter(db, delivery, clock=clock)

    await arbiter.ingest(rest_feature("rest-1", number=1), source="rest")
    await arbiter.ingest(rest_feature("rest-2", number=2), source="rest")
    assert await arbiter.ingest(same_feature(), source="same") is ArbitrationOutcome.REJECTED
    assert delivery.calls == []

    clock.advance(45)
    assert await arbiter.release_due() == 2
    assert [(call[1], call[2]) for call in delivery.calls] == [
        ((destination,), "rest"),
        ((destination,), "rest"),
    ]


@pytest.mark.asyncio
async def test_partial_county_overlap_is_arbitrated_per_destination():
    db, robertson = configured_db()
    sullivan = db.create_destination("Sullivan", "meshcore", 3)
    rule = db.create_route("Sullivan tornadoes", 20, True)
    db.replace_route_counties(rule, [("TNC165", "Sullivan")])
    db.replace_route_events(rule, ["Tornado Warning"])
    db.replace_route_destinations(rule, [sullivan])
    clock = Clock()
    delivery = Delivery()
    arbiter = WeatherArbiter(db, delivery, clock=clock)

    await arbiter.ingest(
        rest_feature(ugcs=["TNC147", "TNC165"]), source="rest"
    )
    await arbiter.ingest(same_feature(ugcs=["TNC147"]), source="same")
    clock.advance(45)
    assert await arbiter.release_due() == 1

    assert [(call[1], call[2]) for call in delivery.calls] == [
        ((robertson,), "same"),
        ((sullivan,), "rest"),
    ]


@pytest.mark.asyncio
async def test_bound_rest_delivery_exception_releases_uncovered_destination():
    db, robertson = configured_db()
    sullivan = db.create_destination("Sullivan", "meshcore", 3)
    rule = db.create_route("Sullivan tornadoes", 20, True)
    db.replace_route_counties(rule, [("TNC165", "Sullivan")])
    db.replace_route_events(rule, ["Tornado Warning"])
    db.replace_route_destinations(rule, [sullivan])
    clock = Clock()
    rest_attempts = 0

    async def deliver(feature, destination_ids, source, canonical_id, on_result):
        nonlocal rest_attempts
        if source == "rest":
            rest_attempts += 1
            if rest_attempts == 1:
                raise RuntimeError("bound REST delivery crashed")
        for destination_id in destination_ids:
            on_result(destination_id, True, "")

    arbiter = WeatherArbiter(db, deliver, clock=clock)
    await arbiter.ingest(same_feature(ugcs=["TNC147"]), source="same")

    with pytest.raises(RuntimeError, match="bound REST delivery crashed"):
        await arbiter.ingest(
            rest_feature(ugcs=["TNC147", "TNC165"]), source="rest"
        )

    row = db._conn.execute(
        """SELECT state,source FROM weather_arbitration_destinations
           WHERE destination_id = ?""",
        (sullivan,),
    ).fetchone()
    assert tuple(row) == ("pending", "")
    clock.advance(45)
    assert await arbiter.release_due() == 1
    assert rest_attempts == 2
    accepted = db._conn.execute(
        """SELECT destination_id,state FROM weather_arbitration_destinations
           WHERE state = 'accepted' ORDER BY destination_id"""
    ).fetchall()
    assert [tuple(row) for row in accepted] == [
        (robertson, "accepted"),
        (sullivan, "accepted"),
    ]


@pytest.mark.asyncio
async def test_restart_preserves_pending_rest_fallback(tmp_path):
    path = tmp_path / "pending.db"
    db, destination = configured_db(str(path))
    clock = Clock()
    await WeatherArbiter(db, Delivery(), clock=clock).ingest(
        rest_feature(), source="rest"
    )
    db.close()

    clock.advance(45)
    reopened = Database(str(path))
    delivery = Delivery()
    arbiter = WeatherArbiter(reopened, delivery, clock=clock)
    assert await arbiter.release_due() == 1
    assert [(call[1], call[2]) for call in delivery.calls] == [
        ((destination,), "rest")
    ]


@pytest.mark.asyncio
async def test_restart_preserves_accepted_same_suppression(tmp_path):
    path = tmp_path / "accepted.db"
    db, _destination = configured_db(str(path))
    clock = Clock()
    delivery = Delivery()
    arbiter = WeatherArbiter(db, delivery, clock=clock)
    await arbiter.ingest(same_feature(), source="same")
    await arbiter.ingest(rest_feature(), source="rest")
    db.close()

    clock.advance(45)
    reopened = Database(str(path))
    after_restart = Delivery()
    restarted = WeatherArbiter(reopened, after_restart, clock=clock)
    assert await restarted.release_due() == 0
    assert after_restart.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("code", "originator"),
    [
        ("RWT", "WXR"),
        ("ADR", "WXR"),
        ("EAT", "WXR"),
        ("TOR", "EAS"),
        ("CEM", "WXR"),
    ],
    ids=["rwt", "administrative", "eat", "non-wxr", "non-weather"],
)
async def test_ineligible_same_alerts_are_rejected(code, originator):
    db, _destination = configured_db()
    delivery = Delivery()
    arbiter = WeatherArbiter(db, delivery, clock=Clock())

    assert await arbiter.ingest(
        same_feature(code=code, originator=originator), source="same"
    ) is ArbitrationOutcome.REJECTED
    assert delivery.calls == []
    assert db.list_arbitration_candidates() == []


@pytest.mark.asyncio
async def test_non_same_weather_rest_alert_is_not_delayed():
    db, _destination = configured_db()
    feature = rest_feature()
    feature["properties"]["event"] = "Hydrologic Outlook"
    arbiter = WeatherArbiter(db, Delivery(), clock=Clock())

    assert await arbiter.ingest(feature, source="rest") is ArbitrationOutcome.INELIGIBLE
    assert db.list_arbitration_candidates() == []


@pytest.mark.asyncio
async def test_bound_rest_update_is_rekeyed_to_same_canonical_chain():
    db, _destination = configured_db()
    arbiter = WeatherArbiter(db, Delivery(), clock=Clock())
    await arbiter.ingest(same_feature(), source="same")
    await arbiter.ingest(rest_feature(), source="rest")
    update = rest_feature("rest-update", action="EXT", message_type="Update")

    rebound = await arbiter.prepare_followup(update)

    assert rebound is not None
    assert rebound["id"] == "same-1"
    assert rebound["properties"]["id"] == "same-1"
    assert rebound["properties"]["references"] == [{"@id": "same-1"}]


@pytest.mark.asyncio
async def test_unbound_rest_cancel_fails_closed():
    db, _destination = configured_db()
    arbiter = WeatherArbiter(db, Delivery(), clock=Clock())
    cancel = rest_feature("rest-cancel", action="CAN", message_type="Cancel")

    assert await arbiter.prepare_followup(cancel) is None


@pytest.mark.asyncio
async def test_pending_rest_cancel_prevents_initial_fallback_release():
    db, _destination = configured_db()
    clock = Clock()
    delivery = Delivery()
    arbiter = WeatherArbiter(db, delivery, clock=clock)
    await arbiter.ingest(rest_feature(), source="rest")
    cancel = rest_feature("rest-cancel", action="CAN", message_type="Cancel")

    rebound = await arbiter.prepare_followup(cancel)

    assert rebound is not None
    clock.advance(45)
    assert await arbiter.release_due() == 0
    assert delivery.calls == []
    stored = db.get_arbitration_candidate("rest-1")
    assert stored is not None
    assert json.loads(stored["rest_feature"])["id"] == "rest-cancel"


@pytest.mark.asyncio
async def test_pending_rest_update_supersedes_initial_fallback():
    db, _destination = configured_db()
    clock = Clock()
    delivery = Delivery()
    arbiter = WeatherArbiter(db, delivery, clock=clock)
    await arbiter.ingest(rest_feature(), source="rest")
    update = rest_feature("rest-update", action="EXT", message_type="Update")

    rebound = await arbiter.prepare_followup(update)

    assert rebound is not None
    clock.advance(45)
    assert await arbiter.release_due() == 0
    assert delivery.calls == []
    stored = db.get_arbitration_candidate("rest-1")
    assert stored is not None
    assert json.loads(stored["rest_feature"])["id"] == "rest-update"


@pytest.mark.asyncio
async def test_followup_persisted_before_enqueue_is_recovered_after_restart(tmp_path):
    path = tmp_path / "followup-before-enqueue.db"
    db, destination = configured_db(str(path))
    clock = Clock()
    arbiter = WeatherArbiter(db, Delivery(), clock=clock)
    await arbiter.ingest(rest_feature(), source="rest")
    update = rest_feature("rest-update", action="EXT", message_type="Update")

    rebound = await arbiter.prepare_followup(update)
    assert rebound is not None
    db.close()  # injected crash before the caller can enqueue the follow-up

    reopened = Database(str(path))
    delivery = Delivery()
    restarted = WeatherArbiter(reopened, delivery, clock=clock)
    assert await restarted.release_due() == 1
    assert [(call[0]["id"], call[1], call[2]) for call in delivery.calls] == [
        ("rest-1", (destination,), "followup")
    ]
    assert await restarted.release_due() == 0


@pytest.mark.asyncio
async def test_followup_enqueue_exception_retries_without_restart():
    db, _destination = configured_db()
    attempts = 0

    async def deliver(feature, destination_ids, source, canonical_id, on_result):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("injected follow-up enqueue failure")
        for destination_id in destination_ids:
            on_result(destination_id, True, "")

    arbiter = WeatherArbiter(db, deliver, clock=Clock())
    await arbiter.ingest(rest_feature(), source="rest")
    rebound = await arbiter.prepare_followup(
        rest_feature("rest-cancel", action="CAN", message_type="Cancel")
    )
    assert rebound is not None

    with pytest.raises(RuntimeError, match="injected follow-up enqueue failure"):
        await arbiter.deliver_prepared_followup(rebound)

    assert await arbiter.ingest(rest_feature(), source="rest") is ArbitrationOutcome.HANDLED
    assert await arbiter.release_due() == 1
    assert attempts == 2
    row = db._conn.execute(
        "SELECT state FROM weather_arbitration_followups WHERE followup_id='rest-cancel'"
    ).fetchone()
    assert row["state"] == "accepted"


@pytest.mark.asyncio
async def test_route_changing_followup_keeps_initial_inflight_recipient():
    db, robertson = configured_db()
    sullivan = db.create_destination("Sullivan", "meshcore", 3)
    rule = db.create_route("Sullivan tornadoes", 20, True)
    db.replace_route_counties(rule, [("TNC165", "Sullivan")])
    db.replace_route_events(rule, ["Tornado Warning"])
    db.replace_route_destinations(rule, [sullivan])
    arbiter = WeatherArbiter(db, Delivery(), clock=Clock())
    await arbiter.ingest(rest_feature(ugcs=["TNC147"]), source="rest")
    assert db.claim_arbitration_destinations("rest-1", [robertson], "rest") == [robertson]

    changed = rest_feature(
        "rest-route-change", ugcs=["TNC165"], action="EXT", message_type="Update"
    )
    rebound = await arbiter.prepare_followup(changed)

    assert rebound is not None
    rows = db._conn.execute(
        "SELECT destination_id,state FROM weather_arbitration_followups "
        "WHERE followup_id='rest-route-change' ORDER BY destination_id"
    ).fetchall()
    assert [tuple(row) for row in rows] == [
        (robertson, "pending"), (sullivan, "pending")
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["CAN", "EXP"])
async def test_failed_terminal_followup_retries_after_restart_when_feed_is_absent(
    tmp_path, action,
):
    path = tmp_path / f"failed-{action.lower()}.db"
    db, destination = configured_db(str(path))
    clock = Clock()
    failed = Delivery(result=False)
    arbiter = WeatherArbiter(db, failed, clock=clock)
    await arbiter.ingest(rest_feature(), source="rest")
    terminal = rest_feature(
        f"rest-{action.lower()}", action=action, message_type="Cancel"
    )
    rebound = await arbiter.prepare_followup(terminal)
    assert rebound is not None
    assert await arbiter.deliver_prepared_followup(rebound) == 1
    db.close()

    reopened = Database(str(path))
    recovered = Delivery()
    restarted = WeatherArbiter(reopened, recovered, clock=clock)
    assert await restarted.release_due() == 1
    assert [(call[0]["properties"]["parameters"]["VTEC"][0], call[1], call[2])
            for call in recovered.calls] == [
        (terminal["properties"]["parameters"]["VTEC"][0], (destination,), "followup")
    ]
    assert await restarted.release_due() == 0


@pytest.mark.asyncio
async def test_malformed_sent_cannot_bypass_older_effective_product_ordering():
    db, _destination = configured_db()
    arbiter = WeatherArbiter(db, Delivery(), clock=Clock())
    await arbiter.ingest(rest_feature(), source="rest")
    current = rest_feature("rest-ext-current", action="EXT", message_type="Update")
    current["properties"].update({
        "sent": "2026-09-06T19:10:00+00:00",
        "effective": "2026-09-06T19:10:00+00:00",
        "expires": "2026-09-06T22:00:00+00:00",
        "ends": "2026-09-06T22:00:00+00:00",
    })
    assert await arbiter.prepare_followup(current) is not None
    incoming = rest_feature("rest-ext-older", action="EXT", message_type="Update")
    incoming["properties"].update({
        "sent": "not-a-timestamp",
        "effective": "2026-09-06T19:05:00+00:00",
        "expires": "2026-09-06T20:00:00+00:00",
        "ends": "2026-09-06T20:00:00+00:00",
    })

    assert await arbiter.prepare_followup(incoming) is None

    stored = db.get_arbitration_candidate("rest-1")
    assert stored is not None
    assert stored["phase"] == "updated"
    assert json.loads(stored["rest_feature"])["id"] == "rest-ext-current"
    assert stored["expires_at"] == "2026-09-06T22:00:00+00:00"


@pytest.mark.asyncio
async def test_ext_persists_lifecycle_retention_and_suppresses_late_same_destination():
    db, robertson = configured_db()
    sullivan = db.create_destination("Sullivan", "meshcore", 3)
    rule = db.create_route("Sullivan tornadoes", 20, True)
    db.replace_route_counties(rule, [("TNC165", "Sullivan")])
    db.replace_route_events(rule, ["Tornado Warning"])
    db.replace_route_destinations(rule, [sullivan])
    clock = Clock()
    delivery = Delivery()
    arbiter = WeatherArbiter(db, delivery, clock=clock)
    await arbiter.ingest(rest_feature(ugcs=["TNC147"]), source="rest")
    extension = rest_feature(
        "rest-extension", ugcs=["TNC165"], action="EXT", message_type="Alert"
    )
    extension["properties"]["expires"] = "2026-09-06T22:00:00+00:00"
    extension["properties"]["ends"] = "2026-09-06T22:00:00+00:00"

    rebound = await arbiter.prepare_followup(extension)

    assert rebound is not None
    assert rebound["properties"]["messageType"] == "Update"
    stored = db.get_arbitration_candidate("rest-1")
    assert stored["phase"] == "updated"
    assert stored["expires_at"] == "2026-09-06T22:00:00+00:00"
    assert stored["retain_until"] == "2026-09-08T22:00:00+00:00"
    assert db.prune_arbitration_candidates("2026-09-08T19:00:01+00:00") == 0

    late_same = same_feature("same-late", ugcs=["TNC165"])
    late_same["properties"]["expires"] = "2026-09-06T22:00:00+00:00"
    late_same["properties"]["ends"] = "2026-09-06T22:00:00+00:00"
    assert await arbiter.ingest(late_same, source="same") is ArbitrationOutcome.HANDLED
    assert delivery.calls == []
    rows = db._conn.execute(
        "SELECT destination_id,state FROM weather_arbitration_destinations "
        "WHERE canonical_id='rest-1' ORDER BY destination_id"
    ).fetchall()
    assert [tuple(row) for row in rows] == [
        (robertson, "superseded"), (sullivan, "superseded")
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("action", "phase"), [("CAN", "cancelled"), ("EXP", "expired")]
)
async def test_terminal_followup_suppresses_late_same_and_replayed_new(action, phase):
    db, destination = configured_db()
    delivery = Delivery()
    arbiter = WeatherArbiter(db, delivery, clock=Clock())
    original = rest_feature()
    await arbiter.ingest(original, source="rest")

    terminal = rest_feature(
        f"rest-{action.lower()}", action=action, message_type="Update"
    )
    rebound = await arbiter.prepare_followup(terminal)

    assert rebound is not None
    assert rebound["properties"]["messageType"] == "Cancel"
    assert db.get_arbitration_candidate("rest-1")["phase"] == phase
    assert await arbiter.ingest(same_feature("same-late"), source="same") is (
        ArbitrationOutcome.HANDLED
    )
    assert await arbiter.ingest(original, source="rest") is ArbitrationOutcome.HANDLED
    assert delivery.calls == []
    row = db._conn.execute(
        "SELECT state,source FROM weather_arbitration_destinations "
        "WHERE canonical_id='rest-1' AND destination_id=?", (destination,)
    ).fetchone()
    assert tuple(row) == ("superseded", "")


@pytest.mark.asyncio
async def test_exact_rest_coverage_closes_failed_same_only_destination_once():
    db, robertson = configured_db()
    sullivan = db.create_destination("Sullivan", "meshcore", 3)
    rule = db.create_route("Sullivan tornadoes", 20, True)
    db.replace_route_counties(rule, [("TNC165", "Sullivan")])
    db.replace_route_events(rule, ["Tornado Warning"])
    db.replace_route_destinations(rule, [sullivan])
    clock = Clock()
    delivery = Delivery(result=None)
    arbiter = WeatherArbiter(db, delivery, clock=clock)
    await arbiter.ingest(
        same_feature(ugcs=["TNC147", "TNC165"]), source="same"
    )
    await arbiter.ingest(rest_feature(ugcs=["TNC147"]), source="rest")

    same_callback = delivery.calls[0][3]
    same_callback(sullivan, False, "radio offline")
    await arbiter.drain()
    same_callback(robertson, False, "radio offline")
    await arbiter.drain()

    assert [(call[1], call[2]) for call in delivery.calls] == [
        ((robertson, sullivan), "same"), ((robertson,), "rest")
    ]
    delivery.calls[-1][3](robertson, True, "")
    clock.advance(45)
    assert await arbiter.release_due() == 0
    assert await arbiter.release_due() == 0
    rows = db._conn.execute(
        "SELECT destination_id,state,rest_eligible "
        "FROM weather_arbitration_destinations ORDER BY destination_id"
    ).fetchall()
    assert [tuple(row) for row in rows] == [
        (robertson, "accepted", 1), (sullivan, "no_fallback", 0)
    ]


@pytest.mark.asyncio
async def test_disjoint_county_same_is_not_suppressed_by_unique_tombstone():
    db, _robertson = configured_db()
    sullivan = db.create_destination("Sullivan", "meshcore", 3)
    rule = db.create_route("Sullivan tornadoes", 20, True)
    db.replace_route_counties(rule, [("TNC165", "Sullivan")])
    db.replace_route_events(rule, ["Tornado Warning"])
    db.replace_route_destinations(rule, [sullivan])
    delivery = Delivery()
    arbiter = WeatherArbiter(db, delivery, clock=Clock())
    await arbiter.ingest(rest_feature(ugcs=["TNC147"]), source="rest")
    await arbiter.prepare_followup(rest_feature("cancel", action="CAN"))

    disjoint = same_feature("same-disjoint", ugcs=["TNC165"])
    disjoint["properties"]["geocode"]["SAME"] = ["047165"]
    outcome = await arbiter.ingest(disjoint, source="same")

    assert outcome is ArbitrationOutcome.HANDLED
    assert [(call[0]["id"], call[1], call[2]) for call in delivery.calls] == [
        ("same-disjoint", (sullivan,), "same")
    ]
    assert len(db.list_arbitration_candidates()) == 2
    stored = db.get_arbitration_candidate("rest-1")
    assert stored is not None
    assert stored["same_feature"] == ""
    assert json.loads(stored["correlation_ugcs"]) == ["TNC147"]
    assert db.get_arbitration_candidate("same-disjoint") is not None


@pytest.mark.asyncio
async def test_disjoint_county_same_is_not_suppressed_by_multiple_tombstones():
    db, _robertson = configured_db()
    sullivan = db.create_destination("Sullivan", "meshcore", 3)
    rule = db.create_route("Sullivan tornadoes", 20, True)
    db.replace_route_counties(rule, [("TNC165", "Sullivan")])
    db.replace_route_events(rule, ["Tornado Warning"])
    db.replace_route_destinations(rule, [sullivan])
    delivery = Delivery()
    arbiter = WeatherArbiter(db, delivery, clock=Clock())
    for number in (1, 2):
        await arbiter.ingest(
            rest_feature(f"rest-{number}", ugcs=["TNC147"], number=number),
            source="rest",
        )
        await arbiter.prepare_followup(rest_feature(
            f"cancel-{number}", action="CAN", number=number
        ))

    disjoint = same_feature("same-disjoint", ugcs=["TNC165"])
    disjoint["properties"]["geocode"]["SAME"] = ["047165"]
    outcome = await arbiter.ingest(disjoint, source="same")

    assert outcome is ArbitrationOutcome.HANDLED
    assert [(call[0]["id"], call[1], call[2]) for call in delivery.calls] == [
        ("same-disjoint", (sullivan,), "same")
    ]
    assert len(db.list_arbitration_candidates()) == 3
    assert db.get_arbitration_candidate("same-disjoint") is not None


@pytest.mark.asyncio
async def test_followup_supersedes_claims_and_restart_replay_keeps_current_lifecycle(tmp_path):
    path = tmp_path / "lifecycle.db"
    db, robertson = configured_db(str(path))
    sullivan = db.create_destination("Sullivan", "meshcore", 3)
    rule = db.create_route("Sullivan tornadoes", 20, True)
    db.replace_route_counties(rule, [("TNC165", "Sullivan")])
    db.replace_route_events(rule, ["Tornado Warning"])
    db.replace_route_destinations(rule, [sullivan])
    clock = Clock()
    delivery = Delivery(result=None)
    arbiter = WeatherArbiter(db, delivery, clock=clock)
    await arbiter.ingest(same_feature(ugcs=["TNC147"]), source="same")
    original = rest_feature(ugcs=["TNC147", "TNC165"])
    await arbiter.ingest(original, source="rest")
    obsolete_callback = delivery.calls[0][3]
    extension = rest_feature(
        "rest-extension", ugcs=["TNC165"], action="EXT", message_type="Update"
    )
    extension["properties"]["expires"] = "2026-09-06T22:00:00+00:00"
    extension["properties"]["ends"] = "2026-09-06T22:00:00+00:00"
    assert await arbiter.prepare_followup(extension) is not None

    obsolete_callback(robertson, False, "late failure")
    obsolete_callback(robertson, True, "")
    await arbiter.drain()
    rows = db._conn.execute(
        "SELECT destination_id,state,source FROM weather_arbitration_destinations "
        "ORDER BY destination_id"
    ).fetchall()
    assert [tuple(row) for row in rows] == [
        (robertson, "superseded", ""), (sullivan, "superseded", "")
    ]
    db.close()

    reopened = Database(str(path))
    recovered = Delivery()
    restarted = WeatherArbiter(reopened, recovered, clock=clock)
    assert await restarted.ingest(original, source="rest") is ArbitrationOutcome.HANDLED
    stored = reopened.get_arbitration_candidate("same-1")
    assert stored["phase"] == "updated"
    assert json.loads(stored["rest_feature"])["id"] == "rest-extension"
    assert stored["expires_at"] == "2026-09-06T22:00:00+00:00"
    assert await restarted.release_due() == 2
    assert [call[1] for call in recovered.calls] == [(robertson, sullivan)]
    assert await restarted.release_due() == 0


@pytest.mark.asyncio
async def test_rest_new_replay_with_same_vtec_keeps_true_canonical_id():
    db, destination = configured_db()
    clock = Clock()
    delivery = Delivery()
    arbiter = WeatherArbiter(db, delivery, clock=clock)
    await arbiter.ingest(rest_feature("rest-1"), source="rest")

    assert await arbiter.ingest(
        rest_feature("feed-replay-id"), source="rest"
    ) is ArbitrationOutcome.HANDLED

    stored = db.get_arbitration_candidate("rest-1")
    assert json.loads(stored["rest_feature"])["id"] == "rest-1"
    clock.advance(45)
    assert await arbiter.release_due() == 1
    assert delivery.calls[0][0]["id"] == "rest-1"
    assert delivery.calls[0][1] == (destination,)


@pytest.mark.asyncio
async def test_fallback_release_loop_runs_independently_of_rest_polling():
    class Arbiter:
        def __init__(self):
            self.calls = 0

        async def release_due(self):
            self.calls += 1
            if self.calls == 2:
                raise asyncio.CancelledError

    arbiter = Arbiter()
    with pytest.raises(asyncio.CancelledError):
        await _run_weather_arbitration(arbiter, interval=0)
    assert arbiter.calls == 2
