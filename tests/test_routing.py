import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from app.db import Database
from app.config import STATE_EXPIRY_HOURS
from app.models import Alert
from app.routing import RoutedDestination, route_alert
from app.filters import FilterRules
from app.poller import WxPoller, _delivery_hash


def _alert(areas="Smith; Robertson", zones=None, event="Tornado Warning",
           alert_id="alert-1", message_type="Alert", references=None,
           headline="warning"):
    zones = ["TNC147", "TNC159"] if zones is None else zones
    return Alert.from_feature({
        "id": alert_id,
        "properties": {
            "event": event,
            "headline": headline,
            "areaDesc": areas,
            "messageType": message_type,
            "references": [{"@id": ref} for ref in (references or [])],
            "effective": "2099-01-01T00:00:00+00:00",
            "expires": "2099-01-01T01:00:00+00:00",
            "geocode": {"UGC": zones},
        },
    })


def test_route_uses_only_configured_county_that_matched(tmp_path):
    db = Database(str(tmp_path / "mesh.db"))
    destination_id = db.create_destination("Robertson mesh", "meshcore", 2)
    rule_id = db.create_route("Robertson warnings", 10, True)
    db.replace_route_counties(rule_id, [("TNC147", "Robertson")])
    db.replace_route_events(rule_id, [], all_warnings=True)
    db.replace_route_destinations(rule_id, [destination_id])

    matches = route_alert(db, _alert())

    assert len(matches) == 1
    assert matches[0].transport == "meshcore"
    assert matches[0].channel == 2
    assert matches[0].matched_areas == ("Robertson",)


class DestinationTx:
    def __init__(self):
        self.sent = []

    def enqueue_destination(self, text, transport, channel, destination_id, on_result=None):
        self.sent.append((text, transport, channel, destination_id))
        if on_result:
            on_result(True)
        return True


class DeferredTx:
    def __init__(self):
        self.sent = []
        self.callbacks = []

    def enqueue_destination(self, text, transport, channel, destination_id,
                            on_result=None):
        self.sent.append((text, transport, channel, destination_id))
        self.callbacks.append(on_result)
        return True


class ChainTx:
    def __init__(self):
        self.queued = []
        self.in_flight = None
        self.chain_results = {}
        self.attempted = []

    def enqueue_destination_chain(self, text, transport, channel, destination_id,
                                  correlation_key, require_prior_success=False,
                                  on_result=None):
        self.queued.append({
            "text": text, "transport": transport, "channel": channel,
            "destination_id": destination_id, "key": correlation_key,
            "required": require_prior_success, "callback": on_result,
        })
        return True

    def cancel_queued_correlation(self, key):
        kept = []
        removed = 0
        for item in self.queued:
            if item["key"] == key:
                removed += 1
                item["callback"](False, "superseded")
            else:
                kept.append(item)
        self.queued = kept
        return removed

    def begin_next(self):
        self.in_flight = self.queued.pop(0)
        return self.in_flight

    def finish(self, ok):
        item, self.in_flight = self.in_flight, None
        self.attempted.append(item["text"])
        self.chain_results[item["key"]] = ok
        item["callback"](ok, "radio unavailable" if not ok else "")

    def run_next(self, ok=True):
        item = self.begin_next()
        if item["required"] and not self.chain_results.get(item["key"]):
            item, self.in_flight = self.in_flight, None
            item["callback"](False, "predecessor was not accepted")
            return
        self.finish(ok)


async def test_poller_formats_and_sends_robertson_not_first_area(tmp_path):
    db = Database(str(tmp_path / "mesh.db"))
    db.set_setting("dry_run", False)
    destination_id = db.create_destination("Robertson mesh", "meshcore", 2)
    rule_id = db.create_route("Robertson warnings", 10, True)
    db.replace_route_counties(rule_id, [("TNC147", "Robertson")])
    db.replace_route_events(rule_id, [], all_warnings=True)
    db.replace_route_destinations(rule_id, [destination_id])
    tx = DestinationTx()
    poller = WxPoller(db, tx)

    await poller._process(
        _alert().raw, FilterRules([], ["Warning"], []), "America/Chicago", 0, False,
    )

    assert len(tx.sent) == 1
    assert "Robertson County" in tx.sent[0][0]
    assert "Smith" not in tx.sent[0][0]
    history = db.query_history()
    assert history[0]["area"] == "Smith; Robertson"


@pytest.mark.parametrize(
    ("results", "expected"),
    [([True, True], "accepted"), ([True, False], "partial"), ([False, False], "failed")],
)
async def test_history_tracks_all_destination_outcomes(tmp_path, results, expected):
    db = Database(str(tmp_path / "mesh.db"))
    for name, transport, channel in (("one", "meshcore", 1), ("two", "meshtastic", 2)):
        destination_id = db.create_destination(name, transport, channel)
        rule_id = db.create_route(name, 10, True)
        db.replace_route_counties(rule_id, [("TNC147", "Robertson")])
        db.replace_route_events(rule_id, [], all_warnings=True)
        db.replace_route_destinations(rule_id, [destination_id])
    tx = DeferredTx()
    poller = WxPoller(db, tx)

    await poller._process(
        _alert(areas="Robertson; Full NWS Area", zones=["TNC147"]).raw,
        FilterRules([], ["Warning"], []), "America/Chicago", 0, False,
    )
    rows = db.query_history()
    assert len(rows) == 1
    assert rows[0]["disposition"] == "queued"
    assert rows[0]["area"] == "Robertson; Full NWS Area"

    tx.callbacks[0](results[0], "first failed" if not results[0] else "")
    if len(results) > 1:
        assert db.query_history()[0]["disposition"] == "queued"
    tx.callbacks[1](results[1], "second failed" if not results[1] else "")

    rows = db.query_history()
    assert len(rows) == 1
    assert rows[0]["disposition"] == expected


async def test_poller_persists_attempt_before_enqueue_and_finalizes_callback(tmp_path):
    class InspectingTx:
        callback = None
        state_seen_during_enqueue = None

        def enqueue_destination(self, text, transport, channel, destination_id,
                                on_result=None):
            self.state_seen_during_enqueue = db.query_delivery_attempts()[0]["state"]
            self.callback = on_result
            return True

    db = Database(str(tmp_path / "mesh.db"))
    destination_id = db.create_destination("Robertson", "meshcore", 2)
    rule_id = db.create_route("Robertson", 10, True)
    db.replace_route_counties(rule_id, [("TNC147", "Robertson")])
    db.replace_route_events(rule_id, [], all_warnings=True)
    db.replace_route_destinations(rule_id, [destination_id])
    tx = InspectingTx()

    await WxPoller(db, tx)._process(
        _alert(areas="Robertson", zones=["TNC147"]).raw,
        FilterRules([], ["Warning"], []), "America/Chicago", 0, False,
    )

    assert tx.state_seen_during_enqueue == "queued"
    queued = dict(db.query_delivery_attempts()[0])
    assert queued["destination_id"] == destination_id
    assert queued["destination_name"] == "Robertson"
    assert queued["matched_areas"] == '["Robertson"]'
    assert queued["message_text"].encode("utf-8")
    assert tx.callback is not None
    tx.callback(True)
    accepted = db.query_delivery_attempts()[0]
    assert accepted["id"] == queued["id"]
    assert accepted["state"] == "accepted"


async def test_enqueue_exception_marks_canonical_history_failed(tmp_path):
    class ExplodingTx:
        def enqueue_destination(self, *args, **kwargs):
            raise RuntimeError("queue unavailable")

    db = Database(str(tmp_path / "mesh.db"))
    destination_id = db.create_destination("Robertson", "meshcore", 2)
    rule_id = db.create_route("Robertson", 10, True)
    db.replace_route_counties(rule_id, [("TNC147", "Robertson")])
    db.replace_route_events(rule_id, [], all_warnings=True)
    db.replace_route_destinations(rule_id, [destination_id])

    await WxPoller(db, ExplodingTx())._process(
        _alert(areas="Robertson", zones=["TNC147"]).raw,
        FilterRules([], ["Warning"], []), "America/Chicago", 0, False,
    )

    row = db.query_history()[0]
    assert row["disposition"] == "failed"
    assert "queue unavailable" in row["detail"]


async def test_fast_repoll_does_not_queue_same_pending_destination_twice(tmp_path):
    db = Database(str(tmp_path / "mesh.db"))
    destination_id = db.create_destination("Robertson mesh", "meshcore", 2)
    rule_id = db.create_route("Robertson warnings", 10, True)
    db.replace_route_counties(rule_id, [("TNC147", "Robertson")])
    db.replace_route_events(rule_id, [], all_warnings=True)
    db.replace_route_destinations(rule_id, [destination_id])
    tx = DeferredTx()
    poller = WxPoller(db, tx)
    rules = FilterRules([], ["Warning"], [])

    await poller._process(_alert().raw, rules, "America/Chicago", 0, False)
    await poller._process(_alert().raw, rules, "America/Chicago", 0, False)

    assert len(tx.sent) == 1


async def test_failed_pending_destination_is_retryable(tmp_path):
    db = Database(str(tmp_path / "mesh.db"))
    destination_id = db.create_destination("Robertson mesh", "meshcore", 2)
    rule_id = db.create_route("Robertson warnings", 10, True)
    db.replace_route_counties(rule_id, [("TNC147", "Robertson")])
    db.replace_route_events(rule_id, [], all_warnings=True)
    db.replace_route_destinations(rule_id, [destination_id])
    tx = DeferredTx()
    poller = WxPoller(db, tx)
    rules = FilterRules([], ["Warning"], [])

    await poller._process(_alert().raw, rules, "America/Chicago", 0, False)
    tx.callbacks[0](False, "radio unavailable")
    await poller._process(_alert().raw, rules, "America/Chicago", 0, False)

    assert len(tx.sent) == 2


async def test_queued_original_is_removed_when_cancel_arrives(tmp_path):
    db = Database(str(tmp_path / "mesh.db"))
    destination_id = db.create_destination("Robertson", "meshcore", 2)
    rule_id = db.create_route("Robertson", 10, True)
    db.replace_route_counties(rule_id, [("TNC147", "Robertson")])
    db.replace_route_events(rule_id, [], all_warnings=True)
    db.replace_route_destinations(rule_id, [destination_id])
    tx = ChainTx()
    poller = WxPoller(db, tx)
    rules = FilterRules([], ["Warning"], [])

    await poller._process(_alert(areas="Robertson", zones=["TNC147"]).raw,
                          rules, "America/Chicago", 0, False)
    cancel = _alert(areas="", zones=[], alert_id="alert-2", message_type="Cancel",
                    references=["alert-1"])
    await poller._process(cancel.raw, rules, "America/Chicago", 0, False)

    assert tx.queued == []
    assert tx.attempted == []
    assert db.get_delivery_state("alert-1", destination_id) is None


async def test_queued_original_is_superseded_by_latest_update(tmp_path):
    db = Database(str(tmp_path / "mesh.db"))
    destination_id = db.create_destination("Robertson", "meshcore", 2)
    rule_id = db.create_route("Robertson", 10, True)
    db.replace_route_counties(rule_id, [("TNC147", "Robertson")])
    db.replace_route_events(rule_id, [], all_warnings=True)
    db.replace_route_destinations(rule_id, [destination_id])
    tx = ChainTx()
    poller = WxPoller(db, tx)
    rules = FilterRules([], ["Warning"], [])

    await poller._process(_alert(areas="Robertson", zones=["TNC147"]).raw,
                          rules, "America/Chicago", 0, False)
    update = _alert(areas="Robertson", zones=["TNC147"], alert_id="alert-2",
                    message_type="Update", references=["alert-1"], headline="updated")
    await poller._process(update.raw, rules, "America/Chicago", 0, False)

    assert len(tx.queued) == 1
    tx.run_next(True)
    state = db.get_delivery_state("alert-1", destination_id)
    assert state["last_alert_id"] == "alert-2"
    assert state["disposition"] == "update"


async def test_unmatched_update_supersedes_queued_original_without_false_cleared(tmp_path):
    db = Database(str(tmp_path / "mesh.db"))
    destination_id = db.create_destination("Robertson", "meshcore", 2)
    rule_id = db.create_route("Robertson", 10, True)
    db.replace_route_counties(rule_id, [("TNC147", "Robertson")])
    db.replace_route_events(rule_id, [], all_warnings=True)
    db.replace_route_destinations(rule_id, [destination_id])
    tx = ChainTx()
    poller = WxPoller(db, tx)
    rules = FilterRules([], ["Warning"], [])

    await poller._process(_alert(areas="Robertson", zones=["TNC147"]).raw,
                          rules, "America/Chicago", 0, False)
    update = _alert(areas="Smith", zones=["TNC159"], alert_id="alert-2",
                    message_type="Update", references=["alert-1"], headline="moved")
    await poller._process(update.raw, rules, "America/Chicago", 0, False)

    assert tx.queued == []
    assert tx.attempted == []
    attempts = db.query_delivery_attempts(destination_id=destination_id)
    assert len(attempts) == 1
    assert attempts[0]["alert_id"] == "alert-1"
    assert attempts[0]["state"] == "superseded"
    assert "CLEARED" not in attempts[0]["message_text"]


@pytest.mark.parametrize("original_ok", [True, False])
async def test_repeated_unmatched_update_replaces_conditional_clear(tmp_path, original_ok):
    db = Database(str(tmp_path / "mesh.db"))
    destination_id = db.create_destination("Robertson", "meshcore", 2)
    rule_id = db.create_route("Robertson", 10, True)
    db.replace_route_counties(rule_id, [("TNC147", "Robertson")])
    db.replace_route_events(rule_id, [], all_warnings=True)
    db.replace_route_destinations(rule_id, [destination_id])
    tx = ChainTx()
    poller = WxPoller(db, tx)
    rules = FilterRules([], ["Warning"], [])

    await poller._process(_alert(areas="Robertson", zones=["TNC147"]).raw,
                          rules, "America/Chicago", 0, False)
    tx.begin_next()
    first_update = _alert(
        areas="Smith", zones=["TNC159"], alert_id="alert-2",
        message_type="Update", references=["alert-1"], headline="moved once",
    )
    await poller._process(first_update.raw, rules, "America/Chicago", 0, False)
    second_update = _alert(
        areas="Smith", zones=["TNC159"], alert_id="alert-3",
        message_type="Update", references=["alert-1"], headline="moved twice",
    )
    await poller._process(second_update.raw, rules, "America/Chicago", 0, False)

    assert len(tx.queued) == 1
    assert "CLEARED" in tx.queued[0]["text"]
    assert tx.queued[0]["required"] is True
    tx.finish(original_ok)
    tx.run_next(True)

    attempts = {
        row["alert_id"]: row for row in db.query_delivery_attempts(
            destination_id=destination_id
        )
    }
    assert attempts["alert-2"]["state"] == "superseded"
    state = db.get_delivery_state("alert-1", destination_id)
    if original_ok:
        assert len(tx.attempted) == 2
        assert attempts["alert-1"]["state"] == "accepted"
        assert attempts["alert-3"]["state"] == "accepted"
        assert state["last_alert_id"] == "alert-3"
        assert state["disposition"] == "cleared"
    else:
        assert len(tx.attempted) == 1
        assert attempts["alert-1"]["state"] == "failed"
        assert attempts["alert-3"]["state"] == "skipped"
        assert state is None
    assert tx.chain_results == {}


@pytest.mark.parametrize("original_ok", [True, False])
async def test_inflight_original_cancel_is_conditional_on_acceptance(tmp_path, original_ok):
    db = Database(str(tmp_path / "mesh.db"))
    destination_id = db.create_destination("Robertson", "meshcore", 2)
    rule_id = db.create_route("Robertson", 10, True)
    db.replace_route_counties(rule_id, [("TNC147", "Robertson")])
    db.replace_route_events(rule_id, [], all_warnings=True)
    db.replace_route_destinations(rule_id, [destination_id])
    tx = ChainTx()
    poller = WxPoller(db, tx)
    rules = FilterRules([], ["Warning"], [])

    await poller._process(_alert(areas="Robertson", zones=["TNC147"]).raw,
                          rules, "America/Chicago", 0, False)
    tx.begin_next()
    cancel = _alert(areas="", zones=[], alert_id="alert-2", message_type="Cancel",
                    references=["alert-1"])
    await poller._process(cancel.raw, rules, "America/Chicago", 0, False)
    tx.finish(original_ok)
    tx.run_next(True)

    state = db.get_delivery_state("alert-1", destination_id)
    if original_ok:
        assert len(tx.attempted) == 2
        assert state["disposition"] == "cancelled"
    else:
        assert len(tx.attempted) == 1
        assert state is None
    assert tx.chain_results == {}


@pytest.mark.parametrize("original_ok", [True, False])
async def test_cancel_replaces_queued_update_behind_inflight_original(tmp_path, original_ok):
    db = Database(str(tmp_path / "mesh.db"))
    destination_id = db.create_destination("Robertson", "meshcore", 2)
    rule_id = db.create_route("Robertson", 10, True)
    db.replace_route_counties(rule_id, [("TNC147", "Robertson")])
    db.replace_route_events(rule_id, [], all_warnings=True)
    db.replace_route_destinations(rule_id, [destination_id])
    tx = ChainTx()
    poller = WxPoller(db, tx)
    rules = FilterRules([], ["Warning"], [])

    await poller._process(_alert(areas="Robertson", zones=["TNC147"]).raw,
                          rules, "America/Chicago", 0, False)
    tx.begin_next()
    update = _alert(
        areas="Robertson", zones=["TNC147"], alert_id="alert-2",
        message_type="Update", references=["alert-1"], headline="updated",
    )
    await poller._process(update.raw, rules, "America/Chicago", 0, False)
    cancel = _alert(
        areas="", zones=[], alert_id="alert-3", message_type="Cancel",
        references=["alert-1"], headline="cancelled",
    )
    await poller._process(cancel.raw, rules, "America/Chicago", 0, False)

    assert len(tx.queued) == 1
    assert "CANCELLED" in tx.queued[0]["text"]
    assert tx.queued[0]["required"] is True
    tx.finish(original_ok)
    tx.run_next(True)

    attempts = {
        row["alert_id"]: row for row in db.query_delivery_attempts(
            destination_id=destination_id
        )
    }
    assert attempts["alert-2"]["state"] == "superseded"
    state = db.get_delivery_state("alert-1", destination_id)
    if original_ok:
        assert len(tx.attempted) == 2
        assert attempts["alert-1"]["state"] == "accepted"
        assert attempts["alert-3"]["state"] == "accepted"
        assert state["last_alert_id"] == "alert-3"
        assert state["disposition"] == "cancelled"
    else:
        assert len(tx.attempted) == 1
        assert attempts["alert-1"]["state"] == "failed"
        assert attempts["alert-3"]["state"] == "skipped"
        assert state is None
    assert tx.chain_results == {}


async def test_many_supersessions_keep_only_latest_conditional_successor(tmp_path):
    db = Database(str(tmp_path / "mesh.db"))
    destination_id = db.create_destination("Robertson", "meshcore", 2)
    rule_id = db.create_route("Robertson", 10, True)
    db.replace_route_counties(rule_id, [("TNC147", "Robertson")])
    db.replace_route_events(rule_id, [], all_warnings=True)
    db.replace_route_destinations(rule_id, [destination_id])
    tx = ChainTx()
    poller = WxPoller(db, tx)
    rules = FilterRules([], ["Warning"], [])

    await poller._process(_alert(areas="Robertson", zones=["TNC147"]).raw,
                          rules, "America/Chicago", 0, False)
    tx.begin_next()
    successors = [
        _alert(areas="Robertson", zones=["TNC147"], alert_id="alert-2",
               message_type="Update", references=["alert-1"], headline="matched"),
        _alert(areas="Smith", zones=["TNC159"], alert_id="alert-3",
               message_type="Update", references=["alert-1"], headline="moved"),
        _alert(areas="Robertson", zones=["TNC147"], alert_id="alert-4",
               message_type="Update", references=["alert-1"], headline="returned"),
        _alert(areas="", zones=[], alert_id="alert-5", message_type="Cancel",
               references=["alert-1"], headline="cancel one"),
        _alert(areas="", zones=[], alert_id="alert-6", message_type="Cancel",
               references=["alert-1"], headline="cancel two"),
    ]
    for successor in successors:
        await poller._process(successor.raw, rules, "America/Chicago", 0, False)
        assert len(tx.queued) == 1

    before_send = {
        row["alert_id"]: row["state"] for row in db.query_delivery_attempts(
            destination_id=destination_id
        )
    }
    assert before_send == {
        "alert-1": "queued",
        "alert-2": "superseded",
        "alert-3": "superseded",
        "alert-4": "superseded",
        "alert-5": "superseded",
        "alert-6": "queued",
    }
    assert "CANCELLED" in tx.queued[0]["text"]
    assert tx.queued[0]["required"] is True

    tx.finish(True)
    tx.run_next(True)

    attempts = {
        row["alert_id"]: row["state"] for row in db.query_delivery_attempts(
            destination_id=destination_id
        )
    }
    assert attempts == {
        "alert-1": "accepted",
        "alert-2": "superseded",
        "alert-3": "superseded",
        "alert-4": "superseded",
        "alert-5": "superseded",
        "alert-6": "accepted",
    }
    assert len(tx.attempted) == 2
    state = db.get_delivery_state("alert-1", destination_id)
    assert state["last_alert_id"] == "alert-6"
    assert state["disposition"] == "cancelled"
    assert poller._pending_deliveries == {}
    assert poller._conditional_chains == set()
    assert tx.chain_results == {}


async def test_dry_run_state_is_not_treated_as_accepted_delivery(tmp_path):
    db = Database(str(tmp_path / "mesh.db"))
    destination_id = db.create_destination("Robertson mesh", "meshcore", 2)
    rule_id = db.create_route("Robertson warnings", 10, True)
    db.replace_route_counties(rule_id, [("TNC147", "Robertson")])
    db.replace_route_events(rule_id, [], all_warnings=True)
    db.replace_route_destinations(rule_id, [destination_id])
    tx = DestinationTx()
    poller = WxPoller(db, tx)
    rules = FilterRules([], ["Warning"], [])

    await poller._process(_alert().raw, rules, "America/Chicago", 0, True)
    state = db.get_delivery_state("alert-1", destination_id)
    assert state is None
    assert db.query_history()[0]["disposition"] == "dry_run"

    cancel = _alert(areas="", zones=[], alert_id="alert-cancel",
                    message_type="Cancel", references=["alert-1"])
    await poller._process(cancel.raw, rules, "America/Chicago", 0, False)
    assert tx.sent == []


async def test_dry_run_update_preserves_last_accepted_delivery_snapshot(tmp_path):
    db = Database(str(tmp_path / "mesh.db"))
    destination_id = db.create_destination("Robertson mesh", "meshcore", 2)
    rule_id = db.create_route("Robertson warnings", 10, True)
    db.replace_route_counties(rule_id, [("TNC147", "Robertson")])
    db.replace_route_events(rule_id, [], all_warnings=True)
    db.replace_route_destinations(rule_id, [destination_id])
    tx = DestinationTx()
    poller = WxPoller(db, tx)
    rules = FilterRules([], ["Warning"], [])
    original = _alert(areas="Robertson", zones=["TNC147"])

    await poller._process(original.raw, rules, "America/Chicago", 0, False)
    accepted = dict(db.get_delivery_state("alert-1", destination_id))
    update = _alert(areas="Smith", zones=["TNC159"], alert_id="alert-2",
                    message_type="Update", references=["alert-1"])
    await poller._process(update.raw, rules, "America/Chicago", 0, True)

    assert dict(db.get_delivery_state("alert-1", destination_id)) == accepted
    cancel = _alert(areas="", zones=[], alert_id="alert-3", message_type="Cancel",
                    references=["alert-1"])
    await poller._process(cancel.raw, rules, "America/Chicago", 0, False)
    assert len(tx.sent) == 2
    assert "CANCELLED" in tx.sent[1][0]
    assert "Robertson County" in tx.sent[1][0]


async def test_global_filter_blocks_new_alert_before_routing(tmp_path):
    db = Database(str(tmp_path / "mesh.db"))
    destination_id = db.create_destination("Robertson mesh", "meshcore", 2)
    rule_id = db.create_route("Robertson advisories", 10, True)
    db.replace_route_counties(rule_id, [("TNC147", "Robertson")])
    db.replace_route_events(rule_id, ["Heat Advisory"])
    db.replace_route_destinations(rule_id, [destination_id])
    tx = DestinationTx()

    await WxPoller(db, tx)._process(
        _alert(event="Heat Advisory", zones=["TNC147"]).raw,
        FilterRules([], ["Warning"], []), "America/Chicago", 0, False,
    )

    assert tx.sent == []
    assert db.query_history()[0]["disposition"] == "filtered"


async def test_included_alert_without_route_is_logged_as_no_route(tmp_path):
    db = Database(str(tmp_path / "mesh.db"))
    tx = DestinationTx()

    await WxPoller(db, tx)._process(
        _alert().raw, FilterRules([], ["Warning"], []),
        "America/Chicago", 0, False,
    )

    assert tx.sent == []
    row = db.query_history()[0]
    assert row["disposition"] == "no_route"
    assert row["detail"] == "no matching route"


def test_ugc_is_authoritative_and_overlapping_rules_union_counties(tmp_path):
    db = Database(str(tmp_path / "mesh.db"))
    destination_id = db.create_destination("county mesh", "meshcore", 3)
    first = db.create_route("Robertson", 10, True)
    db.replace_route_counties(first, [("TNC147", "Robertson")])
    db.replace_route_events(first, [], all_warnings=True)
    db.replace_route_destinations(first, [destination_id])
    second = db.create_route("Smith", 20, True)
    db.replace_route_counties(second, [("TNC159", "Smith")])
    db.replace_route_events(second, [], all_warnings=True)
    db.replace_route_destinations(second, [destination_id])

    matches = route_alert(db, _alert(zones=["TNC147", "TNC159"]))
    assert len(matches) == 1
    assert matches[0].matched_areas == ("Robertson", "Smith")

    # A same-named county from another state must not match when UGC is present.
    assert route_alert(db, _alert(areas="Robertson", zones=["KYC201"])) == []


def test_explicit_empty_ugc_does_not_fall_back_to_area_names(tmp_path):
    db = Database(str(tmp_path / "mesh.db"))
    destination_id = db.create_destination("Robertson", "meshcore", 3)
    rule_id = db.create_route("Robertson", 10, True)
    db.replace_route_counties(rule_id, [("TNC147", "Robertson")])
    db.replace_route_events(rule_id, [], all_warnings=True)
    db.replace_route_destinations(rule_id, [destination_id])

    assert route_alert(db, _alert(areas="Robertson", zones=[])) == []


def test_absent_ugc_falls_back_to_area_names(tmp_path):
    db = Database(str(tmp_path / "mesh.db"))
    destination_id = db.create_destination("Robertson", "meshcore", 3)
    rule_id = db.create_route("Robertson", 10, True)
    db.replace_route_counties(rule_id, [("TNC147", "Robertson")])
    db.replace_route_events(rule_id, [], all_warnings=True)
    db.replace_route_destinations(rule_id, [destination_id])
    alert = _alert(areas="Robertson", zones=[])
    del alert.raw["properties"]["geocode"]["UGC"]

    matches = route_alert(db, alert)
    assert [match.matched_areas for match in matches] == [("Robertson",)]


def test_delivery_hash_changes_when_only_routed_time_semantics_change():
    first = _alert()
    changed = _alert()
    changed.onset = "2099-01-01T00:30:00+00:00"
    changed.ends = "2099-01-01T02:00:00+00:00"

    assert _delivery_hash(first, ("Smith", "Robertson")) != _delivery_hash(
        changed, ("Smith", "Robertson")
    )
    assert _delivery_hash(first, ("Smith", "Robertson")) == _delivery_hash(
        first, ("Robertson", "Smith")
    )


def test_pending_identity_includes_destination_id(tmp_path):
    poller = WxPoller(Database(str(tmp_path / "mesh.db")), DeferredTx())
    alert = _alert()
    first = RoutedDestination(1, "first", "meshcore", 7, ("Robertson",))
    second = RoutedDestination(2, "second", "meshcore", 7, ("Robertson",))

    assert poller._pending_key(alert, first) != poller._pending_key(alert, second)


def test_delivery_attempt_snapshots_are_durable_and_finalized_by_exact_id(tmp_path):
    path = tmp_path / "mesh.db"
    db = Database(str(path))
    destination_id = db.create_destination("Robertson mesh", "meshcore", 7)

    first_id = db.create_delivery_attempt(
        root_alert_id="root-1", alert_id="alert-1", destination_id=destination_id,
        destination_name="Robertson mesh", transport="meshcore", channel=7,
        matched_areas=["Robertson"], message_text="first payload",
        event="Tornado Warning", headline="first", expires="2099-01-01T01:00:00Z",
        msg_hash="hash-1", disposition="sent",
    )
    second_id = db.create_delivery_attempt(
        root_alert_id="root-1", alert_id="alert-1", destination_id=destination_id,
        destination_name="Robertson mesh", transport="meshcore", channel=7,
        matched_areas=["Robertson"], message_text="retry payload",
        event="Tornado Warning", headline="first", expires="2099-01-01T01:00:00Z",
        msg_hash="hash-1", disposition="sent",
    )
    db.finalize_delivery_attempt(first_id, "failed", "radio unavailable")
    db.finalize_delivery_attempt(second_id, "accepted")
    db.close()

    reopened = Database(str(path))
    attempts = [dict(row) for row in reopened.query_delivery_attempts(alert_id="alert-1")]

    assert [row["id"] for row in attempts] == [second_id, first_id]
    assert [row["state"] for row in attempts] == ["accepted", "failed"]
    assert attempts[1]["error"] == "radio unavailable"
    assert attempts[1]["destination_name"] == "Robertson mesh"
    assert attempts[1]["transport"] == "meshcore" and attempts[1]["channel"] == 7
    assert attempts[1]["matched_areas"] == '["Robertson"]'
    assert attempts[1]["message_text"] == "first payload"
    assert attempts[1]["queued_at"] and attempts[1]["finalized_at"]


def test_expired_delivery_state_is_purged_and_no_longer_blocks_destination_delete(tmp_path):
    db = Database(str(tmp_path / "mesh.db"))
    destination_id = db.create_destination("old", "meshcore", 9)
    db.upsert_delivery_state(
        "old-alert", "old-alert", destination_id, "meshcore", 9,
        ["Robertson"], "Tornado Warning", "old", "2020-01-01T00:00:00+00:00",
        "old-hash", "sent",
    )
    with pytest.raises(sqlite3.IntegrityError):
        db._conn.execute("DELETE FROM destinations WHERE id = ?", (destination_id,))

    assert db.purge_expired_state() == 1
    db._conn.execute("DELETE FROM destinations WHERE id = ?", (destination_id,))
    db._conn.commit()
    assert db.get_destination(destination_id) is None


def test_expiry_uses_parsed_aware_datetimes_and_keeps_invalid_values(tmp_path):
    db = Database(str(tmp_path / "mesh.db"))
    cutoff = datetime.now(timezone.utc) - timedelta(hours=STATE_EXPIRY_HOURS)
    active_with_earlier_local_date = (cutoff + timedelta(hours=1)).astimezone(
        timezone(-timedelta(hours=12))
    ).isoformat()
    expired_with_later_local_date = (cutoff - timedelta(hours=1)).astimezone(
        timezone(timedelta(hours=14))
    ).isoformat()
    for alert_id, expires in (
        ("active-offset", active_with_earlier_local_date),
        ("expired-offset", expired_with_later_local_date),
        ("invalid", "not-a-timestamp"),
    ):
        db.upsert_state(alert_id, "Warning", alert_id, expires, alert_id, "sent", expires)

    assert db.purge_expired_state() == 1
    assert db.get_state("expired-offset") is None
    assert db.get_state("active-offset") is not None
    assert db.get_state("invalid") is not None


def test_destination_delete_is_blocked_while_a_route_references_it(tmp_path):
    db = Database(str(tmp_path / "mesh.db"))
    destination_id = db.create_destination("active", "meshcore", 9)
    rule_id = db.create_route("active route")
    db.replace_route_counties(rule_id, [("TNC147", "Robertson")])
    db.replace_route_destinations(rule_id, [destination_id])

    with pytest.raises(sqlite3.IntegrityError):
        db._conn.execute("DELETE FROM destinations WHERE id = ?", (destination_id,))


def test_channel_references_include_disabled_destinations_and_active_snapshots(tmp_path):
    db = Database(str(tmp_path / "mesh.db"))
    disabled_id = db.create_destination("disabled route", "meshcore", 9, enabled=False)
    moved_id = db.create_destination("moved endpoint", "meshcore", 10)
    db.upsert_delivery_state(
        "active-root", "active-update", moved_id, "meshcore", 9,
        ["Robertson"], "Tornado Warning", "active", "2099-01-01T00:00:00Z",
        "active-hash", "update",
    )
    db.upsert_delivery_state(
        "closed-root", "closed-cancel", moved_id, "meshcore", 9,
        ["Robertson"], "Tornado Warning", "closed", "2099-01-01T00:00:00Z",
        "closed-hash", "cancelled",
    )

    references = db.channel_references("meshcore", 9)

    assert [row["id"] for row in references["destinations"]] == [disabled_id]
    assert references["destinations"][0]["enabled"] == 0
    assert [row["root_alert_id"] for row in references["active_delivery_snapshots"]] == [
        "active-root"
    ]
    assert references["is_referenced"] is True
    assert db.channel_references("meshcore", 8)["is_referenced"] is False


def test_old_database_migrates_without_data_loss_or_automatic_routes(tmp_path):
    path = tmp_path / "old.db"
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE transmit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL,
            channel INTEGER, byte_count INTEGER, success INTEGER,
            manual INTEGER, text TEXT, error TEXT
        );
        INSERT INTO settings(key, value) VALUES ('dry_run', 'false');
        INSERT INTO transmit_log(ts, channel, byte_count, success, manual, text, error)
        VALUES ('2026-01-01T00:00:00+00:00', 3, 7, 1, 0, 'legacy', '');
    """)
    conn.commit()
    conn.close()

    db = Database(str(path))

    row = db.query_transmit_log()[0]
    assert row["text"] == "legacy" and row["channel"] == 3
    assert "transport" in row.keys() and "destination_id" in row.keys()
    assert db.routing_rows() == []
    assert db._conn.execute("SELECT COUNT(*) FROM routing_rules").fetchone()[0] == 0


async def test_cancel_with_empty_geocode_uses_original_destination_and_county(tmp_path):
    db = Database(str(tmp_path / "mesh.db"))
    destination_id = db.create_destination("Robertson mesh", "meshcore", 2)
    rule_id = db.create_route("Robertson warnings", 10, True)
    db.replace_route_counties(rule_id, [("TNC147", "Robertson")])
    db.replace_route_events(rule_id, [], all_warnings=True)
    db.replace_route_destinations(rule_id, [destination_id])
    tx = DestinationTx()
    poller = WxPoller(db, tx)
    rules = FilterRules([], ["Warning"], [])

    await poller._process(_alert().raw, rules, "America/Chicago", 0, False)
    cancel = _alert(areas="", zones=[], alert_id="alert-cancel", message_type="Cancel",
                    references=["alert-1"])
    await poller._process(cancel.raw, rules, "America/Chicago", 0, False)

    assert len(tx.sent) == 2
    assert "CANCELLED" in tx.sent[1][0]
    assert "Robertson County" in tx.sent[1][0]


async def test_same_alert_new_destination_and_changed_counties_are_not_globally_deduped(tmp_path):
    db = Database(str(tmp_path / "mesh.db"))
    first_destination = db.create_destination("first", "meshcore", 1)
    first_rule = db.create_route("Robertson", 10, True)
    db.replace_route_counties(first_rule, [("TNC147", "Robertson"), ("TNC159", "Smith")])
    db.replace_route_events(first_rule, [], all_warnings=True)
    db.replace_route_destinations(first_rule, [first_destination])
    tx = DestinationTx()
    poller = WxPoller(db, tx)
    rules = FilterRules([], ["Warning"], [])

    await poller._process(_alert(areas="Robertson", zones=["TNC147"]).raw,
                          rules, "America/Chicago", 0, False)
    second_destination = db.create_destination("second", "meshtastic", 4)
    second_rule = db.create_route("Smith second", 20, True)
    db.replace_route_counties(second_rule, [("TNC159", "Smith")])
    db.replace_route_events(second_rule, [], all_warnings=True)
    db.replace_route_destinations(second_rule, [second_destination])
    await poller._process(_alert().raw, rules, "America/Chicago", 0, False)

    assert len(tx.sent) == 3
    assert "Smith County" in tx.sent[1][0]  # existing destination got an area update
    assert tx.sent[2][1:3] == ("meshtastic", 4)  # newly matching destination also got it


async def test_referenced_update_combines_current_matches_with_prior_destinations(tmp_path):
    db = Database(str(tmp_path / "mesh.db"))
    first_destination = db.create_destination("first", "meshcore", 1)
    first_rule = db.create_route("first counties", 10, True)
    db.replace_route_counties(first_rule, [("TNC147", "Robertson"), ("TNC159", "Smith")])
    db.replace_route_events(first_rule, [], all_warnings=True)
    db.replace_route_destinations(first_rule, [first_destination])
    tx = DestinationTx()
    poller = WxPoller(db, tx)
    rules = FilterRules([], ["Warning"], [])
    await poller._process(_alert(areas="Robertson", zones=["TNC147"]).raw,
                          rules, "America/Chicago", 0, False)

    second_destination = db.create_destination("second", "meshtastic", 4)
    second_rule = db.create_route("Smith second", 20, True)
    db.replace_route_counties(second_rule, [("TNC159", "Smith")])
    db.replace_route_events(second_rule, [], all_warnings=True)
    db.replace_route_destinations(second_rule, [second_destination])
    update = _alert(alert_id="alert-2", message_type="Update", references=["alert-1"])
    await poller._process(update.raw, rules, "America/Chicago", 0, False)

    assert len(tx.sent) == 3
    assert "Robertson County, Smith County" in tx.sent[1][0]
    assert tx.sent[2][1:3] == ("meshtastic", 4)


async def test_referenced_update_clears_destination_that_lost_its_county_once(tmp_path):
    db = Database(str(tmp_path / "mesh.db"))
    destination_id = db.create_destination("Smith", "meshcore", 1)
    rule_id = db.create_route("Smith warnings", 10, True)
    db.replace_route_counties(rule_id, [("TNC159", "Smith")])
    db.replace_route_events(rule_id, [], all_warnings=True)
    db.replace_route_destinations(rule_id, [destination_id])
    tx = DestinationTx()
    poller = WxPoller(db, tx)
    rules = FilterRules([], ["Warning"], [])
    await poller._process(_alert(areas="Smith", zones=["TNC159"]).raw,
                          rules, "America/Chicago", 0, False)

    update = _alert(areas="Robertson", zones=["TNC147"], alert_id="alert-2",
                    message_type="Update", references=["alert-1"])
    await poller._process(update.raw, rules, "America/Chicago", 0, False)
    await poller._process(update.raw, rules, "America/Chicago", 0, False)

    assert len(tx.sent) == 2
    assert "CLEARED: Tornado Warning for Smith County" in tx.sent[1][0]


async def test_disabled_prior_destination_receives_no_further_messages(tmp_path):
    db = Database(str(tmp_path / "mesh.db"))
    destination_id = db.create_destination("Robertson", "meshcore", 1)
    rule_id = db.create_route("Robertson warnings", 10, True)
    db.replace_route_counties(rule_id, [("TNC147", "Robertson")])
    db.replace_route_events(rule_id, [], all_warnings=True)
    db.replace_route_destinations(rule_id, [destination_id])
    tx = DestinationTx()
    poller = WxPoller(db, tx)
    rules = FilterRules([], ["Warning"], [])
    await poller._process(_alert(areas="Robertson", zones=["TNC147"]).raw,
                          rules, "America/Chicago", 0, False)

    db._conn.execute("UPDATE destinations SET enabled = 0 WHERE id = ?", (destination_id,))
    db._conn.commit()
    cancel = _alert(areas="", zones=[], alert_id="alert-cancel", message_type="Cancel",
                    references=["alert-1"])
    await poller._process(cancel.raw, rules, "America/Chicago", 0, False)

    assert len(tx.sent) == 1


async def test_route_deletion_does_not_block_cancel_to_prior_recipient(tmp_path):
    db = Database(str(tmp_path / "mesh.db"))
    destination_id = db.create_destination("Robertson", "meshcore", 1)
    rule_id = db.create_route("Robertson warnings", 10, True)
    db.replace_route_counties(rule_id, [("TNC147", "Robertson")])
    db.replace_route_events(rule_id, [], all_warnings=True)
    db.replace_route_destinations(rule_id, [destination_id])
    tx = DestinationTx()
    poller = WxPoller(db, tx)
    rules = FilterRules([], ["Warning"], [])
    await poller._process(_alert(areas="Robertson", zones=["TNC147"]).raw,
                          rules, "America/Chicago", 0, False)

    db._conn.execute("DELETE FROM routing_rules WHERE id = ?", (rule_id,))
    db._conn.commit()
    cancel = _alert(areas="", zones=[], alert_id="alert-cancel", message_type="Cancel",
                    references=["alert-1"])
    await poller._process(cancel.raw, rules, "America/Chicago", 0, False)

    assert len(tx.sent) == 2
    assert tx.sent[1][1:3] == ("meshcore", 1)
    assert "Robertson County" in tx.sent[1][0]
