from __future__ import annotations

import json
import sqlite3

import pytest

from app.db import Database, _persisted_rest_action


def feature(action: str = "NEW", *, alert_id: str = "rest-1", ugcs=None,
            expires: str = "2026-09-06T19:00:00+00:00",
            sent: str | None = None, effective: str | None = None,
            onset: str | None = None) -> str:
    product_times = {
        name: value for name, value in (
            ("sent", sent), ("effective", effective), ("onset", onset),
        ) if value is not None
    }
    return json.dumps({
        "id": alert_id,
        "properties": {
            "expires": expires,
            **product_times,
            "geocode": {"UGC": ugcs or ["TNC147"]},
            "parameters": {
                "VTEC": [f"/O.{action}.KOHX.TO.W.0001.260906T1830Z-260906T1900Z/"]
            },
        },
    })


def test_late_followup_success_cannot_accept_or_reconcile_newer_followup():
    db = Database(":memory:")
    destination = db.create_destination("Robertson", "meshtastic", 2)
    db.create_arbitration_candidate_with_destinations(
        canonical_id="c1", event="Tornado Warning", office="KOHX",
        issued_at="2026-09-06T18:30:00+00:00",
        expires_at="2026-09-06T19:00:00+00:00",
        deadline="2026-09-06T18:30:45+00:00",
        rest_feature=feature("NEW"), vtec_key="vtec-1",
        rest_destination_ids=[destination],
    )
    assert db.apply_arbitration_followup(
        "c1", "vtec-1", "EXT", feature("EXT", alert_id="followup-1"),
        "2026-09-06T20:00:00+00:00", [destination],
    )
    assert db.claim_arbitration_followup("c1", "followup-1") == [destination]
    attempt = db.create_delivery_attempt(
        root_alert_id="c1", alert_id="c1", destination_id=destination,
        destination_name="Robertson", transport="meshtastic", channel=2,
        matched_areas=["Robertson"], message_text="old followup", event="Tornado Warning",
        headline="old", expires="2026-09-06T20:00:00+00:00", msg_hash="old",
        disposition="update", arbitration_canonical_id="c1",
        arbitration_followup_id="followup-1",
    )
    assert db.apply_arbitration_followup(
        "c1", "vtec-1", "CAN", feature("CAN", alert_id="followup-2"),
        "2026-09-06T20:00:00+00:00", [destination],
    )
    assert db.claim_arbitration_followup("c1", "followup-2") == [destination]

    assert not db.accept_arbitrated_followup(
        canonical_id="c1", followup_id="followup-1",
        destination_id=destination, attempt_id=attempt,
    )
    db.add_transmit_log(
        2, len("old followup"), True, "old followup", transport="meshtastic",
        destination_id=destination, delivery_attempt_id=attempt,
    )
    assert db.reconcile_successful_arbitration_transmits() == 0
    rows = db._conn.execute(
        "SELECT followup_id,state FROM weather_arbitration_followups "
        "ORDER BY followup_id"
    ).fetchall()
    assert [tuple(row) for row in rows] == [
        ("followup-1", "superseded"), ("followup-2", "queued")
    ]


@pytest.mark.parametrize("vtec", [
    "junk-line\n/O.NEW.KOHX.TO.W.0001.260906T1830Z-260906T1900Z/",
    "/O.NEW.KOHX.TO.W.0001.260906T1830Z-260906T1900Z/\n/junk/",
    " /O.NEW.KOHX.TO.W.0001.260906T1830Z-260906T1900Z/ ",
    (
        "/O.NEW.KOHX.TO.W.0001.260906T1830Z-260906T1900Z/"
        "/O.CON.KOHX.TO.W.0001.260906T1830Z-260906T1900Z/"
    ),
    "junk\u2028/O.NEW.KOHX.TO.W.0001.260906T1830Z-260906T1900Z/",
    "/O.NEW.KOHX.TO.W.0001.260906T1830Z-260906T1900Z/\u2029junk",
    (
        "/O.NEW.KOHX.TO.W.0001.260906T1830Z-260906T1900Z/\u2028"
        "/O.CON.KOHX.TO.W.0001.260906T1830Z-260906T1900Z/"
    ),
    (
        "/O.CON.KOHX.TO.W.0001.260906T1830Z-260906T1900Z/\u2029"
        "/O.NEW.KOHX.TO.W.0001.260906T1830Z-260906T1900Z/"
    ),
])
def test_persisted_rest_action_rejects_non_exact_vtec_parameter(vtec: str) -> None:
    product = json.loads(feature())
    product["properties"]["parameters"]["VTEC"] = [vtec]

    assert _persisted_rest_action(product) is None


def test_persisted_rest_action_handles_exact_vtec_parameter() -> None:
    assert _persisted_rest_action(json.loads(feature())) == "NEW"


def test_current_arbitration_schema_migrates_twice_with_lifecycle_backfill(tmp_path):
    path = tmp_path / "current.db"
    conn = sqlite3.connect(path)
    conn.executescript("""
        PRAGMA foreign_keys=ON;
        CREATE TABLE weather_arbitration_candidates (
            canonical_id TEXT PRIMARY KEY, event TEXT NOT NULL, office TEXT NOT NULL,
            issued_at TEXT NOT NULL, expires_at TEXT NOT NULL, deadline TEXT NOT NULL,
            rest_feature TEXT NOT NULL DEFAULT '', same_feature TEXT NOT NULL DEFAULT '',
            vtec_key TEXT UNIQUE, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        CREATE TABLE weather_arbitration_destinations (
            canonical_id TEXT NOT NULL REFERENCES weather_arbitration_candidates(canonical_id)
                ON DELETE CASCADE,
            destination_id INTEGER NOT NULL,
            state TEXT NOT NULL CHECK(state IN ('pending','queued','accepted','superseded')),
            source TEXT NOT NULL DEFAULT '' CHECK(source IN ('','same','rest')),
            updated_at TEXT NOT NULL,
            PRIMARY KEY(canonical_id, destination_id)
        );
        CREATE TABLE weather_arbitration_followups (
            canonical_id TEXT NOT NULL REFERENCES weather_arbitration_candidates(canonical_id)
                ON DELETE CASCADE,
            followup_id TEXT NOT NULL, destination_id INTEGER NOT NULL,
            feature TEXT NOT NULL,
            action TEXT NOT NULL CHECK(action IN ('CON','EXT','CAN','EXP')),
            state TEXT NOT NULL CHECK(state IN
                ('pending','queued','accepted','no_target','superseded')),
            error TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(canonical_id,followup_id,destination_id)
        );
    """)
    conn.execute(
        "INSERT INTO weather_arbitration_candidates VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        ("c1", "Tornado Warning", "KOHX", "2026-09-06T18:30:00+00:00",
         "2026-09-06T19:00:00+00:00", "2026-09-06T18:30:45+00:00",
         feature("EXT", ugcs=["TNC147", "TNC165"]),
         json.dumps({"properties": {"geocode": {"UGC": ["TNC147"]}}}),
         "vtec-1", "2026-09-06T18:30:00+00:00", "2026-09-06T18:31:00+00:00"),
    )
    conn.execute(
        "INSERT INTO weather_arbitration_destinations VALUES (?,?,?,?,?)",
        ("c1", 7, "accepted", "same", "2026-09-06T18:31:00+00:00"),
    )
    stored_followup_feature = json.dumps(
        json.loads(feature("CAN", alert_id="cancel-1")), indent=2
    )
    conn.execute(
        "INSERT INTO weather_arbitration_followups VALUES (?,?,?,?,?,?,?,?,?)",
        ("c1", "cancel-1", 7, stored_followup_feature, "CAN",
         "pending", "retry", "2026-09-06T18:32:00+00:00",
         "2026-09-06T18:32:00+00:00"),
    )
    conn.commit()
    conn.close()

    for _ in range(2):
        db = Database(str(path))
        row = db.get_arbitration_candidate("c1")
        assert row is not None
        assert row["phase"] == "updated"
        assert row["retain_until"] == "2026-09-08T19:00:00+00:00"
        assert json.loads(row["correlation_ugcs"]) == ["TNC147", "TNC165"]
        destination = db._conn.execute(
            "SELECT * FROM weather_arbitration_destinations WHERE canonical_id='c1'"
        ).fetchone()
        assert destination["state"] == "accepted"
        assert destination["source"] == "same"
        assert destination["same_eligible"] == 1
        assert destination["rest_eligible"] == 0
        followup = db._conn.execute(
            "SELECT * FROM weather_arbitration_followups WHERE followup_id='cancel-1'"
        ).fetchone()
        assert followup is not None
        assert followup["state"] == "pending"
        assert followup["action"] == "CAN"
        assert followup["feature"] == stored_followup_feature
        assert db._conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert db._conn.execute("PRAGMA foreign_key_check").fetchall() == []
        db.close()


def test_migration_scrubs_present_invalid_same_without_enabling_rest_fallback(
    tmp_path,
):
    path = tmp_path / "raw-same.db"
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE weather_arbitration_candidates (
            canonical_id TEXT PRIMARY KEY, event TEXT NOT NULL, office TEXT NOT NULL,
            issued_at TEXT NOT NULL, expires_at TEXT NOT NULL, deadline TEXT NOT NULL,
            rest_feature TEXT NOT NULL DEFAULT '', same_feature TEXT NOT NULL DEFAULT '',
            vtec_key TEXT UNIQUE, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        CREATE TABLE weather_arbitration_destinations (
            canonical_id TEXT NOT NULL REFERENCES weather_arbitration_candidates(canonical_id)
                ON DELETE CASCADE,
            destination_id INTEGER NOT NULL,
            state TEXT NOT NULL CHECK(state IN ('pending','queued','accepted','superseded')),
            source TEXT NOT NULL DEFAULT '' CHECK(source IN ('','same','rest')),
            updated_at TEXT NOT NULL,
            PRIMARY KEY(canonical_id, destination_id)
        );
    """)
    conn.execute(
        "INSERT INTO weather_arbitration_candidates VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        ("c1", "Tornado Warning", "KOHX", "2026-09-06T18:30:00+00:00",
         "2026-09-06T19:00:00+00:00", "2026-09-06T18:30:45+00:00",
         feature("NEW"), "ZCZC-WXR-TOR-047125+0030-2491830-KOHX/NWS-", "vtec-1",
         "2026-09-06T18:30:00+00:00", "2026-09-06T18:30:00+00:00"),
    )
    conn.execute(
        "INSERT INTO weather_arbitration_destinations VALUES (?,?,?,?,?)",
        ("c1", 7, "pending", "", "2026-09-06T18:30:00+00:00"),
    )
    conn.commit()
    conn.close()

    db = Database(str(path))
    row = db.get_arbitration_candidate("c1")
    assert row is not None
    assert row["phase"] == "initial"
    assert row["same_feature"] == ""
    assert row["rest_feature"] != ""
    destination = db._conn.execute(
        "SELECT rest_eligible FROM weather_arbitration_destinations "
        "WHERE canonical_id='c1' AND destination_id=7"
    ).fetchone()
    assert destination["rest_eligible"] == 0
    assert db.due_arbitration_candidates("2026-09-06T18:31:00+00:00") == []
    assert db.claim_arbitration_destinations("c1", [7], "rest") == []
    db.close()


def test_migration_scrubs_malformed_nonempty_same_without_enabling_rest_fallback(
    tmp_path,
):
    path = tmp_path / "malformed-same.db"
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE weather_arbitration_candidates (
            canonical_id TEXT PRIMARY KEY, event TEXT NOT NULL, office TEXT NOT NULL,
            issued_at TEXT NOT NULL, expires_at TEXT NOT NULL, deadline TEXT NOT NULL,
            rest_feature TEXT NOT NULL DEFAULT '', same_feature TEXT NOT NULL DEFAULT '',
            vtec_key TEXT UNIQUE, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        CREATE TABLE weather_arbitration_destinations (
            canonical_id TEXT NOT NULL REFERENCES weather_arbitration_candidates(canonical_id)
                ON DELETE CASCADE,
            destination_id INTEGER NOT NULL,
            state TEXT NOT NULL CHECK(state IN ('pending','queued','accepted','superseded')),
            source TEXT NOT NULL DEFAULT '' CHECK(source IN ('','same','rest')),
            updated_at TEXT NOT NULL,
            PRIMARY KEY(canonical_id, destination_id)
        );
    """)
    conn.execute(
        "INSERT INTO weather_arbitration_candidates VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        ("c1", "Tornado Warning", "KOHX", "2026-09-06T18:30:00+00:00",
         "2026-09-06T19:00:00+00:00", "2026-09-06T18:30:45+00:00",
         feature("NEW"), "not-json", "vtec-1", "2026-09-06T18:30:00+00:00",
         "2026-09-06T18:30:00+00:00"),
    )
    conn.execute(
        "INSERT INTO weather_arbitration_destinations VALUES (?,?,?,?,?)",
        ("c1", 7, "pending", "", "2026-09-06T18:30:00+00:00"),
    )
    conn.commit()
    conn.close()

    db = Database(str(path))

    assert db.get_arbitration_candidate("c1")["same_feature"] == ""
    destination = db._conn.execute(
        "SELECT rest_eligible FROM weather_arbitration_destinations"
    ).fetchone()
    assert destination["rest_eligible"] == 0
    assert db.due_arbitration_candidates("2026-09-06T18:31:00+00:00") == []
    assert db.claim_arbitration_destinations("c1", [7], "rest") == []
    db.close()


def test_legacy_migration_preserves_only_unambiguous_rest_pending_fallback(tmp_path):
    path = tmp_path / "rest-only-pending.db"
    conn = sqlite3.connect(path)
    conn.executescript("""
        PRAGMA foreign_keys=ON;
        CREATE TABLE weather_arbitration_candidates (
            canonical_id TEXT PRIMARY KEY, event TEXT NOT NULL, office TEXT NOT NULL,
            issued_at TEXT NOT NULL, expires_at TEXT NOT NULL, deadline TEXT NOT NULL,
            rest_feature TEXT NOT NULL DEFAULT '', same_feature TEXT NOT NULL DEFAULT '',
            vtec_key TEXT UNIQUE, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        CREATE TABLE weather_arbitration_destinations (
            canonical_id TEXT NOT NULL REFERENCES weather_arbitration_candidates(canonical_id)
                ON DELETE CASCADE,
            destination_id INTEGER NOT NULL,
            state TEXT NOT NULL CHECK(state IN ('pending','queued','accepted','superseded')),
            source TEXT NOT NULL DEFAULT '' CHECK(source IN ('','same','rest')),
            updated_at TEXT NOT NULL,
            PRIMARY KEY(canonical_id, destination_id)
        );
    """)
    candidate_values = (
        "Tornado Warning", "KOHX", "2026-09-06T18:30:00+00:00",
        "2026-09-06T19:00:00+00:00", "2026-09-06T18:30:45+00:00",
    )
    conn.execute(
        "INSERT INTO weather_arbitration_candidates VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        ("rest-only", *candidate_values, feature("NEW"), "", "vtec-rest",
         "2026-09-06T18:30:00+00:00", "2026-09-06T18:30:00+00:00"),
    )
    conn.execute(
        "INSERT INTO weather_arbitration_candidates VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        ("mixed", *candidate_values, feature("NEW", alert_id="mixed"),
         json.dumps({"id": "same"}), "vtec-mixed",
         "2026-09-06T18:30:00+00:00", "2026-09-06T18:30:00+00:00"),
    )
    conn.executemany(
        "INSERT INTO weather_arbitration_destinations VALUES (?,?,?,?,?)",
        [
            ("rest-only", 7, "pending", "", "2026-09-06T18:30:00+00:00"),
            ("mixed", 8, "pending", "", "2026-09-06T18:30:00+00:00"),
        ],
    )
    conn.commit()
    conn.close()

    for _ in range(2):
        db = Database(str(path))
        rows = db._conn.execute(
            "SELECT canonical_id,state,source,same_eligible,rest_eligible "
            "FROM weather_arbitration_destinations ORDER BY canonical_id"
        ).fetchall()
        assert [tuple(row) for row in rows] == [
            ("mixed", "pending", "", 0, 0),
            ("rest-only", "pending", "", 0, 1),
        ]
        due = db.due_arbitration_candidates("2026-09-06T18:31:00+00:00")
        assert [row["canonical_id"] for row in due] == ["rest-only"]
        assert db.claim_arbitration_destinations("rest-only", [7], "rest") == [7]
        db.release_arbitration_destinations("rest-only", [7], "rest")
        assert db._conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert db._conn.execute("PRAGMA foreign_key_check").fetchall() == []
        db.close()


def test_legacy_migration_rejects_action_like_but_invalid_rest_vtec(tmp_path):
    path = tmp_path / "invalid-rest-vtec.db"
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE weather_arbitration_candidates (
            canonical_id TEXT PRIMARY KEY, event TEXT NOT NULL, office TEXT NOT NULL,
            issued_at TEXT NOT NULL, expires_at TEXT NOT NULL, deadline TEXT NOT NULL,
            rest_feature TEXT NOT NULL DEFAULT '', same_feature TEXT NOT NULL DEFAULT '',
            vtec_key TEXT UNIQUE, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        CREATE TABLE weather_arbitration_destinations (
            canonical_id TEXT NOT NULL REFERENCES weather_arbitration_candidates(canonical_id)
                ON DELETE CASCADE,
            destination_id INTEGER NOT NULL,
            state TEXT NOT NULL CHECK(state IN ('pending','queued','accepted','superseded')),
            source TEXT NOT NULL DEFAULT '' CHECK(source IN ('','same','rest')),
            updated_at TEXT NOT NULL,
            PRIMARY KEY(canonical_id, destination_id)
        );
    """)
    malformed = json.loads(feature("NEW"))
    malformed["properties"]["parameters"]["VTEC"] = ["garbage /O.NEW. garbage"]
    conn.execute(
        "INSERT INTO weather_arbitration_candidates VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        ("invalid", "Tornado Warning", "KOHX", "2026-09-06T18:30:00+00:00",
         "2026-09-06T19:00:00+00:00", "2026-09-06T18:30:45+00:00",
         json.dumps(malformed), "", "vtec-invalid", "2026-09-06T18:30:00+00:00",
         "2026-09-06T18:30:00+00:00"),
    )
    conn.execute(
        "INSERT INTO weather_arbitration_destinations VALUES (?,?,?,?,?)",
        ("invalid", 7, "pending", "", "2026-09-06T18:30:00+00:00"),
    )
    conn.commit()
    conn.close()

    db = Database(str(path))

    candidate = db.get_arbitration_candidate("invalid")
    assert candidate["phase"] == "expired"
    assert candidate["rest_feature"] == ""
    destination = db._conn.execute(
        "SELECT rest_eligible FROM weather_arbitration_destinations"
    ).fetchone()
    assert destination["rest_eligible"] == 0
    assert db.due_arbitration_candidates("2026-09-06T18:31:00+00:00") == []
    assert db.claim_arbitration_destinations("invalid", [7], "rest") == []
    db.close()


def test_atomic_creation_persists_initial_memberships_and_retention():
    db = Database(":memory:")
    db.create_arbitration_candidate_with_destinations(
        canonical_id="c1", event="Tornado Warning", office="KOHX",
        issued_at="2026-09-06T18:30:00+00:00",
        expires_at="2026-09-06T19:00:00+00:00",
        deadline="2026-09-06T18:30:45+00:00",
        same_destination_ids=[1, 2], rest_destination_ids=[2, 3],
        correlation_ugcs=["TNC165", "TNC147"],
    )

    candidate = db.get_arbitration_candidate("c1")
    assert candidate["phase"] == "initial"
    assert candidate["retain_until"] == "2026-09-08T19:00:00+00:00"
    assert json.loads(candidate["correlation_ugcs"]) == ["TNC147", "TNC165"]
    rows = db._conn.execute(
        "SELECT destination_id,state,same_eligible,rest_eligible "
        "FROM weather_arbitration_destinations ORDER BY destination_id"
    ).fetchall()
    assert [tuple(row) for row in rows] == [
        (1, "pending", 1, 0), (2, "pending", 1, 1), (3, "pending", 0, 1),
    ]


def test_record_same_on_non_initial_candidate_is_superseded_and_unclaimable():
    db = Database(":memory:")
    db.create_arbitration_candidate(
        canonical_id="c1", event="Tornado Warning", office="KOHX",
        issued_at="2026-09-06T18:30:00+00:00",
        expires_at="2026-09-06T19:00:00+00:00",
        deadline="2026-09-06T18:30:45+00:00",
        rest_feature=feature("CAN"), vtec_key="vtec-1",
    )
    db._conn.execute(
        "UPDATE weather_arbitration_candidates SET phase='cancelled' WHERE canonical_id='c1'"
    )
    db._conn.commit()

    claimable = db.record_arbitration_same(
        "c1", json.dumps({"id": "same-1", "properties": {
            "geocode": {"UGC": ["TNC165"]}}}), [11], ["TNC165"]
    )

    assert claimable == []
    candidate = db.get_arbitration_candidate("c1")
    assert json.loads(candidate["correlation_ugcs"]) == ["TNC147", "TNC165"]
    row = db._conn.execute(
        "SELECT state,same_eligible,rest_eligible FROM weather_arbitration_destinations"
    ).fetchone()
    assert tuple(row) == ("superseded", 1, 0)


def test_rest_new_refreshes_exact_eligibility_and_closes_safe_same_only_rows():
    db = Database(":memory:")
    db.create_arbitration_candidate_with_destinations(
        canonical_id="c1", event="Tornado Warning", office="KOHX",
        issued_at="2026-09-06T18:30:00+00:00",
        expires_at="2026-09-06T19:00:00+00:00",
        deadline="2026-09-06T18:30:45+00:00",
        same_feature=json.dumps({"id": "same"}), same_destination_ids=[1, 2],
    )
    db._conn.execute(
        "UPDATE weather_arbitration_destinations SET state='accepted',source='same' "
        "WHERE destination_id=2"
    )
    db._conn.commit()

    assert db.bind_arbitration_rest_new(
        "c1", feature("NEW", ugcs=["TNC147", "TNC165"]), "vtec-1", [2, 3],
        expires_at="2026-09-06T20:00:00+00:00",
        correlation_ugcs=["TNC147", "TNC165"],
    )

    rows = db._conn.execute(
        "SELECT destination_id,state,source,same_eligible,rest_eligible "
        "FROM weather_arbitration_destinations ORDER BY destination_id"
    ).fetchall()
    assert [tuple(row) for row in rows] == [
        (1, "no_fallback", "", 1, 0),
        (2, "accepted", "same", 1, 1),
        (3, "pending", "", 0, 1),
    ]
    candidate = db.get_arbitration_candidate("c1")
    assert candidate["expires_at"] == "2026-09-06T20:00:00+00:00"
    assert candidate["retain_until"] == "2026-09-08T20:00:00+00:00"


def test_ext_followup_extends_expiry_retention_and_prunes_only_at_new_retention():
    db = Database(":memory:")
    db.create_arbitration_candidate_with_destinations(
        canonical_id="c1", event="Tornado Warning", office="KOHX",
        issued_at="2026-09-06T18:30:00+00:00",
        expires_at="2026-09-06T19:00:00+00:00",
        deadline="2026-09-06T18:30:45+00:00",
        rest_feature=feature("NEW"), vtec_key="vtec-1", rest_destination_ids=[1],
    )

    assert db.apply_arbitration_followup(
        "c1", "vtec-1", "EXT", feature(
            "EXT", alert_id="ext-1", expires="2026-09-06T22:00:00+00:00"
        ), "2026-09-06T22:00:00+00:00", [1], ["TNC147"],
        now="2026-09-06T20:00:00+00:00",
    )

    row = db.get_arbitration_candidate("c1")
    assert row["phase"] == "updated"
    assert row["expires_at"] == "2026-09-06T22:00:00+00:00"
    assert row["retain_until"] == "2026-09-08T22:00:00+00:00"
    assert db.prune_arbitration_candidates("2026-09-08T19:00:01+00:00") == 0
    assert db.prune_arbitration_candidates("2026-09-08T22:00:00+00:00") == 1


def test_stale_con_followup_cannot_replace_newer_ext_feature():
    db = Database(":memory:")
    db.create_arbitration_candidate_with_destinations(
        canonical_id="c1", event="Tornado Warning", office="KOHX",
        issued_at="2026-09-06T18:30:00+00:00",
        expires_at="2026-09-06T19:00:00+00:00",
        deadline="2026-09-06T18:30:45+00:00",
        rest_feature=feature("NEW"), vtec_key="vtec-1", rest_destination_ids=[1],
    )
    extension = feature(
        "EXT", alert_id="ext-newer", expires="2026-09-06T22:00:00+00:00",
        sent="2026-09-06T19:10:00+00:00",
    )
    assert db.apply_arbitration_followup(
        "c1", "vtec-1", "EXT", extension, "2026-09-06T22:00:00+00:00", [1],
        now="2026-09-06T19:10:00+00:00",
    )
    assert db.apply_arbitration_followup(
        "c1", "vtec-1", "EXT", extension, "2026-09-06T22:00:00+00:00", [1],
        now="2026-09-06T19:10:30+00:00",
    )
    stale_continuation = feature(
        "CON", alert_id="con-older", expires="2026-09-06T20:00:00+00:00",
        sent="2026-09-06T19:05:00+00:00",
    )

    assert not db.apply_arbitration_followup(
        "c1", "vtec-1", "CON", stale_continuation,
        "2026-09-06T20:00:00+00:00", [1],
        now="2026-09-06T19:11:00+00:00",
    )

    candidate = db.get_arbitration_candidate("c1")
    assert candidate["rest_feature"] == extension
    assert candidate["expires_at"] == "2026-09-06T22:00:00+00:00"


@pytest.mark.parametrize("action", ["CAN", "EXP"])
def test_stale_terminal_followup_cannot_replace_newer_ext_feature(action):
    db = Database(":memory:")
    db.create_arbitration_candidate_with_destinations(
        canonical_id="c1", event="Tornado Warning", office="KOHX",
        issued_at="2026-09-06T18:30:00+00:00",
        expires_at="2026-09-06T19:00:00+00:00",
        deadline="2026-09-06T18:30:45+00:00",
        rest_feature=feature("NEW"), vtec_key="vtec-1", rest_destination_ids=[1],
    )
    extension = feature(
        "EXT", alert_id="ext-newer", expires="2026-09-06T22:00:00+00:00",
        sent="2026-09-06T19:10:00+00:00",
    )
    assert db.apply_arbitration_followup(
        "c1", "vtec-1", "EXT", extension, "2026-09-06T22:00:00+00:00", [1],
    )
    destinations_before = [tuple(row) for row in db._conn.execute(
        "SELECT destination_id,state,source,rest_eligible "
        "FROM weather_arbitration_destinations WHERE canonical_id='c1'"
    ).fetchall()]
    stale_terminal = feature(
        action, alert_id=f"{action.lower()}-older", ugcs=["TNC165"],
        expires="2026-09-06T20:00:00+00:00",
        sent="2026-09-06T19:05:00+00:00",
    )

    assert not db.apply_arbitration_followup(
        "c1", "vtec-1", action, stale_terminal,
        "2026-09-06T20:00:00+00:00", [2],
    )

    candidate = db.get_arbitration_candidate("c1")
    assert candidate["phase"] == "updated"
    assert candidate["rest_feature"] == extension
    assert candidate["expires_at"] == "2026-09-06T22:00:00+00:00"
    assert [tuple(row) for row in db._conn.execute(
        "SELECT destination_id,state,source,rest_eligible "
        "FROM weather_arbitration_destinations WHERE canonical_id='c1'"
    ).fetchall()] == destinations_before

    later_terminal = feature(
        action, alert_id=f"{action.lower()}-later",
        expires="2026-09-06T20:00:00+00:00",
        sent="2026-09-06T19:15:00+00:00",
    )
    assert db.apply_arbitration_followup(
        "c1", "vtec-1", action, later_terminal,
        "2026-09-06T20:00:00+00:00", [1],
    )
    assert db.get_arbitration_candidate("c1")["phase"] == (
        "cancelled" if action == "CAN" else "expired"
    )


@pytest.mark.parametrize(
    ("fallback_effective", "accepted"),
    [
        ("2026-09-06T19:05:00+00:00", False),
        ("2026-09-06T19:15:00+00:00", True),
    ],
)
def test_malformed_sent_uses_valid_effective_for_product_ordering(
    fallback_effective, accepted,
):
    db = Database(":memory:")
    db.create_arbitration_candidate_with_destinations(
        canonical_id="c1", event="Tornado Warning", office="KOHX",
        issued_at="2026-09-06T18:30:00+00:00",
        expires_at="2026-09-06T19:00:00+00:00",
        deadline="2026-09-06T18:30:45+00:00",
        rest_feature=feature("NEW"), vtec_key="vtec-1", rest_destination_ids=[1],
    )
    current = feature(
        "EXT", alert_id="ext-current", expires="2026-09-06T22:00:00+00:00",
        sent="2026-09-06T19:10:00+00:00",
        effective="2026-09-06T19:10:00+00:00",
    )
    assert db.apply_arbitration_followup(
        "c1", "vtec-1", "EXT", current, "2026-09-06T22:00:00+00:00", [1],
    )
    incoming = feature(
        "EXT", alert_id="ext-fallback", expires="2026-09-06T20:00:00+00:00",
        sent="not-a-timestamp", effective=fallback_effective,
    )

    assert db.apply_arbitration_followup(
        "c1", "vtec-1", "EXT", incoming, "2026-09-06T20:00:00+00:00", [1],
    ) is accepted

    candidate = db.get_arbitration_candidate("c1")
    assert candidate["rest_feature"] == (incoming if accepted else current)
    assert candidate["expires_at"] == "2026-09-06T22:00:00+00:00"


def test_newer_product_may_shorten_current_alert_but_retention_stays_monotonic():
    db = Database(":memory:")
    db.create_arbitration_candidate_with_destinations(
        canonical_id="c1", event="Tornado Warning", office="KOHX",
        issued_at="2026-09-06T18:30:00+00:00",
        expires_at="2026-09-06T19:00:00+00:00",
        deadline="2026-09-06T18:30:45+00:00",
        rest_feature=feature("NEW"), vtec_key="vtec-1", rest_destination_ids=[1],
    )
    extension = feature(
        "EXT", alert_id="ext", expires="2026-09-06T22:00:00+00:00",
        sent="2026-09-06T19:10:00+00:00",
        effective="2026-09-06T19:30:00+00:00",
    )
    assert db.apply_arbitration_followup(
        "c1", "vtec-1", "EXT", extension, "2026-09-06T22:00:00+00:00", [1],
        now="2026-09-06T19:10:00+00:00",
    )
    newer_continuation = feature(
        "CON", alert_id="con-newer", expires="2026-09-06T20:00:00+00:00",
        sent="2026-09-06T19:15:00+00:00",
        effective="2026-09-06T19:00:00+00:00",
    )

    assert db.apply_arbitration_followup(
        "c1", "vtec-1", "CON", newer_continuation,
        "2026-09-06T20:00:00+00:00", [1],
        now="2026-09-06T19:15:00+00:00",
    )

    candidate = db.get_arbitration_candidate("c1")
    assert candidate["rest_feature"] == newer_continuation
    assert candidate["expires_at"] == "2026-09-06T22:00:00+00:00"
    assert candidate["retain_until"] == "2026-09-08T22:00:00+00:00"


def test_equal_product_time_cannot_switch_ext_to_shorter_con():
    db = Database(":memory:")
    db.create_arbitration_candidate_with_destinations(
        canonical_id="c1", event="Tornado Warning", office="KOHX",
        issued_at="2026-09-06T18:30:00+00:00",
        expires_at="2026-09-06T19:00:00+00:00",
        deadline="2026-09-06T18:30:45+00:00",
        rest_feature=feature("NEW"), vtec_key="vtec-1", rest_destination_ids=[1],
    )
    extension = feature(
        "EXT", alert_id="ext", expires="2026-09-06T22:00:00+00:00",
        sent="2026-09-06T19:10:00+00:00",
    )
    assert db.apply_arbitration_followup(
        "c1", "vtec-1", "EXT", extension, "2026-09-06T22:00:00+00:00", [1],
    )

    assert not db.apply_arbitration_followup(
        "c1", "vtec-1", "CON",
        feature(
            "CON", alert_id="con", expires="2026-09-06T20:00:00+00:00",
            sent="2026-09-06T19:10:00+00:00",
        ),
        "2026-09-06T20:00:00+00:00", [1],
    )
    assert db.get_arbitration_candidate("c1")["rest_feature"] == extension


@pytest.mark.parametrize("action", ["EXT", "CON"])
def test_equal_product_time_same_action_cannot_regress_expiry(action):
    db = Database(":memory:")
    db.create_arbitration_candidate_with_destinations(
        canonical_id="c1", event="Tornado Warning", office="KOHX",
        issued_at="2026-09-06T18:30:00+00:00",
        expires_at="2026-09-06T19:00:00+00:00",
        deadline="2026-09-06T18:30:45+00:00",
        rest_feature=feature("NEW"), vtec_key="vtec-1", rest_destination_ids=[1],
    )
    current = feature(
        action, alert_id=f"{action.lower()}-current",
        expires="2026-09-06T22:00:00+00:00",
        sent="2026-09-06T19:10:00+00:00",
    )
    assert db.apply_arbitration_followup(
        "c1", "vtec-1", action, current, "2026-09-06T22:00:00+00:00", [1],
    )
    before = db.get_arbitration_candidate("c1")
    assert before is not None

    regressing = feature(
        action, alert_id=f"{action.lower()}-different",
        ugcs=["TNC165"], expires="2026-09-06T20:00:00+00:00",
        sent="2026-09-06T19:10:00+00:00",
    )
    assert not db.apply_arbitration_followup(
        "c1", "vtec-1", action, regressing,
        "2026-09-06T20:00:00+00:00", [2],
    )

    after = db.get_arbitration_candidate("c1")
    assert after is not None
    assert after["rest_feature"] == current
    assert after["phase"] == before["phase"] == "updated"
    assert after["expires_at"] == before["expires_at"] == "2026-09-06T22:00:00+00:00"
    assert after["retain_until"] == before["retain_until"]


@pytest.mark.parametrize("incoming_action", ["EXT", "CON", "CAN", "EXP"])
def test_unordered_differing_followup_cannot_regress_current_expiry(incoming_action):
    db = Database(":memory:")
    db.create_arbitration_candidate_with_destinations(
        canonical_id="c1", event="Tornado Warning", office="KOHX",
        issued_at="2026-09-06T18:30:00+00:00",
        expires_at="2026-09-06T19:00:00+00:00",
        deadline="2026-09-06T18:30:45+00:00",
        rest_feature=feature("NEW"), vtec_key="vtec-1", rest_destination_ids=[1],
    )
    extension = feature(
        "EXT", alert_id="ext", expires="2026-09-06T22:00:00+00:00",
    )
    assert db.apply_arbitration_followup(
        "c1", "vtec-1", "EXT", extension, "2026-09-06T22:00:00+00:00", [1],
    )

    assert db.apply_arbitration_followup(
        "c1", "vtec-1", "EXT", extension, "2026-09-06T22:00:00+00:00", [99],
    )
    assert not db.apply_arbitration_followup(
        "c1", "vtec-1", incoming_action,
        feature(
            incoming_action, alert_id=f"{incoming_action.lower()}-different",
            expires="2026-09-06T20:00:00+00:00",
        ),
        "2026-09-06T20:00:00+00:00", [1],
    )
    assert db.get_arbitration_candidate("c1")["rest_feature"] == extension


def test_terminal_followup_replay_is_exact_idempotent_and_retention_only_grows():
    db = Database(":memory:")
    db.create_arbitration_candidate_with_destinations(
        canonical_id="c1", event="Tornado Warning", office="KOHX",
        issued_at="2026-09-06T18:30:00+00:00",
        expires_at="2026-09-06T19:00:00+00:00",
        deadline="2026-09-06T18:30:45+00:00",
        rest_feature=feature("NEW"), vtec_key="vtec-1", rest_destination_ids=[1],
    )
    terminal = feature("CAN", alert_id="cancel-1")
    assert db.apply_arbitration_followup(
        "c1", "vtec-1", "CAN", terminal, "2026-09-06T19:00:00+00:00", [1],
        now="2026-09-06T20:00:00+00:00",
    )
    first = db.get_arbitration_candidate("c1")

    assert db.apply_arbitration_followup(
        "c1", "vtec-1", "CAN", terminal, "2026-09-06T19:00:00+00:00", [99],
        now="2026-09-07T20:00:00+00:00",
    )
    replayed = db.get_arbitration_candidate("c1")
    assert replayed["phase"] == "cancelled"
    assert replayed["retain_until"] > first["retain_until"]
    assert [tuple(row) for row in db._conn.execute(
        "SELECT destination_id FROM weather_arbitration_destinations ORDER BY destination_id"
    ).fetchall()] == [(1,)]

    assert not db.apply_arbitration_followup(
        "c1", "vtec-1", "EXP", feature("EXP", alert_id="expire-1"),
        "2026-09-06T19:00:00+00:00", [1], now="2026-09-07T21:00:00+00:00",
    )
    assert not db.apply_arbitration_followup(
        "c1", "vtec-1", "CAN", feature("CAN", alert_id="different-cancel"),
        "2026-09-06T19:00:00+00:00", [1], now="2026-09-07T21:00:00+00:00",
    )
    assert db.get_arbitration_candidate("c1")["phase"] == "cancelled"


def test_due_and_rest_claim_require_initial_pending_rest_eligible():
    db = Database(":memory:")
    for canonical_id in ("initial", "updated"):
        db.create_arbitration_candidate_with_destinations(
            canonical_id=canonical_id, event="Tornado Warning", office="KOHX",
            issued_at="2026-09-06T18:30:00+00:00",
            expires_at="2026-09-06T19:00:00+00:00",
            deadline="2026-09-06T18:30:45+00:00",
            rest_feature=feature("NEW", alert_id=canonical_id),
            rest_destination_ids=[1, 2], same_destination_ids=[3],
        )
    db._conn.execute(
        "UPDATE weather_arbitration_candidates SET phase='updated' WHERE canonical_id='updated'"
    )
    db._conn.execute(
        "UPDATE weather_arbitration_destinations SET state='no_fallback' "
        "WHERE canonical_id='initial' AND destination_id=2"
    )
    db._conn.commit()

    due = db.due_arbitration_candidates("2026-09-06T18:31:00+00:00")
    assert [row["canonical_id"] for row in due] == ["initial"]
    assert db.claim_arbitration_destinations("initial", [1, 2, 3], "rest") == [1]
    assert db.claim_arbitration_destinations("updated", [1], "rest") == []


def test_queued_recovery_respects_candidate_phase_and_accepted_is_unchanged():
    db = Database(":memory:")
    for canonical_id, phase in (("initial", "initial"), ("updated", "updated")):
        db.create_arbitration_candidate_with_destinations(
            canonical_id=canonical_id, event="Tornado Warning", office="KOHX",
            issued_at="2026-09-06T18:30:00+00:00",
            expires_at="2026-09-06T19:00:00+00:00",
            deadline="2026-09-06T18:30:45+00:00",
            rest_feature=feature("NEW"), rest_destination_ids=[1, 2],
        )
        db._conn.execute(
            "UPDATE weather_arbitration_candidates SET phase=? WHERE canonical_id=?",
            (phase, canonical_id),
        )
        db._conn.execute(
            "UPDATE weather_arbitration_destinations SET state='queued',source='rest' "
            "WHERE canonical_id=? AND destination_id=1", (canonical_id,),
        )
        db._conn.execute(
            "UPDATE weather_arbitration_destinations SET state='accepted',source='rest' "
            "WHERE canonical_id=? AND destination_id=2", (canonical_id,),
        )
        db._conn.commit()

    assert db.recover_queued_arbitration_destinations() == 2
    rows = db._conn.execute(
        "SELECT canonical_id,destination_id,state,source "
        "FROM weather_arbitration_destinations ORDER BY canonical_id,destination_id"
    ).fetchall()
    assert [tuple(row) for row in rows] == [
        ("initial", 1, "pending", ""), ("initial", 2, "accepted", "rest"),
        ("updated", 1, "superseded", ""), ("updated", 2, "accepted", "rest"),
    ]
