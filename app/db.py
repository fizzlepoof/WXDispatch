"""SQLite storage: settings, alert state (dedupe), history, transmit log, errors.

The database is the source of truth. A single connection is shared across the
asyncio loop and the transmit worker thread, guarded by a lock. SQLite calls
are fast local operations, so running them synchronously is fine.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .config import DEFAULT_SETTINGS, STATE_EXPIRY_HOURS
from .nwws import parse_vtec_parameter

SCHEMA = """
CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS alert_state (
    nws_id      TEXT PRIMARY KEY,
    event       TEXT,
    headline    TEXT,
    expires     TEXT,
    msg_hash    TEXT,
    sent_ts     TEXT,
    disposition TEXT,
    updated_at  TEXT
);

CREATE TABLE IF NOT EXISTS destinations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    transport TEXT NOT NULL CHECK(transport IN ('meshcore','meshtastic')),
    channel INTEGER NOT NULL CHECK(channel BETWEEN 0 AND 255),
    enabled INTEGER NOT NULL DEFAULT 1,
    UNIQUE(transport, channel)
);

CREATE TABLE IF NOT EXISTS routing_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    priority INTEGER NOT NULL DEFAULT 100,
    all_warnings INTEGER NOT NULL DEFAULT 0,
    all_warnings_details INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS route_counties (
    rule_id INTEGER NOT NULL REFERENCES routing_rules(id) ON DELETE CASCADE,
    zone_code TEXT NOT NULL,
    county_name TEXT NOT NULL,
    PRIMARY KEY(rule_id, zone_code)
);

CREATE TABLE IF NOT EXISTS route_events (
    rule_id INTEGER NOT NULL REFERENCES routing_rules(id) ON DELETE CASCADE,
    event TEXT NOT NULL,
    detail_enabled INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY(rule_id, event)
);

CREATE TABLE IF NOT EXISTS route_destinations (
    rule_id INTEGER NOT NULL REFERENCES routing_rules(id) ON DELETE CASCADE,
    destination_id INTEGER NOT NULL REFERENCES destinations(id) ON DELETE RESTRICT,
    PRIMARY KEY(rule_id, destination_id)
);

CREATE TABLE IF NOT EXISTS alert_delivery_state (
    chain_id INTEGER PRIMARY KEY AUTOINCREMENT,
    root_alert_id TEXT NOT NULL,
    last_alert_id TEXT NOT NULL,
    destination_id INTEGER NOT NULL REFERENCES destinations(id) ON DELETE RESTRICT,
    transport TEXT NOT NULL,
    channel INTEGER NOT NULL,
    matched_areas TEXT NOT NULL DEFAULT '[]',
    event TEXT, headline TEXT, expires TEXT, msg_hash TEXT,
    disposition TEXT, sent_ts TEXT, updated_at TEXT NOT NULL,
    detail_text TEXT NOT NULL DEFAULT '',
    detail_alert_id TEXT NOT NULL DEFAULT '',
    detail_hash TEXT NOT NULL DEFAULT '',
    detail_state TEXT NOT NULL DEFAULT '',
    detail_error TEXT NOT NULL DEFAULT '',
    UNIQUE(root_alert_id, destination_id)
);

CREATE TABLE IF NOT EXISTS delivery_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    root_alert_id TEXT NOT NULL,
    alert_id TEXT NOT NULL,
    destination_id INTEGER NOT NULL,
    destination_name TEXT NOT NULL,
    transport TEXT NOT NULL,
    channel INTEGER NOT NULL,
    matched_areas TEXT NOT NULL,
    message_text TEXT NOT NULL,
    event TEXT,
    headline TEXT,
    expires TEXT,
    msg_hash TEXT NOT NULL,
    disposition TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('queued','accepted','failed','superseded','skipped')),
    error TEXT NOT NULL DEFAULT '',
    queued_at TEXT NOT NULL,
    finalized_at TEXT,
    arbitration_canonical_id TEXT,
    arbitration_followup_id TEXT
);

CREATE TABLE IF NOT EXISTS meshwx_delivery_state (
    alert_id TEXT PRIMARY KEY,
    msg_hash TEXT NOT NULL,
    channel INTEGER NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('queued','accepted','failed','superseded')),
    error TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS weather_arbitration_candidates (
    canonical_id TEXT PRIMARY KEY,
    event TEXT NOT NULL,
    office TEXT NOT NULL,
    issued_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    deadline TEXT NOT NULL,
    rest_feature TEXT NOT NULL DEFAULT '',
    same_feature TEXT NOT NULL DEFAULT '',
    vtec_key TEXT UNIQUE,
    phase TEXT NOT NULL DEFAULT 'initial'
        CHECK(phase IN ('initial','updated','cancelled','expired')),
    retain_until TEXT NOT NULL,
    correlation_ugcs TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS weather_arbitration_destinations (
    canonical_id TEXT NOT NULL REFERENCES weather_arbitration_candidates(canonical_id)
        ON DELETE CASCADE,
    destination_id INTEGER NOT NULL,
    state TEXT NOT NULL
        CHECK(state IN ('pending','queued','accepted','superseded','no_fallback')),
    source TEXT NOT NULL DEFAULT '' CHECK(source IN ('','same','rest')),
    same_eligible INTEGER NOT NULL DEFAULT 0 CHECK(same_eligible IN (0,1)),
    rest_eligible INTEGER NOT NULL DEFAULT 0 CHECK(rest_eligible IN (0,1)),
    updated_at TEXT NOT NULL,
    PRIMARY KEY(canonical_id, destination_id)
);

CREATE TABLE IF NOT EXISTS weather_arbitration_followups (
    canonical_id TEXT NOT NULL REFERENCES weather_arbitration_candidates(canonical_id)
        ON DELETE CASCADE,
    followup_id TEXT NOT NULL,
    destination_id INTEGER NOT NULL,
    feature TEXT NOT NULL,
    action TEXT NOT NULL CHECK(action IN ('CON','EXT','CAN','EXP')),
    state TEXT NOT NULL CHECK(state IN
        ('pending','queued','accepted','no_target','superseded')),
    error TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(canonical_id, followup_id, destination_id)
);

CREATE TABLE IF NOT EXISTS history (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    ts               TEXT NOT NULL,
    nws_id           TEXT,
    event            TEXT,
    area             TEXT,
    disposition      TEXT,
    transmitted_text TEXT,
    detail           TEXT
);

CREATE TABLE IF NOT EXISTS transmit_log (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts         TEXT NOT NULL,
    channel    INTEGER,
    byte_count INTEGER,
    success    INTEGER,
    manual     INTEGER,
    text       TEXT,
    error      TEXT,
    transport  TEXT,
    destination_id INTEGER,
    followup_text TEXT NOT NULL DEFAULT '',
    delivery_attempt_id INTEGER
);

CREATE TABLE IF NOT EXISTS errors (
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
    ts     TEXT NOT NULL,
    source TEXT,
    message TEXT
);

CREATE TABLE IF NOT EXISTS events (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    ts      TEXT NOT NULL,
    level   TEXT,
    message TEXT
);

-- IPAWS (FEMA) alerts: kept fully separate from the NWS weather pipeline above.
CREATE TABLE IF NOT EXISTS ipaws_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT NOT NULL,
    identifier  TEXT UNIQUE,
    sender      TEXT,
    event       TEXT,
    area        TEXT,
    headline    TEXT,
    msg_type    TEXT,
    status      TEXT,
    sent        TEXT,
    text        TEXT,
    transmitted INTEGER,
    error       TEXT
);

CREATE INDEX IF NOT EXISTS idx_history_ts ON history(ts);
CREATE INDEX IF NOT EXISTS idx_history_disp ON history(disposition);
CREATE INDEX IF NOT EXISTS idx_txlog_ts ON transmit_log(ts);
CREATE INDEX IF NOT EXISTS idx_ipaws_ts ON ipaws_log(ts);
CREATE INDEX IF NOT EXISTS idx_routes_priority ON routing_rules(priority, id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_routes_name_unique
    ON routing_rules(name COLLATE NOCASE);
CREATE INDEX IF NOT EXISTS idx_delivery_last ON alert_delivery_state(last_alert_id);
CREATE INDEX IF NOT EXISTS idx_attempt_alert ON delivery_attempts(alert_id, id);
CREATE INDEX IF NOT EXISTS idx_attempt_destination ON delivery_attempts(destination_id, id);
CREATE INDEX IF NOT EXISTS idx_meshwx_updated ON meshwx_delivery_state(updated_at);
CREATE INDEX IF NOT EXISTS idx_weather_arbitration_deadline
    ON weather_arbitration_candidates(deadline);
CREATE INDEX IF NOT EXISTS idx_weather_arbitration_destination_state
    ON weather_arbitration_destinations(state);
CREATE INDEX IF NOT EXISTS idx_weather_arbitration_followup_state
    ON weather_arbitration_followups(state);
"""


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _parse_aware_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip())
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)


def _safe_feature(value: Any) -> tuple[str, dict[str, Any] | None]:
    """Return JSON safe to persist; raw SAME payloads and malformed JSON are cleared."""
    if not isinstance(value, str) or not value:
        return "", None
    if "ZCZC-" in value:
        return "", None
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return "", None
    if not isinstance(parsed, dict):
        return "", None
    return json.dumps(parsed, sort_keys=True, separators=(",", ":")), parsed


def _feature_ugcs(feature: dict[str, Any] | None) -> set[str]:
    if feature is None:
        return set()
    props = feature.get("properties")
    geocode = props.get("geocode") if isinstance(props, dict) else None
    values = geocode.get("UGC") if isinstance(geocode, dict) else None
    return {
        str(value).strip().upper() for value in values
        if isinstance(value, str) and value.strip()
    } if isinstance(values, list) else set()


def _persisted_rest_action(feature: dict[str, Any] | None) -> str | None:
    if feature is None:
        return None
    props = feature.get("properties")
    params = props.get("parameters") if isinstance(props, dict) else None
    values = params.get("VTEC") if isinstance(params, dict) else None
    if not isinstance(values, list) or len(values) != 1 or not isinstance(values[0], str):
        return None
    try:
        vtec = parse_vtec_parameter(values[0])
    except (TypeError, ValueError):
        return None
    if vtec is None or vtec.action not in {"NEW", "CON", "EXT", "CAN", "EXP"}:
        return None
    return vtec.action


def _feature_product_timestamp(feature: dict[str, Any] | None) -> datetime | None:
    if feature is None:
        return None
    props = feature.get("properties")
    if not isinstance(props, dict):
        return None
    for name in ("sent", "effective", "onset"):
        parsed = _parse_aware_timestamp(props.get(name))
        if parsed is not None:
            return parsed
    return None


class Database:
    def __init__(self, path: str):
        self.path = path
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        # A write that hits a lock should WAIT (up to 5s) for it to clear rather
        # than fail instantly with "database is locked".
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._lock = threading.Lock()
        self._arbitrated_acceptance_test_hook: Any = None
        self._init_schema()
        self._seed_settings()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._migrate_arbitration_schema_locked()
            attempt_columns = {
                row[1] for row in self._conn.execute(
                    "PRAGMA table_info(delivery_attempts)"
                )
            }
            if "arbitration_canonical_id" not in attempt_columns:
                self._conn.execute(
                    "ALTER TABLE delivery_attempts ADD COLUMN arbitration_canonical_id TEXT"
                )
            if "arbitration_followup_id" not in attempt_columns:
                self._conn.execute(
                    "ALTER TABLE delivery_attempts ADD COLUMN arbitration_followup_id TEXT"
                )
            columns = {row[1] for row in self._conn.execute("PRAGMA table_info(transmit_log)")}
            if "transport" not in columns:
                self._conn.execute("ALTER TABLE transmit_log ADD COLUMN transport TEXT")
            if "destination_id" not in columns:
                self._conn.execute("ALTER TABLE transmit_log ADD COLUMN destination_id INTEGER")
            if "followup_text" not in columns:
                self._conn.execute(
                    "ALTER TABLE transmit_log ADD COLUMN followup_text TEXT NOT NULL DEFAULT ''"
                )
            if "delivery_attempt_id" not in columns:
                self._conn.execute(
                    "ALTER TABLE transmit_log ADD COLUMN delivery_attempt_id INTEGER"
                )
            route_columns = {
                row[1] for row in self._conn.execute("PRAGMA table_info(route_events)")
            }
            if "detail_enabled" not in route_columns:
                self._conn.execute(
                    "ALTER TABLE route_events ADD COLUMN detail_enabled INTEGER NOT NULL DEFAULT 1"
                )
            rule_columns = {
                row[1] for row in self._conn.execute("PRAGMA table_info(routing_rules)")
            }
            if "all_warnings_details" not in rule_columns:
                self._conn.execute(
                    "ALTER TABLE routing_rules ADD COLUMN all_warnings_details "
                    "INTEGER NOT NULL DEFAULT 1"
                )
            delivery_columns = {
                row[1] for row in self._conn.execute(
                    "PRAGMA table_info(alert_delivery_state)"
                )
            }
            delivery_migrations = {
                "detail_text": (
                    "ALTER TABLE alert_delivery_state ADD COLUMN "
                    "detail_text TEXT NOT NULL DEFAULT ''"
                ),
                "detail_alert_id": (
                    "ALTER TABLE alert_delivery_state ADD COLUMN "
                    "detail_alert_id TEXT NOT NULL DEFAULT ''"
                ),
                "detail_hash": (
                    "ALTER TABLE alert_delivery_state ADD COLUMN "
                    "detail_hash TEXT NOT NULL DEFAULT ''"
                ),
                "detail_state": (
                    "ALTER TABLE alert_delivery_state ADD COLUMN "
                    "detail_state TEXT NOT NULL DEFAULT ''"
                ),
                "detail_error": (
                    "ALTER TABLE alert_delivery_state ADD COLUMN "
                    "detail_error TEXT NOT NULL DEFAULT ''"
                ),
            }
            for name, statement in delivery_migrations.items():
                if name not in delivery_columns:
                    self._conn.execute(statement)
            self._conn.commit()

    def _migrate_arbitration_schema_locked(self) -> None:
        """Normalize every historic arbitration schema in one transaction."""
        candidate_columns = {
            row[1] for row in self._conn.execute(
                "PRAGMA table_info(weather_arbitration_candidates)"
            )
        }
        destination_columns = {
            row[1] for row in self._conn.execute(
                "PRAGMA table_info(weather_arbitration_destinations)"
            )
        }
        candidate_sql_row = self._conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' "
            "AND name='weather_arbitration_candidates'"
        ).fetchone()
        candidate_sql = str(candidate_sql_row[0] or "") if candidate_sql_row else ""
        destination_sql_row = self._conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' "
            "AND name='weather_arbitration_destinations'"
        ).fetchone()
        destination_sql = str(destination_sql_row[0] or "") if destination_sql_row else ""
        candidate_checks_current = all(
            f"'{value}'" in candidate_sql
            for value in ("initial", "updated", "cancelled", "expired")
        )
        destination_checks_current = all(
            f"'{value}'" in destination_sql
            for value in ("pending", "queued", "accepted", "superseded", "no_fallback")
        )
        if (
            {"phase", "retain_until", "correlation_ugcs"} <= candidate_columns
            and {"same_eligible", "rest_eligible"} <= destination_columns
            and candidate_checks_current
            and destination_checks_current
        ):
            return

        candidates = [
            dict(row) for row in self._conn.execute(
                "SELECT * FROM weather_arbitration_candidates"
            ).fetchall()
        ]
        destinations = [
            dict(row) for row in self._conn.execute(
                "SELECT * FROM weather_arbitration_destinations"
            ).fetchall()
        ]
        followups = [
            dict(row) for row in self._conn.execute(
                "SELECT * FROM weather_arbitration_followups"
            ).fetchall()
        ]
        migration_now = datetime.now(UTC)
        normalized_candidates: list[tuple[Any, ...]] = []
        candidate_ids: set[str] = set()
        rest_only_initial_ids: set[str] = set()
        for row in candidates:
            canonical_id = str(row.get("canonical_id", ""))
            if not canonical_id:
                continue
            raw_rest = row.get("rest_feature", "")
            raw_same = row.get("same_feature", "")
            raw_same_present = bool(raw_same)
            rest_feature, parsed_rest = _safe_feature(raw_rest)
            same_feature, parsed_same = _safe_feature(raw_same)
            malformed_rest = bool(raw_rest and parsed_rest is None)
            action = _persisted_rest_action(parsed_rest)
            stored_phase = row.get("phase")
            valid_stored_phase = stored_phase if stored_phase in {
                "initial", "updated", "cancelled", "expired"
            } else None
            if malformed_rest or (raw_rest and action is None):
                phase = "expired"
                rest_feature = ""
            elif valid_stored_phase in {"cancelled", "expired"}:
                phase = valid_stored_phase
            elif action in {"CON", "EXT"}:
                phase = "updated"
            elif action == "CAN":
                phase = "cancelled"
            elif action == "EXP":
                phase = "expired"
            else:
                phase = valid_stored_phase or "initial"
            expires = _parse_aware_timestamp(row.get("expires_at"))
            stored_retention = _parse_aware_timestamp(row.get("retain_until"))
            retention = (expires + timedelta(hours=48)) if expires else (
                migration_now + timedelta(hours=48)
            )
            if stored_retention is not None:
                retention = max(retention, stored_retention)
            if phase in {"cancelled", "expired"}:
                retention = max(retention, migration_now + timedelta(hours=48))
            ugcs = _feature_ugcs(parsed_rest) | _feature_ugcs(parsed_same)
            try:
                stored_ugcs = json.loads(row.get("correlation_ugcs") or "[]")
            except (TypeError, ValueError):
                stored_ugcs = []
            if isinstance(stored_ugcs, list):
                ugcs.update(
                    str(value).strip().upper() for value in stored_ugcs
                    if isinstance(value, str) and value.strip()
                )
            normalized_candidates.append((
                canonical_id, str(row.get("event", "")), str(row.get("office", "")),
                str(row.get("issued_at", "")), str(row.get("expires_at", "")),
                str(row.get("deadline", "")), rest_feature, same_feature,
                row.get("vtec_key"), phase, retention.isoformat(),
                json.dumps(sorted(ugcs), separators=(",", ":")),
                str(row.get("created_at") or migration_now.isoformat()),
                str(row.get("updated_at") or migration_now.isoformat()),
            ))
            candidate_ids.add(canonical_id)
            if (
                phase == "initial" and rest_feature
                and not raw_same_present and not same_feature
            ):
                rest_only_initial_ids.add(canonical_id)

        self._conn.execute("BEGIN IMMEDIATE")
        try:
            self._conn.execute("""
                CREATE TABLE weather_arbitration_candidates_new (
                    canonical_id TEXT PRIMARY KEY,
                    event TEXT NOT NULL, office TEXT NOT NULL,
                    issued_at TEXT NOT NULL, expires_at TEXT NOT NULL,
                    deadline TEXT NOT NULL,
                    rest_feature TEXT NOT NULL DEFAULT '',
                    same_feature TEXT NOT NULL DEFAULT '',
                    vtec_key TEXT UNIQUE,
                    phase TEXT NOT NULL DEFAULT 'initial'
                        CHECK(phase IN ('initial','updated','cancelled','expired')),
                    retain_until TEXT NOT NULL,
                    correlation_ugcs TEXT NOT NULL DEFAULT '[]',
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
                )
            """)
            self._conn.execute("""
                CREATE TABLE weather_arbitration_destinations_new (
                    canonical_id TEXT NOT NULL
                        REFERENCES weather_arbitration_candidates_new(canonical_id)
                        ON DELETE CASCADE,
                    destination_id INTEGER NOT NULL,
                    state TEXT NOT NULL CHECK(state IN
                        ('pending','queued','accepted','superseded','no_fallback')),
                    source TEXT NOT NULL DEFAULT '' CHECK(source IN ('','same','rest')),
                    same_eligible INTEGER NOT NULL DEFAULT 0 CHECK(same_eligible IN (0,1)),
                    rest_eligible INTEGER NOT NULL DEFAULT 0 CHECK(rest_eligible IN (0,1)),
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(canonical_id,destination_id)
                )
            """)
            self._conn.executemany(
                "INSERT INTO weather_arbitration_candidates_new VALUES "
                "(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", normalized_candidates,
            )
            normalized_destinations = []
            for row in destinations:
                canonical_id = str(row.get("canonical_id", ""))
                if canonical_id not in candidate_ids:
                    continue
                source = row.get("source") if row.get("source") in {"", "same", "rest"} else ""
                state = row.get("state")
                if state not in {"pending", "queued", "accepted", "superseded", "no_fallback"}:
                    state = "superseded"
                normalized_destinations.append((
                    canonical_id, int(row.get("destination_id", 0)), state, source,
                    int(bool(row.get("same_eligible", source == "same"))),
                    int(bool(row.get(
                        "rest_eligible",
                        source == "rest" or (
                            source == "" and state == "pending"
                            and canonical_id in rest_only_initial_ids
                        ),
                    ))),
                    str(row.get("updated_at") or migration_now.isoformat()),
                ))
            self._conn.executemany(
                "INSERT INTO weather_arbitration_destinations_new VALUES (?,?,?,?,?,?,?)",
                normalized_destinations,
            )
            self._conn.execute("DROP TABLE weather_arbitration_followups")
            self._conn.execute("DROP TABLE weather_arbitration_destinations")
            self._conn.execute("DROP TABLE weather_arbitration_candidates")
            self._conn.execute(
                "ALTER TABLE weather_arbitration_candidates_new "
                "RENAME TO weather_arbitration_candidates"
            )
            self._conn.execute(
                "ALTER TABLE weather_arbitration_destinations_new "
                "RENAME TO weather_arbitration_destinations"
            )
            self._conn.execute("""
                CREATE TABLE weather_arbitration_followups (
                    canonical_id TEXT NOT NULL
                        REFERENCES weather_arbitration_candidates(canonical_id)
                        ON DELETE CASCADE,
                    followup_id TEXT NOT NULL, destination_id INTEGER NOT NULL,
                    feature TEXT NOT NULL,
                    action TEXT NOT NULL CHECK(action IN ('CON','EXT','CAN','EXP')),
                    state TEXT NOT NULL CHECK(state IN
                        ('pending','queued','accepted','no_target','superseded')),
                    error TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                    PRIMARY KEY(canonical_id,followup_id,destination_id)
                )
            """)
            normalized_followups = []
            for row in followups:
                canonical_id = str(row.get("canonical_id", ""))
                followup_id = str(row.get("followup_id", ""))
                action = str(row.get("action", ""))
                state = str(row.get("state", ""))
                raw_feature = row.get("feature", "")
                safe_feature, _parsed = _safe_feature(raw_feature)
                if (
                    canonical_id not in candidate_ids or not followup_id
                    or action not in {"CON", "EXT", "CAN", "EXP"}
                    or state not in {
                        "pending", "queued", "accepted", "no_target", "superseded"
                    }
                    or not safe_feature
                ):
                    continue
                normalized_followups.append((
                    canonical_id, followup_id, int(row.get("destination_id", 0)),
                    str(raw_feature), action, state, str(row.get("error", "")),
                    str(row.get("created_at") or migration_now.isoformat()),
                    str(row.get("updated_at") or migration_now.isoformat()),
                ))
            self._conn.executemany(
                """INSERT INTO weather_arbitration_followups
                   (canonical_id,followup_id,destination_id,feature,action,state,
                    error,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)""",
                normalized_followups,
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_weather_arbitration_deadline "
                "ON weather_arbitration_candidates(deadline)"
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_weather_arbitration_destination_state "
                "ON weather_arbitration_destinations(state)"
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_weather_arbitration_followup_state "
                "ON weather_arbitration_followups(state)"
            )
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    def _seed_settings(self) -> None:
        with self._lock:
            cur = self._conn.execute("SELECT key FROM settings")
            existing = {r["key"] for r in cur.fetchall()}
            for key, value in DEFAULT_SETTINGS.items():
                if key not in existing:
                    self._conn.execute(
                        "INSERT INTO settings(key, value) VALUES (?, ?)",
                        (key, json.dumps(value)),
                    )
            self._conn.commit()

    # ---- settings -------------------------------------------------------
    def get_setting(self, key: str, default: Any = None) -> Any:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM settings WHERE key = ?", (key,)
            ).fetchone()
        if row is None:
            return default
        return json.loads(row["value"])

    def all_settings(self) -> dict:
        with self._lock:
            rows = self._conn.execute("SELECT key, value FROM settings").fetchall()
        return {r["key"]: json.loads(r["value"]) for r in rows}

    def set_setting(self, key: str, value: Any) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO settings(key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, json.dumps(value)),
            )
            self._conn.commit()

    # ---- destinations and user-defined routes --------------------------
    def create_destination(self, name: str, transport: str, channel: int,
                           enabled: bool = True) -> int:
        with self._lock:
            try:
                cur = self._conn.execute(
                    "INSERT INTO destinations(name, transport, channel, enabled) VALUES (?, ?, ?, ?)",
                    (name, transport, channel, int(enabled)),
                )
                self._conn.commit()
                destination_id = cur.lastrowid
                assert destination_id is not None
                return int(destination_id)
            except sqlite3.Error:
                self._conn.rollback()
                raise

    def get_destination(self, destination_id: int):
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM destinations WHERE id = ?", (destination_id,)
            ).fetchone()

    def list_destinations(self) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM destinations ORDER BY id"
            ).fetchall()

    def channel_references(self, transport: str, channel: int) -> dict[str, Any]:
        """Describe every durable reference that makes an endpoint unsafe to clear."""
        with self._lock:
            destinations = self._conn.execute(
                """SELECT * FROM destinations
                   WHERE transport = ? AND channel = ? ORDER BY id""",
                (transport, channel),
            ).fetchall()
            snapshots = self._conn.execute(
                """SELECT * FROM alert_delivery_state
                   WHERE transport = ? AND channel = ?
                     AND disposition IN ('sent', 'update')
                   ORDER BY chain_id""",
                (transport, channel),
            ).fetchall()
        return {
            "is_referenced": bool(destinations or snapshots),
            "destinations": destinations,
            "active_delivery_snapshots": snapshots,
        }

    def update_destination(self, destination_id: int, name: str, transport: str,
                           channel: int, enabled: bool) -> bool:
        with self._lock:
            try:
                cur = self._conn.execute(
                    """UPDATE destinations
                       SET name = ?, transport = ?, channel = ?, enabled = ?
                       WHERE id = ?""",
                    (name, transport, channel, int(enabled), destination_id),
                )
                self._conn.commit()
                return cur.rowcount == 1
            except sqlite3.Error:
                self._conn.rollback()
                raise

    def toggle_destination(self, destination_id: int) -> bool:
        with self._lock:
            try:
                cur = self._conn.execute(
                    "UPDATE destinations SET enabled = NOT enabled WHERE id = ?",
                    (destination_id,),
                )
                self._conn.commit()
                return cur.rowcount == 1
            except sqlite3.Error:
                self._conn.rollback()
                raise

    def delete_destination(self, destination_id: int) -> bool:
        with self._lock:
            try:
                cur = self._conn.execute(
                    "DELETE FROM destinations WHERE id = ?", (destination_id,)
                )
                self._conn.commit()
                return cur.rowcount == 1
            except sqlite3.Error:
                self._conn.rollback()
                raise

    def create_route(self, name: str, priority: int = 100,
                     enabled: bool = True) -> int:
        with self._lock:
            try:
                cur = self._conn.execute(
                    "INSERT INTO routing_rules(name, enabled, priority) VALUES (?, ?, ?)",
                    (name, int(enabled), priority),
                )
                self._conn.commit()
                rule_id = cur.lastrowid
                assert rule_id is not None
                return int(rule_id)
            except sqlite3.Error:
                self._conn.rollback()
                raise

    def create_routing_rule(
        self, name: str, priority: int, enabled: bool, all_warnings: bool,
        counties: list[tuple[str, str]], events: list[str],
        destination_ids: list[int], detail_events: list[str] | None = None,
        all_warnings_details: bool = True,
    ) -> int:
        """Create a complete manual rule atomically."""
        unique_counties = list(dict.fromkeys(counties))
        unique_events = list(dict.fromkeys(events))
        detail_set = set(unique_events if detail_events is None else detail_events)
        unique_destinations = list(dict.fromkeys(destination_ids))
        with self._lock:
            try:
                self._conn.execute("BEGIN")
                cur = self._conn.execute(
                    """INSERT INTO routing_rules
                       (name, enabled, priority, all_warnings, all_warnings_details)
                       VALUES (?, ?, ?, ?, ?)""",
                    (name, int(enabled), priority, int(all_warnings),
                     int(all_warnings_details)),
                )
                lastrowid = cur.lastrowid
                assert lastrowid is not None
                rule_id = int(lastrowid)
                self._conn.executemany(
                    "INSERT INTO route_counties(rule_id, zone_code, county_name) VALUES (?, ?, ?)",
                    [(rule_id, code, county_name) for code, county_name in unique_counties],
                )
                self._conn.executemany(
                    "INSERT INTO route_events(rule_id, event, detail_enabled) VALUES (?, ?, ?)",
                    [(rule_id, event, int(event in detail_set)) for event in unique_events],
                )
                self._conn.executemany(
                    "INSERT INTO route_destinations(rule_id, destination_id) VALUES (?, ?)",
                    [(rule_id, destination_id) for destination_id in unique_destinations],
                )
                self._conn.commit()
                return rule_id
            except Exception:
                self._conn.rollback()
                raise

    def update_routing_rule(
        self, rule_id: int, name: str, priority: int, enabled: bool,
        all_warnings: bool, counties: list[tuple[str, str]], events: list[str],
        destination_ids: list[int], detail_events: list[str] | None = None,
        all_warnings_details: bool = True,
    ) -> bool:
        """Replace a manual rule and all associations atomically."""
        unique_counties = list(dict.fromkeys(counties))
        unique_events = list(dict.fromkeys(events))
        detail_set = set(unique_events if detail_events is None else detail_events)
        unique_destinations = list(dict.fromkeys(destination_ids))
        with self._lock:
            try:
                self._conn.execute("BEGIN")
                cur = self._conn.execute(
                    """UPDATE routing_rules SET name = ?, priority = ?, enabled = ?,
                       all_warnings = ?, all_warnings_details = ? WHERE id = ?""",
                    (name, priority, int(enabled), int(all_warnings),
                     int(all_warnings_details), rule_id),
                )
                if cur.rowcount != 1:
                    self._conn.rollback()
                    return False
                self._conn.execute("DELETE FROM route_counties WHERE rule_id = ?", (rule_id,))
                self._conn.execute("DELETE FROM route_events WHERE rule_id = ?", (rule_id,))
                self._conn.execute("DELETE FROM route_destinations WHERE rule_id = ?", (rule_id,))
                self._conn.executemany(
                    "INSERT INTO route_counties(rule_id, zone_code, county_name) VALUES (?, ?, ?)",
                    [(rule_id, code, county_name) for code, county_name in unique_counties],
                )
                self._conn.executemany(
                    "INSERT INTO route_events(rule_id, event, detail_enabled) VALUES (?, ?, ?)",
                    [(rule_id, event, int(event in detail_set)) for event in unique_events],
                )
                self._conn.executemany(
                    "INSERT INTO route_destinations(rule_id, destination_id) VALUES (?, ?)",
                    [(rule_id, destination_id) for destination_id in unique_destinations],
                )
                self._conn.commit()
                return True
            except Exception:
                self._conn.rollback()
                raise

    def toggle_route(self, rule_id: int) -> bool:
        with self._lock:
            try:
                cur = self._conn.execute(
                    "UPDATE routing_rules SET enabled = NOT enabled WHERE id = ?",
                    (rule_id,),
                )
                self._conn.commit()
                return cur.rowcount == 1
            except sqlite3.Error:
                self._conn.rollback()
                raise

    def move_route(self, rule_id: int, direction: str) -> bool:
        """Move one rule and rewrite priorities to an unambiguous stable order."""
        if direction not in {"up", "down"}:
            raise ValueError("direction must be 'up' or 'down'")
        with self._lock:
            try:
                self._conn.execute("BEGIN")
                rows = self._conn.execute(
                    "SELECT id FROM routing_rules ORDER BY priority, id"
                ).fetchall()
                ids = [int(row["id"]) for row in rows]
                if rule_id not in ids:
                    self._conn.rollback()
                    return False
                index = ids.index(rule_id)
                other = index - 1 if direction == "up" else index + 1
                if 0 <= other < len(ids):
                    ids[index], ids[other] = ids[other], ids[index]
                self._conn.executemany(
                    "UPDATE routing_rules SET priority = ? WHERE id = ?",
                    [((position + 1) * 10, value) for position, value in enumerate(ids)],
                )
                self._conn.commit()
                return True
            except Exception:
                self._conn.rollback()
                raise

    def delete_route(self, rule_id: int) -> bool:
        """Delete only a rule; FK cascades remove its association rows."""
        with self._lock:
            try:
                self._conn.execute("BEGIN")
                cur = self._conn.execute(
                    "DELETE FROM routing_rules WHERE id = ?", (rule_id,)
                )
                self._conn.commit()
                return cur.rowcount == 1
            except sqlite3.Error:
                self._conn.rollback()
                raise

    def replace_route_counties(self, rule_id: int,
                               counties: list[tuple[str, str]]) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM route_counties WHERE rule_id = ?", (rule_id,))
            self._conn.executemany(
                "INSERT INTO route_counties(rule_id, zone_code, county_name) VALUES (?, ?, ?)",
                [(rule_id, code, name) for code, name in counties],
            )
            self._conn.commit()

    def replace_route_events(self, rule_id: int, events: list[str],
                             all_warnings: bool = False,
                             detail_events: list[str] | None = None,
                             all_warnings_details: bool = True) -> None:
        detail_set = set(events if detail_events is None else detail_events)
        with self._lock:
            self._conn.execute("DELETE FROM route_events WHERE rule_id = ?", (rule_id,))
            self._conn.executemany(
                "INSERT INTO route_events(rule_id, event, detail_enabled) VALUES (?, ?, ?)",
                [(rule_id, event, int(event in detail_set)) for event in events],
            )
            self._conn.execute(
                """UPDATE routing_rules
                   SET all_warnings = ?, all_warnings_details = ? WHERE id = ?""",
                (int(all_warnings), int(all_warnings_details), rule_id),
            )
            self._conn.commit()

    def replace_route_destinations(self, rule_id: int,
                                   destination_ids: list[int]) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM route_destinations WHERE rule_id = ?", (rule_id,))
            self._conn.executemany(
                "INSERT INTO route_destinations(rule_id, destination_id) VALUES (?, ?)",
                [(rule_id, destination_id) for destination_id in destination_ids],
            )
            self._conn.commit()

    def list_routes(self) -> list[dict[str, Any]]:
        """Return every user-created rule with its associations in stable order."""
        with self._lock:
            rules = self._conn.execute(
                "SELECT * FROM routing_rules ORDER BY priority, id"
            ).fetchall()
            return [self._route_detail_locked(row) for row in rules]

    def get_route(self, rule_id: int) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM routing_rules WHERE id = ?", (rule_id,)
            ).fetchone()
            return self._route_detail_locked(row) if row is not None else None

    def _route_detail_locked(self, row: sqlite3.Row) -> dict[str, Any]:
        rule_id = int(row["id"])
        rule = dict(row)
        rule["enabled"] = bool(rule["enabled"])
        rule["all_warnings"] = bool(rule["all_warnings"])
        rule["all_warnings_details"] = bool(rule["all_warnings_details"])
        rule["counties"] = [dict(value) for value in self._conn.execute(
            "SELECT zone_code, county_name FROM route_counties "
            "WHERE rule_id = ? ORDER BY zone_code", (rule_id,),
        ).fetchall()]
        rule["events"] = [value["event"] for value in self._conn.execute(
            "SELECT event FROM route_events WHERE rule_id = ? ORDER BY event", (rule_id,),
        ).fetchall()]
        rule["detail_events"] = [value["event"] for value in self._conn.execute(
            "SELECT event FROM route_events WHERE rule_id = ? AND detail_enabled = 1 "
            "ORDER BY event", (rule_id,),
        ).fetchall()]
        rule["destinations"] = [dict(value) for value in self._conn.execute(
            """SELECT d.* FROM route_destinations rd
               JOIN destinations d ON d.id = rd.destination_id
               WHERE rd.rule_id = ? ORDER BY d.id""", (rule_id,),
        ).fetchall()]
        return rule

    def routing_rows(self) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                """SELECT r.id rule_id, r.name rule_name, r.enabled rule_enabled,
                          r.priority, r.all_warnings, r.all_warnings_details,
                          c.zone_code, c.county_name, e.event, e.detail_enabled,
                          d.id destination_id, d.name destination_name,
                          d.transport, d.channel, d.enabled destination_enabled
                   FROM routing_rules r
                   JOIN route_counties c ON c.rule_id = r.id
                   JOIN route_destinations rd ON rd.rule_id = r.id
                   JOIN destinations d ON d.id = rd.destination_id
                   LEFT JOIN route_events e ON e.rule_id = r.id
                   ORDER BY r.priority, r.id, c.zone_code, d.id"""
            ).fetchall()

    def enabled_route_zones(self) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                """SELECT DISTINCT c.zone_code
                   FROM route_counties c
                   JOIN routing_rules r ON r.id = c.rule_id
                   WHERE r.enabled = 1
                   ORDER BY r.priority, r.id, c.rowid"""
            ).fetchall()
        return [str(row["zone_code"]).strip().upper() for row in rows
                if str(row["zone_code"]).strip()]

    def delivery_states_for_alerts(self, alert_ids: list[str]) -> list[sqlite3.Row]:
        ids = [value for value in alert_ids if value]
        if not ids:
            return []
        placeholders = ",".join("?" for _ in ids)
        with self._lock:
            return self._conn.execute(
                f"SELECT * FROM alert_delivery_state WHERE last_alert_id IN ({placeholders}) "
                f"OR root_alert_id IN ({placeholders}) "
                f"OR detail_alert_id IN ({placeholders}) ORDER BY chain_id",
                ids + ids + ids,
            ).fetchall()

    def get_delivery_state(self, root_alert_id: str, destination_id: int):
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM alert_delivery_state WHERE root_alert_id = ? AND destination_id = ?",
                (root_alert_id, destination_id),
            ).fetchone()

    def upsert_delivery_state(self, root_alert_id: str, last_alert_id: str,
                              destination_id: int, transport: str, channel: int,
                              matched_areas: list[str], event: str, headline: str,
                              expires: str, msg_hash: str, disposition: str) -> None:
        now = _now()
        with self._lock:
            self._conn.execute(
                """INSERT INTO alert_delivery_state
                   (root_alert_id,last_alert_id,destination_id,transport,channel,matched_areas,
                    event,headline,expires,msg_hash,disposition,sent_ts,updated_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(root_alert_id,destination_id) DO UPDATE SET
                     last_alert_id=excluded.last_alert_id, transport=excluded.transport,
                     channel=excluded.channel, matched_areas=excluded.matched_areas,
                     event=excluded.event, headline=excluded.headline, expires=excluded.expires,
                     msg_hash=excluded.msg_hash, disposition=excluded.disposition,
                     updated_at=excluded.updated_at""",
                (root_alert_id, last_alert_id, destination_id, transport, channel,
                 json.dumps(matched_areas), event, headline, expires, msg_hash,
                 disposition, now, now),
            )
            self._conn.commit()

    def queue_detail_delivery(self, root_alert_id: str, destination_id: int,
                              alert_id: str, text: str, msg_hash: str) -> bool:
        with self._lock:
            cur = self._conn.execute(
                """UPDATE alert_delivery_state
                   SET detail_alert_id = ?, detail_text = ?, detail_hash = ?,
                       detail_state = 'queued', detail_error = '', updated_at = ?
                   WHERE root_alert_id = ? AND destination_id = ?""",
                (alert_id, text, msg_hash, _now(), root_alert_id, destination_id),
            )
            self._conn.commit()
            return cur.rowcount == 1

    def finalize_detail_delivery(self, root_alert_id: str, destination_id: int,
                                 alert_id: str, msg_hash: str, state: str,
                                 error: str = "") -> bool:
        if state not in {"accepted", "failed", "superseded"}:
            raise ValueError("invalid detail delivery state")
        with self._lock:
            cur = self._conn.execute(
                """UPDATE alert_delivery_state
                   SET detail_state = ?, detail_error = ?, updated_at = ?
                   WHERE root_alert_id = ? AND destination_id = ?
                     AND detail_alert_id = ? AND detail_hash = ?
                     AND detail_state = 'queued'""",
                (state, error, _now(), root_alert_id, destination_id,
                 alert_id, msg_hash),
            )
            self._conn.commit()
            return cur.rowcount == 1

    def supersede_queued_detail(self, root_alert_id: str, destination_id: int) -> bool:
        with self._lock:
            cur = self._conn.execute(
                """UPDATE alert_delivery_state
                   SET detail_state = 'superseded', detail_error = 'superseded',
                       updated_at = ?
                   WHERE root_alert_id = ? AND destination_id = ?
                     AND detail_state = 'queued'""",
                (_now(), root_alert_id, destination_id),
            )
            self._conn.commit()
            return cur.rowcount == 1

    def recover_queued_detail_deliveries(self) -> int:
        with self._lock:
            cur = self._conn.execute(
                """UPDATE alert_delivery_state
                   SET detail_state = 'failed',
                       detail_error = 'interrupted before completion', updated_at = ?
                   WHERE detail_state = 'queued'""",
                (_now(),),
            )
            self._conn.commit()
            return cur.rowcount

    def get_meshwx_delivery(self, alert_id: str):
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM meshwx_delivery_state WHERE alert_id = ?", (alert_id,)
            ).fetchone()

    def set_meshwx_delivery(self, alert_id: str, msg_hash: str, channel: int,
                            state: str, error: str = "") -> None:
        if state not in {"queued", "accepted", "failed", "superseded"}:
            raise ValueError("invalid MeshWX delivery state")
        with self._lock:
            self._conn.execute(
                """INSERT INTO meshwx_delivery_state
                   (alert_id,msg_hash,channel,state,error,updated_at)
                   VALUES(?,?,?,?,?,?)
                   ON CONFLICT(alert_id) DO UPDATE SET
                     msg_hash=excluded.msg_hash, channel=excluded.channel,
                     state=excluded.state, error=excluded.error,
                     updated_at=excluded.updated_at""",
                (alert_id, msg_hash, channel, state, error, _now()),
            )
            self._conn.commit()

    def supersede_meshwx_deliveries(self, alert_ids) -> int:
        ids = [value for value in alert_ids if value]
        if not ids:
            return 0
        placeholders = ",".join("?" for _ in ids)
        with self._lock:
            cur = self._conn.execute(
                f"""UPDATE meshwx_delivery_state
                    SET state = 'superseded', error = 'superseded', updated_at = ?
                    WHERE alert_id IN ({placeholders}) AND state != 'superseded'""",
                [_now(), *ids],
            )
            self._conn.commit()
            return cur.rowcount

    def recover_queued_meshwx_deliveries(self) -> int:
        """Make interrupted queued sends retryable after process startup."""
        with self._lock:
            cur = self._conn.execute(
                """UPDATE meshwx_delivery_state
                   SET state = 'failed', error = 'interrupted before completion',
                       updated_at = ?
                   WHERE state = 'queued'""",
                (_now(),),
            )
            self._conn.commit()
            return cur.rowcount

    def create_delivery_attempt(
        self, *, root_alert_id: str, alert_id: str, destination_id: int,
        destination_name: str, transport: str, channel: int,
        matched_areas: list[str], message_text: str, event: str,
        headline: str, expires: str, msg_hash: str, disposition: str,
        arbitration_canonical_id: str | None = None,
        arbitration_followup_id: str | None = None,
    ) -> int:
        """Persist the immutable snapshot for one enqueue attempt."""
        with self._lock:
            cur = self._conn.execute(
                """INSERT INTO delivery_attempts
                   (root_alert_id,alert_id,destination_id,destination_name,transport,
                    channel,matched_areas,message_text,event,headline,expires,msg_hash,
                    disposition,state,queued_at,arbitration_canonical_id,
                    arbitration_followup_id)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,'queued',?,?,?)""",
                (root_alert_id, alert_id, destination_id, destination_name, transport,
                 channel, json.dumps(matched_areas), message_text, event, headline,
                 expires, msg_hash, disposition, _now(), arbitration_canonical_id,
                 arbitration_followup_id),
            )
            self._conn.commit()
            assert cur.lastrowid is not None
            return int(cur.lastrowid)

    def finalize_delivery_attempt(self, attempt_id: int, state: str,
                                  error: str = "") -> bool:
        if state not in {"accepted", "failed", "superseded", "skipped"}:
            raise ValueError("invalid terminal delivery attempt state")
        with self._lock:
            cur = self._conn.execute(
                """UPDATE delivery_attempts SET state = ?, error = ?, finalized_at = ?
                   WHERE id = ? AND state = 'queued'""",
                (state, error, _now(), attempt_id),
            )
            self._conn.commit()
            return cur.rowcount == 1

    def accept_arbitrated_delivery(
        self, *, canonical_id: str, destination_id: int, source: str,
        attempt_id: int,
    ) -> bool:
        """Atomically accept an arbitration claim, attempt, and delivery chain."""
        if source not in {"same", "rest"}:
            raise ValueError("invalid arbitration source")
        now = _now()
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                arbitration = self._conn.execute(
                    """UPDATE weather_arbitration_destinations AS d
                       SET state='accepted',source=?,updated_at=?
                       WHERE d.canonical_id=? AND d.destination_id=?
                         AND d.state='queued' AND d.source=?
                         AND EXISTS (
                           SELECT 1 FROM weather_arbitration_candidates c
                           WHERE c.canonical_id=d.canonical_id AND c.phase='initial'
                         )""",
                    (source, now, canonical_id, destination_id, source),
                )
                if arbitration.rowcount != 1:
                    self._conn.rollback()
                    return False
                test_hook = self._arbitrated_acceptance_test_hook
                if test_hook is not None:
                    test_hook()
                attempt = self._conn.execute(
                    "SELECT * FROM delivery_attempts WHERE id=? AND destination_id=?",
                    (attempt_id, destination_id),
                ).fetchone()
                if attempt is None:
                    self._conn.rollback()
                    return False
                finalized = self._conn.execute(
                    """UPDATE delivery_attempts
                       SET state='accepted',error='',finalized_at=?
                       WHERE id=? AND state='queued'""",
                    (now, attempt_id),
                )
                if finalized.rowcount != 1:
                    self._conn.rollback()
                    return False
                self._conn.execute(
                    """INSERT INTO alert_delivery_state
                       (root_alert_id,last_alert_id,destination_id,transport,channel,
                        matched_areas,event,headline,expires,msg_hash,disposition,
                        sent_ts,updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(root_alert_id,destination_id) DO UPDATE SET
                         last_alert_id=excluded.last_alert_id,
                         transport=excluded.transport,channel=excluded.channel,
                         matched_areas=excluded.matched_areas,event=excluded.event,
                         headline=excluded.headline,expires=excluded.expires,
                         msg_hash=excluded.msg_hash,disposition=excluded.disposition,
                         updated_at=excluded.updated_at""",
                    (
                        attempt["root_alert_id"], attempt["alert_id"], destination_id,
                        attempt["transport"], attempt["channel"],
                        attempt["matched_areas"], attempt["event"], attempt["headline"],
                        attempt["expires"], attempt["msg_hash"], attempt["disposition"],
                        now, now,
                    ),
                )
                self._conn.commit()
                return True
            except Exception:
                self._conn.rollback()
                raise

    def query_delivery_attempts(
        self, *, alert_id: str | None = None,
        destination_id: int | None = None, state: str | None = None,
        limit: int = 200,
    ) -> list[sqlite3.Row]:
        clauses: list[str] = []
        params: list[Any] = []
        if alert_id is not None:
            clauses.append("alert_id = ?")
            params.append(alert_id)
        if destination_id is not None:
            clauses.append("destination_id = ?")
            params.append(destination_id)
        if state is not None:
            clauses.append("state = ?")
            params.append(state)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        params.append(limit)
        with self._lock:
            return self._conn.execute(
                f"SELECT * FROM delivery_attempts{where} ORDER BY id DESC LIMIT ?",
                params,
            ).fetchall()

    def get_delivery_attempt(self, attempt_id: int) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM delivery_attempts WHERE id=?", (attempt_id,)
            ).fetchone()

    def has_successful_delivery_transmit(self, attempt_id: int) -> bool:
        with self._lock:
            return self._conn.execute(
                """SELECT 1 FROM transmit_log
                   WHERE delivery_attempt_id=? AND success=1 AND manual=0
                   LIMIT 1""",
                (attempt_id,),
            ).fetchone() is not None

    # ---- SAME-primary / REST-fallback arbitration ----------------------
    def create_arbitration_candidate(
        self, *, canonical_id: str, event: str, office: str, issued_at: str,
        expires_at: str, deadline: str, rest_feature: str = "",
        same_feature: str = "", vtec_key: str | None = None,
        correlation_ugcs=(),
    ) -> None:
        self.create_arbitration_candidate_with_destinations(
            canonical_id=canonical_id, event=event, office=office,
            issued_at=issued_at, expires_at=expires_at, deadline=deadline,
            rest_feature=rest_feature, same_feature=same_feature,
            vtec_key=vtec_key, destination_ids=[],
            correlation_ugcs=correlation_ugcs,
        )

    def create_arbitration_candidate_with_destinations(
        self, *, canonical_id: str, event: str, office: str, issued_at: str,
        expires_at: str, deadline: str, destination_ids=(),
        rest_feature: str = "", same_feature: str = "",
        vtec_key: str | None = None, same_destination_ids=None,
        rest_destination_ids=None, correlation_ugcs=(),
    ) -> None:
        if "ZCZC-" in rest_feature or "ZCZC-" in same_feature:
            raise ValueError("raw SAME headers must not be persisted")
        expiry = _parse_aware_timestamp(expires_at)
        if expiry is None:
            raise ValueError("expires_at must be an aware ISO timestamp")
        now = _now()
        legacy = {int(value) for value in destination_ids}
        same_ids = {int(value) for value in (
            legacy if same_destination_ids is None and same_feature else
            (same_destination_ids or ())
        )}
        rest_ids = {int(value) for value in (
            legacy if rest_destination_ids is None and rest_feature else
            (rest_destination_ids or ())
        )}
        if same_destination_ids is None and rest_destination_ids is None and not (
            same_feature or rest_feature
        ):
            same_ids = legacy
            rest_ids = legacy
        all_ids = sorted(legacy | same_ids | rest_ids)
        destinations = [
            (canonical_id, destination_id, int(destination_id in same_ids),
             int(destination_id in rest_ids), now)
            for destination_id in all_ids
        ]
        ugcs = sorted({str(value).strip().upper() for value in correlation_ugcs
                       if str(value).strip()})
        if not ugcs:
            _, parsed_rest = _safe_feature(rest_feature)
            _, parsed_same = _safe_feature(same_feature)
            ugcs = sorted(_feature_ugcs(parsed_rest) | _feature_ugcs(parsed_same))
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                self._conn.execute(
                    """INSERT INTO weather_arbitration_candidates
                       (canonical_id,event,office,issued_at,expires_at,deadline,
                        rest_feature,same_feature,vtec_key,phase,retain_until,
                        correlation_ugcs,created_at,updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,'initial',?,?,?,?)""",
                    (canonical_id, event, office, issued_at, expires_at, deadline,
                     rest_feature, same_feature, vtec_key,
                     (expiry + timedelta(hours=48)).isoformat(),
                     json.dumps(ugcs, separators=(",", ":")), now, now),
                )
                self._conn.executemany(
                    """INSERT INTO weather_arbitration_destinations
                       (canonical_id,destination_id,state,source,same_eligible,
                        rest_eligible,updated_at)
                       VALUES(?,?,'pending','',?,?,?)""",
                    destinations,
                )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def record_arbitration_same(
        self, canonical_id: str, same_feature: str, destination_ids,
        correlation_ugcs=(),
    ) -> list[int]:
        """Persist a redacted SAME observation and membership atomically."""
        if "ZCZC-" in same_feature:
            raise ValueError("raw SAME headers must not be persisted")
        now = _now()
        ids = sorted({int(value) for value in destination_ids})
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                row = self._conn.execute(
                    "SELECT phase,correlation_ugcs FROM weather_arbitration_candidates "
                    "WHERE canonical_id=?", (canonical_id,),
                ).fetchone()
                if row is None:
                    self._conn.rollback()
                    return []
                ugcs = set(json.loads(row["correlation_ugcs"] or "[]"))
                ugcs.update(str(value).strip().upper() for value in correlation_ugcs
                            if str(value).strip())
                state = "pending" if row["phase"] == "initial" else "superseded"
                self._conn.execute(
                    """UPDATE weather_arbitration_candidates
                       SET same_feature=?, correlation_ugcs=?, updated_at=?
                       WHERE canonical_id=?""",
                    (same_feature, json.dumps(sorted(ugcs), separators=(",", ":")),
                     now, canonical_id),
                )
                for destination_id in ids:
                    self._conn.execute(
                        """INSERT INTO weather_arbitration_destinations
                           (canonical_id,destination_id,state,source,same_eligible,
                            rest_eligible,updated_at) VALUES(?,?,?,'',1,0,?)
                           ON CONFLICT(canonical_id,destination_id) DO UPDATE SET
                             same_eligible=1, updated_at=excluded.updated_at""",
                        (canonical_id, destination_id, state, now),
                    )
                claimable = [int(item[0]) for item in self._conn.execute(
                    """SELECT destination_id FROM weather_arbitration_destinations
                       WHERE canonical_id=? AND state='pending' AND same_eligible=1
                       ORDER BY destination_id""", (canonical_id,),
                ).fetchall()] if row["phase"] == "initial" else []
                self._conn.commit()
                return claimable
            except Exception:
                self._conn.rollback()
                raise

    def add_arbitration_destinations(self, canonical_id: str,
                                     destination_ids) -> None:
        """Compatibility helper for legacy SAME call sites."""
        now = _now()
        values = [(canonical_id, int(value), now) for value in destination_ids]
        with self._lock:
            self._conn.executemany(
                """INSERT INTO weather_arbitration_destinations
                   (canonical_id,destination_id,state,source,same_eligible,
                    rest_eligible,updated_at)
                   VALUES(?,?,'pending','',1,0,?)
                   ON CONFLICT(canonical_id,destination_id) DO UPDATE SET
                     same_eligible=1,updated_at=excluded.updated_at""",
                values,
            )
            self._conn.commit()

    def due_arbitration_candidates(self, now: str) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                """SELECT c.* FROM weather_arbitration_candidates c
                   WHERE c.deadline <= ? AND c.rest_feature != ''
                     AND c.phase = 'initial'
                     AND EXISTS (
                       SELECT 1 FROM weather_arbitration_destinations d
                       WHERE d.canonical_id = c.canonical_id
                         AND d.state = 'pending' AND d.rest_eligible = 1
                     ) ORDER BY c.deadline, c.canonical_id""",
                (now,),
            ).fetchall()

    def list_arbitration_candidates(self) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM weather_arbitration_candidates ORDER BY created_at, canonical_id"
            ).fetchall()

    def get_arbitration_candidate(self, canonical_id: str) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM weather_arbitration_candidates WHERE canonical_id = ?",
                (canonical_id,),
            ).fetchone()

    def get_arbitration_candidate_by_vtec(self, vtec_key: str) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM weather_arbitration_candidates WHERE vtec_key = ?",
                (vtec_key,),
            ).fetchone()

    def set_arbitration_same_feature(self, canonical_id: str,
                                     same_feature: str) -> bool:
        if "ZCZC-" in same_feature:
            raise ValueError("raw SAME headers must not be persisted")
        with self._lock:
            cur = self._conn.execute(
                """UPDATE weather_arbitration_candidates
                   SET same_feature = ?, updated_at = ? WHERE canonical_id = ?""",
                (same_feature, _now(), canonical_id),
            )
            self._conn.commit()
            return cur.rowcount == 1

    def bind_arbitration_rest_with_destinations(
        self, canonical_id: str, rest_feature: str, vtec_key: str,
        destination_ids,
    ) -> bool:
        """Compatibility wrapper for an atomic REST NEW bind."""
        return self.bind_arbitration_rest_new(
            canonical_id, rest_feature, vtec_key, destination_ids
        )

    def bind_arbitration_rest_new(
        self, canonical_id: str, rest_feature: str, vtec_key: str,
        destination_ids, *, expires_at: str | None = None,
        correlation_ugcs=(),
    ) -> bool:
        if "ZCZC-" in rest_feature:
            raise ValueError("raw SAME headers must not be persisted")
        now = _now()
        ids = sorted({int(value) for value in destination_ids})
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                row = self._conn.execute(
                    "SELECT * FROM weather_arbitration_candidates WHERE canonical_id=?",
                    (canonical_id,),
                ).fetchone()
                if row is None or (row["vtec_key"] not in {None, vtec_key}):
                    self._conn.rollback()
                    return False
                phase = str(row["phase"])
                current_expiry = _parse_aware_timestamp(row["expires_at"])
                proposed_expiry = _parse_aware_timestamp(expires_at) if expires_at else None
                expiry = max(value for value in (current_expiry, proposed_expiry) if value)
                ugcs = set(json.loads(row["correlation_ugcs"] or "[]"))
                ugcs.update(str(value).strip().upper() for value in correlation_ugcs
                            if str(value).strip())
                _, parsed = _safe_feature(rest_feature)
                ugcs.update(_feature_ugcs(parsed))
                if phase == "initial":
                    self._conn.execute(
                        """UPDATE weather_arbitration_candidates SET
                           rest_feature=?,vtec_key=?,expires_at=?,retain_until=?,
                           correlation_ugcs=?,updated_at=? WHERE canonical_id=?""",
                        (rest_feature, vtec_key, expiry.isoformat(),
                         (expiry + timedelta(hours=48)).isoformat(),
                         json.dumps(sorted(ugcs), separators=(",", ":")), now,
                         canonical_id),
                    )
                self._conn.execute(
                    "UPDATE weather_arbitration_destinations SET rest_eligible=0 "
                    "WHERE canonical_id=?", (canonical_id,),
                )
                for destination_id in ids:
                    self._conn.execute(
                        """INSERT INTO weather_arbitration_destinations
                           (canonical_id,destination_id,state,source,same_eligible,
                            rest_eligible,updated_at) VALUES(?,?,?,'',0,1,?)
                           ON CONFLICT(canonical_id,destination_id) DO UPDATE SET
                             rest_eligible=1,updated_at=excluded.updated_at""",
                        (canonical_id, destination_id,
                         "pending" if phase == "initial" else "superseded", now),
                    )
                self._conn.execute(
                    """UPDATE weather_arbitration_destinations
                       SET state='no_fallback',source='',updated_at=?
                       WHERE canonical_id=? AND state='pending'
                         AND same_eligible=1 AND rest_eligible=0""",
                    (now, canonical_id),
                )
                if phase != "initial":
                    self._conn.execute(
                        """UPDATE weather_arbitration_destinations
                           SET state='superseded',source='',updated_at=?
                           WHERE canonical_id=? AND state IN ('pending','queued')""",
                        (now, canonical_id),
                    )
                self._conn.commit()
                return True
            except sqlite3.IntegrityError:
                self._conn.rollback()
                return False
            except Exception:
                self._conn.rollback()
                raise

    def apply_arbitration_followup(
        self, canonical_id: str, expected_vtec: str, action: str,
        current_feature: str, expires_at: str, destination_ids,
        correlation_ugcs=(), *, now: str | None = None,
    ) -> bool:
        if action not in {"CON", "EXT", "CAN", "EXP"}:
            raise ValueError("invalid VTEC follow-up action")
        if "ZCZC-" in current_feature:
            raise ValueError("raw SAME headers must not be persisted")
        proposed_expiry = _parse_aware_timestamp(expires_at)
        observed = _parse_aware_timestamp(now or _now())
        if proposed_expiry is None or observed is None:
            raise ValueError("follow-up timestamps must be aware ISO timestamps")
        ids = sorted({int(value) for value in destination_ids})
        _, parsed_feature = _safe_feature(current_feature)
        raw_followup_id = parsed_feature.get("id") if parsed_feature is not None else None
        followup_id = str(raw_followup_id or "")
        if not followup_id or len(followup_id) > 512:
            raise ValueError("follow-up id must be a bounded non-empty string")
        updated_at = observed.isoformat()
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                row = self._conn.execute(
                    "SELECT * FROM weather_arbitration_candidates WHERE canonical_id=?",
                    (canonical_id,),
                ).fetchone()
                if row is None or row["vtec_key"] != expected_vtec:
                    self._conn.rollback()
                    return False
                current_expiry = _parse_aware_timestamp(row["expires_at"])
                if row["rest_feature"] != current_feature:
                    _, persisted = _safe_feature(row["rest_feature"])
                    _, incoming = _safe_feature(current_feature)
                    persisted_order = _feature_product_timestamp(persisted)
                    incoming_order = _feature_product_timestamp(incoming)
                    regresses_expiry = (
                        current_expiry is not None and proposed_expiry < current_expiry
                    )
                    if (
                        persisted_order is not None and incoming_order is not None
                        and incoming_order < persisted_order
                    ):
                        self._conn.rollback()
                        return False
                    same_order = (
                        persisted_order is not None and incoming_order == persisted_order
                    )
                    incomparable_order = (
                        persisted_order is None or incoming_order is None
                    )
                    if regresses_expiry and (same_order or incomparable_order):
                        self._conn.rollback()
                        return False
                target_phase = {"CON": "updated", "EXT": "updated",
                                "CAN": "cancelled", "EXP": "expired"}[action]
                if row["phase"] in {"cancelled", "expired"}:
                    if (row["phase"] != target_phase
                            or row["rest_feature"] != current_feature):
                        self._conn.rollback()
                        return False
                    expiry = max(current_expiry or proposed_expiry, proposed_expiry)
                    retention = max(
                        _parse_aware_timestamp(row["retain_until"]) or expiry,
                        expiry + timedelta(hours=48),
                        observed + timedelta(hours=48),
                    )
                    self._conn.execute(
                        """UPDATE weather_arbitration_candidates
                           SET expires_at=?,retain_until=?,updated_at=?
                           WHERE canonical_id=?""",
                        (expiry.isoformat(), retention.isoformat(), updated_at,
                         canonical_id),
                    )
                    self._conn.commit()
                    return True
                expiry = max(current_expiry or proposed_expiry, proposed_expiry)
                phase = target_phase
                retention = max(
                    _parse_aware_timestamp(row["retain_until"]) or expiry,
                    expiry + timedelta(hours=48),
                    observed + timedelta(hours=48) if phase in {"cancelled", "expired"}
                    else expiry,
                )
                ugcs = set(json.loads(row["correlation_ugcs"] or "[]"))
                ugcs.update(str(value).strip().upper() for value in correlation_ugcs
                            if str(value).strip())
                _, parsed = _safe_feature(current_feature)
                ugcs.update(_feature_ugcs(parsed))
                self._conn.execute(
                    """UPDATE weather_arbitration_candidates SET rest_feature=?,
                       expires_at=?,retain_until=?,phase=?,correlation_ugcs=?,updated_at=?
                       WHERE canonical_id=?""",
                    (current_feature, expiry.isoformat(), retention.isoformat(), phase,
                     json.dumps(sorted(ugcs), separators=(",", ":")), updated_at,
                     canonical_id),
                )
                self._conn.execute(
                    "UPDATE weather_arbitration_destinations SET rest_eligible=0 "
                    "WHERE canonical_id=?", (canonical_id,),
                )
                for destination_id in ids:
                    self._conn.execute(
                        """INSERT INTO weather_arbitration_destinations
                           (canonical_id,destination_id,state,source,same_eligible,
                            rest_eligible,updated_at) VALUES(?,?,'superseded','',0,1,?)
                           ON CONFLICT(canonical_id,destination_id) DO UPDATE SET
                             rest_eligible=1,updated_at=excluded.updated_at""",
                        (canonical_id, destination_id, updated_at),
                    )
                self._conn.execute(
                    """UPDATE weather_arbitration_destinations
                       SET state='superseded',source='',updated_at=?
                       WHERE canonical_id=? AND state IN ('pending','queued')""",
                    (updated_at, canonical_id),
                )
                self._conn.execute(
                    """UPDATE weather_arbitration_followups
                       SET state='superseded',error='superseded',updated_at=?
                       WHERE canonical_id=? AND followup_id!=?
                         AND state IN ('pending','queued')""",
                    (updated_at, canonical_id, followup_id),
                )
                self._conn.executemany(
                    """INSERT INTO weather_arbitration_followups
                       (canonical_id,followup_id,destination_id,feature,action,state,
                        error,created_at,updated_at)
                       VALUES(?,?,?,?,?,'pending','',?,?)
                       ON CONFLICT(canonical_id,followup_id,destination_id) DO NOTHING""",
                    [
                        (canonical_id, followup_id, destination_id, current_feature,
                         action, updated_at, updated_at)
                        for destination_id in ids
                    ],
                )
                self._conn.commit()
                return True
            except Exception:
                self._conn.rollback()
                raise

    def accept_arbitrated_followup(
        self, *, canonical_id: str, followup_id: str, destination_id: int,
        attempt_id: int,
    ) -> bool:
        """Atomically accept the exact follow-up, attempt, and recipient chain."""
        now = _now()
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                outbox = self._conn.execute(
                    """UPDATE weather_arbitration_followups
                       SET state='accepted',error='',updated_at=?
                       WHERE canonical_id=? AND followup_id=? AND destination_id=?
                         AND state='queued'""",
                    (now, canonical_id, followup_id, destination_id),
                )
                if outbox.rowcount != 1:
                    self._conn.rollback()
                    return False
                attempt = self._conn.execute(
                    """SELECT * FROM delivery_attempts
                       WHERE id=? AND destination_id=?
                         AND arbitration_canonical_id=?
                         AND arbitration_followup_id=?""",
                    (attempt_id, destination_id, canonical_id, followup_id),
                ).fetchone()
                if attempt is None:
                    self._conn.rollback()
                    return False
                finalized = self._conn.execute(
                    """UPDATE delivery_attempts
                       SET state='accepted',error='',finalized_at=?
                       WHERE id=? AND state='queued'""",
                    (now, attempt_id),
                )
                if finalized.rowcount != 1:
                    self._conn.rollback()
                    return False
                self._conn.execute(
                    """INSERT INTO alert_delivery_state
                       (root_alert_id,last_alert_id,destination_id,transport,channel,
                        matched_areas,event,headline,expires,msg_hash,disposition,
                        sent_ts,updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(root_alert_id,destination_id) DO UPDATE SET
                         last_alert_id=excluded.last_alert_id,
                         transport=excluded.transport,channel=excluded.channel,
                         matched_areas=excluded.matched_areas,event=excluded.event,
                         headline=excluded.headline,expires=excluded.expires,
                         msg_hash=excluded.msg_hash,disposition=excluded.disposition,
                         updated_at=excluded.updated_at""",
                    (
                        attempt["root_alert_id"], attempt["alert_id"], destination_id,
                        attempt["transport"], attempt["channel"],
                        attempt["matched_areas"], attempt["event"], attempt["headline"],
                        attempt["expires"], attempt["msg_hash"], attempt["disposition"],
                        now, now,
                    ),
                )
                self._conn.commit()
                return True
            except Exception:
                self._conn.rollback()
                raise

    def pending_arbitration_followups(
        self, canonical_id: str | None = None,
    ) -> list[sqlite3.Row]:
        clause = " AND canonical_id=?" if canonical_id is not None else ""
        params = (canonical_id,) if canonical_id is not None else ()
        with self._lock:
            return self._conn.execute(
                """SELECT canonical_id,followup_id,feature,action
                   FROM weather_arbitration_followups
                   WHERE state='pending'""" + clause +
                " GROUP BY canonical_id,followup_id,feature,action "
                "ORDER BY MIN(created_at),canonical_id,followup_id",
                params,
            ).fetchall()

    def claim_arbitration_followup(self, canonical_id: str,
                                   followup_id: str) -> list[int]:
        now = _now()
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                rows = self._conn.execute(
                    """SELECT destination_id FROM weather_arbitration_followups
                       WHERE canonical_id=? AND followup_id=? AND state='pending'
                       ORDER BY destination_id""",
                    (canonical_id, followup_id),
                ).fetchall()
                ids = [int(row["destination_id"]) for row in rows]
                if ids:
                    self._conn.execute(
                        """UPDATE weather_arbitration_followups
                           SET state='queued',error='',updated_at=?
                           WHERE canonical_id=? AND followup_id=? AND state='pending'""",
                        (now, canonical_id, followup_id),
                    )
                self._conn.commit()
                return ids
            except Exception:
                self._conn.rollback()
                raise

    def finish_arbitration_followup(
        self, canonical_id: str, followup_id: str, destination_id: int, outcome,
        error: str = "",
    ) -> bool:
        value = getattr(outcome, "value", outcome)
        if isinstance(value, bool):
            value = "accepted" if value else "retryable_failure"
        states = {
            "accepted": "accepted",
            "retryable_failure": "pending",
            "permanent_no_target": "no_target",
        }
        if value not in states:
            raise ValueError("invalid follow-up delivery outcome")
        with self._lock:
            cur = self._conn.execute(
                """UPDATE weather_arbitration_followups
                   SET state=?,error=?,updated_at=?
                   WHERE canonical_id=? AND followup_id=? AND destination_id=?
                     AND state='queued'""",
                (states[value], error, _now(), canonical_id, followup_id,
                 int(destination_id)),
            )
            self._conn.commit()
            return cur.rowcount == 1

    def release_arbitration_followup(self, canonical_id: str,
                                     followup_id: str) -> int:
        with self._lock:
            cur = self._conn.execute(
                """UPDATE weather_arbitration_followups
                   SET state='pending',error='interrupted before enqueue',updated_at=?
                   WHERE canonical_id=? AND followup_id=? AND state='queued'""",
                (_now(), canonical_id, followup_id),
            )
            self._conn.commit()
            return cur.rowcount

    def recover_queued_arbitration_followups(self) -> int:
        with self._lock:
            cur = self._conn.execute(
                """UPDATE weather_arbitration_followups
                   SET state='pending',error='interrupted before completion',updated_at=?
                   WHERE state='queued'""",
                (_now(),),
            )
            self._conn.commit()
            return cur.rowcount

    def supersede_arbitration_rest(self, canonical_id: str,
                                   rest_feature: str) -> bool:
        """Persist a follow-up and retire any unaccepted initial fallback."""
        if "ZCZC-" in rest_feature:
            raise ValueError("raw SAME headers must not be persisted")
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                cur = self._conn.execute(
                    """UPDATE weather_arbitration_candidates
                       SET rest_feature = ?, updated_at = ?
                       WHERE canonical_id = ? AND rest_feature != ''""",
                    (rest_feature, _now(), canonical_id),
                )
                if cur.rowcount == 1:
                    self._conn.execute(
                        """UPDATE weather_arbitration_destinations
                           SET state = 'superseded', source = '', updated_at = ?
                           WHERE canonical_id = ? AND state IN ('pending','queued')""",
                        (_now(), canonical_id),
                    )
                self._conn.commit()
                return cur.rowcount == 1
            except Exception:
                self._conn.rollback()
                raise

    def pending_arbitration_destinations(self, canonical_id: str) -> list[int]:
        with self._lock:
            rows = self._conn.execute(
                """SELECT destination_id FROM weather_arbitration_destinations
                   WHERE canonical_id = ? AND state = 'pending'
                   ORDER BY destination_id""",
                (canonical_id,),
            ).fetchall()
        return [int(row["destination_id"]) for row in rows]

    def active_arbitration_destinations(self, canonical_id: str) -> list[int]:
        """Recipients whose initial arbitration send remains pending or in flight."""
        with self._lock:
            rows = self._conn.execute(
                """SELECT destination_id FROM weather_arbitration_destinations
                   WHERE canonical_id=? AND state IN ('pending','queued')
                   ORDER BY destination_id""",
                (canonical_id,),
            ).fetchall()
        return [int(row["destination_id"]) for row in rows]

    def claim_arbitration_destinations(self, canonical_id: str,
                                       destination_ids, source: str) -> list[int]:
        if source not in {"same", "rest"}:
            raise ValueError("invalid arbitration source")
        claimed: list[int] = []
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                for destination_id in destination_ids:
                    cur = self._conn.execute(
                        """UPDATE weather_arbitration_destinations AS d
                           SET state = 'queued', source = ?, updated_at = ?
                           WHERE d.canonical_id = ? AND d.destination_id = ?
                             AND d.state = 'pending'
                             AND CASE WHEN ? = 'rest' THEN d.rest_eligible
                                      ELSE d.same_eligible END = 1
                             AND EXISTS (
                               SELECT 1 FROM weather_arbitration_candidates c
                               WHERE c.canonical_id=d.canonical_id
                                 AND c.phase='initial'
                             )""",
                        (source, _now(), canonical_id, int(destination_id), source),
                    )
                    if cur.rowcount == 1:
                        claimed.append(int(destination_id))
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        return claimed

    def finish_arbitration_destination(self, canonical_id: str,
                                       destination_id: int, source: str,
                                       outcome) -> bool:
        value = getattr(outcome, "value", outcome)
        if isinstance(value, bool):
            value = "accepted" if value else "retryable_failure"
        if value not in {"accepted", "retryable_failure", "permanent_no_target"}:
            raise ValueError("invalid arbitration delivery outcome")
        with self._lock:
            if value == "accepted":
                state, stored_source = "accepted", source
            elif value == "permanent_no_target":
                state, stored_source = "no_fallback", ""
            else:
                no_fallback = self._conn.execute(
                    """SELECT 1 FROM weather_arbitration_destinations d
                       JOIN weather_arbitration_candidates c
                         ON c.canonical_id=d.canonical_id
                       WHERE d.canonical_id=? AND d.destination_id=?
                         AND ?='same' AND d.rest_eligible=0
                         AND c.rest_feature!=''""",
                    (canonical_id, destination_id, source),
                ).fetchone()
                state = "no_fallback" if no_fallback else "pending"
                stored_source = ""
            cur = self._conn.execute(
                """UPDATE weather_arbitration_destinations
                   SET state = ?, source = ?, updated_at = ?
                   WHERE canonical_id = ? AND destination_id = ?
                     AND state = 'queued' AND source = ?""",
                (state, stored_source, _now(), canonical_id,
                 destination_id, source),
            )
            self._conn.commit()
            return cur.rowcount == 1

    def release_arbitration_destinations(self, canonical_id: str,
                                         destination_ids, source: str) -> int:
        if source not in {"same", "rest"}:
            raise ValueError("invalid arbitration source")
        released = 0
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                for destination_id in destination_ids:
                    cur = self._conn.execute(
                        """UPDATE weather_arbitration_destinations
                           SET state = 'pending', source = '', updated_at = ?
                           WHERE canonical_id = ? AND destination_id = ?
                             AND state = 'queued' AND source = ?""",
                        (_now(), canonical_id, int(destination_id), source),
                    )
                    released += cur.rowcount
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        return released

    def reconcile_successful_arbitration_transmits(self) -> int:
        """Finalize queued arbitration sends with exact durable transport evidence."""
        now = _now()
        reconciled = 0
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                rows = self._conn.execute(
                    """SELECT a.*, d.canonical_id,
                              d.source AS arbitration_source, f.followup_id
                       FROM delivery_attempts a
                       JOIN transmit_log t ON t.delivery_attempt_id=a.id
                         AND t.success=1 AND t.manual=0
                         AND t.destination_id=a.destination_id
                         AND t.transport=a.transport AND t.channel=a.channel
                         AND t.text=a.message_text
                       LEFT JOIN weather_arbitration_destinations d
                         ON d.canonical_id=a.root_alert_id
                        AND d.destination_id=a.destination_id
                        AND d.state='queued'
                       LEFT JOIN weather_arbitration_followups f
                         ON f.canonical_id=a.arbitration_canonical_id
                        AND f.followup_id=a.arbitration_followup_id
                        AND f.destination_id=a.destination_id
                        AND f.state='queued'
                       WHERE a.state='queued'
                         AND (d.canonical_id IS NOT NULL OR f.canonical_id IS NOT NULL)
                       ORDER BY a.id"""
                ).fetchall()
                for attempt in rows:
                    canonical_id = str(
                        attempt["canonical_id"] or attempt["root_alert_id"]
                    )
                    if attempt["followup_id"] is not None:
                        changed = self._conn.execute(
                            """UPDATE weather_arbitration_followups
                               SET state='accepted',error='',updated_at=?
                               WHERE canonical_id=? AND followup_id=?
                                 AND destination_id=? AND state='queued'""",
                            (now, canonical_id, attempt["followup_id"],
                             attempt["destination_id"]),
                        ).rowcount
                    else:
                        changed = self._conn.execute(
                            """UPDATE weather_arbitration_destinations
                               SET state='accepted',updated_at=?
                               WHERE canonical_id=? AND destination_id=?
                                 AND state='queued' AND source=?""",
                            (now, canonical_id, attempt["destination_id"],
                             attempt["arbitration_source"]),
                        ).rowcount
                    if changed != 1:
                        continue
                    finalized = self._conn.execute(
                        """UPDATE delivery_attempts
                           SET state='accepted',error='',finalized_at=?
                           WHERE id=? AND state='queued'""",
                        (now, attempt["id"]),
                    )
                    if finalized.rowcount != 1:
                        raise RuntimeError(
                            "arbitration transmit reconciliation raced"
                        )
                    self._conn.execute(
                        """INSERT INTO alert_delivery_state
                           (root_alert_id,last_alert_id,destination_id,transport,
                            channel,matched_areas,event,headline,expires,msg_hash,
                            disposition,sent_ts,updated_at)
                           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                           ON CONFLICT(root_alert_id,destination_id) DO UPDATE SET
                             last_alert_id=excluded.last_alert_id,
                             transport=excluded.transport,
                             channel=excluded.channel,
                             matched_areas=excluded.matched_areas,
                             event=excluded.event,headline=excluded.headline,
                             expires=excluded.expires,msg_hash=excluded.msg_hash,
                             disposition=excluded.disposition,
                             updated_at=excluded.updated_at""",
                        (
                            attempt["root_alert_id"], attempt["alert_id"],
                            attempt["destination_id"], attempt["transport"],
                            attempt["channel"], attempt["matched_areas"],
                            attempt["event"], attempt["headline"],
                            attempt["expires"], attempt["msg_hash"],
                            attempt["disposition"], now, now,
                        ),
                    )
                    reconciled += 1
                self._conn.commit()
                return reconciled
            except Exception:
                self._conn.rollback()
                raise

    def recover_queued_arbitration_destinations(self) -> int:
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                pending = self._conn.execute(
                    """UPDATE weather_arbitration_destinations AS d
                       SET state='pending',source='',updated_at=?
                       WHERE d.state='queued' AND EXISTS (
                         SELECT 1 FROM weather_arbitration_candidates c
                         WHERE c.canonical_id=d.canonical_id AND c.phase='initial'
                       )""", (_now(),),
                )
                superseded = self._conn.execute(
                    """UPDATE weather_arbitration_destinations AS d
                       SET state='superseded',source='',updated_at=?
                       WHERE d.state='queued' AND EXISTS (
                         SELECT 1 FROM weather_arbitration_candidates c
                         WHERE c.canonical_id=d.canonical_id AND c.phase!='initial'
                       )""", (_now(),),
                )
                self._conn.commit()
                return pending.rowcount + superseded.rowcount
            except Exception:
                self._conn.rollback()
                raise

    def prune_arbitration_candidates(self, now: str) -> int:
        parsed = _parse_aware_timestamp(now)
        if parsed is None:
            raise ValueError("now must be an aware ISO timestamp")
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM weather_arbitration_candidates WHERE retain_until <= ?",
                (parsed.isoformat(),),
            )
            self._conn.commit()
            return cur.rowcount

    # ---- alert dedupe state --------------------------------------------
    def get_state(self, nws_id: str) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM alert_state WHERE nws_id = ?", (nws_id,)
            ).fetchone()

    def upsert_state(
        self,
        nws_id: str,
        event: str,
        headline: str,
        expires: str,
        msg_hash: str,
        disposition: str,
        sent_ts: str | None,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """INSERT INTO alert_state
                   (nws_id, event, headline, expires, msg_hash, sent_ts,
                    disposition, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(nws_id) DO UPDATE SET
                       event=excluded.event,
                       headline=excluded.headline,
                       expires=excluded.expires,
                       msg_hash=excluded.msg_hash,
                       sent_ts=COALESCE(excluded.sent_ts, alert_state.sent_ts),
                       disposition=excluded.disposition,
                       updated_at=excluded.updated_at""",
                (nws_id, event, headline, expires, msg_hash, sent_ts,
                 disposition, _now()),
            )
            self._conn.commit()

    def purge_expired_state(self) -> int:
        """Remove global and per-destination state 48h past alert expiry."""
        cutoff = datetime.now(UTC) - timedelta(hours=STATE_EXPIRY_HOURS)
        with self._lock:
            delivery_ids = [
                row["chain_id"] for row in self._conn.execute(
                    "SELECT chain_id, expires FROM alert_delivery_state"
                ).fetchall()
                if (parsed := _parse_aware_timestamp(row["expires"])) is not None
                and parsed < cutoff
            ]
            alert_ids = [
                row["nws_id"] for row in self._conn.execute(
                    "SELECT nws_id, expires FROM alert_state"
                ).fetchall()
                if (parsed := _parse_aware_timestamp(row["expires"])) is not None
                and parsed < cutoff
            ]
            self._conn.executemany(
                "DELETE FROM alert_delivery_state WHERE chain_id = ?",
                [(value,) for value in delivery_ids],
            )
            self._conn.executemany(
                "DELETE FROM alert_state WHERE nws_id = ?",
                [(value,) for value in alert_ids],
            )
            self._conn.commit()
            return len(delivery_ids) + len(alert_ids)

    # ---- history --------------------------------------------------------
    def add_history(
        self,
        nws_id: str,
        event: str,
        area: str,
        disposition: str,
        transmitted_text: str = "",
        detail: str = "",
    ) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO history(ts, nws_id, event, area, disposition, "
                "transmitted_text, detail) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (_now(), nws_id, event, area, disposition,
                 transmitted_text, detail),
            )
            self._conn.commit()

    def history_exists(self, nws_id: str, disposition: str | None = None) -> bool:
        # True if a history row exists for this alert id (optionally a specific
        # disposition). With disposition=None it dedupes by id alone, so each
        # alert pulled from NOAA is logged only once despite re-polling.
        with self._lock:
            if disposition is None:
                row = self._conn.execute(
                    "SELECT 1 FROM history WHERE nws_id = ? LIMIT 1",
                    (nws_id,),
                ).fetchone()
            else:
                row = self._conn.execute(
                    "SELECT 1 FROM history WHERE nws_id = ? AND disposition = ? "
                    "LIMIT 1",
                    (nws_id, disposition),
                ).fetchone()
        return row is not None

    def update_history(self, nws_id: str, disposition: str,
                       transmitted_text: str | None = None,
                       detail: str | None = None) -> None:
        """Update the canonical history row without interpolating caller values."""
        with self._lock:
            self._conn.execute(
                """UPDATE history SET disposition = ?,
                   transmitted_text = COALESCE(?, transmitted_text),
                   detail = COALESCE(?, detail) WHERE nws_id = ?""",
                (disposition, transmitted_text, detail, nws_id),
            )
            self._conn.commit()

    def prune_history(self, keep_days: int = 90) -> int:
        # Delete history rows older than keep_days; bounds long-term growth.
        cutoff = (
            datetime.now(UTC) - timedelta(days=keep_days)
        ).isoformat(timespec="seconds")
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM history WHERE ts < ?", (cutoff,)
            )
            self._conn.execute(
                "DELETE FROM meshwx_delivery_state WHERE updated_at < ?", (cutoff,)
            )
            self._conn.commit()
            return cur.rowcount

    def query_history(
        self,
        disposition: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int = 200,
    ) -> list[sqlite3.Row]:
        clauses, params = [], []
        if disposition:
            clauses.append("disposition = ?")
            params.append(disposition)
        if date_from:
            clauses.append("ts >= ?")
            params.append(date_from)
        if date_to:
            clauses.append("ts <= ?")
            params.append(date_to)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        with self._lock:
            return self._conn.execute(
                f"SELECT * FROM history {where} ORDER BY id DESC LIMIT ?",
                params,
            ).fetchall()

    # ---- transmit log ---------------------------------------------------
    def add_transmit_log(
        self,
        channel: int,
        byte_count: int,
        success: bool,
        text: str,
        manual: bool = False,
        error: str = "",
        transport: str = "meshtastic",
        destination_id: int | None = None,
        followup_text: str = "",
        delivery_attempt_id: int | None = None,
    ) -> None:
        vals = (_now(), channel, byte_count, int(success), int(manual), text, error,
                transport, destination_id, followup_text, delivery_attempt_id)
        with self._lock:
            self._conn.execute(
                """INSERT INTO transmit_log
                   (ts,channel,byte_count,success,manual,text,error,transport,
                    destination_id,followup_text,delivery_attempt_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                vals,
            )
            self._conn.commit()

    def query_transmit_log(self, limit: int = 200) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM transmit_log ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()

    def get_transmit_log(self, entry_id: int):
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM transmit_log WHERE id = ?", (entry_id,)
            ).fetchone()

    # ---- errors ---------------------------------------------------------
    def add_error(self, source: str, message: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO errors(ts, source, message) VALUES (?, ?, ?)",
                (_now(), source, message),
            )
            self._conn.commit()

    def recent_errors(self, limit: int = 50) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM errors ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()

    def clear_errors(self) -> int:
        with self._lock:
            cur = self._conn.execute("DELETE FROM errors")
            self._conn.commit()
            return cur.rowcount

    # ---- events (dashboard feed) ---------------------------------------
    def add_event(self, level: str, message: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO events(ts, level, message) VALUES (?, ?, ?)",
                (_now(), level, message),
            )
            self._conn.commit()

    def recent_events(self, limit: int = 10) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()

    # ---- IPAWS (separate from the weather pipeline) --------------------
    def ipaws_seen(self, identifier: str) -> bool:
        with self._lock:
            return self._conn.execute(
                "SELECT 1 FROM ipaws_log WHERE identifier = ?", (identifier,)
            ).fetchone() is not None

    def add_ipaws(self, identifier, sender, event, area, headline, msg_type,
                  status, sent, text, transmitted, error="") -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR IGNORE INTO ipaws_log(ts, identifier, sender, event, area, "
                "headline, msg_type, status, sent, text, transmitted, error) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (_now(), identifier, sender, event, area, headline, msg_type,
                 status, sent, text, 1 if transmitted else 0, error),
            )
            self._conn.commit()

    def update_ipaws(self, identifier: str, transmitted: bool, error: str = "") -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE ipaws_log SET transmitted = ?, error = ? WHERE identifier = ?",
                (1 if transmitted else 0, error, identifier))
            self._conn.commit()

    def get_ipaws(self, identifier: str):
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM ipaws_log WHERE identifier = ?", (identifier,)
            ).fetchone()

    def query_ipaws(self, limit: int = 60) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM ipaws_log ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()

    def prune_ipaws(self, keep_days: int = 14) -> int:
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM ipaws_log WHERE ts < datetime('now', ?)",
                (f"-{keep_days:d} days",))
            self._conn.commit()
            return cur.rowcount

    def close(self) -> None:
        with self._lock:
            self._conn.close()
