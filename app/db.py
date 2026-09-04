"""SQLite storage: settings, alert state (dedupe), history, transmit log, errors.

The database is the source of truth. A single connection is shared across the
asyncio loop and the transmit worker thread, guarded by a lock. SQLite calls
are fast local operations, so running them synchronously is fine.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from .config import DEFAULT_SETTINGS, STATE_EXPIRY_HOURS

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
    all_warnings INTEGER NOT NULL DEFAULT 0
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
    finalized_at TEXT
);

CREATE TABLE IF NOT EXISTS meshwx_delivery_state (
    alert_id TEXT PRIMARY KEY,
    msg_hash TEXT NOT NULL,
    channel INTEGER NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('queued','accepted','failed','superseded')),
    error TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL
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
    destination_id INTEGER
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
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _parse_aware_timestamp(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


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
        self._init_schema()
        self._seed_settings()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(SCHEMA)
            columns = {row[1] for row in self._conn.execute("PRAGMA table_info(transmit_log)")}
            if "transport" not in columns:
                self._conn.execute("ALTER TABLE transmit_log ADD COLUMN transport TEXT")
            if "destination_id" not in columns:
                self._conn.execute("ALTER TABLE transmit_log ADD COLUMN destination_id INTEGER")
            self._conn.commit()

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
        destination_ids: list[int],
    ) -> int:
        """Create a complete manual rule atomically."""
        unique_counties = list(dict.fromkeys(counties))
        unique_events = list(dict.fromkeys(events))
        unique_destinations = list(dict.fromkeys(destination_ids))
        with self._lock:
            try:
                self._conn.execute("BEGIN")
                cur = self._conn.execute(
                    """INSERT INTO routing_rules(name, enabled, priority, all_warnings)
                       VALUES (?, ?, ?, ?)""",
                    (name, int(enabled), priority, int(all_warnings)),
                )
                lastrowid = cur.lastrowid
                assert lastrowid is not None
                rule_id = int(lastrowid)
                self._conn.executemany(
                    "INSERT INTO route_counties(rule_id, zone_code, county_name) VALUES (?, ?, ?)",
                    [(rule_id, code, county_name) for code, county_name in unique_counties],
                )
                self._conn.executemany(
                    "INSERT INTO route_events(rule_id, event) VALUES (?, ?)",
                    [(rule_id, event) for event in unique_events],
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
        destination_ids: list[int],
    ) -> bool:
        """Replace a manual rule and all associations atomically."""
        unique_counties = list(dict.fromkeys(counties))
        unique_events = list(dict.fromkeys(events))
        unique_destinations = list(dict.fromkeys(destination_ids))
        with self._lock:
            try:
                self._conn.execute("BEGIN")
                cur = self._conn.execute(
                    """UPDATE routing_rules SET name = ?, priority = ?, enabled = ?,
                       all_warnings = ? WHERE id = ?""",
                    (name, priority, int(enabled), int(all_warnings), rule_id),
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
                    "INSERT INTO route_events(rule_id, event) VALUES (?, ?)",
                    [(rule_id, event) for event in unique_events],
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
                             all_warnings: bool = False) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM route_events WHERE rule_id = ?", (rule_id,))
            self._conn.executemany(
                "INSERT INTO route_events(rule_id, event) VALUES (?, ?)",
                [(rule_id, event) for event in events],
            )
            self._conn.execute(
                "UPDATE routing_rules SET all_warnings = ? WHERE id = ?",
                (int(all_warnings), rule_id),
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

    def get_route(self, rule_id: int) -> Optional[dict[str, Any]]:
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
        rule["counties"] = [dict(value) for value in self._conn.execute(
            "SELECT zone_code, county_name FROM route_counties "
            "WHERE rule_id = ? ORDER BY zone_code", (rule_id,),
        ).fetchall()]
        rule["events"] = [value["event"] for value in self._conn.execute(
            "SELECT event FROM route_events WHERE rule_id = ? ORDER BY event", (rule_id,),
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
                          r.priority, r.all_warnings, c.zone_code, c.county_name,
                          e.event, d.id destination_id, d.name destination_name,
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
                f"OR root_alert_id IN ({placeholders}) ORDER BY chain_id", ids + ids,
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
    ) -> int:
        """Persist the immutable snapshot for one enqueue attempt."""
        with self._lock:
            cur = self._conn.execute(
                """INSERT INTO delivery_attempts
                   (root_alert_id,alert_id,destination_id,destination_name,transport,
                    channel,matched_areas,message_text,event,headline,expires,msg_hash,
                    disposition,state,queued_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,'queued',?)""",
                (root_alert_id, alert_id, destination_id, destination_name, transport,
                 channel, json.dumps(matched_areas), message_text, event, headline,
                 expires, msg_hash, disposition, _now()),
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

    def query_delivery_attempts(
        self, *, alert_id: Optional[str] = None,
        destination_id: Optional[int] = None, state: Optional[str] = None,
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
                "SELECT * FROM delivery_attempts%s ORDER BY id DESC LIMIT ?" % where,
                params,
            ).fetchall()

    # ---- alert dedupe state --------------------------------------------
    def get_state(self, nws_id: str) -> Optional[sqlite3.Row]:
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
        sent_ts: Optional[str],
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
        cutoff = datetime.now(timezone.utc) - timedelta(hours=STATE_EXPIRY_HOURS)
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

    def history_exists(self, nws_id: str, disposition: Optional[str] = None) -> bool:
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
                       transmitted_text: Optional[str] = None,
                       detail: Optional[str] = None) -> None:
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
            datetime.now(timezone.utc) - timedelta(days=keep_days)
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
        disposition: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
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
        destination_id: Optional[int] = None,
    ) -> None:
        cols = ("ts, channel, byte_count, success, manual, text, error, transport, destination_id")
        vals = (_now(), channel, byte_count, int(success), int(manual), text, error,
                transport, destination_id)
        with self._lock:
            self._conn.execute(
                "INSERT INTO transmit_log(%s) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)" % cols, vals)
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
                ("-%d days" % keep_days,))
            self._conn.commit()
            return cur.rowcount

    def close(self) -> None:
        with self._lock:
            self._conn.close()
