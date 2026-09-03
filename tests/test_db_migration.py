import shutil
import sqlite3

from app.db import Database


HEAD_SCHEMA = """
CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE alert_state (
    nws_id TEXT PRIMARY KEY, event TEXT, headline TEXT, expires TEXT,
    msg_hash TEXT, sent_ts TEXT, disposition TEXT, updated_at TEXT
);
CREATE TABLE history (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL, nws_id TEXT,
    event TEXT, area TEXT, disposition TEXT, transmitted_text TEXT, detail TEXT
);
CREATE TABLE transmit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL, channel INTEGER,
    byte_count INTEGER, success INTEGER, manual INTEGER, text TEXT, error TEXT,
    transport TEXT
);
CREATE TABLE errors (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL, source TEXT, message TEXT
);
CREATE TABLE events (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL, level TEXT, message TEXT
);
CREATE TABLE ipaws_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL,
    identifier TEXT UNIQUE, sender TEXT, event TEXT, area TEXT, headline TEXT,
    msg_type TEXT, status TEXT, sent TEXT, text TEXT, transmitted INTEGER, error TEXT
);
CREATE INDEX idx_history_ts ON history(ts);
CREATE INDEX idx_history_disp ON history(disposition);
CREATE INDEX idx_txlog_ts ON transmit_log(ts);
CREATE INDEX idx_ipaws_ts ON ipaws_log(ts);
"""

LEGACY_TABLES = (
    "settings", "alert_state", "history", "transmit_log", "errors", "events", "ipaws_log"
)


def _legacy_rows(conn):
    conn.row_factory = sqlite3.Row
    return {
        table: [dict(row) for row in conn.execute(f"SELECT * FROM {table} ORDER BY rowid")]
        for table in LEGACY_TABLES
    }


def test_complete_head_database_migrates_reopens_without_data_loss(tmp_path):
    source = tmp_path / "head-era.db"
    conn = sqlite3.connect(source)
    conn.executescript(HEAD_SCHEMA)
    conn.executemany("INSERT INTO settings(key, value) VALUES (?, ?)", [
        ("dry_run", "false"), ("zones", '"TNC147"'),
    ])
    conn.executemany(
        "INSERT INTO alert_state VALUES (?,?,?,?,?,?,?,?)",
        [
            ("a-1", "Tornado Warning", "one", "2099-01-01T00:00:00Z", "h1",
             "2026-01-01T00:00:00Z", "sent", "2026-01-01T00:00:00Z"),
            ("a-2", "Flood Warning", "two", "2099-01-02T00:00:00Z", "h2",
             "2026-01-02T00:00:00Z", "update", "2026-01-02T00:00:00Z"),
        ],
    )
    conn.executemany(
        "INSERT INTO history(ts,nws_id,event,area,disposition,transmitted_text,detail) "
        "VALUES (?,?,?,?,?,?,?)",
        [
            ("2026-01-01T00:00:00Z", "a-1", "Tornado Warning", "Robertson", "sent", "p1", "d1"),
            ("2026-01-02T00:00:00Z", "a-2", "Flood Warning", "Smith", "filtered", "", "d2"),
        ],
    )
    conn.executemany(
        "INSERT INTO transmit_log(ts,channel,byte_count,success,manual,text,error,transport) "
        "VALUES (?,?,?,?,?,?,?,?)",
        [
            ("2026-01-01T00:00:00Z", 3, 7, 1, 0, "legacy one", "", "meshtastic"),
            ("2026-01-02T00:00:00Z", 4, 8, 0, 1, "legacy two", "offline", "meshcore"),
        ],
    )
    conn.executemany(
        "INSERT INTO errors(ts,source,message) VALUES (?,?,?)",
        [("2026-01-01T00:00:00Z", "nws", "one"), ("2026-01-02T00:00:00Z", "radio", "two")],
    )
    conn.executemany(
        "INSERT INTO events(ts,level,message) VALUES (?,?,?)",
        [("2026-01-01T00:00:00Z", "INFO", "one"), ("2026-01-02T00:00:00Z", "WARN", "two")],
    )
    conn.executemany(
        "INSERT INTO ipaws_log(ts,identifier,sender,event,area,headline,msg_type,status,sent,text,transmitted,error) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            ("2026-01-01T00:00:00Z", "i-1", "sender", "Civil Emergency", "A", "h1", "Alert", "Actual", "s1", "t1", 1, ""),
            ("2026-01-02T00:00:00Z", "i-2", "sender", "Test", "B", "h2", "Update", "Test", "s2", "t2", 0, "queued"),
        ],
    )
    conn.commit()
    expected = _legacy_rows(conn)
    conn.close()

    migrated = tmp_path / "mesh.db"
    shutil.copy2(source, migrated)
    for _ in range(2):
        db = Database(str(migrated))
        assert db._conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert db._conn.execute("PRAGMA foreign_key_check").fetchall() == []
        actual = _legacy_rows(db._conn)
        for table, rows in expected.items():
            for expected_row in rows:
                assert any(
                    all(candidate[key] == value for key, value in expected_row.items())
                    for candidate in actual[table]
                ), (table, expected_row)
        assert db._conn.execute("SELECT COUNT(*) FROM destinations").fetchone()[0] == 0
        assert db._conn.execute("SELECT COUNT(*) FROM routing_rules").fetchone()[0] == 0
        assert db._conn.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0] == 0
        db.close()
