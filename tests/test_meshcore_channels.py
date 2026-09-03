from __future__ import annotations

import asyncio
from dataclasses import dataclass
import logging

import pytest
from meshcore import EventType
from meshcore.commands.device import DeviceCommands
from meshcore.events import Event, EventDispatcher

from app.transmit import (
    MeshCoreTransmitter,
    TransmitManager,
    _suppress_meshcore_dependency_logging,
)
from app.db import Database


SECRET_SENTINEL = bytes([
    0x00, 0xFF, 0x80, 0xC3, 0xA9, 0x7F, 0x01, 0xFE,
    0x10, 0x90, 0xE2, 0x98, 0x83, 0x42, 0xAA, 0x55,
])


@dataclass
class Result:
    type: object
    payload: object


class FakeCommands:
    def __init__(self, *, max_channels=40):
        self.max_channels = max_channels
        self.channels = {}
        self.set_calls = []
        self.get_calls = []
        self.set_result_type = EventType.OK
        self.readback = None
        self.readback_index = None
        self.device_result_type = EventType.DEVICE_INFO
        self.device_payload: object = {
            "fw ver": 3, "max_channels": self.max_channels,
            "model": "OpenHop", "ver": "test",
        }
        self.get_result_types = {}

    async def send_device_query(self):
        return Result(self.device_result_type, self.device_payload)

    async def get_channel(self, index):
        self.get_calls.append(index)
        if self.readback is not None:
            name, secret = self.readback
        else:
            name, secret = self.channels.get(index, ("", b"\x00" * 16))
        return Result(
            self.get_result_types.get(index, EventType.CHANNEL_INFO),
            {"channel_idx": (index if self.readback_index is None else self.readback_index),
             "channel_name": name,
             "channel_secret": secret},
        )

    async def set_channel(self, index, name, secret):
        self.set_calls.append((index, name, secret))
        if self.set_result_type == EventType.OK:
            self.channels[index] = (name, secret)
        return Result(self.set_result_type, {})

    async def send_chan_msg(self, channel, text):
        return Result(EventType.OK, {})


def transmitter(commands):
    tx = MeshCoreTransmitter("tcp", host="unused")
    tx._mc = type("Companion", (), {"commands": commands})()
    return tx


async def test_set_channel_requires_ok_then_exact_readback():
    commands = FakeCommands()
    tx = transmitter(commands)
    secret = bytes(range(16))

    channel = await tx.set_channel(17, "Weather Ops", secret)

    assert commands.set_calls == [(17, "Weather Ops", secret)]
    assert channel == {"index": 17, "name": "Weather Ops"}


@pytest.mark.parametrize("result_type", [EventType.ERROR, None])
async def test_set_channel_requires_explicit_ok(result_type):
    commands = FakeCommands()
    commands.set_result_type = result_type
    tx = transmitter(commands)

    with pytest.raises(RuntimeError, match="accept"):
        await tx.set_channel(2, "Weather", b"s" * 16)


@pytest.mark.parametrize(
    "readback",
    [("Other", b"s" * 16), ("Weather", b"x" * 16)],
)
async def test_set_channel_rejects_mismatched_readback(readback):
    commands = FakeCommands()
    commands.readback = readback
    tx = transmitter(commands)

    with pytest.raises(RuntimeError, match="readback"):
        await tx.set_channel(2, "Weather", b"s" * 16)


@pytest.mark.parametrize(
    ("index", "name", "secret", "message"),
    [
        (2, "Weather", b"short", "exactly 16 bytes"),
        (2, "#Weather", b"s" * 16, "beginning with #"),
        (40, "Weather", b"s" * 16, "out of range"),
        (-1, "Weather", b"s" * 16, "out of range"),
        (True, "Weather", b"s" * 16, "out of range"),
        (2, "", b"s" * 16, "required"),
        (2, "é" * 17, b"s" * 16, "32 UTF-8 bytes"),
    ],
)
async def test_set_channel_validates_wire_values(index, name, secret, message):
    commands = FakeCommands(max_channels=40)
    tx = transmitter(commands)

    with pytest.raises(ValueError, match=message):
        await tx.set_channel(index, name, secret)

    assert commands.set_calls == []


async def test_set_channel_rejects_embedded_nul_before_writing():
    commands = FakeCommands()
    tx = transmitter(commands)

    with pytest.raises(ValueError, match="NUL"):
        await tx.set_channel(2, "County\x00Hidden", b"s" * 16)

    assert commands.set_calls == []


async def test_public_read_channel_never_returns_plaintext_secret():
    commands = FakeCommands(max_channels=40)
    secret = SECRET_SENTINEL
    commands.channels[12] = ("County WX", secret)
    tx = transmitter(commands)

    channel = await tx.read_channel(12)

    assert channel == {"index": 12, "name": "County WX"}
    assert secret not in repr(channel).encode("utf-8")
    assert commands.get_calls == [12]


async def test_read_channels_enumerates_every_device_reported_slot_beyond_eight():
    commands = FakeCommands(max_channels=40)
    commands.channels[0] = ("Public", b"p" * 16)
    commands.channels[17] = ("County WX", b"q" * 16)
    tx = transmitter(commands)

    channels = await tx.read_channels()
    info = await tx.read_info()

    assert channels == [
        {"index": 0, "name": "Public"},
        {"index": 17, "name": "County WX"},
    ]
    assert commands.get_calls == list(range(40))
    assert info["max_channels"] == 40


@pytest.mark.parametrize("result_type", [EventType.ERROR, EventType.CHANNEL_INFO, None])
async def test_channel_operations_reject_non_device_info_responses(result_type):
    commands = FakeCommands()
    commands.device_result_type = result_type
    tx = transmitter(commands)

    with pytest.raises(RuntimeError, match="device information"):
        await tx.read_channels()


async def test_read_info_does_not_hide_device_info_errors():
    commands = FakeCommands()
    commands.device_result_type = EventType.ERROR
    tx = transmitter(commands)

    with pytest.raises(RuntimeError, match="device information"):
        await tx.read_info()


async def test_device_info_requires_a_mapping_payload():
    commands = FakeCommands()
    commands.device_payload = []
    tx = transmitter(commands)

    with pytest.raises(RuntimeError, match="device information payload"):
        await tx.read_channels()


@pytest.mark.parametrize("max_channels", [0, -1, True, "40", None])
async def test_present_invalid_max_channels_never_uses_legacy_fallback(max_channels):
    commands = FakeCommands()
    commands.device_payload = {"model": "Broken", "max_channels": max_channels}
    tx = transmitter(commands)

    with pytest.raises(RuntimeError, match="max_channels"):
        await tx.read_channels()


async def test_valid_legacy_device_info_without_max_channels_uses_eight_slots():
    commands = FakeCommands()
    commands.device_payload = {"model": "Legacy", "ver": "old"}
    tx = transmitter(commands)

    await tx.read_channels()

    assert commands.get_calls == list(range(8))


async def test_clear_channel_writes_blank_zero_secret_and_verifies_readback():
    commands = FakeCommands(max_channels=40)
    commands.channels[9] = ("Old", b"o" * 16)
    tx = transmitter(commands)

    channel = await tx.clear_channel(9)

    assert commands.set_calls == [(9, "", b"\x00" * 16)]
    assert channel == {"index": 9, "name": ""}


@pytest.mark.parametrize(
    ("result_type", "readback_index", "readback"),
    [
        (EventType.ERROR, None, None),
        (EventType.OK, 8, ("", b"\x00" * 16)),
        (EventType.OK, None, ("Still here", b"\x00" * 16)),
        (EventType.OK, None, ("", b"x" * 16)),
    ],
)
async def test_clear_channel_requires_ok_and_exact_blank_readback(
        result_type, readback_index, readback):
    commands = FakeCommands()
    commands.set_result_type = result_type
    commands.readback_index = readback_index
    commands.readback = readback
    tx = transmitter(commands)

    with pytest.raises(RuntimeError, match="accept|readback"):
        await tx.clear_channel(9)


def configured_manager(commands):
    db = Database(":memory:")
    manager = TransmitManager(db)
    transport = manager._transports["meshcore"]
    transport.enabled = True
    transport.target = "configured-openhop:4000"
    transport.connected = True
    transport.tx = transmitter(commands)
    return db, manager, transport


async def test_manager_set_uses_configured_meshcore_and_caches_only_safe_metadata():
    commands = FakeCommands(max_channels=40)
    commands.channels[0] = ("Public", b"p" * 16)
    db, manager, transport = configured_manager(commands)
    secret = b"s" * 16

    result = await manager.set_meshcore_channel(12, "County WX", secret)

    assert result == {
        "ok": True,
        "error": "",
        "channel": {"index": 12, "name": "County WX"},
        "channels": [
            {"index": 0, "name": "Public"},
            {"index": 12, "name": "County WX"},
        ],
        "model": "OpenHop (fw test)",
        "max_channels": 40,
    }
    assert transport.tx._mc.commands is commands
    assert db.get_setting("meshcore_channels") == result["channels"]
    assert db.get_setting("meshcore_model") == result["model"]
    assert db.get_setting("meshcore_max_channels") == 40
    database_text = "\n".join(db._conn.iterdump())
    assert (b"s" * 16).decode() not in database_text
    assert "secret" not in repr(result).lower()
    assert db.query_transmit_log() == []
    assert db.recent_errors() == []


async def test_binary_secret_never_reaches_public_state_logs_or_errors(caplog):
    commands = FakeCommands(max_channels=2)
    db, manager, transport = configured_manager(commands)
    caplog.set_level(logging.DEBUG)

    result = await manager.set_meshcore_channel(
        0, "Sentinel Channel", SECRET_SENTINEL)
    public_channel = await transport.tx.read_channel(0)
    commands.set_result_type = EventType.ERROR
    failed_result = await manager.set_meshcore_channel(
        1, "Rejected Channel", SECRET_SENTINEL)

    public_values = {
        "result": result,
        "public_channel": public_channel,
        "cache": db.get_setting("meshcore_channels"),
        "database": "\n".join(db._conn.iterdump()),
        "logs": caplog.text,
        "failed_result": failed_result,
        "errors": db.recent_errors(),
    }
    public_text = repr(public_values)
    assert SECRET_SENTINEL not in public_text.encode("utf-8")
    assert repr(SECRET_SENTINEL) not in public_text
    assert SECRET_SENTINEL.hex() not in public_text
    assert all("secret" not in repr(value).lower()
               for key, value in public_values.items() if key != "database")


@pytest.mark.parametrize(
    ("operation", "safe_error"),
    [
        ("set", "MeshCore channel update failed"),
        ("inspect", "MeshCore channel inspection failed"),
        ("clear", "MeshCore channel clear failed"),
    ],
)
async def test_manager_channel_errors_never_expose_exception_secrets(
        caplog, operation, safe_error):
    commands = FakeCommands(max_channels=2)
    db, manager, transport = configured_manager(commands)
    leaked_secret = "DISTINCTIVE-CHANNEL-SECRET"
    leaked_frame = "20deadbeefcafefeed"

    class LeakyTransmitter:
        connected = True

        async def set_channel(self, *args):
            raise RuntimeError(f"{leaked_secret} raw={leaked_frame}")

        async def read_channel(self, *args):
            raise RuntimeError(f"{leaked_secret} raw={leaked_frame}")

        async def clear_channel(self, *args):
            raise RuntimeError(f"{leaked_secret} raw={leaked_frame}")

    transport.tx = LeakyTransmitter()
    caplog.set_level(logging.DEBUG)

    if operation == "set":
        result = await manager.set_meshcore_channel(0, "Weather", b"s" * 16)
    elif operation == "inspect":
        result = await manager.inspect_meshcore_channel(0)
    else:
        result = await manager.clear_meshcore_channel(0)

    assert result == {"ok": False, "error": safe_error}
    exposed = repr({
        "result": result,
        "logs": caplog.text,
        "errors": db.recent_errors(),
        "transport_error": transport.error,
    })
    assert leaked_secret not in exposed
    assert leaked_frame not in exposed


@pytest.mark.parametrize(
    ("operation", "safe_error"),
    [
        ("set", "MeshCore channel update failed"),
        ("inspect", "MeshCore channel inspection failed"),
        ("clear", "MeshCore channel clear failed"),
    ],
)
async def test_channel_reconnect_failure_never_returns_connection_exception(
        caplog, operation, safe_error):
    commands = FakeCommands(max_channels=2)
    db, manager, transport = configured_manager(commands)
    transport.conn = "tcp"
    transport.tx._mc.connection_manager = type(
        "ConnectionManager", (), {"is_connected": False})()
    leaked = "DISTINCTIVE-SECRET raw=3c2000cafebabe"

    async def reconnect(t):
        t.connected = False
        t.error = f"connect failed: {leaked}"
        return False

    manager._reconnect = reconnect
    caplog.set_level(logging.DEBUG)

    if operation == "set":
        result = await manager.set_meshcore_channel(0, "Weather", b"s" * 16)
    elif operation == "inspect":
        result = await manager.inspect_meshcore_channel(0)
    else:
        result = await manager.clear_meshcore_channel(0)

    assert result == {"ok": False, "error": safe_error}
    exposed = repr({
        "result": result,
        "status": manager.status(),
        "errors": db.recent_errors(),
        "logs": caplog.text,
    })
    assert leaked not in exposed


@pytest.mark.parametrize("fail_set", [False, True])
async def test_dependency_debug_logs_suppress_channel_secret_and_restore_logger(
        caplog, fail_set):
    meshcore_logger = logging.getLogger("meshcore")
    original_disabled = meshcore_logger.disabled
    meshcore_logger.disabled = False
    caplog.set_level(logging.DEBUG, logger="meshcore")
    dispatcher = EventDispatcher()
    await dispatcher.start()
    commands = DeviceCommands()
    commands.set_dispatcher(dispatcher)

    class Connection:
        async def send(self, data):
            if data[0] == 0x16:
                event = Event(EventType.DEVICE_INFO, {"max_channels": 2})
            elif data[0] == 0x20:
                if fail_set:
                    raise RuntimeError("secret frame rejected: %s" % data.hex())
                event = Event(EventType.OK, {})
            else:
                event = Event(EventType.CHANNEL_INFO, {
                    "channel_idx": data[1],
                    "channel_name": "Sentinel Channel",
                    "channel_secret": SECRET_SENTINEL,
                })
            await dispatcher.dispatch(event)

    commands.set_connection(Connection())
    tx = transmitter(commands)

    try:
        if fail_set:
            with pytest.raises(RuntimeError, match="did not accept"):
                await tx.set_channel(0, "Sentinel Channel", SECRET_SENTINEL)
        else:
            assert await tx.set_channel(0, "Sentinel Channel", SECRET_SENTINEL) == {
                "index": 0, "name": "Sentinel Channel",
            }

        assert meshcore_logger.disabled is False
        assert SECRET_SENTINEL.hex() not in caplog.text
        assert repr(SECRET_SENTINEL) not in caplog.text
    finally:
        meshcore_logger.disabled = original_disabled
        await dispatcher.stop()


async def test_dependency_log_suppression_survives_overlapping_failure(caplog):
    meshcore_logger = logging.getLogger("meshcore")
    original_disabled = meshcore_logger.disabled
    meshcore_logger.disabled = False
    caplog.set_level(logging.DEBUG, logger="meshcore")
    first_entered = asyncio.Event()
    second_entered = asyncio.Event()
    release_first = asyncio.Event()
    release_second = asyncio.Event()
    leaked_frame = "OVERLAP-SECRET-RAW-FRAME"

    async def first_operation():
        with _suppress_meshcore_dependency_logging():
            first_entered.set()
            await release_first.wait()
            raise RuntimeError("expected operation failure")

    async def second_operation():
        await first_entered.wait()
        with _suppress_meshcore_dependency_logging():
            second_entered.set()
            await release_second.wait()

    first = asyncio.create_task(first_operation())
    second = asyncio.create_task(second_operation())
    try:
        await second_entered.wait()
        assert meshcore_logger.disabled is True
        release_first.set()
        with pytest.raises(RuntimeError, match="expected operation failure"):
            await first

        assert meshcore_logger.disabled is True
        meshcore_logger.debug(leaked_frame)
        release_second.set()
        await second

        assert meshcore_logger.disabled is False
        assert leaked_frame not in caplog.text
    finally:
        for task in (first, second):
            if not task.done():
                task.cancel()
        await asyncio.gather(first, second, return_exceptions=True)
        meshcore_logger.disabled = original_disabled


async def test_meshcore_test_and_resend_logs_only_claim_companion_acceptance(caplog):
    commands = FakeCommands(max_channels=2)
    _, manager, _ = configured_manager(commands)
    caplog.set_level(logging.INFO, logger="mesh_wx.tx")

    assert await manager.send_to("meshcore", "test") == (True, "")
    assert await manager.resend("meshcore", "retry", 7) == (True, "")

    class AcceptedTransmitter:
        async def send_text(self, text, channel):
            pass

    meshtastic = manager._transports["meshtastic"]
    meshtastic.enabled = True
    meshtastic.connected = True
    meshtastic.tx = AcceptedTransmitter()
    assert await manager.send_to("meshtastic", "test") == (True, "")
    assert await manager.resend("meshtastic", "retry", 8) == (True, "")

    messages = [record.getMessage() for record in caplog.records
                if record.name == "mesh_wx.tx"]
    assert "test accepted by MeshCore companion on ch 1" in messages
    assert "resend accepted by MeshCore companion on ch 7" in messages
    assert "test transmitted via meshtastic on ch 1" in messages
    assert "resent via meshtastic on ch 8" in messages
    assert all("transmitted via meshcore" not in message.lower() for message in messages)
    assert all("resent via meshcore" not in message.lower() for message in messages)


async def test_manager_clear_and_inspect_never_return_secret():
    commands = FakeCommands(max_channels=40)
    commands.channels[9] = ("Old", b"o" * 16)
    db, manager, _ = configured_manager(commands)

    inspected = await manager.inspect_meshcore_channel(9)
    cleared = await manager.clear_meshcore_channel(9)

    assert inspected["channel"] == {"index": 9, "name": "Old"}
    assert cleared["ok"] is True
    assert cleared["channel"] == {"index": 9, "name": ""}
    assert commands.set_calls == [(9, "", b"\x00" * 16)]
    assert all("secret" not in repr(result).lower() for result in (inspected, cleared))
    assert (b"o" * 16).decode() not in "\n".join(db._conn.iterdump())


async def test_enumeration_error_fails_without_persisting_partial_cache():
    commands = FakeCommands(max_channels=3)
    commands.channels[0] = ("Public", b"p" * 16)
    commands.get_result_types[1] = EventType.ERROR
    db, manager, _ = configured_manager(commands)
    original = [{"index": 2, "name": "Existing"}]
    db.set_setting("meshcore_channels", original)

    result = await manager.inspect_meshcore_channel(0)

    assert result == {
        "ok": False,
        "error": "MeshCore channel inspection failed",
    }
    assert db.get_setting("meshcore_channels") == original


async def test_verified_set_stays_successful_when_cache_refresh_fails():
    commands = FakeCommands(max_channels=3)
    commands.get_result_types[1] = EventType.ERROR
    db, manager, _ = configured_manager(commands)
    original = [{"index": 2, "name": "Existing"}]
    db.set_setting("meshcore_channels", original)

    result = await manager.set_meshcore_channel(0, "Weather", b"s" * 16)

    assert commands.set_calls == [(0, "Weather", b"s" * 16)]
    assert result == {
        "ok": True,
        "error": "",
        "channel": {"index": 0, "name": "Weather"},
        "cache_refreshed": False,
        "warning": "Channel updated, but channel cache refresh failed",
    }
    assert db.get_setting("meshcore_channels") == original


async def test_verified_clear_stays_successful_when_cache_refresh_fails():
    commands = FakeCommands(max_channels=3)
    commands.channels[0] = ("Old", b"o" * 16)
    commands.get_result_types[1] = EventType.ERROR
    db, manager, _ = configured_manager(commands)

    result = await manager.clear_meshcore_channel(0)

    assert commands.set_calls == [(0, "", b"\x00" * 16)]
    assert result == {
        "ok": True,
        "error": "",
        "channel": {"index": 0, "name": ""},
        "cache_refreshed": False,
        "warning": "Channel cleared, but channel cache refresh failed",
    }


@pytest.mark.parametrize("operation", ["set", "inspect", "clear"])
async def test_channel_crud_reconnects_stale_tcp_before_one_retry(operation):
    commands = FakeCommands(max_channels=2)
    commands.channels[0] = ("Old", b"o" * 16)
    _, manager, transport = configured_manager(commands)
    transport.conn = "tcp"
    stale_calls = []

    class StaleTransmitter:
        connected = False

        async def set_channel(self, *args):
            stale_calls.append(("set", args))
            raise RuntimeError("stale TCP transport")

        async def read_channel(self, *args):
            stale_calls.append(("inspect", args))
            raise RuntimeError("stale TCP transport")

        async def clear_channel(self, *args):
            stale_calls.append(("clear", args))
            raise RuntimeError("stale TCP transport")

    transport.tx = StaleTransmitter()
    reconnects = 0

    async def reconnect(t):
        nonlocal reconnects
        reconnects += 1
        assert t is transport
        assert manager._lock.locked()
        assert transport.connected is False
        transport.tx = transmitter(commands)
        transport.connected = True
        return True

    manager._reconnect = reconnect

    if operation == "set":
        result = await manager.set_meshcore_channel(0, "New", b"n" * 16)
    elif operation == "inspect":
        result = await manager.inspect_meshcore_channel(0)
    else:
        result = await manager.clear_meshcore_channel(0)

    assert result["ok"] is True
    assert reconnects == 1
    assert stale_calls == []


async def test_inspect_reconnects_and_retries_once_after_mid_read_tcp_reset():
    commands = FakeCommands(max_channels=2)
    commands.channels[0] = ("Recovered", b"r" * 16)
    _, manager, transport = configured_manager(commands)
    transport.conn = "tcp"
    read_calls = 0

    class ResettingTransmitter:
        connected = True

        async def read_channel(self, index):
            nonlocal read_calls
            read_calls += 1
            raise ConnectionResetError("peer reset during channel read")

    transport.tx = ResettingTransmitter()
    reconnects = 0

    async def reconnect(t):
        nonlocal reconnects
        reconnects += 1
        assert manager._lock.locked()
        assert t.connected is False
        t.tx = transmitter(commands)
        t.connected = True
        return True

    manager._reconnect = reconnect

    result = await manager.inspect_meshcore_channel(0)

    assert result["ok"] is True
    assert result["channel"] == {"index": 0, "name": "Recovered"}
    assert read_calls == 1
    assert reconnects == 1


async def test_real_inspect_path_retries_sanitized_mid_read_reset():
    class ResettingCommands(FakeCommands):
        async def get_channel(self, index):
            raise ConnectionResetError("raw frame 3c03001f00")

    initial_commands = ResettingCommands(max_channels=2)
    recovered_commands = FakeCommands(max_channels=2)
    recovered_commands.channels[0] = ("Recovered", b"r" * 16)
    _, manager, transport = configured_manager(initial_commands)
    transport.conn = "tcp"
    reconnects = 0

    async def reconnect(t):
        nonlocal reconnects
        reconnects += 1
        assert manager._lock.locked()
        assert t.connected is False
        t.tx = transmitter(recovered_commands)
        t.connected = True
        return True

    manager._reconnect = reconnect

    result = await manager.inspect_meshcore_channel(0)

    assert result["ok"] is True
    assert result["channel"] == {"index": 0, "name": "Recovered"}
    assert reconnects == 1


async def test_set_reconnects_and_retries_after_tcp_reset_before_command():
    recovered_commands = FakeCommands(max_channels=2)
    _, manager, transport = configured_manager(recovered_commands)
    transport.conn = "tcp"
    device_queries = 0

    class ResetBeforeCommand(FakeCommands):
        async def send_device_query(self):
            nonlocal device_queries
            device_queries += 1
            raise ConnectionResetError("peer reset before channel command")

    initial_commands = ResetBeforeCommand(max_channels=2)
    transport.tx = transmitter(initial_commands)
    reconnects = 0

    async def reconnect(t):
        nonlocal reconnects
        reconnects += 1
        assert manager._lock.locked()
        assert t.connected is False
        t.tx = transmitter(recovered_commands)
        t.connected = True
        return True

    manager._reconnect = reconnect

    result = await manager.set_meshcore_channel(0, "Weather", b"s" * 16)

    assert result["ok"] is True
    assert device_queries == 1
    assert initial_commands.set_calls == []
    assert recovered_commands.set_calls == [(0, "Weather", b"s" * 16)]
    assert reconnects == 1


async def test_clear_reconnects_and_retries_after_tcp_reset_before_command():
    recovered_commands = FakeCommands(max_channels=2)
    recovered_commands.channels[0] = ("Old", b"o" * 16)
    _, manager, transport = configured_manager(recovered_commands)
    transport.conn = "tcp"

    class ResetBeforeCommand(FakeCommands):
        async def send_device_query(self):
            raise ConnectionResetError("peer reset before channel clear")

    initial_commands = ResetBeforeCommand(max_channels=2)
    transport.tx = transmitter(initial_commands)
    reconnects = 0

    async def reconnect(t):
        nonlocal reconnects
        reconnects += 1
        assert manager._lock.locked()
        assert t.connected is False
        t.tx = transmitter(recovered_commands)
        t.connected = True
        return True

    manager._reconnect = reconnect

    result = await manager.clear_meshcore_channel(0)

    assert result["ok"] is True
    assert initial_commands.set_calls == []
    assert recovered_commands.set_calls == [(0, "", b"\x00" * 16)]
    assert reconnects == 1


@pytest.mark.parametrize(
    ("operation", "safe_error"),
    [
        ("set", "MeshCore channel update failed"),
        ("clear", "MeshCore channel clear failed"),
    ],
)
async def test_mutation_timeout_is_ambiguous_and_never_retries_command(
        caplog, operation, safe_error):
    leaked = "DISTINCTIVE-SECRET raw=20cafedeadbeef"

    class AmbiguousCommands(FakeCommands):
        async def set_channel(self, index, name, secret):
            self.set_calls.append((index, name, secret))
            self.channels[index] = (name, secret)  # it may have reached the companion
            raise asyncio.TimeoutError(leaked)

    initial_commands = AmbiguousCommands(max_channels=2)
    recovered_commands = FakeCommands(max_channels=2)
    db, manager, transport = configured_manager(initial_commands)
    transport.conn = "tcp"
    reconnects = 0

    async def reconnect(t):
        nonlocal reconnects
        reconnects += 1
        assert manager._lock.locked()
        assert t.connected is False
        t.tx = transmitter(recovered_commands)
        t.connected = True
        return True

    manager._reconnect = reconnect
    caplog.set_level(logging.DEBUG)

    if operation == "set":
        result = await manager.set_meshcore_channel(0, "Weather", b"s" * 16)
        expected_call = (0, "Weather", b"s" * 16)
    else:
        result = await manager.clear_meshcore_channel(0)
        expected_call = (0, "", b"\x00" * 16)

    assert result == {"ok": False, "error": safe_error}
    assert initial_commands.set_calls == [expected_call]
    assert recovered_commands.set_calls == []
    assert reconnects == 1
    assert leaked not in repr((result, caplog.text, db.recent_errors()))


@pytest.mark.parametrize("operation", ["set", "clear"])
async def test_reset_after_mutation_ok_never_repeats_verified_command(operation):
    class ResetOnReadback(FakeCommands):
        async def get_channel(self, index):
            raise ConnectionResetError("reset after companion OK")

    initial_commands = ResetOnReadback(max_channels=2)
    recovered_commands = FakeCommands(max_channels=2)
    _, manager, transport = configured_manager(initial_commands)
    transport.conn = "tcp"
    reconnects = 0

    async def reconnect(t):
        nonlocal reconnects
        reconnects += 1
        assert manager._lock.locked()
        assert t.connected is False
        t.tx = transmitter(recovered_commands)
        t.connected = True
        return True

    manager._reconnect = reconnect

    if operation == "set":
        result = await manager.set_meshcore_channel(0, "Weather", b"s" * 16)
        expected_call = (0, "Weather", b"s" * 16)
    else:
        result = await manager.clear_meshcore_channel(0)
        expected_call = (0, "", b"\x00" * 16)

    assert result["ok"] is False
    assert initial_commands.set_calls == [expected_call]
    assert recovered_commands.set_calls == []
    assert reconnects == 1


async def test_meshcore_tcp_connected_reflects_library_connection_state():
    tx = MeshCoreTransmitter("tcp", host="unused")
    connection_manager = type("ConnectionManager", (), {"is_connected": False})()
    tx._mc = type("Companion", (), {"connection_manager": connection_manager})()

    assert tx.connected is False


async def test_send_and_channel_crud_serialize_under_manager_lock():
    commands = FakeCommands(max_channels=2)
    _, manager, transport = configured_manager(commands)
    channel_tx = transmitter(commands)
    send_entered = asyncio.Event()
    release_send = asyncio.Event()

    class BlockingTransmitter:
        connected = True

        async def send_text(self, text, channel):
            send_entered.set()
            await release_send.wait()

        async def set_channel(self, *args):
            return await channel_tx.set_channel(*args)

        async def read_channels(self):
            return await channel_tx.read_channels()

        async def read_info(self):
            return await channel_tx.read_info()

    transport.tx = BlockingTransmitter()
    send_task = asyncio.create_task(manager.send_to("meshcore", "test"))
    await send_entered.wait()

    crud_task = asyncio.create_task(
        manager.set_meshcore_channel(0, "Weather", b"w" * 16))
    await asyncio.sleep(0)

    assert commands.set_calls == []
    release_send.set()
    send_result, crud_result = await asyncio.gather(send_task, crud_task)
    assert send_result == (True, "")
    assert crud_result["ok"] is True
    assert commands.set_calls == [(0, "Weather", b"w" * 16)]
