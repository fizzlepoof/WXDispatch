import pytest
from meshcore import EventType

from app.db import Database
from app.transmit import MeshCoreTransmitter, TransmitManager, TxUnsent


class FakeRadio:
    def __init__(self):
        self.calls = []

    async def send_text(self, text, channel):
        self.calls.append((text, channel))

    async def send_binary(self, payload, channel):
        self.calls.append((payload, channel))


async def test_destination_item_sends_only_specific_transport_channel_and_caps_utf8():
    db = Database(":memory:")
    manager = TransmitManager(db)
    meshcore = manager._transports["meshcore"]
    meshtastic = manager._transports["meshtastic"]
    meshcore.enabled = meshtastic.enabled = True
    meshcore.connected = meshtastic.connected = True
    meshcore.tx, meshtastic.tx = FakeRadio(), FakeRadio()

    manager.enqueue_destination("é" * 200, "meshcore", 7, 44)
    item = manager._queue.popleft()
    ok, error = await manager._transmit_item(item)

    assert ok and not error
    assert meshtastic.tx.calls == []
    assert meshcore.tx.calls[0][1] == 7
    assert len(meshcore.tx.calls[0][0].encode("utf-8")) <= 195
    row = db.query_transmit_log()[0]
    assert row["transport"] == "meshcore" and row["channel"] == 7
    assert row["destination_id"] == 44


async def test_structured_weather_item_sends_binary_only_to_meshcore():
    db = Database(":memory:")
    manager = TransmitManager(db)
    meshcore = manager._transports["meshcore"]
    meshtastic = manager._transports["meshtastic"]
    meshcore.enabled = meshtastic.enabled = True
    meshcore.connected = meshtastic.connected = True
    meshcore.tx, meshtastic.tx = FakeRadio(), FakeRadio()

    manager.enqueue_meshwx(b"\x04\x21\xff", 7)
    assert not manager._queue
    item = manager._queue_low.popleft()
    ok, error = await manager._transmit_item(item)

    assert ok and not error
    assert meshcore.tx.calls == [(b"\x04\x21\xff", 7)]
    assert meshtastic.tx.calls == []
    assert item.log_tx is False


class Commands:
    def __init__(self, result_type=EventType.OK):
        self.result_type = result_type
        self.raw = []

    async def send_chan_msg(self, channel, text):
        return type("Result", (), {"type": self.result_type, "payload": {}})()

    async def send_device_query(self):
        return type(
            "Result",
            (),
            {"type": EventType.DEVICE_INFO, "payload": {"max_channels": 8}},
        )()

    async def send(self, data, expected):
        self.raw.append((data, expected))
        return type("Result", (), {"type": self.result_type, "payload": {}})()


async def test_meshcore_success_means_accepted_by_companion_without_flood_counter():
    tx = MeshCoreTransmitter("tcp", host="unused")
    tx._mc = type("Companion", (), {"commands": Commands()})()
    await tx.send_text("weather", 2)


async def test_meshcore_binary_channel_send_uses_raw_channel_data_command():
    commands = Commands()
    tx = MeshCoreTransmitter("tcp", host="unused")
    tx._mc = type("Companion", (), {"commands": commands})()
    await tx.send_binary(b"\x04\x20\xff", 7)

    data, expected = commands.raw[0]
    assert data == b"\x3e\x07\xff\xff\xff\x04\x20\xff"
    assert expected == [EventType.OK, EventType.ERROR]


async def test_meshcore_binary_channel_send_rejects_oversize_before_companion_call():
    commands = Commands()
    tx = MeshCoreTransmitter("tcp", host="unused")
    tx._mc = type("Companion", (), {"commands": commands})()

    with pytest.raises(TxUnsent, match="163"):
        await tx.send_binary(b"x" * 164, 7)

    assert commands.raw == []


async def test_meshcore_binary_channel_send_rejects_device_out_of_range_channel():
    commands = Commands()
    tx = MeshCoreTransmitter("tcp", host="unused")
    tx._mc = type("Companion", (), {"commands": commands})()

    with pytest.raises(TxUnsent, match="non-Public channel"):
        await tx.send_binary(b"payload", 8)

    assert commands.raw == []


def test_structured_weather_never_displaces_full_high_priority_queue():
    manager = TransmitManager(Database(":memory:"))
    for index in range(manager._queue.maxlen):
        manager.enqueue_destination(str(index), "meshcore", 2, index)
    first = manager._queue[0].text
    results = []

    accepted = manager.enqueue_meshwx(
        b"\x04\x21", 7, on_result=lambda ok, err="": results.append((ok, err))
    )

    assert accepted
    assert manager._queue[0].text == first
    assert len(manager._queue) == manager._queue.maxlen
    assert len(manager._queue_low) == 1
    assert results == []


def test_structured_weather_can_be_cancelled_by_correlation_key():
    manager = TransmitManager(Database(":memory:"))
    results = []
    key = ("meshwx", "alert-1")
    manager.enqueue_meshwx(
        b"\x04\x21", 7, correlation_key=key,
        on_result=lambda ok, err="": results.append((ok, err)),
    )

    assert manager.cancel_queued_correlation(key) == 1
    assert not manager._queue_low
    assert results == [(False, "superseded")]


async def test_meshcore_binary_channel_send_requires_explicit_ok():
    tx = MeshCoreTransmitter("tcp", host="unused")
    tx._mc = type("Companion", (), {"commands": Commands(EventType.ERROR)})()

    with pytest.raises(TxUnsent, match="binary channel data"):
        await tx.send_binary(b"payload", 7)


@pytest.mark.parametrize("result_type", [EventType.ERROR, None])
async def test_meshcore_requires_explicit_ok_confirmation(result_type):
    tx = MeshCoreTransmitter("tcp", host="unused")
    tx._mc = type("Companion", (), {"commands": Commands(result_type)})()

    with pytest.raises(TxUnsent, match="companion"):
        await tx.send_text("weather", 2)


async def test_worker_exception_reports_failure_callback(monkeypatch):
    db = Database(":memory:")
    manager = TransmitManager(db)
    results = []

    async def explode(item):
        raise RuntimeError("transmit log failed")

    async def no_delay(_seconds):
        manager._stopped = True

    monkeypatch.setattr(manager, "_transmit_item", explode)
    monkeypatch.setattr("app.transmit.asyncio.sleep", no_delay)
    manager.enqueue("weather", on_result=lambda ok, err="": results.append((ok, err)))

    await manager._worker()

    assert results == [(False, "transmit log failed")]


def test_cancel_queued_correlation_reports_superseded_item():
    manager = TransmitManager(Database(":memory:"))
    results = []
    manager.enqueue_destination_chain(
        "old", "meshcore", 2, 7, ("root", 7),
        on_result=lambda ok, err="": results.append((ok, err)),
    )

    removed = manager.cancel_queued_correlation(("root", 7))

    assert removed == 1
    assert manager.queue_depth == 0
    assert results == [(False, "superseded")]


async def test_conditional_successor_is_skipped_after_predecessor_failure(monkeypatch):
    manager = TransmitManager(Database(":memory:"))
    results = []
    attempted = []

    async def fail_first(item):
        attempted.append(item.text)
        return False, "radio unavailable"

    async def no_delay(_seconds):
        return None

    def result(label):
        def callback(ok, err=""):
            results.append((label, ok, err))
            if label == "cancel":
                manager._stopped = True
        return callback

    monkeypatch.setattr(manager, "_transmit_item", fail_first)
    monkeypatch.setattr("app.transmit.asyncio.sleep", no_delay)
    manager.enqueue_destination_chain(
        "original", "meshcore", 2, 7, ("root", 7), on_result=result("original"),
    )
    manager.enqueue_destination_chain(
        "cancel", "meshcore", 2, 7, ("root", 7),
        require_prior_success=True, on_result=result("cancel"),
    )

    await manager._worker()

    assert attempted == ["original"]
    assert results[1] == ("cancel", False, "predecessor was not accepted")
