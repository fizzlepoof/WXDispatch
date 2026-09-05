from __future__ import annotations

import asyncio
import logging
import os
import sys
import ssl
from types import SimpleNamespace
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from app.nwws import MAX_STANZA_BYTES, NWWSHistory, parse_nwws_stanza
from app.nwws_runtime import (
    EXPECTED_ROOM,
    AuthenticationError,
    InboundStanza,
    NWWSRuntimeConfig,
    NWWSRuntimeService,
    RuntimeState,
    SlixmppTransport,
    FIXED_ENDPOINT,
)

STANZA = b"""<message type='groupchat' from='nwws@conference.nwws-oi.weather.gov/source'>
<x xmlns='nwws-oi' cccc='KOHX' ttaaii='WFUS54' issue='2026-09-05T01:00:00Z'
 awipsid='TOROHX' id='worker.42'>WFUS54 KOHX 050100
TOROHX
TNC021-050200-
/O.NEW.KOHX.TO.W.0042.260905T0100Z-260905T0200Z/
Warning text.</x></message>"""


def test_delayed_payload_is_non_actionable_history() -> None:
    delayed = STANZA.replace(
        b"</x></message>",
        b"</x><delay xmlns='urn:xmpp:delay' stamp='2026-09-05T01:01:00Z'/></message>",
    )
    assert isinstance(parse_nwws_stanza(delayed), NWWSHistory)


class FakeTransport:
    def __init__(self, outcomes: asyncio.Queue[object]) -> None:
        self.outcomes = outcomes
        self.calls: list[dict] = []
        self.closed = 0

    async def run(self, **kwargs) -> None:
        self.calls.append(kwargs)
        kwargs["on_connected"]()
        outcome = await self.outcomes.get()
        if isinstance(outcome, BaseException):
            raise outcome

    async def close(self) -> None:
        self.closed += 1


class FakeClock:
    def __init__(self) -> None:
        self.sleeps: list[float] = []
        self.block = asyncio.Event()

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        await self.block.wait()


async def spin() -> None:
    for _ in range(8):
        await asyncio.sleep(0)


async def fake_resolver(_host: str, _port: int):
    return (("203.0.113.8", 5222),)


def secret_file(tmp_path: Path, value: str = "correct horse battery staple") -> Path:
    path = tmp_path / "nwws.secret"
    path.write_text(value + "\n")
    if os.name == "posix":
        path.chmod(0o600)
    return path


def config(tmp_path: Path, **changes) -> NWWSRuntimeConfig:
    values = {"enabled": True, "username": "wxdispatch", "password_file": secret_file(tmp_path)}
    values.update(changes)
    return NWWSRuntimeConfig(**values)


@pytest.mark.asyncio
async def test_disabled_is_inert_without_import_or_credential_read(tmp_path: Path, monkeypatch) -> None:
    password = tmp_path / "must-not-read"
    imported_before = "slixmpp" in sys.modules
    called = False

    def factory():
        nonlocal called
        called = True
        raise AssertionError("transport factory must stay inert")

    service = NWWSRuntimeService(
        NWWSRuntimeConfig(enabled=False, username="", password_file=password),
        transport_factory=factory,
        resolver=lambda *_: (_ for _ in ()).throw(AssertionError("no DNS")),
    )
    await service.start()

    assert service.health.state is RuntimeState.DISABLED
    assert called is False
    assert password.exists() is False
    assert ("slixmpp" in sys.modules) is imported_before
    await service.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("username", ["", "a@b", " space ", "x" * 65, "../user", "é"])
async def test_bad_username_is_misconfigured_without_connect(tmp_path: Path, username: str) -> None:
    called = False

    def factory():
        nonlocal called
        called = True

    service = NWWSRuntimeService(config(tmp_path, username=username), transport_factory=factory)
    await service.start()
    assert service.health.state is RuntimeState.MISCONFIGURED
    assert service.health.error_category == "credentials"
    assert called is False


@pytest.mark.asyncio
async def test_password_file_must_be_private_regular_and_not_symlink(tmp_path: Path) -> None:
    target = secret_file(tmp_path)
    link = tmp_path / "link"
    link.symlink_to(target)
    for path in (link, tmp_path):
        service = NWWSRuntimeService(
            NWWSRuntimeConfig(enabled=True, username="wxdispatch", password_file=path),
            transport_factory=lambda: pytest.fail("must not connect"),
        )
        await service.start()
        assert service.health.state is RuntimeState.MISCONFIGURED
    if os.name == "posix":
        target.chmod(0o640)
        service = NWWSRuntimeService(
            NWWSRuntimeConfig(enabled=True, username="wxdispatch", password_file=target),
            transport_factory=lambda: None,
        )
        await service.start()
        assert service.health.state is RuntimeState.MISCONFIGURED


@pytest.mark.asyncio
async def test_invalid_password_path_type_is_misconfigured() -> None:
    service = NWWSRuntimeService(
        NWWSRuntimeConfig(
            enabled=True, username="wxdispatch", password_file=123  # type: ignore[arg-type]
        ),
        transport_factory=lambda: pytest.fail("must not connect"),
    )
    await service.start()
    assert service.health.state is RuntimeState.MISCONFIGURED
    assert service.health.error_category == "credentials"


@pytest.mark.asyncio
async def test_connect_uses_only_fixed_secure_endpoint_and_resolves_each_attempt(tmp_path: Path) -> None:
    outcomes: asyncio.Queue[object] = asyncio.Queue()
    outcomes.put_nowait(RuntimeError("lost"))
    outcomes.put_nowait(asyncio.CancelledError())
    transports: list[FakeTransport] = []
    resolutions = 0

    async def resolver(host: str, port: int):
        nonlocal resolutions
        resolutions += 1
        assert (host, port) == ("nwws-oi.weather.gov", 5222)
        return (("203.0.113.8", 5222),)

    def factory():
        transport = FakeTransport(outcomes)
        transports.append(transport)
        return transport

    service = NWWSRuntimeService(config(tmp_path), transport_factory=factory, resolver=resolver, rng=lambda cap: 0)
    await service.start()
    await spin()
    assert resolutions >= 2
    call = transports[0].calls[0]
    assert call["endpoint"].host == "nwws-oi.weather.gov"
    assert call["endpoint"].port == 5222
    assert call["endpoint"].resource == "nwws"
    assert call["endpoint"].room == EXPECTED_ROOM
    assert call["endpoint"].require_tls is True
    assert call["username"] == "wxdispatch"
    assert call["password"] == "correct horse battery staple"
    await service.stop()


@pytest.mark.asyncio
async def test_filters_room_history_office_replays_and_malformed(tmp_path: Path) -> None:
    outcomes: asyncio.Queue[object] = asyncio.Queue()
    transport = FakeTransport(outcomes)
    delivered = []
    service = NWWSRuntimeService(
        config(tmp_path), transport_factory=lambda: transport,
        on_product=delivered.append, resolver=fake_resolver,
    )
    await service.start()
    await spin()
    emit = transport.calls[0]["on_stanza"]
    emit(InboundStanza(STANZA, "attacker@conference.example/x", "groupchat"))
    emit(InboundStanza(b"<message type='groupchat'><body>old</body></message>", EXPECTED_ROOM + "/x", "groupchat"))
    emit(InboundStanza(STANZA.replace(b"KOHX", b"KOUN"), EXPECTED_ROOM + "/x", "groupchat"))
    emit(InboundStanza(STANZA, EXPECTED_ROOM + "/x", "groupchat"))
    emit(InboundStanza(STANZA, EXPECTED_ROOM + "/x", "groupchat"))
    emit(InboundStanza(b"not xml", EXPECTED_ROOM + "/x", "groupchat"))
    await spin()
    assert [item.stream_id for item in delivered] == ["worker.42"]
    assert service.health.received == 6
    assert service.health.delivered == 1
    assert service.health.ignored == 4
    assert service.health.malformed == 1
    await service.stop()


@pytest.mark.asyncio
async def test_bounded_queue_drops_oversize_and_full_input(tmp_path: Path) -> None:
    outcomes: asyncio.Queue[object] = asyncio.Queue()
    transport = FakeTransport(outcomes)
    service = NWWSRuntimeService(
        config(tmp_path, queue_size=1), transport_factory=lambda: transport,
        resolver=fake_resolver,
    )
    await service.start()
    await spin()
    emit = transport.calls[0]["on_stanza"]
    emit(InboundStanza(b"x" * (MAX_STANZA_BYTES + 1), EXPECTED_ROOM + "/x", "groupchat"))
    emit(InboundStanza(STANZA, EXPECTED_ROOM + "/x", "groupchat"))
    emit(InboundStanza(STANZA.replace(b"worker.42", b"worker.43"), EXPECTED_ROOM + "/x", "groupchat"))
    await asyncio.sleep(0)
    assert service.health.dropped >= 2
    await service.stop()


@pytest.mark.asyncio
async def test_malformed_envelope_is_dropped_without_killing_consumer(tmp_path: Path) -> None:
    transport = FakeTransport(asyncio.Queue())
    delivered = []
    service = NWWSRuntimeService(
        config(tmp_path), transport_factory=lambda: transport,
        resolver=fake_resolver, on_product=delivered.append,
    )
    await service.start()
    await spin()
    emit = transport.calls[0]["on_stanza"]
    emit(InboundStanza(STANZA, None, "groupchat"))  # type: ignore[arg-type]
    emit(InboundStanza(STANZA, EXPECTED_ROOM + "/x", "groupchat"))
    await spin()
    assert service.health.dropped == 1
    assert len(delivered) == 1
    await service.stop()


def test_queue_limit_and_health_are_immutable(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        config(tmp_path, queue_size=257)
    service = NWWSRuntimeService(NWWSRuntimeConfig(enabled=False))
    with pytest.raises(FrozenInstanceError):
        service.health.received = 10  # type: ignore[misc]


@pytest.mark.asyncio
async def test_reconnect_backoff_and_auth_cooldown_are_deterministic(tmp_path: Path) -> None:
    outcomes: asyncio.Queue[object] = asyncio.Queue()
    outcomes.put_nowait(RuntimeError("password must never appear"))
    clock = FakeClock()
    service = NWWSRuntimeService(
        config(tmp_path), transport_factory=lambda: FakeTransport(outcomes),
        resolver=fake_resolver, sleep=clock.sleep, rng=lambda cap: cap / 2
    )
    await service.start()
    await spin()
    assert clock.sleeps == [0.5]
    assert service.health.state is RuntimeState.BACKOFF
    assert service.health.error_category == "transport"
    assert "password" not in repr(service.health).lower()
    clock.block.set()
    await service.stop()

    outcomes2: asyncio.Queue[object] = asyncio.Queue()
    outcomes2.put_nowait(AuthenticationError())
    clock2 = FakeClock()
    service2 = NWWSRuntimeService(
        config(tmp_path), transport_factory=lambda: FakeTransport(outcomes2),
        resolver=fake_resolver, sleep=clock2.sleep
    )
    await service2.start()
    await spin()
    assert clock2.sleeps == [1800]
    assert service2.health.error_category == "authentication"
    clock2.block.set()
    await service2.stop()


@pytest.mark.asyncio
async def test_single_instance_lease_and_clean_shutdown(tmp_path: Path) -> None:
    first_transport = FakeTransport(asyncio.Queue())
    first = NWWSRuntimeService(
        config(tmp_path), transport_factory=lambda: first_transport, resolver=fake_resolver
    )
    second = NWWSRuntimeService(
        config(tmp_path), transport_factory=lambda: pytest.fail("leased"), resolver=fake_resolver
    )
    await first.start()
    assert first.health.state is RuntimeState.CONNECTING
    await spin()
    await second.start()
    assert second.health.state is RuntimeState.MISCONFIGURED
    assert second.health.error_category == "instance_in_use"
    await first.stop()
    assert first.health.state is RuntimeState.STOPPED
    assert first_transport.closed == 1

    replacement_transport = FakeTransport(asyncio.Queue())
    replacement = NWWSRuntimeService(
        config(tmp_path), transport_factory=lambda: replacement_transport, resolver=fake_resolver
    )
    await replacement.start()
    await spin()
    assert replacement.health.state is RuntimeState.CONNECTED
    await replacement.stop()


@pytest.mark.asyncio
async def test_slixmpp_adapter_uses_117_api_and_verified_starttls(monkeypatch) -> None:
    instances = []

    class Muc:
        def __init__(self) -> None:
            self.joins = []

        async def join_muc_wait(self, room, nick, *, maxstanzas=None) -> None:
            self.joins.append((room, nick, maxstanzas))

        def leave_muc(self, room, nick) -> None:
            pass

    class Client:
        def __init__(self, jid, password) -> None:
            self.jid = jid
            self.password = password
            self.handlers = {}
            self.plugins = Muc()
            self.features = {"starttls"}
            self.ssl_context = ssl.create_default_context()
            self.transport = SimpleNamespace(get_extra_info=lambda name: object() if name == "ssl_object" else None)
            instances.append(self)

        def register_plugin(self, name) -> None:
            pass

        def add_event_handler(self, name, callback) -> None:
            self.handlers[name] = callback

        def event(self, name) -> None:
            callback = self.handlers.get(name)
            if callback is not None:
                callback(None)

        def __getitem__(self, name):
            assert name == "xep_0045"
            return self.plugins

        async def connect(self, host=None, port=None):
            self.connected_to = (host, port)
            await self.handlers["session_start"](None)
            self.handlers["disconnected"](None)
            return True

        async def disconnect(self, wait=2.0) -> None:
            pass

        async def _handle_stream_features(self, features):
            return "handled"

        def reschedule_connection_attempt(self):
            return "unsafe internal retry"

    monkeypatch.setitem(sys.modules, "slixmpp", SimpleNamespace(ClientXMPP=Client))
    connected = []
    logging.getLogger("slixmpp.xmlstream.xmlstream").setLevel(logging.DEBUG)
    adapter = SlixmppTransport()
    assert logging.getLogger("slixmpp.xmlstream.xmlstream").getEffectiveLevel() >= logging.INFO
    await adapter.run(
        endpoint=FIXED_ENDPOINT, username="wxdispatch", password="not-logged",
        addresses=(("203.0.113.8", 5222),), on_stanza=lambda _: None,
        on_connected=lambda: connected.append(True),
    )
    assert instances[0].connected_to == ("nwws-oi.weather.gov", 5222)
    assert instances[0].ssl_context.check_hostname is True
    assert instances[0].ssl_context.verify_mode == ssl.CERT_REQUIRED
    assert instances[0].plugins.joins == [(EXPECTED_ROOM, "nwws", 0)]
    assert connected == [True]
    instances[0].features.clear()
    with pytest.raises(ssl.SSLError, match="STARTTLS"):
        await instances[0]._handle_stream_features({"features": {}})
    assert instances[0].reschedule_connection_attempt() is None
