"""Disabled-safe, standalone NWWS-OI XMPP runtime adapter.

This module deliberately has no application, database, or transmitter imports.  The
optional XMPP implementation is imported only after an enabled service has validated
its local credentials.
"""
from __future__ import annotations

import asyncio
import inspect
import logging
import os
import random
import re
import socket
import ssl
import stat
import threading
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Awaitable, Callable, Protocol, Sequence

from app.nwws import MAX_STANZA_BYTES, NWWSHistory, NWWSParseError, NWWSProduct, SequenceTracker, parse_nwws_stanza

NWWS_HOST = "nwws-oi.weather.gov"
NWWS_PORT = 5222
NWWS_DOMAIN = "nwws-oi.weather.gov"
NWWS_RESOURCE = "nwws"
EXPECTED_ROOM = "nwws@conference.nwws-oi.weather.gov"
MAX_QUEUE_SIZE = 256
MAX_PASSWORD_BYTES = 4096
AUTH_COOLDOWN_SECONDS = 1800.0
MAX_BACKOFF_SECONDS = 300.0
_USERNAME = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9_.-]{0,62}[A-Za-z0-9])?")
_OFFICE = re.compile(r"[A-Z]{4}")


class AuthenticationError(ConnectionError):
    """The server rejected the NWWS-OI account."""


class RuntimeState(str, Enum):
    DISABLED = "disabled"
    MISCONFIGURED = "misconfigured"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    BACKOFF = "backoff"
    STOPPED = "stopped"


@dataclass(frozen=True, slots=True)
class NWWSEndpoint:
    host: str = NWWS_HOST
    port: int = NWWS_PORT
    domain: str = NWWS_DOMAIN
    resource: str = NWWS_RESOURCE
    room: str = EXPECTED_ROOM
    require_tls: bool = True


FIXED_ENDPOINT = NWWSEndpoint()


@dataclass(frozen=True, slots=True)
class NWWSRuntimeConfig:
    enabled: bool = False
    username: str = ""
    password_file: str | os.PathLike[str] | None = None
    offices: frozenset[str] | Sequence[str] = frozenset({"KOHX"})
    queue_size: int = MAX_QUEUE_SIZE

    def __post_init__(self) -> None:
        if isinstance(self.queue_size, bool) or not 1 <= self.queue_size <= MAX_QUEUE_SIZE:
            raise ValueError("queue_size must be between 1 and 256")
        try:
            offices = frozenset(self.offices)
        except TypeError as exc:
            raise ValueError("offices must be an iterable") from exc
        object.__setattr__(self, "offices", offices)


@dataclass(frozen=True, slots=True)
class InboundStanza:
    xml: bytes | str
    from_jid: str
    message_type: str


@dataclass(frozen=True, slots=True)
class NWWSRuntimeHealth:
    state: RuntimeState
    received: int = 0
    delivered: int = 0
    ignored: int = 0
    malformed: int = 0
    dropped: int = 0
    reconnects: int = 0
    last_connected_at: datetime | None = None
    last_message_at: datetime | None = None
    error_category: str | None = None


StanzaCallback = Callable[[InboundStanza], None]


class NWWSTransport(Protocol):
    """Minimal receive-only transport boundary used by the runtime."""

    async def run(
        self,
        *,
        endpoint: NWWSEndpoint,
        username: str,
        password: str,
        addresses: Sequence[tuple],
        on_stanza: StanzaCallback,
        on_connected: Callable[[], None],
    ) -> None: ...

    async def close(self) -> None: ...


Resolver = Callable[[str, int], Awaitable[Sequence[tuple]]]
Sleep = Callable[[float], Awaitable[None]]
ProductCallback = Callable[[NWWSProduct], object]
TransportFactory = Callable[[], NWWSTransport]


class _ConfigurationError(ValueError):
    pass


def _validate_config(config: NWWSRuntimeConfig) -> None:
    if not isinstance(config.username, str) or _USERNAME.fullmatch(config.username) is None:
        raise _ConfigurationError("invalid username")
    offices = config.offices
    if not offices or len(offices) > 64 or any(not isinstance(value, str) or _OFFICE.fullmatch(value) is None for value in offices):
        raise _ConfigurationError("invalid offices")
    if config.password_file is None:
        raise _ConfigurationError("missing password file")


def _read_password(path_value: str | os.PathLike[str] | None) -> str:
    if path_value is None:
        raise _ConfigurationError("missing password file")
    try:
        path = Path(path_value)
        before = path.lstat()
    except (OSError, ValueError, TypeError) as exc:
        raise _ConfigurationError("unusable password file") from exc
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise _ConfigurationError("password file must be a regular non-symlink")
    if os.name == "posix" and before.st_mode & 0o077:
        raise _ConfigurationError("password file permissions are too broad")

    flags = os.O_RDONLY
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(path, flags)
        try:
            current = os.fstat(fd)
            if not stat.S_ISREG(current.st_mode) or (current.st_dev, current.st_ino) != (before.st_dev, before.st_ino):
                raise _ConfigurationError("password file changed during validation")
            if os.name == "posix" and current.st_mode & 0o077:
                raise _ConfigurationError("password file permissions are too broad")
            if current.st_size > MAX_PASSWORD_BYTES:
                raise _ConfigurationError("password file is too large")
            data = os.read(fd, MAX_PASSWORD_BYTES + 1)
        finally:
            os.close(fd)
    except _ConfigurationError:
        raise
    except OSError as exc:
        raise _ConfigurationError("unusable password file") from exc
    if len(data) > MAX_PASSWORD_BYTES:
        raise _ConfigurationError("password file is too large")
    try:
        password = data.decode("utf-8").rstrip("\r\n")
    except UnicodeDecodeError as exc:
        raise _ConfigurationError("password file is not UTF-8") from exc
    if not password or any(ord(char) < 0x20 or ord(char) == 0x7F for char in password):
        raise _ConfigurationError("password is empty or contains control characters")
    return password


async def _default_resolver(host: str, port: int) -> Sequence[tuple]:
    loop = asyncio.get_running_loop()
    records = await loop.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    # Only bounded socket addresses cross the transport boundary.
    unique: list[tuple] = []
    for _family, _type, _proto, _canonname, sockaddr in records[:32]:
        address = tuple(sockaddr)
        if address not in unique:
            unique.append(address)
    if not unique:
        raise OSError("NWWS hostname did not resolve")
    return tuple(unique)


class NWWSRuntimeService:
    """Own one bounded receive-only NWWS-OI connection loop."""

    _lease_lock = threading.Lock()
    _lease_owner: "NWWSRuntimeService | None" = None

    def __init__(
        self,
        config: NWWSRuntimeConfig,
        *,
        on_product: ProductCallback | None = None,
        transport_factory: TransportFactory | None = None,
        resolver: Resolver | None = None,
        sleep: Sleep = asyncio.sleep,
        rng: Callable[[float], float] | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.config = config
        self._on_product = on_product or (lambda _product: None)
        self._transport_factory = transport_factory
        self._resolver = resolver or _default_resolver
        self._sleep = sleep
        self._rng = rng or (lambda cap: random.uniform(0.0, cap))
        self._now = now or (lambda: datetime.now(timezone.utc))
        initial = RuntimeState.DISABLED if not config.enabled else RuntimeState.STOPPED
        self._health = NWWSRuntimeHealth(initial)
        self._queue: asyncio.Queue[InboundStanza] | None = None
        self._connection_task: asyncio.Task[None] | None = None
        self._worker_task: asyncio.Task[None] | None = None
        self._active_transport: NWWSTransport | None = None
        self._password: str | None = None
        self._stopping = False
        self._leased = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._tracker = SequenceTracker()
        self._callback_slots = threading.BoundedSemaphore(config.queue_size)

    @property
    def health(self) -> NWWSRuntimeHealth:
        return self._health

    def _set_health(self, **changes: object) -> None:
        self._health = replace(self._health, **changes)

    def _acquire_lease(self) -> bool:
        with self._lease_lock:
            if self.__class__._lease_owner not in (None, self):
                return False
            self.__class__._lease_owner = self
            self._leased = True
            return True

    def _release_lease(self) -> None:
        with self._lease_lock:
            if self.__class__._lease_owner is self:
                self.__class__._lease_owner = None
            self._leased = False

    async def start(self) -> None:
        if not self.config.enabled:
            self._set_health(state=RuntimeState.DISABLED, error_category=None)
            return
        if self._connection_task is not None and not self._connection_task.done():
            return
        if not self._acquire_lease():
            self._set_health(state=RuntimeState.MISCONFIGURED, error_category="instance_in_use")
            return
        try:
            _validate_config(self.config)
            password = _read_password(self.config.password_file)
        except _ConfigurationError:
            self._release_lease()
            self._set_health(state=RuntimeState.MISCONFIGURED, error_category="credentials")
            return
        self._password = password
        self._stopping = False
        self._loop = asyncio.get_running_loop()
        self._queue = asyncio.Queue(maxsize=self.config.queue_size)
        self._set_health(state=RuntimeState.CONNECTING, error_category=None)
        self._worker_task = self._loop.create_task(self._consume(), name="nwws-consumer")
        self._connection_task = self._loop.create_task(self._connection_loop(), name="nwws-connection")

    def _receive(self, stanza: InboundStanza) -> None:
        loop = self._loop
        if loop is None or loop.is_closed() or self._stopping:
            return
        try:
            if asyncio.get_running_loop() is loop:
                self._enqueue(stanza)
                return
        except RuntimeError:
            pass
        if not self._callback_slots.acquire(blocking=False):
            self._set_health(dropped=self._health.dropped + 1)
            return

        def enqueue_bounded() -> None:
            try:
                self._enqueue(stanza)
            finally:
                self._callback_slots.release()

        try:
            loop.call_soon_threadsafe(enqueue_bounded)
        except RuntimeError:
            self._callback_slots.release()

    def _enqueue(self, stanza: InboundStanza) -> None:
        queue = self._queue
        if queue is None or self._stopping:
            return
        self._set_health(received=self._health.received + 1, last_message_at=self._now())
        if (
            not isinstance(stanza, InboundStanza)
            or not isinstance(stanza.from_jid, str)
            or not isinstance(stanza.message_type, str)
            or not isinstance(stanza.xml, (bytes, str))
        ):
            self._set_health(dropped=self._health.dropped + 1)
            return
        try:
            size = len(stanza.xml) if isinstance(stanza.xml, bytes) else len(stanza.xml.encode("utf-8"))
        except (AttributeError, UnicodeError):
            self._set_health(dropped=self._health.dropped + 1)
            return
        if size > MAX_STANZA_BYTES:
            self._set_health(dropped=self._health.dropped + 1)
            return
        try:
            queue.put_nowait(stanza)
        except asyncio.QueueFull:
            self._set_health(dropped=self._health.dropped + 1)

    async def _consume(self) -> None:
        assert self._queue is not None
        while True:
            stanza = await self._queue.get()
            try:
                if stanza.message_type != "groupchat" or stanza.from_jid.split("/", 1)[0] != EXPECTED_ROOM:
                    self._set_health(ignored=self._health.ignored + 1)
                    continue
                try:
                    parsed = parse_nwws_stanza(stanza.xml)
                except (NWWSParseError, TypeError, ValueError):
                    self._set_health(malformed=self._health.malformed + 1)
                    continue
                if isinstance(parsed, NWWSHistory):
                    self._set_health(ignored=self._health.ignored + 1)
                    continue
                if parsed.issuing_office not in self.config.offices:
                    self._set_health(ignored=self._health.ignored + 1)
                    continue
                decision = self._tracker.observe(parsed)
                if not decision.accepted:
                    self._set_health(ignored=self._health.ignored + 1)
                    continue
                result = self._on_product(parsed)
                if inspect.isawaitable(result):
                    await result
                self._set_health(delivered=self._health.delivered + 1)
            except asyncio.CancelledError:
                raise
            except Exception:
                # Callback failures are contained and exposed only as a category.
                self._set_health(error_category="callback")
            finally:
                self._queue.task_done()

    def _connected(self) -> None:
        self._set_health(
            state=RuntimeState.CONNECTED,
            last_connected_at=self._now(),
            error_category=None,
        )

    def _new_transport(self) -> NWWSTransport:
        if self._transport_factory is not None:
            return self._transport_factory()
        return SlixmppTransport()

    async def _connection_loop(self) -> None:
        failures = 0
        try:
            while not self._stopping:
                self._set_health(state=RuntimeState.CONNECTING, error_category=None)
                transport: NWWSTransport | None = None
                try:
                    addresses = await self._resolver(FIXED_ENDPOINT.host, FIXED_ENDPOINT.port)
                    if not addresses:
                        raise OSError("NWWS hostname did not resolve")
                    transport = self._new_transport()
                    self._active_transport = transport
                    assert self._password is not None
                    await transport.run(
                        endpoint=FIXED_ENDPOINT,
                        username=self.config.username,
                        password=self._password,
                        addresses=tuple(addresses),
                        on_stanza=self._receive,
                        on_connected=self._connected,
                    )
                    if self._stopping:
                        break
                    failures = 0
                    category = "transport"
                    delay_cap = 1.0
                except asyncio.CancelledError:
                    raise
                except AuthenticationError:
                    category = "authentication"
                    delay_cap = AUTH_COOLDOWN_SECONDS
                    failures = 0
                except (asyncio.TimeoutError, TimeoutError):
                    category = "timeout"
                    delay_cap = AUTH_COOLDOWN_SECONDS
                    failures = 0
                except Exception:
                    failures += 1
                    category = "transport"
                    delay_cap = min(MAX_BACKOFF_SECONDS, float(2 ** (failures - 1)))
                finally:
                    if transport is not None:
                        try:
                            await transport.close()
                        except Exception:
                            pass
                    if self._active_transport is transport:
                        self._active_transport = None
                if self._stopping:
                    break
                delay = delay_cap if delay_cap == AUTH_COOLDOWN_SECONDS else max(0.0, min(delay_cap, float(self._rng(delay_cap))))
                self._set_health(
                    state=RuntimeState.BACKOFF,
                    reconnects=self._health.reconnects + 1,
                    error_category=category,
                )
                await self._sleep(delay)
        finally:
            self._release_lease()

    async def stop(self) -> None:
        if not self.config.enabled:
            self._set_health(state=RuntimeState.DISABLED)
            return
        self._stopping = True
        tasks = [task for task in (self._connection_task, self._worker_task) if task is not None and not task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._connection_task = None
        self._worker_task = None
        self._active_transport = None
        self._queue = None
        self._password = None
        self._release_lease()
        self._set_health(state=RuntimeState.STOPPED, error_category=None)


class SlixmppTransport:
    """Production TLS/XEP-0045 transport, imported only on first construction."""

    def __init__(self) -> None:
        try:
            import slixmpp  # type: ignore[import-not-found]
        except ImportError as exc:
            raise RuntimeError("slixmpp is required when NWWS-OI is enabled") from exc
        self._slixmpp = slixmpp
        protocol_logger = logging.getLogger("slixmpp.xmlstream.xmlstream")
        if protocol_logger.getEffectiveLevel() < logging.INFO:
            protocol_logger.setLevel(logging.INFO)
        self._client = None
        self._room: str | None = None

    async def run(
        self,
        *,
        endpoint: NWWSEndpoint,
        username: str,
        password: str,
        addresses: Sequence[tuple],
        on_stanza: StanzaCallback,
        on_connected: Callable[[], None],
    ) -> None:
        if endpoint != FIXED_ENDPOINT or not endpoint.require_tls:
            raise ValueError("only the fixed TLS NWWS-OI endpoint is permitted")
        jid = f"{username}@{endpoint.domain}/{endpoint.resource}"
        base_client = self._slixmpp.ClientXMPP

        class SecureClient(base_client):
            def reschedule_connection_attempt(self):
                self._current_connection_attempt = None
                self.event("connection_failed")
                return None

            async def _handle_stream_features(self, features):
                offered = features["features"]
                if "starttls" not in self.features and "starttls" not in offered:
                    result = self.disconnect()
                    if inspect.isawaitable(result):
                        await result
                    raise ssl.SSLError("verified STARTTLS is required")
                return await super()._handle_stream_features(features)

        client = SecureClient(jid, password)
        self._client = client
        self._room = endpoint.room
        context = ssl.create_default_context()
        context.check_hostname = True
        context.verify_mode = ssl.CERT_REQUIRED
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        client.ssl_context = context
        client.register_plugin("xep_0030")
        client.register_plugin("xep_0045")
        disconnected = asyncio.get_running_loop().create_future()

        async def session_start(_event) -> None:
            try:
                ssl_object = client.transport.get_extra_info("ssl_object") if client.transport else None
                if "starttls" not in client.features or ssl_object is None:
                    raise ssl.SSLError("verified STARTTLS is required")
                await client["xep_0045"].join_muc_wait(
                    endpoint.room, endpoint.resource, maxstanzas=0
                )
                on_connected()
            except Exception as exc:
                if not disconnected.done():
                    disconnected.set_exception(exc)

        def message(message) -> None:
            try:
                message_type = str(message["type"])
                from_jid = str(message["from"])
                if message_type != "groupchat" or from_jid.split("/", 1)[0] != endpoint.room:
                    return
                on_stanza(InboundStanza(str(message), from_jid, message_type))
            except Exception:
                return

        def auth_failed(_event) -> None:
            if not disconnected.done():
                disconnected.set_exception(AuthenticationError())

        def connection_failed(_event=None) -> None:
            if not disconnected.done():
                disconnected.set_exception(ConnectionError("NWWS connection failed"))

        def closed(_event) -> None:
            if not disconnected.done():
                disconnected.set_result(None)

        client.add_event_handler("session_start", session_start)
        client.add_event_handler("groupchat_message", message)
        client.add_event_handler("failed_auth", auth_failed)
        client.add_event_handler("connection_failed", connection_failed)
        client.add_event_handler("disconnected", closed)
        # Resolve explicitly every service attempt; retain the DNS hostname for TLS
        # certificate and XMPP identity verification rather than connecting by IP.
        if not addresses:
            raise OSError("NWWS hostname did not resolve")
        client.enable_starttls = True
        client.enable_direct_tls = False
        connected = client.connect(endpoint.host, endpoint.port)
        if inspect.isawaitable(connected):
            connected = await connected
        if connected is False:
            raise ConnectionError("NWWS connection failed")
        await disconnected

    async def close(self) -> None:
        client = self._client
        self._client = None
        if client is None:
            return
        if self._room is not None:
            try:
                result = client["xep_0045"].leave_muc(self._room, FIXED_ENDPOINT.resource)
                if inspect.isawaitable(result):
                    await result
            except Exception:
                pass
        try:
            result = client.disconnect(wait=2.0)
            if inspect.isawaitable(result):
                await result
        except Exception:
            pass
