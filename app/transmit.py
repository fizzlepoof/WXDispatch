"""Multi-protocol transmit layer - Meshtastic + MeshCore, side by side.

Each protocol is an independent transport with its own enable flag and
connection (serial or TCP). TransmitManager fans every outbound message out to
ALL enabled transports, paces bursts, and manages per-transport reconnects.
Dry-run lives in the poller; manual sends bypass pacing. Backends are imported
lazily so a missing optional dependency (e.g. meshcore) never breaks startup.
"""
from __future__ import annotations

import abc
import asyncio
import hashlib
import logging
import threading
import time
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass

from .config import BURST_GAP_SECONDS, REPEAT_GAP_SECONDS, QUEUE_MAX, MAX_PAYLOAD_BYTES


class TxUnsent(Exception):
    """Raised when a radio did not actually transmit a message. `category` tells
    the sender WHY, so it can apply the right correction instead of blindly
    retrying:
      'duty_cycle' - radio hit its airtime/duty-cycle cap: wait, then retry
      'queue_full' - the radio's TX queue is full: wait for it to drain, retry
      'too_large'  - message exceeds the radio payload: shrink it (unrecoverable here)
      'no_channel' - the configured channel index does not exist: config error
      'link'       - radio interface down / no response: reconnect, then retry
      'unsent'     - radio accepted the command but did not key up: wait/reconnect
    """
    def __init__(self, category: str, detail: str):
        super().__init__(detail)
        self.category = category
        self.detail = detail


def _is_meshcore_link_failure(exc: BaseException) -> bool:
    return isinstance(exc, (
        ConnectionError,
        EOFError,
        asyncio.TimeoutError,
        asyncio.IncompleteReadError,
    ))


class _MeshCoreChannelLinkError(RuntimeError):
    """A sanitized channel-link failure with mutation retry safety metadata."""

    def __init__(self, *, retry_safe: bool):
        super().__init__("MeshCore channel connection failed")
        self.retry_safe = retry_safe

logger = logging.getLogger("mesh_wx.tx")

_meshcore_log_suppression_lock = threading.RLock()
_meshcore_log_suppression_depth = 0
_meshcore_log_prior_disabled = False


@contextmanager
def _suppress_meshcore_dependency_logging():
    """Silence secret-bearing dependency frames across overlapping operations."""
    global _meshcore_log_suppression_depth, _meshcore_log_prior_disabled
    meshcore_logger = logging.getLogger("meshcore")
    with _meshcore_log_suppression_lock:
        if _meshcore_log_suppression_depth == 0:
            _meshcore_log_prior_disabled = meshcore_logger.disabled
            meshcore_logger.disabled = True
        _meshcore_log_suppression_depth += 1
    try:
        yield
    finally:
        with _meshcore_log_suppression_lock:
            _meshcore_log_suppression_depth -= 1
            if _meshcore_log_suppression_depth == 0:
                meshcore_logger.disabled = _meshcore_log_prior_disabled


def _cap_text(text: str) -> str:
    while len(text.encode("utf-8")) > MAX_PAYLOAD_BYTES:
        text = text[:-1]
    return text


class Transmitter(abc.ABC):
    label = "?"

    @abc.abstractmethod
    async def connect(self) -> None: ...
    @abc.abstractmethod
    async def send_text(self, text: str, channel: int) -> None: ...
    @abc.abstractmethod
    async def close(self) -> None: ...
    @property
    @abc.abstractmethod
    def connected(self) -> bool: ...

    async def read_channels(self) -> list:
        """Return the channels configured on the device: [{index, name}]. Optional."""
        return []


class MeshtasticTransmitter(Transmitter):
    label = "Meshtastic"

    def __init__(self, conn: str, port: str = "", host: str = ""):
        self.conn, self.port, self.host = conn, port, host
        self._iface = None
        self._qstatus = {}   # firmware TX-queue verdict per packet id: {id: res}

    async def connect(self) -> None:
        if self._iface is not None:
            await self.close()

        def _open():
            if self.conn == "tcp":
                from meshtastic.tcp_interface import TCPInterface
                h, _, p = (self.host or "").partition(":")
                if p:  # explicit host:port
                    iface = TCPInterface(hostname=h, portNumber=int(p))
                else:
                    iface = TCPInterface(hostname=h)
            else:
                from meshtastic.serial_interface import SerialInterface
                iface = SerialInterface(devPath=self.port)
            # Hook the firmware's per-packet TX-queue status so each send can be
            # verified: the radio replies with a QueueStatus for the packet id,
            # res==0 means it was accepted into the TX queue (will transmit),
            # res!=0 means it was rejected/dropped (the silent-drop failure).
            self._qstatus = {}
            orig = iface._handleQueueStatusFromRadio
            def _hook(qs):
                try:
                    if len(self._qstatus) > 256:
                        self._qstatus.clear()
                    self._qstatus[qs.mesh_packet_id] = (qs.res, qs.free)
                except Exception:
                    pass
                return orig(qs)
            iface._handleQueueStatusFromRadio = _hook
            return iface

        self._iface = await asyncio.get_event_loop().run_in_executor(None, _open)

    async def send_text(self, text: str, channel: int) -> None:
        if self._iface is None:
            raise RuntimeError("not connected")

        # res -> (category, name). The radio tells us WHY a packet was not queued.
        cat_by_res = {4: "link", 6: "no_channel", 7: "too_large", 8: "link",
                      9: "duty_cycle"}

        def _send_and_verify():
            pkt = self._iface.sendText(text, channelIndex=channel)
            pid = getattr(pkt, "id", None)
            if pid is None:
                return  # no id to track: fall back to best-effort
            waited = 0.0
            while waited < 6.0:               # await the firmware's verdict
                if pid in self._qstatus:
                    res, free = self._qstatus.pop(pid)
                    if res == 0:
                        return                # accepted into TX queue -> transmits
                    from meshtastic import mesh_pb2
                    try:
                        name = mesh_pb2.Routing.Error.Name(res)
                    except Exception:
                        name = "res=%s" % res
                    cat = cat_by_res.get(res, "unsent")
                    if free == 0 and cat == "unsent":
                        cat = "queue_full"
                    raise TxUnsent(cat, "radio did not transmit (%s)" % name)
                time.sleep(0.15)
                waited += 0.15
            raise TxUnsent("link", "radio did not transmit (no queue confirmation)")

        await asyncio.get_event_loop().run_in_executor(None, _send_and_verify)

    async def close(self) -> None:
        if self._iface is not None:
            iface, self._iface = self._iface, None
            try:
                await asyncio.get_event_loop().run_in_executor(None, iface.close)
            except Exception:
                pass

    @property
    def connected(self) -> bool:
        return self._iface is not None

    async def read_channels(self) -> list:
        if self._iface is None:
            raise RuntimeError("not connected")

        def _read():
            out = []
            node = getattr(self._iface, "localNode", None)
            for ch in (getattr(node, "channels", None) or []):
                role = int(getattr(ch, "role", 0))  # 0 DISABLED, 1 PRIMARY, 2 SECONDARY
                if role == 0:
                    continue
                name = ""
                if getattr(ch, "settings", None) is not None:
                    name = ch.settings.name or ""
                if not name:
                    name = "LongFast" if role == 1 else ("channel %d" % ch.index)
                out.append({"index": int(ch.index), "name": name})
            return out

        return await asyncio.get_event_loop().run_in_executor(None, _read)

    async def read_info(self) -> dict:
        if self._iface is None:
            return {}

        def _read():
            info = {}
            try:
                ni = self._iface.getMyNodeInfo() or {}
                hw = (ni.get("user") or {}).get("hwModel")
                if hw:
                    info["model"] = str(hw)
            except Exception:
                pass
            try:
                meta = getattr(self._iface, "metadata", None)
                fw = getattr(meta, "firmware_version", "") if meta is not None else ""
                if fw:
                    info["firmware"] = str(fw)
            except Exception:
                pass
            return info

        return await asyncio.get_event_loop().run_in_executor(None, _read)


class MeshCoreTransmitter(Transmitter):
    label = "MeshCore"
    # Older companion firmware does not report its slot count. Preserve the
    # app's previous eight-slot behavior rather than probing arbitrary indexes.
    FALLBACK_MAX_CHANNELS = 8

    def __init__(self, conn: str, port: str = "", host: str = "", baud: int = 115200):
        self.conn, self.port, self.host, self.baud = conn, port, host, baud
        self._mc = None

    async def _device_info(self) -> dict:
        if self._mc is None:
            raise RuntimeError("not connected")
        from meshcore import EventType
        try:
            res = await self._mc.commands.send_device_query()
        except Exception as exc:
            if _is_meshcore_link_failure(exc):
                raise _MeshCoreChannelLinkError(retry_safe=True) from None
            raise
        if getattr(res, "type", None) != EventType.DEVICE_INFO:
            raise RuntimeError("companion did not return device information")
        payload = getattr(res, "payload", None)
        if not isinstance(payload, dict):
            raise RuntimeError("companion returned an invalid device information payload")
        return payload

    @staticmethod
    def _max_channels_from_info(info: dict) -> int:
        if "max_channels" not in info:
            return MeshCoreTransmitter.FALLBACK_MAX_CHANNELS
        value = info["max_channels"]
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise RuntimeError("companion returned invalid max_channels")
        return value

    async def _read_channel_at(self, index: int) -> dict:
        from meshcore import EventType
        try:
            with _suppress_meshcore_dependency_logging():
                res = await self._mc.commands.get_channel(index)
        except Exception as exc:
            if _is_meshcore_link_failure(exc):
                raise _MeshCoreChannelLinkError(retry_safe=True) from None
            raise
        if getattr(res, "type", None) != EventType.CHANNEL_INFO:
            raise RuntimeError("companion did not return channel information")
        payload = getattr(res, "payload", {}) or {}
        return {
            "index": int(payload.get("channel_idx", index)),
            "name": payload.get("channel_name") or "",
            "secret": payload.get("channel_secret"),
        }

    async def set_channel(self, index: int, name: str) -> dict:
        if self._mc is None:
            raise RuntimeError("not connected")
        from meshcore import EventType
        max_channels = self._max_channels_from_info(await self._device_info())
        if not isinstance(index, int) or isinstance(index, bool) or not 0 <= index < max_channels:
            raise ValueError("channel index is out of range")
        if not isinstance(name, str) or not name:
            raise ValueError("channel name is required")
        if "\x00" in name:
            raise ValueError("channel name must not contain NUL")
        if len(name.encode("utf-8")) > 32:
            raise ValueError("channel name must be at most 32 UTF-8 bytes")
        if not name.startswith("#"):
            raise ValueError("hash channel name must begin with #")
        if len(name) == 1:
            raise ValueError("hash channel must include a name after #")
        secret = hashlib.sha256(name.encode("utf-8")).digest()[:16]
        try:
            with _suppress_meshcore_dependency_logging():
                res = await self._mc.commands.set_channel(index, name, secret)
        except Exception as exc:
            if _is_meshcore_link_failure(exc):
                raise _MeshCoreChannelLinkError(retry_safe=False) from None
            raise
        with _suppress_meshcore_dependency_logging():
            if getattr(res, "type", None) != EventType.OK:
                raise RuntimeError("companion did not accept channel update")
            try:
                readback = await self._read_channel_at(index)
            except _MeshCoreChannelLinkError:
                raise _MeshCoreChannelLinkError(retry_safe=False) from None
            if (readback["index"] != index or readback["name"] != name
                    or readback["secret"] != secret):
                raise RuntimeError("channel readback did not match requested update")
        return {"index": index, "name": name}

    async def clear_channel(self, index: int) -> dict:
        if self._mc is None:
            raise RuntimeError("not connected")
        from meshcore import EventType
        max_channels = self._max_channels_from_info(await self._device_info())
        if not isinstance(index, int) or isinstance(index, bool) or not 0 <= index < max_channels:
            raise ValueError("channel index is out of range")
        zero_secret = b"\x00" * 16
        try:
            with _suppress_meshcore_dependency_logging():
                res = await self._mc.commands.set_channel(index, "", zero_secret)
        except Exception as exc:
            if _is_meshcore_link_failure(exc):
                raise _MeshCoreChannelLinkError(retry_safe=False) from None
            raise
        with _suppress_meshcore_dependency_logging():
            if getattr(res, "type", None) != EventType.OK:
                raise RuntimeError("companion did not accept channel clear")
            try:
                readback = await self._read_channel_at(index)
            except _MeshCoreChannelLinkError:
                raise _MeshCoreChannelLinkError(retry_safe=False) from None
            if (readback["index"] != index or readback["name"] != ""
                    or readback["secret"] != zero_secret):
                raise RuntimeError("channel readback did not match requested clear")
        return {"index": index, "name": ""}

    async def connect(self) -> None:
        from meshcore import MeshCore  # lazy: optional dependency
        if self._mc is not None:
            await self.close()
        # default_timeout: cap how long we wait for the device's "OK" confirmation
        #   (channel broadcasts have no real ACK, so a long wait just stalls sends).
        # auto_reconnect: let the library recover a dropped USB/TCP link on its own.
        if self.conn == "tcp":
            h, _, p = self.host.partition(":")
            self._mc = await MeshCore.create_tcp(
                h, int(p or 4000), default_timeout=6.0, auto_reconnect=True)
        else:
            self._mc = await MeshCore.create_serial(
                self.port, self.baud, default_timeout=6.0, auto_reconnect=True)
        if self._mc is None:
            where = self.host if self.conn == "tcp" else self.port
            raise RuntimeError("no response from MeshCore node on %s" % (where or "(unset)"))

    async def send_text(self, text: str, channel: int) -> None:
        if self._mc is None:
            raise RuntimeError("not connected")
        from meshcore import EventType
        res = await self._mc.commands.send_chan_msg(channel, text)
        result_type = getattr(res, "type", None)
        if result_type != EventType.OK:
            payload = getattr(res, "payload", {}) or {}
            reason = payload.get("reason", "") if isinstance(payload, dict) else ""
            detail = "companion rejected channel message" if result_type == EventType.ERROR \
                else "no OK confirmation from companion"
            raise TxUnsent("unsent", detail + ((": " + reason) if reason else ""))
        logger.info("MeshCore channel message accepted by companion")

    async def close(self) -> None:
        if self._mc is not None:
            mc, self._mc = self._mc, None
            try:
                await mc.disconnect()
            except Exception:
                pass

    @property
    def connected(self) -> bool:
        if self._mc is None:
            return False
        if self.conn == "tcp":
            manager = getattr(self._mc, "connection_manager", None)
            if manager is not None:
                return bool(getattr(manager, "is_connected", False))
        return True

    async def read_channel(self, index: int) -> dict:
        """Inspect one slot while keeping protocol secrets private."""
        max_channels = self._max_channels_from_info(await self._device_info())
        if not isinstance(index, int) or isinstance(index, bool) or not 0 <= index < max_channels:
            raise ValueError("channel index is out of range")
        channel = await self._read_channel_at(index)
        return {"index": channel["index"], "name": channel["name"]}

    async def read_channels(self) -> list:
        if self._mc is None:
            raise RuntimeError("not connected")
        info = await self._device_info()
        out = []
        for idx in range(self._max_channels_from_info(info)):
            channel = await self._read_channel_at(idx)
            if channel["name"]:
                out.append({"index": channel["index"], "name": channel["name"]})
        return out

    async def read_info(self) -> dict:
        if self._mc is None:
            return {}
        payload = await self._device_info()
        return {"model": (payload.get("model") or "").strip(),
                "firmware": (payload.get("ver") or "").strip(),
                "max_channels": self._max_channels_from_info(payload)}


def _fmt_model(info: dict) -> str:
    """Human label for a radio from its info dict, e.g. 'Heltec V3 (fw 2.5.9)'."""
    model = (info.get("model") or "").replace("_", " ").strip()
    fw = (info.get("firmware") or "").strip()
    if model and fw:
        return "%s (fw %s)" % (model, fw)
    return model or (("fw %s" % fw) if fw else "")


@dataclass
class Transport:
    name: str                     # "meshtastic" | "meshcore"
    label: str
    enabled: bool
    conn: str                     # "serial" | "tcp"
    channel: int
    target: str                   # serial path or host - display + "configured?" check
    make: object                  # callable() -> Transmitter
    repeat: int = 1               # send each alert this many times (LoRa has no ACK)
    test_channel: int = 1         # channel used for Troubleshoot tests only
    tx: Transmitter | None = None
    connected: bool = False
    error: str = ""


@dataclass
class QueueItem:
    text: str
    on_test: bool = False      # False = live channel (weather), True = test channel (IPAWS)
    log_tx: bool = True        # write the weather transmit_log (False for IPAWS -- logged separately)
    on_result: object = None   # optional callable(ok: bool, err: str) invoked after the send
    transport: str | None = None
    channel: int | None = None
    destination_id: int | None = None
    correlation_key: object = None
    require_prior_success: bool = False


def _build_transports(db) -> dict:
    def g(k, d=None):
        return db.get_setting(k, d)

    def num(k, d=0):
        try:
            return int(g(k, d) or d)
        except (TypeError, ValueError):
            return d

    def rep(k):
        return max(1, min(5, num(k, 2)))

    mt_conn = g("meshtastic_conn", "serial") or "serial"
    mt = Transport(
        name="meshtastic", label="Meshtastic",
        enabled=bool(g("meshtastic_enabled", True)),
        conn=mt_conn, channel=num("channel_index", 0),
        target=(g("meshtastic_host", "") if mt_conn == "tcp" else g("serial_port", "")) or "",
        make=lambda: MeshtasticTransmitter(mt_conn, g("serial_port", "") or "", g("meshtastic_host", "") or ""),
        repeat=rep("meshtastic_repeat"), test_channel=num("meshtastic_test_channel", 1),
    )
    mc_conn = g("meshcore_conn", "serial") or "serial"
    mc = Transport(
        name="meshcore", label="MeshCore",
        enabled=bool(g("meshcore_enabled", False)),
        conn=mc_conn, channel=num("meshcore_channel", 0),
        target=(g("meshcore_host", "") if mc_conn == "tcp" else g("meshcore_port", "")) or "",
        make=lambda: MeshCoreTransmitter(mc_conn, g("meshcore_port", "") or "", g("meshcore_host", "") or ""),
        repeat=rep("meshcore_repeat"), test_channel=num("meshcore_test_channel", 1),
    )
    return {"meshtastic": mt, "meshcore": mc}


class TransmitManager:
    """Serializes node access, paces bursts, fans out to all enabled transports."""

    def __init__(self, db):
        self._db = db
        self._transports = _build_transports(db)
        self._queue: deque[QueueItem] = deque(maxlen=QUEUE_MAX)        # high: weather/live
        self._queue_low: deque[QueueItem] = deque(maxlen=QUEUE_MAX)    # low: IPAWS/test
        self._queue_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._worker_task: asyncio.Task | None = None
        self._reconnect_delay = 2.0
        self._stopped = False
        self._chain_results: dict[object, bool] = {}
        self._meshcore_channel_reconnect_depth = 0

    # ---- lifecycle ------------------------------------------------------
    def start(self) -> None:
        self._stopped = False
        self._worker_task = asyncio.create_task(self._worker(), name="tx-worker")

    async def stop(self) -> None:
        self._stopped = True
        self._queue_event.set()
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        for t in self._transports.values():
            if t.tx:
                await t.tx.close()

    async def reconfigure(self) -> None:
        """Rebuild transports from settings and connect the enabled ones.
        Called at startup and after a settings save."""
        async with self._lock:
            for t in self._transports.values():
                if t.tx:
                    try:
                        await t.tx.close()
                    except Exception:
                        pass
            self._transports = _build_transports(self._db)
            targets = [t for t in self._transports.values() if t.enabled and t.target]
        for t in targets:
            await self._ensure(t)

    # ---- status / compat ------------------------------------------------
    @property
    def connected(self) -> bool:
        return any(t.connected for t in self._transports.values() if t.enabled)

    @property
    def port(self) -> str | None:
        return self._transports["meshtastic"].target or None

    @property
    def queue_depth(self) -> int:
        return len(self._queue)

    @property
    def last_error(self) -> str:
        for t in self._transports.values():
            if t.enabled and t.error:
                return "%s: %s" % (t.label, t.error)
        return ""

    def status(self) -> list[dict]:
        return [
            {"name": t.name, "label": t.label, "enabled": t.enabled,
             "conn": t.conn, "connected": t.connected, "target": t.target,
             "channel": t.channel, "error": t.error}
            for t in self._transports.values()
        ]

    async def set_port(self, port: str) -> None:
        """Back-compat: set the Meshtastic serial port, then reconnect."""
        self._db.set_setting("serial_port", port or "")
        await self.reconfigure()

    async def _refresh_meshcore_channel_cache(self, tx) -> dict:
        channels = await tx.read_channels()
        info = await tx.read_info()
        model = _fmt_model(info)
        max_channels = info.get("max_channels", MeshCoreTransmitter.FALLBACK_MAX_CHANNELS)
        self._db.set_setting("meshcore_channels", channels)
        self._db.set_setting("meshcore_model", model)
        self._db.set_setting("meshcore_max_channels", max_channels)
        return {"channels": channels, "model": model, "max_channels": max_channels}

    async def _ensure_meshcore_channel_transport(self, transport: Transport) -> bool:
        """Reconnect a configured TCP link whose backend reports it stale."""
        if (transport.conn == "tcp" and transport.tx is not None
                and not transport.tx.connected):
            transport.connected = False
            return await self._reconnect_meshcore_channel(transport)
        return await self._ensure(transport)

    async def _reconnect_meshcore_channel(self, transport: Transport) -> bool:
        """Reconnect without publishing errors from a secret-bearing operation."""
        self._meshcore_channel_reconnect_depth += 1
        try:
            try:
                reconnected = await self._reconnect(transport)
            except Exception:
                reconnected = False
        finally:
            self._meshcore_channel_reconnect_depth -= 1
        if not reconnected:
            transport.error = "MeshCore channel connection failed"
        return reconnected

    async def set_meshcore_channel(self, index: int, name: str) -> dict:
        """Set one deterministic hash channel under the shared radio lock."""
        async with self._lock:
            transport = self._transports["meshcore"]
            if not transport.enabled:
                return {"ok": False, "error": "MeshCore is disabled"}
            if not await self._ensure_meshcore_channel_transport(transport) or transport.tx is None:
                return {"ok": False, "error": "MeshCore channel update failed"}
            for attempt in range(2):
                try:
                    channel = await transport.tx.set_channel(index, name)
                    break
                except Exception as exc:
                    retry_safe = (isinstance(exc, _MeshCoreChannelLinkError)
                                  and exc.retry_safe)
                    link_failed = (isinstance(exc, _MeshCoreChannelLinkError)
                                   or _is_meshcore_link_failure(exc))
                    if link_failed and transport.conn == "tcp":
                        transport.connected = False
                        reconnected = await self._reconnect_meshcore_channel(transport)
                        if (attempt == 0 and retry_safe and reconnected
                                and transport.tx is not None):
                            continue
                    return {"ok": False, "error": "MeshCore channel update failed"}
            else:
                return {"ok": False, "error": "MeshCore channel update failed"}
            try:
                snapshot = await self._refresh_meshcore_channel_cache(transport.tx)
            except Exception:
                return {
                    "ok": True,
                    "error": "",
                    "channel": channel,
                    "cache_refreshed": False,
                    "warning": "Channel updated, but channel cache refresh failed",
                }
            return {"ok": True, "error": "", "channel": channel, **snapshot}

    async def inspect_meshcore_channel(self, index: int) -> dict:
        """Inspect a configured slot while keeping its wire secret internal."""
        async with self._lock:
            transport = self._transports["meshcore"]
            if not transport.enabled:
                return {"ok": False, "error": "MeshCore is disabled"}
            if not await self._ensure_meshcore_channel_transport(transport) or transport.tx is None:
                return {"ok": False, "error": "MeshCore channel inspection failed"}
            for attempt in range(2):
                try:
                    channel = await transport.tx.read_channel(index)
                    snapshot = await self._refresh_meshcore_channel_cache(transport.tx)
                    return {"ok": True, "error": "", "channel": channel, **snapshot}
                except Exception as exc:
                    link_failed = (isinstance(exc, _MeshCoreChannelLinkError)
                                   or _is_meshcore_link_failure(exc))
                    if link_failed and transport.conn == "tcp":
                        transport.connected = False
                        if (attempt == 0
                                and await self._reconnect_meshcore_channel(transport)
                                and transport.tx is not None):
                            continue
                    return {"ok": False, "error": "MeshCore channel inspection failed"}
            return {"ok": False, "error": "MeshCore channel inspection failed"}

    async def clear_meshcore_channel(self, index: int) -> dict:
        """Clear one slot on the configured MeshCore companion under the radio lock."""
        async with self._lock:
            transport = self._transports["meshcore"]
            if not transport.enabled:
                return {"ok": False, "error": "MeshCore is disabled"}
            if not await self._ensure_meshcore_channel_transport(transport) or transport.tx is None:
                return {"ok": False, "error": "MeshCore channel clear failed"}
            for attempt in range(2):
                try:
                    channel = await transport.tx.clear_channel(index)
                    break
                except Exception as exc:
                    retry_safe = (isinstance(exc, _MeshCoreChannelLinkError)
                                  and exc.retry_safe)
                    link_failed = (isinstance(exc, _MeshCoreChannelLinkError)
                                   or _is_meshcore_link_failure(exc))
                    if link_failed and transport.conn == "tcp":
                        transport.connected = False
                        reconnected = await self._reconnect_meshcore_channel(transport)
                        if (attempt == 0 and retry_safe and reconnected
                                and transport.tx is not None):
                            continue
                    return {"ok": False, "error": "MeshCore channel clear failed"}
            else:
                return {"ok": False, "error": "MeshCore channel clear failed"}
            try:
                snapshot = await self._refresh_meshcore_channel_cache(transport.tx)
            except Exception:
                return {
                    "ok": True,
                    "error": "",
                    "channel": channel,
                    "cache_refreshed": False,
                    "warning": "Channel cleared, but channel cache refresh failed",
                }
            return {"ok": True, "error": "", "channel": channel, **snapshot}

    # ---- connection -----------------------------------------------------
    async def _ensure(self, t: Transport) -> bool:
        if t.connected and t.tx is not None:
            return True
        if not t.enabled:
            return False
        if not t.target:
            t.error = "no connection configured"
            return False
        err = await self._open_once(t)
        if not err:
            self._reconnect_delay = 2.0
            self._db.add_event("INFO", "%s connected (%s)" % (t.label, t.target))
            logger.info("%s connected at %s", t.name, t.target)
            return True
        t.error = err
        self._db.add_error(t.name, err)
        logger.warning("%s connect failed: %s", t.name, err)
        return False

    async def _open_once(self, t: Transport) -> str:
        """Open t's link once. Returns '' on success or an error string. No DB
        logging so callers (startup vs. reconnect) can decide how to report."""
        try:
            tx = t.make()
            await tx.connect()
            t.tx, t.connected, t.error = tx, True, ""
            return ""
        except Exception as exc:
            t.tx, t.connected = None, False
            return "connect failed: %s" % exc

    @staticmethod
    def _is_port_locked(err: str) -> bool:
        e = err.lower()
        return ("lock" in e or "busy" in e or "errno 11" in e
                or "errno 16" in e or "resource temporarily unavailable" in e)

    # ---- sending --------------------------------------------------------
    def enqueue(self, text: str, channel: int | None = None, on_result=None) -> bool:
        """Queue a weather alert (HIGH priority, live channel). on_result(ok, err)
        fires after the send with the transport-reported outcome."""
        return self._enqueue(self._queue, text, on_test=False, log_tx=True,
                             on_result=on_result)

    def enqueue_destination(self, text: str, transport: str, channel: int,
                            destination_id: int, on_result=None) -> bool:
        if transport not in self._transports:
            if on_result:
                self._safe_result(on_result, False, "unknown transport")
            return False
        return self._enqueue(self._queue, text, on_test=False, log_tx=True,
                             on_result=on_result, transport=transport,
                             channel=channel, destination_id=destination_id)

    def enqueue_destination_chain(self, text: str, transport: str, channel: int,
                                  destination_id: int, correlation_key,
                                  require_prior_success: bool = False,
                                  on_result=None) -> bool:
        if transport not in self._transports:
            if on_result:
                self._safe_result(on_result, False, "unknown transport")
            return False
        return self._enqueue(
            self._queue, text, on_test=False, log_tx=True, on_result=on_result,
            transport=transport, channel=channel, destination_id=destination_id,
            correlation_key=correlation_key,
            require_prior_success=require_prior_success,
        )

    def cancel_queued_correlation(self, correlation_key) -> int:
        """Remove unsent items in a delivery chain; an in-flight item is untouched."""
        removed = []
        for lane in (self._queue, self._queue_low):
            kept = [item for item in lane if item.correlation_key != correlation_key]
            removed.extend(item for item in lane if item.correlation_key == correlation_key)
            lane.clear()
            lane.extend(kept)
        for item in removed:
            if item.on_result is not None:
                self._safe_result(item.on_result, False, "superseded")
        return len(removed)

    def enqueue_ipaws(self, text: str, on_test: bool = False, on_result=None) -> bool:
        """Queue an IPAWS alert (LOW priority: weather always sends first). Real
        alerts go on the live channel (on_test=False); test/exercise messages go
        on the test channel (on_test=True). Not written to the weather transmit_log."""
        return self._enqueue(self._queue_low, text, on_test=on_test, log_tx=False,
                             on_result=on_result)

    def _enqueue(self, lane, text, on_test, log_tx, on_result, transport=None,
                 channel=None, destination_id=None, correlation_key=None,
                 require_prior_success=False) -> bool:
        dropped = len(lane) == lane.maxlen
        if dropped:
            # The item we are about to drop never gets a send: report it failed so
            # the caller does not record it as delivered.
            oldest = lane[0] if lane else None
            if oldest is not None and oldest.on_result is not None:
                self._safe_result(oldest.on_result, False, "dropped (queue full)")
        lane.append(QueueItem(text=_cap_text(text), on_test=on_test, log_tx=log_tx,
                              on_result=on_result, transport=transport, channel=channel,
                              destination_id=destination_id,
                              correlation_key=correlation_key,
                              require_prior_success=require_prior_success))
        self._queue_event.set()
        if dropped:
            logger.warning("transmit queue full; dropped oldest")
            self._db.add_event("WARN", "transmit queue full; dropped oldest message")
        return not dropped

    def _safe_result(self, cb, ok: bool, err: str = "") -> None:
        try:
            r = cb(ok, err)
            if asyncio.iscoroutine(r):
                asyncio.create_task(r)
        except Exception:
            logger.exception("on_result callback error")

    async def _reconnect(self, t: Transport) -> bool:
        """Reset a transport's link, tolerating a zombie / locked serial port.
        A just-closed port often has not released its fd yet, so an instant reopen
        fails with '[Errno 11] could not exclusively lock port', and close() itself
        can hang on a wedged reader thread. So: close firmly with a timeout, then
        reopen with backoff, waiting out a still-locked port instead of giving up."""
        t.connected = False
        sanitize_errors = self._meshcore_channel_reconnect_depth > 0
        if t.tx is not None:
            try:
                await asyncio.wait_for(t.tx.close(), timeout=8)  # don't hang on a stuck close
            except Exception:
                pass
        t.tx = None
        last = "not reopened"
        for i, delay in enumerate((0.5, 1.5, 3.0, 5.0)):
            await asyncio.sleep(delay)          # give the OS time to release the port
            err = await self._open_once(t)
            if not err:
                self._db.add_event(
                    "INFO", "%s link reset%s (%s)" % (
                        t.label, (" after %d tries" % (i + 1)) if i else "", t.target))
                logger.info("%s reconnected at %s", t.name, t.target)
                return True
            last = err
            if self._is_port_locked(err):
                if sanitize_errors:
                    logger.warning(
                        "%s port still locked during channel reconnect (try %d/4)",
                        t.name, i + 1)
                else:
                    logger.warning("%s port still locked (try %d/4), waiting: %s",
                                   t.name, i + 1, err)
            elif sanitize_errors:
                logger.warning("%s channel reconnect failed (try %d/4)",
                               t.name, i + 1)
            else:
                logger.warning("%s reopen failed (try %d/4): %s", t.name, i + 1, err)
        if sanitize_errors:
            t.error = "MeshCore channel connection failed"
            self._db.add_error(t.name, t.error)
        else:
            t.error = last
            self._db.add_error(t.name, "link reset failed: %s" % last)
        return False

    async def _try_send(self, t: Transport, text: str, ch: int) -> tuple[bool, str]:
        """Send and require the backend's success confirmation. On failure, read WHY
        (TxUnsent.category) and apply the matching correction before retrying:
        wait out an airtime/queue limit, reconnect a dead link, or stop early on a
        content/config error that a retry cannot fix. Returns (ok, error)."""
        last = "not connected"
        for attempt in (1, 2, 3):
            if t.tx is None or not t.connected:
                if not await self._ensure(t):
                    last = t.error or "not connected"
                    await asyncio.sleep(1)
                    continue
            try:
                await t.tx.send_text(text, ch)
                if attempt > 1:
                    self._db.add_event(
                        "INFO", "%s sent on attempt %d (%s)" % (t.label, attempt, last))
                return True, ""
            except TxUnsent as exc:
                last = exc.detail
                t.error = last
                cat = exc.category
                logger.warning("%s not sent (attempt %d/3): %s [%s]",
                               t.name, attempt, exc.detail, cat)
                if cat in ("too_large", "no_channel"):
                    break  # a retry cannot fix bad content/config; fail fast with the reason
                if cat == "duty_cycle":
                    await asyncio.sleep(6)          # airtime cap: let the radio cool down
                elif cat == "queue_full":
                    await asyncio.sleep(3)          # let the TX queue drain
                elif cat == "link":
                    await self._reconnect(t)        # interface down / no reply: reopen it
                else:                               # "unsent": brief wait, then reconnect
                    await asyncio.sleep(2)
                    if attempt >= 2:
                        await self._reconnect(t)
            except Exception as exc:
                # Unexpected link error (Broken pipe, serial hiccup): reconnect + retry.
                last = "send failed: %s" % exc
                t.error = last
                logger.warning("%s send error (attempt %d/3): %s", t.name, attempt, exc)
                await self._reconnect(t)
        self._db.add_error(t.name, last)   # a real failure only after all corrections tried
        return False, last

    async def _send_all(self, text: str, manual: bool, on_test: bool | None = None) -> bool:
        any_ok = False
        # Validate before keying any radio: trim to the payload cap (multibyte-safe)
        # so a too-long message never gets silently rejected by the firmware.
        while len(text.encode()) > MAX_PAYLOAD_BYTES:
            text = text[:-1]
        blen = len(text.encode())
        # `on_test` picks the channel (test vs live); `manual` only tags the log
        # (auto vs manual). Automated alerts AND composed manual sends both go on
        # each radio's LIVE channel; only the Troubleshoot test uses the test channel.
        use_test = manual if on_test is None else on_test
        async with self._lock:
            # Each enabled radio gets one confirmed backend operation. Meshtastic
            # confirms queue acceptance; MeshCore only confirms that its companion
            # accepted the command (not that any receiver heard it).
            for t in self._transports.values():
                if not t.enabled:
                    continue
                ch = t.test_channel if use_test else t.channel
                ok, err = await self._try_send(t, text, ch)
                self._db.add_transmit_log(ch, blen, ok, text, manual,
                                          error=("" if ok else err), transport=t.name)
                if ok:
                    any_ok = True
                    if t.name == "meshcore":
                        logger.info("accepted by MeshCore companion on ch %d", ch)
                    else:
                        logger.info("transmitted via %s on ch %d", t.name, ch)
        return any_ok

    async def send_manual(self, text: str) -> bool:
        # A composed manual broadcast is a real message for people, so it goes on
        # each radio's LIVE channel (logged as a manual action).
        return await self._send_all(text, manual=True, on_test=False)

    async def send_test(self, text: str) -> bool:
        # The Troubleshoot canned test goes on each radio's TEST channel.
        return await self._send_all(text, manual=True, on_test=True)

    async def load_channels(self, name: str, conn: str, port: str, host: str):
        """Open a transient connection with the given params; read the device's
        channels and model. Returns (channels|None, model_str, error). Frees the
        live port first so it never double-opens, then restores the live link."""
        async with self._lock:
            t = self._transports.get(name)
            if t is None:
                return None, "", "unknown radio"
            if t.tx is not None:            # release the live connection first
                try:
                    await t.tx.close()
                except Exception:
                    pass
                t.tx, t.connected = None, False
            maker = MeshtasticTransmitter if name == "meshtastic" else MeshCoreTransmitter
            tx = maker(conn or "serial", port or "", host or "")
            try:
                await tx.connect()
                channels = await tx.read_channels()
                model = ""
                try:
                    model = _fmt_model(await tx.read_info())
                except Exception:
                    pass
                result = (channels, model, "")
            except Exception as exc:
                result = (None, "", str(exc))
            finally:
                try:
                    await tx.close()
                except Exception:
                    pass
            # best-effort: bring the live link back so "Connect" doesn't leave it offline
            try:
                await self._ensure(t)
            except Exception:
                pass
            return result

    async def send_to(self, name: str, text: str) -> tuple[bool, str]:
        """Key up a single named radio (bench testing). Returns (ok, error)."""
        blen = len(text.encode())
        async with self._lock:
            t = self._transports.get(name)
            if t is None:
                return False, "unknown radio"
            if not t.enabled:
                return False, "%s is disabled" % t.label
            ch = t.test_channel   # tests go on this radio's test channel
            ok, err = await self._try_send(t, text, ch)
            self._db.add_transmit_log(ch, blen, ok, text, True,
                                      error=("" if ok else err), transport=t.name)
            if ok:
                if t.name == "meshcore":
                    logger.info("test accepted by MeshCore companion on ch %d", ch)
                else:
                    logger.info("test transmitted via %s on ch %d", t.name, ch)
            return ok, ("" if ok else err)

    async def resend(self, name: str, text: str, channel: int) -> tuple[bool, str]:
        """Re-transmit an exact message on a specific radio and channel, logging a
        fresh entry. Backs the transmit-log Resend button."""
        blen = len(text.encode())
        async with self._lock:
            t = self._transports.get(name)
            if t is None:
                return False, "unknown radio"
            if not t.enabled:
                return False, "%s is disabled" % t.label
            ok, err = await self._try_send(t, text, channel)
            self._db.add_transmit_log(channel, blen, ok, text, True,
                                      error=("" if ok else err), transport=t.name)
            if ok:
                if t.name == "meshcore":
                    logger.info("resend accepted by MeshCore companion on ch %d", channel)
                else:
                    logger.info("resent via %s on ch %d", t.name, channel)
            return ok, ("" if ok else err)

    async def _transmit_item(self, item: QueueItem) -> tuple[bool, str]:
        """Send one queued item on the right channel (live vs test), confirmed per
        radio. Weather items also write the transmit_log; IPAWS items do not."""
        blen = len(item.text.encode())
        any_ok, last = False, ""
        async with self._lock:
            transports = ([self._transports[item.transport]] if item.transport else
                          list(self._transports.values()))
            for t in transports:
                if not t.enabled:
                    continue
                ch = (item.channel if item.channel is not None else
                      (t.test_channel if item.on_test else t.channel))
                ok, err = await self._try_send(t, item.text, ch)
                if item.log_tx:
                    self._db.add_transmit_log(ch, blen, ok, item.text, False,
                                              error=("" if ok else err), transport=t.name,
                                              destination_id=item.destination_id)
                any_ok = any_ok or ok
                if not ok:
                    last = err
        return any_ok, ("" if any_ok else last)

    async def _worker(self) -> None:
        first = True
        while not self._stopped:
            if not self._queue and not self._queue_low:
                self._queue_event.clear()
                try:
                    await self._queue_event.wait()
                except asyncio.CancelledError:
                    return
                first = True
                continue
            if not first:
                try:
                    await asyncio.sleep(BURST_GAP_SECONDS)   # pace EVERY send, both lanes
                except asyncio.CancelledError:
                    return
            # Weather (high) always drains before IPAWS (low), so a real warning
            # never waits behind a backlog of secondary alerts.
            item = self._queue.popleft() if self._queue else self._queue_low.popleft()
            if (item.require_prior_success and
                    self._chain_results.get(item.correlation_key) is not True):
                if item.on_result is not None:
                    self._safe_result(
                        item.on_result, False, "predecessor was not accepted"
                    )
                first = False
                continue
            try:
                ok, err = await self._transmit_item(item)
                if item.correlation_key is not None:
                    self._chain_results[item.correlation_key] = ok
                if item.on_result is not None:
                    self._safe_result(item.on_result, ok, err)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # A failure here (e.g. a DB write erroring on a full disk) must not
                # kill the worker -- that would silently stop ALL future broadcasts.
                logger.exception("transmit worker iteration error")
                ok = False
                err = str(exc)
                if item.correlation_key is not None:
                    self._chain_results[item.correlation_key] = False
                if item.on_result is not None:
                    self._safe_result(item.on_result, False, err)
            first = False
            if not ok:
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, 60.0)
