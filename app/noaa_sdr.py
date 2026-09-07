"""Disabled-safe, receive-only NOAA Weather Radio SDR observer.

This module deliberately has no application, database, or transmit integration.  It
only supervises an explicitly selected RTL-SDR and reports confirmed SAME messages.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import math
import os
import random
import shutil
import signal
import struct
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Awaitable, Callable, Final

from app.noaa_same import SameEndMessage, SameObservation, SameRepeatConfirmer, parse_same

NOAA_FREQUENCIES_HZ: Final = frozenset(
    {162_400_000, 162_425_000, 162_450_000, 162_475_000, 162_500_000, 162_525_000, 162_550_000}
)
_SAFE_TOKEN = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._/-")


@dataclass(frozen=True, slots=True)
class NoaaSdrConfig:
    enabled: bool = False
    receiver_id: str = "noaa-wwh37"
    callsign: str = "WWH37"
    device_serial: str = ""
    frequency_hz: int = 162_500_000
    sample_rate: int = 22_050
    ppm: int = 0
    gain: str | float = "auto"
    pcm_chunk_bytes: int = 4096
    stall_timeout: float = 15.0
    drain_timeout: float = 2.0
    terminate_timeout: float = 3.0
    max_line_bytes: int = 512
    max_malformed_lines: int = 25
    stderr_limit_bytes: int = 16_384
    backoff_initial: float = 1.0
    backoff_max: float = 60.0
    thermal_start_hold_c: float = 75.0
    thermal_stop_c: float = 80.0
    thermal_restart_c: float = 74.0
    thermal_poll_interval: float = 5.0

    def __post_init__(self) -> None:
        for name, value, maximum, allow_empty in (
            ("receiver_id", self.receiver_id, 64, False),
            ("callsign", self.callsign, 16, False),
            ("device_serial", self.device_serial, 64, not self.enabled),
        ):
            if not isinstance(value, str) or len(value) > maximum or (not value and not allow_empty):
                raise ValueError(f"{name} must be a bounded non-empty token")
            if value and any(character not in _SAFE_TOKEN for character in value):
                raise ValueError(f"{name} contains unsafe characters")
        if isinstance(self.frequency_hz, bool) or self.frequency_hz not in NOAA_FREQUENCIES_HZ:
            raise ValueError("frequency_hz must be one NOAA Weather Radio channel")
        if self.sample_rate != 22_050:
            raise ValueError("sample_rate is fixed at 22050")
        if isinstance(self.ppm, bool) or not isinstance(self.ppm, int) or not -200 <= self.ppm <= 200:
            raise ValueError("ppm must be an integer from -200 through 200")
        if self.gain != "auto":
            if isinstance(self.gain, bool) or not isinstance(self.gain, (int, float)):
                raise ValueError("gain must be auto or numeric")
            if not math.isfinite(float(self.gain)) or not 0 <= float(self.gain) <= 49.6:
                raise ValueError("gain must be between 0 and 49.6 dB")
        if (
            isinstance(self.pcm_chunk_bytes, bool)
            or not isinstance(self.pcm_chunk_bytes, int)
            or not 2 <= self.pcm_chunk_bytes <= 65_536
            or self.pcm_chunk_bytes % 2
        ):
            raise ValueError("pcm_chunk_bytes must be an even bounded integer")
        if not isinstance(self.max_line_bytes, int) or isinstance(self.max_line_bytes, bool) or not 64 <= self.max_line_bytes <= 512:
            raise ValueError("max_line_bytes must be between 64 and 512")
        if not isinstance(self.max_malformed_lines, int) or isinstance(self.max_malformed_lines, bool) or not 1 <= self.max_malformed_lines <= 10_000:
            raise ValueError("max_malformed_lines must be bounded")
        if not isinstance(self.stderr_limit_bytes, int) or isinstance(self.stderr_limit_bytes, bool) or not 1024 <= self.stderr_limit_bytes <= 1_048_576:
            raise ValueError("stderr_limit_bytes must be bounded")
        for name in ("stall_timeout", "drain_timeout", "terminate_timeout", "thermal_poll_interval"):
            value = float(getattr(self, name))
            if not math.isfinite(value) or not 0 < value <= 300:
                raise ValueError(f"{name} must be positive and bounded")
        for name in ("backoff_initial", "backoff_max"):
            value = float(getattr(self, name))
            if not math.isfinite(value) or not 0 < value <= 60:
                raise ValueError(f"{name} must be positive and no more than 60")
        if self.backoff_initial > self.backoff_max:
            raise ValueError("backoff_initial cannot exceed backoff_max")
        thermal = (self.thermal_restart_c, self.thermal_start_hold_c, self.thermal_stop_c)
        if any(not math.isfinite(float(value)) for value in thermal) or not thermal[0] < thermal[1] < thermal[2]:
            raise ValueError("thermal thresholds must provide restart hysteresis")


def _number(value: int | float) -> str:
    return format(value, "g")


def build_rtl_fm_argv(config: NoaaSdrConfig, *, executable: str = "rtl_fm") -> list[str]:
    argv = [
        executable, "-d", config.device_serial, "-f", str(config.frequency_hz),
        "-M", "fm", "-s", "22050", "-A", "fast", "-F", "9",
        "-E", "deemp", "-E", "dc", "-p", str(config.ppm),
    ]
    if config.gain != "auto":
        argv.extend(("-g", _number(config.gain)))
    argv.append("-")
    return argv


def build_multimon_argv(*, executable: str = "multimon-ng") -> list[str]:
    return [executable, "-q", "-c", "-a", "EAS", "-t", "raw", "-"]


@dataclass(frozen=True, slots=True)
class NoaaSdrHealth:
    state: str = "new"
    restarts: int = 0
    last_pcm: float | None = None
    last_decoder: float | None = None
    last_valid_header: datetime | None = None
    last_rwt: datetime | None = None
    last_eom: datetime | None = None
    audio_rms: float = 0.0
    audio_peak: float = 0.0
    audio_dc: float = 0.0
    audio_zero_fraction: float = 1.0
    temperature_c: float | None = None
    last_error: str | None = None


def project_same_to_feature(
    message: SameObservation | SameEndMessage,
    config: NoaaSdrConfig,
) -> dict | None:
    """Project an actionable confirmed SAME header into the shared alert shape.

    The raw header is used only for an opaque stable ID and is never returned.
    """
    if (
        not isinstance(message, SameObservation)
        or not message.route_eligible
        or message.invalid_locations
    ):
        return None
    same_codes = list(dict.fromkeys(message.locations))
    ugc_codes = list(dict.fromkeys(message.county_ugcs))
    if not same_codes or not ugc_codes:
        return None
    digest = hashlib.sha256(message.raw_header.encode("ascii")).hexdigest()[:32]
    feature_id = f"urn:wxdispatch:noaa-same:{digest}"
    issued = message.issued_at.isoformat()
    expires = message.expires_at.isoformat()
    return {
        "id": feature_id,
        "type": "Feature",
        "geometry": None,
        "properties": {
            "id": feature_id,
            "event": message.event_name[:160],
            "headline": f"{config.callsign} {message.event_name}"[:256],
            "areaDesc": "; ".join(ugc_codes)[:512],
            "effective": issued,
            "onset": issued,
            "expires": expires,
            "ends": expires,
            "messageType": "Alert",
            "status": "Actual",
            "category": "Met",
            # SAME carries an event code and location/purge time, but not CAP's
            # severity/urgency/certainty fields. Do not invent them.
            "severity": "Unknown",
            "urgency": "Unknown",
            "certainty": "Unknown",
            "senderName": f"NOAA Weather Radio {config.callsign}"[:128],
            "geocode": {"SAME": same_codes, "UGC": ugc_codes},
            "parameters": {
                "NWSSource": ["NOAA Weather Radio SAME"],
                "SAMECallsign": [config.callsign],
                "SAMEReceiverID": [config.receiver_id],
                "SAMEFrequencyHz": [str(config.frequency_hz)],
                "SAMEOriginator": [message.originator],
                "SAMEEventCode": [message.event_code],
                "SAMESender": [message.sender],
            },
        },
    }


def raspberry_pi_temperature(
    path: Path = Path("/sys/class/thermal/thermal_zone0/temp"),
) -> float | None:
    """Read the Raspberry Pi CPU temperature without invoking a subprocess."""
    try:
        value = float(path.read_text(encoding="ascii").strip()) / 1000.0
    except (OSError, UnicodeError, ValueError):
        return None
    return value if math.isfinite(value) and -50 <= value <= 150 else None


class _CycleEnded(RuntimeError):
    pass


class _ThermalStop(_CycleEnded):
    pass


class NoaaSdrSupervisor:
    """Supervise a single fixed-frequency, receive-only decoder pipeline."""

    def __init__(
        self,
        config: NoaaSdrConfig,
        *,
        callback: Callable[[SameObservation | SameEndMessage], object] | None = None,
        process_factory: Callable[..., Awaitable[object]] = asyncio.create_subprocess_exec,
        executable_lookup: Callable[[str], str | None] = shutil.which,
        thermal_provider: Callable[[], float | None] | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        jitter: Callable[[], float] = random.random,
        wall_clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.config = config
        self.callback = callback
        self._spawn = process_factory
        self._lookup = executable_lookup
        self._temperature = thermal_provider
        self._sleep = sleep
        self._jitter = jitter
        self._wall_clock = wall_clock
        self._monotonic = monotonic
        self._health = NoaaSdrHealth()
        self._processes: list[object] = []
        self._confirmer = SameRepeatConfirmer(confirmation_window=5.0)
        self._thermal_latched = False

    @property
    def health(self) -> NoaaSdrHealth:
        return self._health

    def _update(self, **changes: object) -> None:
        self._health = replace(self._health, **changes)

    async def run(self, *, max_cycles: int | None = None) -> None:
        if not self.config.enabled:
            self._update(state="disabled")
            return
        rtl_path = self._lookup("rtl_fm")
        multimon_path = self._lookup("multimon-ng")
        missing = [name for name, path in (("rtl_fm", rtl_path), ("multimon-ng", multimon_path)) if not path]
        if missing:
            self._update(state="missing_tools", last_error=f"missing executable: {', '.join(missing)}")
            raise RuntimeError(self._health.last_error)
        assert rtl_path is not None and multimon_path is not None

        cycles = 0
        delay = self.config.backoff_initial
        try:
            while max_cycles is None or cycles < max_cycles:
                await self._wait_until_cool()
                cycles += 1
                try:
                    await self._run_cycle(rtl_path, multimon_path)
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    self._update(
                        state="backoff", restarts=self._health.restarts + 1,
                        last_error=f"{type(error).__name__}: {error}"[:512],
                    )
                if max_cycles is not None and cycles >= max_cycles:
                    break
                jitter_factor = 0.75 + 0.25 * min(1.0, max(0.0, float(self._jitter())))
                await self._sleep(min(self.config.backoff_max, delay) * jitter_factor)
                delay = min(self.config.backoff_max, delay * 2)
        finally:
            await self._stop_processes()
            if max_cycles is None or self._health.state not in {"backoff", "missing_tools"}:
                self._update(state="stopped")

    async def _wait_until_cool(self) -> None:
        if self._temperature is None:
            return
        while True:
            temperature = self._read_temperature()
            threshold = (
                self.config.thermal_restart_c
                if self._thermal_latched
                else self.config.thermal_start_hold_c
            )
            if temperature is None:
                # A missing reading must not clear an over-temperature latch.
                if self._thermal_latched:
                    self._update(state="thermal_hold")
                    await self._sleep(self.config.thermal_poll_interval)
                    continue
                return
            if temperature < threshold:
                self._thermal_latched = False
                return
            self._update(state="thermal_hold")
            await self._sleep(self.config.thermal_poll_interval)

    def _read_temperature(self) -> float | None:
        assert self._temperature is not None
        reading = self._temperature()
        if reading is None:
            self._update(temperature_c=None)
            return None
        value = float(reading)
        if not math.isfinite(value) or not -50 <= value <= 150:
            raise RuntimeError("invalid thermal reading")
        self._update(temperature_c=value)
        return value

    async def _run_cycle(self, rtl_path: str, multimon_path: str) -> None:
        common = {
            "stdin": asyncio.subprocess.PIPE,
            "stdout": asyncio.subprocess.PIPE,
            "stderr": asyncio.subprocess.PIPE,
            "start_new_session": True,
            "limit": self.config.max_line_bytes + 1,
        }
        rtl_args = build_rtl_fm_argv(self.config, executable=rtl_path)
        rtl = await self._spawn(*rtl_args, **{**common, "stdin": asyncio.subprocess.DEVNULL})
        self._processes.append(rtl)
        try:
            decoder = await self._spawn(*build_multimon_argv(executable=multimon_path), **common)
            self._processes.append(decoder)
        except BaseException:
            await self._stop_processes()
            raise
        self._update(state="running", last_error=None)

        tasks = [
            asyncio.create_task(self._pump_pcm(rtl, decoder)),
            asyncio.create_task(self._decode_lines(decoder)),
            asyncio.create_task(self._drain_stderr(rtl)),
            asyncio.create_task(self._drain_stderr(decoder)),
            asyncio.create_task(self._watch_exit(rtl, "rtl_fm")),
            asyncio.create_task(self._watch_exit(decoder, "multimon-ng")),
        ]
        if self._temperature is not None:
            tasks.append(asyncio.create_task(self._watch_temperature()))
        try:
            active = {tasks[0], tasks[1], tasks[4], tasks[5]}
            if len(tasks) == 7:
                active.add(tasks[6])
            done, _ = await asyncio.wait(active, return_when=asyncio.FIRST_COMPLETED)
            # Let an already-readable decoder line win the same scheduling turn.
            await asyncio.sleep(0)
            errors = [task.exception() for task in done if not task.cancelled() and task.exception()]
            if errors:
                raise errors[0]
            raise _CycleEnded("receiver or decoder reached EOF")
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self._stop_processes()

    async def _pump_pcm(self, rtl: object, decoder: object) -> None:
        source = getattr(rtl, "stdout", None)
        sink = getattr(decoder, "stdin", None)
        if source is None or sink is None:
            raise _CycleEnded("pipeline missing PCM stream")
        pending = b""
        while True:
            chunk = await asyncio.wait_for(source.read(self.config.pcm_chunk_bytes), self.config.stall_timeout)
            if not chunk:
                if pending:
                    raise _CycleEnded("unaligned PCM EOF")
                return
            pending += chunk
            aligned_length = len(pending) & ~1
            if not aligned_length:
                continue
            aligned, pending = pending[:aligned_length], pending[aligned_length:]
            self._record_audio(aligned)
            sink.write(aligned)
            try:
                await asyncio.wait_for(sink.drain(), self.config.drain_timeout)
            except asyncio.TimeoutError as error:
                raise _CycleEnded("decoder backpressure") from error
            self._update(last_pcm=self._monotonic())

    def _record_audio(self, pcm: bytes) -> None:
        count = len(pcm) // 2
        samples = struct.unpack(f"<{count}h", pcm)
        normalized = tuple(sample / 32768.0 for sample in samples)
        dc = sum(normalized) / count
        rms = math.sqrt(sum(sample * sample for sample in normalized) / count)
        peak = max(abs(sample) for sample in normalized)
        zeros = sum(sample == 0 for sample in samples) / count
        self._update(
            audio_rms=min(1.0, max(0.0, rms)), audio_peak=min(1.0, max(0.0, peak)),
            audio_dc=min(1.0, max(-1.0, dc)), audio_zero_fraction=zeros,
        )

    async def _decode_lines(self, decoder: object) -> None:
        source = getattr(decoder, "stdout", None)
        if source is None:
            raise _CycleEnded("decoder stdout unavailable")
        malformed = 0
        while True:
            # multimon-ng emits nothing between SAME headers. Decoder silence is
            # normal; PCM stalling and child-process exit are monitored by the
            # sibling tasks in _run_cycle.
            line = await source.readline()
            if not line:
                return
            self._update(last_decoder=self._monotonic())
            if len(line) > self.config.max_line_bytes or not line.isascii():
                malformed += 1
            else:
                try:
                    parsed = parse_same(line.decode("ascii"), now=self._wall_clock())
                except (TypeError, ValueError):
                    malformed += 1
                else:
                    malformed = 0
                    confirmed = self._confirmer.process(parsed, monotonic_now=self._monotonic())
                    if confirmed is not None:
                        now = self._wall_clock()
                        if isinstance(confirmed, SameEndMessage):
                            self._update(last_eom=now)
                        else:
                            changes: dict[str, object] = {"last_valid_header": now}
                            if confirmed.event_code == "RWT":
                                changes["last_rwt"] = now
                            self._update(**changes)
                        if self.callback is not None:
                            result = self.callback(confirmed)
                            if inspect.isawaitable(result):
                                await result
            if malformed >= self.config.max_malformed_lines:
                raise _CycleEnded("malformed decoder flood")

    async def _drain_stderr(self, process: object) -> None:
        source = getattr(process, "stderr", None)
        if source is None:
            return
        retained = 0
        while True:
            chunk = await source.read(min(4096, self.config.stderr_limit_bytes))
            if not chunk:
                return
            # Always drain; retain/account for at most the configured amount per cycle.
            retained = min(self.config.stderr_limit_bytes, retained + len(chunk))
            if retained >= self.config.stderr_limit_bytes:
                await self._sleep(0.05)

    async def _watch_exit(self, process: object, name: str) -> None:
        returncode = await process.wait()
        raise _CycleEnded(f"{name} exited with status {returncode}")

    async def _watch_temperature(self) -> None:
        while True:
            temperature = self._read_temperature()
            if temperature is not None and temperature >= self.config.thermal_stop_c:
                self._thermal_latched = True
                raise _ThermalStop(f"temperature {temperature:.1f} C")
            await self._sleep(self.config.thermal_poll_interval)

    async def _stop_processes(self) -> None:
        processes, self._processes = self._processes, []
        for process in processes:
            if getattr(process, "returncode", None) is not None:
                continue
            self._signal_process(process, signal.SIGTERM)
        for process in processes:
            if getattr(process, "returncode", None) is not None:
                continue
            try:
                await asyncio.wait_for(process.wait(), self.config.terminate_timeout)
            except (asyncio.TimeoutError, ProcessLookupError):
                self._signal_process(process, signal.SIGKILL)
                try:
                    await asyncio.wait_for(process.wait(), self.config.terminate_timeout)
                except (asyncio.TimeoutError, ProcessLookupError):
                    pass
            stream = getattr(process, "stdin", None)
            if stream is not None:
                stream.close()
                try:
                    await stream.wait_closed()
                except (AttributeError, BrokenPipeError, ConnectionResetError):
                    pass

    @staticmethod
    def _signal_process(process: object, sig: signal.Signals) -> None:
        try:
            if isinstance(process, asyncio.subprocess.Process):
                os.killpg(process.pid, sig)
            elif sig == signal.SIGTERM:
                process.terminate()
            else:
                process.kill()
        except ProcessLookupError:
            pass
