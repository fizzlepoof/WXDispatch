from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.noaa_same import SameEndMessage, SameObservation
from app.noaa_sdr import NoaaSdrConfig, NoaaSdrHealth, NoaaSdrSupervisor, build_multimon_argv, build_rtl_fm_argv


def test_config_defaults_and_exact_safe_commands() -> None:
    config = NoaaSdrConfig(enabled=True, device_serial="000123")
    assert config.frequency_hz == 162_500_000
    assert config.sample_rate == 22_050
    assert build_rtl_fm_argv(config, executable="/usr/bin/rtl_fm") == [
        "/usr/bin/rtl_fm", "-d", "000123", "-f", "162500000", "-M", "fm",
        "-s", "22050", "-A", "fast", "-F", "9", "-E", "deemp", "-E", "dc",
        "-p", "0", "-",
    ]
    assert build_multimon_argv(executable="/usr/bin/multimon-ng") == [
        "/usr/bin/multimon-ng", "-q", "-c", "-a", "EAS", "-t", "raw", "-"
    ]
    assert "-T" not in build_rtl_fm_argv(config)
    assert "-l" not in build_rtl_fm_argv(config)
    assert build_rtl_fm_argv(config).count("-f") == 1


@pytest.mark.parametrize("frequency", [162_400_000, 162_425_000, 162_450_000, 162_475_000, 162_500_000, 162_525_000, 162_550_000])
def test_all_seven_noaa_frequencies_are_allowed(frequency: int) -> None:
    assert NoaaSdrConfig(enabled=True, device_serial="x", frequency_hz=frequency).frequency_hz == frequency


@pytest.mark.parametrize("changes", [
    {"device_serial": ""}, {"device_serial": "x" * 65}, {"receiver_id": "bad space"},
    {"callsign": "bad!"}, {"frequency_hz": 162_500_001}, {"sample_rate": 44_100},
    {"ppm": 201}, {"ppm": -201}, {"gain": -0.1}, {"gain": 50},
    {"pcm_chunk_bytes": 3}, {"pcm_chunk_bytes": 200_000}, {"stall_timeout": 0},
    {"drain_timeout": 0}, {"max_line_bytes": 513}, {"max_malformed_lines": 0},
    {"backoff_initial": 0}, {"backoff_max": 61}, {"terminate_timeout": 0},
])
def test_config_rejects_unsafe_or_unbounded_values(changes: dict[str, object]) -> None:
    args: dict[str, object] = {"enabled": True, "device_serial": "x"}
    args.update(changes)
    with pytest.raises(ValueError):
        NoaaSdrConfig(**args)


def test_gain_is_optional_or_bounded_numeric() -> None:
    auto = NoaaSdrConfig(enabled=True, device_serial="x", gain="auto")
    manual = NoaaSdrConfig(enabled=True, device_serial="x", gain=28.0)
    assert "-g" not in build_rtl_fm_argv(auto)
    argv = build_rtl_fm_argv(manual)
    assert argv[argv.index("-g") + 1] == "28"


class Reader:
    def __init__(self, chunks: list[bytes] | None = None, lines: list[bytes] | None = None):
        self.chunks = list(chunks or [])
        self.lines = list(lines or [])
    async def read(self, _n: int = -1) -> bytes:
        return self.chunks.pop(0) if self.chunks else b""
    async def readline(self) -> bytes:
        return self.lines.pop(0) if self.lines else b""


class HangingReader(Reader):
    async def read(self, _n: int = -1) -> bytes:
        await asyncio.Future()
        return b""
    async def readline(self) -> bytes:
        await asyncio.Future()
        return b""


class Writer:
    def __init__(self, *, block: bool = False):
        self.data: list[bytes] = []
        self.closed = False
        self.block = block
    def write(self, data: bytes) -> None:
        self.data.append(data)
    async def drain(self) -> None:
        if self.block:
            await asyncio.Future()
    def close(self) -> None:
        self.closed = True
    async def wait_closed(self) -> None:
        return None


class Process:
    _pid = 5000
    def __init__(self, *, stdout: Reader, stderr: Reader | None = None, stdin: Writer | None = None, returncode: int = 0):
        Process._pid += 1
        self.pid = Process._pid
        self.stdout, self.stderr, self.stdin = stdout, stderr or Reader(), stdin
        self.returncode: int | None = None
        self.final_returncode = returncode
        self.terminated = False
        self.killed = False
        self._done = asyncio.Event()
    async def wait(self) -> int:
        await self._done.wait()
        self.returncode = self.final_returncode
        return self.returncode
    def terminate(self) -> None:
        self.terminated = True
        self._done.set()
    def kill(self) -> None:
        self.killed = True
        self._done.set()
    def finish(self) -> None:
        self._done.set()


class Factory:
    def __init__(self, runs: list[tuple[Process, Process]]):
        self.runs = list(runs)
        self.calls: list[tuple[list[str], dict[str, object]]] = []
    async def __call__(self, *argv: str, **kwargs: object) -> Process:
        self.calls.append((list(argv), kwargs))
        pair = self.runs[0]
        process = pair[len(self.calls) % 2 - 1]
        if len(self.calls) % 2 == 0:
            self.runs.pop(0)
        return process


def make_pair(*, pcm: list[bytes] | None = None, lines: list[bytes] | None = None, block: bool = False, rc: int = 0) -> tuple[Process, Process]:
    return (
        Process(stdout=Reader(chunks=pcm), returncode=rc),
        Process(stdout=Reader(lines=lines), stdin=Writer(block=block), returncode=rc),
    )


@pytest.mark.asyncio
async def test_disabled_does_no_lookup_spawn_or_thermal_read() -> None:
    calls: list[str] = []
    async def factory(*args: object, **kwargs: object) -> Process:
        raise AssertionError("spawned")
    supervisor = NoaaSdrSupervisor(
        NoaaSdrConfig(enabled=False), process_factory=factory,
        executable_lookup=lambda name: calls.append(name), thermal_provider=lambda: (_ for _ in ()).throw(AssertionError()),
    )
    await supervisor.run()
    assert calls == []
    assert supervisor.health.state == "disabled"


@pytest.mark.asyncio
async def test_enabled_requires_both_executables_before_spawn() -> None:
    supervisor = NoaaSdrSupervisor(
        NoaaSdrConfig(enabled=True, device_serial="x"),
        executable_lookup=lambda name: "/bin/rtl_fm" if name == "rtl_fm" else None,
    )
    with pytest.raises(RuntimeError, match="multimon-ng"):
        await supervisor.run(max_cycles=1)
    assert supervisor.health.state == "missing_tools"


@pytest.mark.asyncio
async def test_pumps_aligned_bounded_pcm_confirms_headers_and_eom() -> None:
    header = b"ZCZC-WXR-TOR-047125+0030-2491830-KOHX/NWS-\n"
    rtl, decoder = make_pair(pcm=[b"\x01\x00\x02", b"\x00\x03\x00", b""], lines=[header, header, b"NNNN\n", b""])
    factory = Factory([(rtl, decoder)])
    observed: list[SameObservation | SameEndMessage] = []
    supervisor = NoaaSdrSupervisor(
        NoaaSdrConfig(enabled=True, device_serial="x", pcm_chunk_bytes=4, stall_timeout=1),
        callback=observed.append, process_factory=factory, executable_lookup=lambda n: f"/usr/bin/{n}",
        wall_clock=lambda: datetime(2026, 9, 6, 18, 31, tzinfo=timezone.utc),
    )
    await supervisor.run(max_cycles=1)
    assert decoder.stdin is not None
    assert decoder.stdin.data == [b"\x01\x00", b"\x02\x00\x03\x00"]
    assert [type(item) for item in observed] == [SameObservation, SameEndMessage]
    assert all(call[1].get("start_new_session") is True for call in factory.calls)
    assert all(call[1].get("limit") == 513 for call in factory.calls)
    health = supervisor.health
    assert isinstance(health, NoaaSdrHealth)
    assert health.last_valid_header is not None and health.last_eom is not None
    assert 0 <= health.audio_zero_fraction <= 1
    assert 0 <= health.audio_peak <= 1
    assert "rssi" not in repr(health).lower()


@pytest.mark.asyncio
async def test_malformed_flood_restarts_with_capped_backoff() -> None:
    lines = [b"garbage\n"] * 4
    pairs = [make_pair(pcm=[b"\x00\x00"], lines=lines) for _ in range(3)]
    sleeps: list[float] = []
    async def sleep(delay: float) -> None:
        sleeps.append(delay)
    supervisor = NoaaSdrSupervisor(
        NoaaSdrConfig(enabled=True, device_serial="x", max_malformed_lines=3, backoff_initial=40, backoff_max=60),
        process_factory=Factory(pairs), executable_lookup=lambda n: n, sleep=sleep, jitter=lambda: 1.0,
    )
    await supervisor.run(max_cycles=3)
    assert supervisor.health.restarts == 3
    assert sleeps == [40, 60]
    assert supervisor.health.state == "backoff"


@pytest.mark.asyncio
async def test_backpressure_terminates_processes_and_cancellation_leaks_none() -> None:
    rtl, decoder = make_pair(pcm=[b"\x01\x00"], block=True)
    factory = Factory([(rtl, decoder)])
    supervisor = NoaaSdrSupervisor(
        NoaaSdrConfig(enabled=True, device_serial="x", drain_timeout=.01, terminate_timeout=.01),
        process_factory=factory, executable_lookup=lambda n: n,
    )
    task = asyncio.create_task(supervisor.run())
    await asyncio.sleep(.03)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert rtl.terminated or rtl.killed
    assert decoder.terminated or decoder.killed
    assert supervisor.health.state == "stopped"


@pytest.mark.asyncio
async def test_nonzero_child_exit_is_detected_and_reported() -> None:
    rtl = Process(stdout=HangingReader(), returncode=7)
    decoder = Process(stdout=HangingReader(), stdin=Writer())
    rtl.finish()
    supervisor = NoaaSdrSupervisor(
        NoaaSdrConfig(enabled=True, device_serial="x"),
        process_factory=Factory([(rtl, decoder)]), executable_lookup=lambda n: n,
    )
    await supervisor.run(max_cycles=1)
    assert supervisor.health.restarts == 1
    assert supervisor.health.last_error is not None
    assert "status 7" in supervisor.health.last_error


@pytest.mark.asyncio
async def test_pcm_or_decoder_stall_forces_restart() -> None:
    rtl = Process(stdout=HangingReader())
    decoder = Process(stdout=HangingReader(), stdin=Writer())
    supervisor = NoaaSdrSupervisor(
        NoaaSdrConfig(enabled=True, device_serial="x", stall_timeout=.01),
        process_factory=Factory([(rtl, decoder)]), executable_lookup=lambda n: n,
    )
    await supervisor.run(max_cycles=1)
    assert supervisor.health.restarts == 1
    assert supervisor.health.state == "backoff"


@pytest.mark.asyncio
async def test_thermal_start_hold_and_hot_stop_with_hysteresis() -> None:
    temperatures = iter([76.0, 74.0, 81.0, 74.0])
    pair = make_pair(pcm=[b"\x00\x00"])
    sleeps: list[float] = []
    supervisor = NoaaSdrSupervisor(
        NoaaSdrConfig(enabled=True, device_serial="x", thermal_poll_interval=.01),
        process_factory=Factory([pair]), executable_lookup=lambda n: n,
        thermal_provider=lambda: next(temperatures), sleep=lambda delay: _record_sleep(sleeps, delay),
    )
    await supervisor.run(max_cycles=1)
    assert sleeps and supervisor.health.temperature_c == 81.0
    assert pair[0].terminated or pair[0].killed


async def _record_sleep(values: list[float], value: float) -> None:
    values.append(value)


@pytest.mark.asyncio
async def test_hot_stop_latches_until_restart_temperature_not_start_threshold() -> None:
    temperatures = iter([74.0, 81.0, 74.5, 73.9, 73.0])
    reads: list[float] = []
    spawn_read_counts: list[int] = []
    sleeps: list[float] = []
    base_factory = Factory([
        make_pair(pcm=[b"\x00\x00"]),
        make_pair(pcm=[b"\x00\x00"]),
    ])

    def temperature() -> float:
        value = next(temperatures)
        reads.append(value)
        return value

    async def factory(*argv: str, **kwargs: object) -> Process:
        spawn_read_counts.append(len(reads))
        return await base_factory(*argv, **kwargs)

    supervisor = NoaaSdrSupervisor(
        NoaaSdrConfig(
            enabled=True, device_serial="x", thermal_poll_interval=.01,
            backoff_initial=1, backoff_max=1,
        ),
        process_factory=factory, executable_lookup=lambda n: n,
        thermal_provider=temperature,
        sleep=lambda delay: _record_sleep(sleeps, delay), jitter=lambda: 1,
    )

    await supervisor.run(max_cycles=2)

    assert .01 in sleeps
    assert len(base_factory.calls) == 4
    assert spawn_read_counts[2] >= 4
