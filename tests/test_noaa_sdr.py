from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from app.config import NoaaSdrAppConfig, load_noaa_sdr_config
from app.main import _noaa_sdr_handler, create_app
from app.noaa_same import SameEndMessage, SameObservation, parse_same
from app.noaa_sdr import (
    NoaaSdrConfig, NoaaSdrHealth, NoaaSdrSupervisor, build_multimon_argv,
    build_rtl_fm_argv, project_same_to_feature, raspberry_pi_temperature,
)


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


def test_sdr_env_config_is_disabled_and_shadowed_by_default(monkeypatch) -> None:
    for key in (
        "MESH_WX_NOAA_SDR_ENABLED", "MESH_WX_NOAA_SDR_DEVICE_SERIAL",
        "MESH_WX_NOAA_SDR_FREQUENCY_HZ", "MESH_WX_NOAA_SDR_CALLSIGN",
        "MESH_WX_NOAA_SDR_RECEIVER_ID", "MESH_WX_NOAA_SDR_SHADOW",
    ):
        monkeypatch.delenv(key, raising=False)

    configured = load_noaa_sdr_config()

    assert configured.receiver.enabled is False
    assert configured.shadow is True
    assert configured.error is None


def test_sdr_env_requires_explicit_serial_and_frequency_when_enabled(monkeypatch) -> None:
    monkeypatch.setenv("MESH_WX_NOAA_SDR_ENABLED", "true")
    monkeypatch.delenv("MESH_WX_NOAA_SDR_DEVICE_SERIAL", raising=False)
    monkeypatch.delenv("MESH_WX_NOAA_SDR_FREQUENCY_HZ", raising=False)

    configured = load_noaa_sdr_config()

    assert configured.receiver.enabled is False
    assert configured.shadow is True
    assert configured.error == "configuration"


def test_sdr_env_rejects_unsafe_direct_routing(monkeypatch) -> None:
    monkeypatch.setenv("MESH_WX_NOAA_SDR_ENABLED", "true")
    monkeypatch.setenv("MESH_WX_NOAA_SDR_DEVICE_SERIAL", "000123")
    monkeypatch.setenv("MESH_WX_NOAA_SDR_FREQUENCY_HZ", "162500000")
    monkeypatch.setenv("MESH_WX_NOAA_SDR_CALLSIGN", "WWH37")
    monkeypatch.setenv("MESH_WX_NOAA_SDR_RECEIVER_ID", "rocky-attic")
    monkeypatch.setenv("MESH_WX_NOAA_SDR_SHADOW", "false")

    configured = load_noaa_sdr_config()

    assert configured.receiver.enabled is False
    assert configured.shadow is True
    assert configured.error == "direct-routing-unsupported"


def test_sdr_env_loads_wwh37_shadow_configuration(monkeypatch) -> None:
    monkeypatch.setenv("MESH_WX_NOAA_SDR_ENABLED", "true")
    monkeypatch.setenv("MESH_WX_NOAA_SDR_DEVICE_SERIAL", "000123")
    monkeypatch.setenv("MESH_WX_NOAA_SDR_FREQUENCY_HZ", "162500000")
    monkeypatch.setenv("MESH_WX_NOAA_SDR_CALLSIGN", "WWH37")
    monkeypatch.setenv("MESH_WX_NOAA_SDR_RECEIVER_ID", "rocky-attic")
    monkeypatch.setenv("MESH_WX_NOAA_SDR_SHADOW", "true")

    configured = load_noaa_sdr_config()

    assert configured.receiver == NoaaSdrConfig(
        enabled=True, device_serial="000123", frequency_hz=162_500_000,
        callsign="WWH37", receiver_id="rocky-attic",
    )
    assert configured.shadow is True
    assert configured.error is None


def test_route_eligible_same_projects_to_bounded_stable_nws_feature() -> None:
    now = datetime(2026, 9, 6, 18, 31, tzinfo=timezone.utc)
    observation = parse_same(
        "ZCZC-WXR-TOR-047125+0030-2491830-KOHX/NWS-", now=now,
    )
    assert isinstance(observation, SameObservation)
    config = NoaaSdrConfig(
        enabled=True, device_serial="000123", frequency_hz=162_500_000,
        callsign="WWH37", receiver_id="rocky-attic",
    )

    first = project_same_to_feature(observation, config)
    second = project_same_to_feature(observation, config)

    assert first == second
    assert first is not None
    assert first["type"] == "Feature"
    assert first["geometry"] is None
    assert first["id"].startswith("urn:wxdispatch:noaa-same:")
    assert len(first["id"]) <= 80
    properties = first["properties"]
    assert properties["event"] == "Tornado Warning"
    assert properties["geocode"] == {"SAME": ["047125"], "UGC": ["TNC125"]}
    assert properties["effective"] == "2026-09-06T18:30:00+00:00"
    assert properties["expires"] == "2026-09-06T19:00:00+00:00"
    assert properties["parameters"]["SAMECallsign"] == ["WWH37"]
    assert properties["parameters"]["SAMEFrequencyHz"] == ["162500000"]
    assert properties["parameters"]["SAMESender"] == ["KOHX/NWS"]
    assert properties["severity"] == "Unknown"
    assert properties["urgency"] == "Unknown"
    assert properties["certainty"] == "Unknown"
    assert "raw_header" not in repr(first)
    assert observation.raw_header not in repr(first)


@pytest.mark.parametrize("header", [
    "ZCZC-WXR-RWT-047125+0030-2491830-KOHX/NWS-",
    "ZCZC-WXR-ADR-047125+0030-2491830-KOHX/NWS-",
    "ZCZC-WXR-TOR-099999+0030-2491830-KOHX/NWS-",
])
def test_test_administrative_and_invalid_same_do_not_project(header: str) -> None:
    observation = parse_same(
        header, now=datetime(2026, 9, 6, 18, 31, tzinfo=timezone.utc),
    )
    config = NoaaSdrConfig(enabled=True, device_serial="000123")

    assert project_same_to_feature(observation, config) is None


def test_eom_does_not_project() -> None:
    config = NoaaSdrConfig(enabled=True, device_serial="000123")
    assert project_same_to_feature(SameEndMessage(), config) is None


@pytest.mark.asyncio
async def test_sdr_handler_feeds_only_actionable_features_to_shared_poller() -> None:
    calls = []

    class Poller:
        async def ingest_feature(self, feature, *, source, shadow):
            calls.append((feature, source, shadow))
            return True

    config = NoaaSdrConfig(enabled=True, device_serial="000123")
    observation = parse_same(
        "ZCZC-WXR-TOR-047125+0030-2491830-KOHX/NWS-",
        now=datetime(2026, 9, 6, 18, 31, tzinfo=timezone.utc),
    )
    handler = _noaa_sdr_handler(Poller(), config=config, shadow=True)

    await handler(observation)
    await handler(SameEndMessage())

    assert len(calls) == 1
    assert calls[0][0]["properties"]["event"] == "Tornado Warning"
    assert calls[0][1:] == ("noaa_sdr", True)


def test_raspberry_pi_temperature_reads_bounded_sysfs_value(tmp_path: Path) -> None:
    thermal = tmp_path / "temp"
    thermal.write_text("70625\n")
    assert raspberry_pi_temperature(thermal) == 70.625
    thermal.write_text("not-temperature\n")
    assert raspberry_pi_temperature(thermal) is None


def test_app_lifespan_starts_cancels_and_awaits_sdr_task(monkeypatch, tmp_path: Path) -> None:
    import app.main as main_module

    instances = []

    class Supervisor:
        def __init__(self, config, *, callback, thermal_provider):
            self.config = config
            self.callback = callback
            self.thermal_provider = thermal_provider
            self.health = SimpleNamespace(state="new")
            self.started = False
            self.cancelled = False
            instances.append(self)

        async def run(self):
            self.started = True
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                self.cancelled = True
                raise

    receiver = NoaaSdrConfig(enabled=True, device_serial="000123")
    monkeypatch.setenv("MESH_WX_DB", str(tmp_path / "app.db"))
    monkeypatch.setattr(main_module, "NoaaSdrSupervisor", Supervisor)
    monkeypatch.setattr(
        main_module, "load_noaa_sdr_config",
        lambda: NoaaSdrAppConfig(receiver=receiver, shadow=True),
    )

    with TestClient(create_app()) as client:
        assert client.get("/healthz").status_code == 200
        assert instances[0].started is True
        assert instances[0].thermal_provider is main_module.raspberry_pi_temperature
        assert client.app.state.noaa_sdr_task.done() is False

    assert instances[0].cancelled is True
    assert client.app.state.noaa_sdr_task.done() is True


def test_sdr_task_failure_does_not_crash_web_app(monkeypatch, tmp_path: Path) -> None:
    import app.main as main_module

    class Supervisor:
        health = SimpleNamespace(state="backoff", last_error="receiver failed")

        def __init__(self, *args, **kwargs):
            pass

        async def run(self):
            raise RuntimeError("receiver failed")

    monkeypatch.setenv("MESH_WX_DB", str(tmp_path / "app.db"))
    monkeypatch.setattr(main_module, "NoaaSdrSupervisor", Supervisor)
    monkeypatch.setattr(
        main_module, "load_noaa_sdr_config",
        lambda: NoaaSdrAppConfig(
            receiver=NoaaSdrConfig(enabled=True, device_serial="000123"),
            shadow=True,
        ),
    )

    with TestClient(create_app()) as client:
        assert client.get("/healthz").text == "ok"
        assert client.app.state.noaa_sdr_task.done() is True


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
async def test_accepts_single_multimon_consensus_header_and_eom() -> None:
    # multimon-ng's EAS demodulator already requires two matching RF headers
    # before emitting one consensus line; do not require a second output line.
    header = b"ZCZC-WXR-TOR-047125+0030-2491830-KOHX/NWS-\n"
    rtl, decoder = make_pair(pcm=[b"\x01\x00\x02", b"\x00\x03\x00", b""], lines=[header, b"NNNN\n", b""])
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
async def test_health_records_safe_confirmed_same_summary(caplog) -> None:
    caplog.set_level("INFO", logger="mesh_wx.noaa_sdr")
    header = b"ZCZC-WXR-RWT-047125+0030-2491830-KOHX/NWS-\n"
    rtl, decoder = make_pair(pcm=[b"\x01\x00"], lines=[header, b"NNNN\n", b""])
    supervisor = NoaaSdrSupervisor(
        NoaaSdrConfig(enabled=True, device_serial="x"),
        process_factory=Factory([(rtl, decoder)]),
        executable_lookup=lambda name: name,
        wall_clock=lambda: datetime(2026, 9, 6, 18, 31, tzinfo=timezone.utc),
    )

    await supervisor.run(max_cycles=1)

    health = supervisor.health
    assert health.confirmed_headers == 1
    assert health.confirmed_eom == 1
    assert health.last_event_code == "RWT"
    assert health.last_event_name == "Required Weekly Test"
    assert health.last_location_count == 1
    assert health.last_rwt == datetime(2026, 9, 6, 18, 31, tzinfo=timezone.utc)
    assert "ZCZC" not in repr(health)
    assert "NOAA SAME header received callsign=WWH37 event=RWT locations=1" in caplog.text
    assert "NOAA SAME end marker received callsign=WWH37" in caplog.text
    assert header.decode().strip() not in caplog.text


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
async def test_pcm_stall_forces_restart() -> None:
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
async def test_thermal_latch_stays_fail_closed_when_sensor_becomes_unavailable() -> None:
    readings = iter([None, 70.0])
    sleeps: list[float] = []

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    supervisor = NoaaSdrSupervisor(
        NoaaSdrConfig(enabled=True, device_serial="x", thermal_poll_interval=.01),
        executable_lookup=lambda n: n,
        thermal_provider=lambda: next(readings),
        sleep=sleep,
    )
    supervisor._thermal_latched = True

    await supervisor._wait_until_cool()

    assert sleeps == [.01]
    assert supervisor._thermal_latched is False
    assert supervisor.health.temperature_c == 70.0


@pytest.mark.asyncio
async def test_decoder_silence_does_not_restart_while_pcm_is_alive() -> None:
    """multimon emits no stdout between SAME headers; that silence is healthy."""
    decoder = Process(stdout=HangingReader(), stdin=Writer())
    supervisor = NoaaSdrSupervisor(
        NoaaSdrConfig(enabled=True, device_serial="x", stall_timeout=.01),
    )

    task = asyncio.create_task(supervisor._decode_lines(decoder))
    await asyncio.sleep(.03)

    assert task.done() is False
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


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
