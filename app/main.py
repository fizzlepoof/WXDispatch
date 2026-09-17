"""FastAPI application: lifespan wiring + web UI routes."""
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from . import __version__
from .arbitration import WeatherArbiter
from .config import load_bootstrap, load_noaa_sdr_config, load_nwws_config
from .db import Database
from .ipaws import IpawsPoller
from .logging_setup import setup_logging
from .noaa_same import SameEndMessage, SameObservation
from .noaa_sdr import (
    NoaaSdrConfig,
    NoaaSdrSupervisor,
    project_same_to_feature,
    raspberry_pi_temperature,
)
from .nwws import NWWSProduct, project_to_feature
from .nwws_runtime import NWWSRuntimeService
from .poller import WxPoller
from .transmit import TransmitManager
from .watchdog import Liveness
from .web.routes import router

logger = logging.getLogger("mesh_wx.main")


def _nwws_product_handler(poller: WxPoller, *, shadow: bool):
    async def handle(product: NWWSProduct) -> None:
        feature = project_to_feature(product)
        if feature is None:
            return
        await poller.ingest_feature(feature, source="nwws", shadow=shadow)

    return handle


def _noaa_sdr_handler(poller: WxPoller, *, config: NoaaSdrConfig, shadow: bool):
    async def handle(message: SameObservation | SameEndMessage) -> None:
        feature = project_same_to_feature(message, config)
        if feature is not None:
            await poller.ingest_feature(feature, source="noaa_sdr", shadow=shadow)

    return handle


async def _run_noaa_sdr(supervisor: NoaaSdrSupervisor) -> None:
    """Contain receiver failures so web and internet alert paths stay up."""
    try:
        await supervisor.run()
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("NOAA SDR receiver stopped unexpectedly")


async def _heartbeat(liveness: Liveness) -> None:
    """Refresh the liveness heartbeat while the event loop is healthy."""
    while True:
        liveness.beat()
        await asyncio.sleep(5)


async def _run_weather_arbitration(arbiter, *, interval: float = 1.0) -> None:
    """Release expired REST fallbacks independently of the REST poll cadence."""
    while True:
        try:
            await arbiter.release_due()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("weather arbitration fallback release failed")
        await asyncio.sleep(interval)


async def _startup_serial(db: Database, tx: TransmitManager) -> None:
    """Auto-discover a Meshtastic serial node if enabled and none saved, then
    connect every enabled transport (Meshtastic + MeshCore)."""
    mt_serial = (bool(db.get_setting("meshtastic_enabled", True))
                 and (db.get_setting("meshtastic_conn", "serial") or "serial") == "serial")
    if mt_serial and not db.get_setting("serial_port", ""):
        logger.info("no serial port saved; scanning for a node")
        from .serial_discovery import discover_port
        # Never probe a port the user assigned to MeshCore (probing locks it).
        exclude = {db.get_setting("meshcore_port", "") or ""}
        loop = asyncio.get_event_loop()
        found = await loop.run_in_executor(None, lambda: discover_port(exclude))
        if found:
            db.set_setting("serial_port", found)
            db.add_event("INFO", f"auto-discovered node at {found}")
        else:
            db.add_event("WARN", "no node found during startup scan")
            logger.warning("no node found during startup scan")
    await tx.reconfigure()


@asynccontextmanager
async def lifespan(app: FastAPI):
    cfg = load_bootstrap()
    setup_logging()
    logger.info("starting WXDispatch (db=%s)", cfg.db_path)

    db = Database(cfg.db_path)
    tx = TransmitManager(db)
    poller = WxPoller(db, tx)
    ipaws = IpawsPoller(db, tx)
    nwws_config = load_nwws_config()
    nwws = NWWSRuntimeService(
        nwws_config.runtime,
        on_product=_nwws_product_handler(poller, shadow=nwws_config.shadow),
    )
    noaa_sdr_config = load_noaa_sdr_config()
    weather_arbiter = None
    arbitration_task = None
    if noaa_sdr_config.receiver.enabled and not noaa_sdr_config.shadow:
        weather_arbiter = WeatherArbiter(db, poller.deliver_arbitrated)
        poller.set_arbiter(weather_arbiter)
        arbitration_task = asyncio.create_task(
            _run_weather_arbitration(weather_arbiter),
            name="same-rest-arbitration",
        )
    noaa_sdr = NoaaSdrSupervisor(
        noaa_sdr_config.receiver,
        callback=_noaa_sdr_handler(
            poller, config=noaa_sdr_config.receiver, shadow=noaa_sdr_config.shadow,
        ),
        thermal_provider=raspberry_pi_temperature,
    )

    app.state.cfg = cfg
    app.state.db = db
    app.state.tx = tx
    app.state.poller = poller
    app.state.ipaws = ipaws
    app.state.nwws = nwws
    app.state.nwws_shadow = nwws_config.shadow
    app.state.noaa_sdr = noaa_sdr
    app.state.noaa_sdr_shadow = noaa_sdr_config.shadow
    app.state.noaa_sdr_config_error = noaa_sdr_config.error
    app.state.weather_arbiter = weather_arbiter
    app.state.arbitration_task = arbitration_task

    # Liveness watchdog: force a restart if the event loop ever wedges.
    liveness = Liveness(stall_seconds=90.0)
    liveness.start()
    beat_task = asyncio.create_task(_heartbeat(liveness))
    app.state.liveness = liveness

    tx.start()
    # Connect the radios in the background so serial probing never blocks the
    # web server from coming up (a non-node USB port can take 30s+ to time out).
    startup_task = asyncio.create_task(_startup_serial(db, tx))
    app.state.startup_task = startup_task
    poller.start()
    ipaws.start()
    await nwws.start()
    noaa_sdr_task = asyncio.create_task(_run_noaa_sdr(noaa_sdr), name="noaa-sdr")
    app.state.noaa_sdr_task = noaa_sdr_task
    logger.info("NWWS runtime state=%s shadow=%s", nwws.health.state.value, nwws_config.shadow)

    try:
        yield
    finally:
        logger.info("shutting down WXDispatch")
        liveness.stop()          # first: never force-exit during a clean shutdown
        beat_task.cancel()
        if not startup_task.done():
            startup_task.cancel()
        if not noaa_sdr_task.done():
            noaa_sdr_task.cancel()
        if arbitration_task is not None and not arbitration_task.done():
            arbitration_task.cancel()
        try:
            await noaa_sdr_task
        except asyncio.CancelledError:
            pass
        if arbitration_task is not None:
            try:
                await arbitration_task
            except asyncio.CancelledError:
                pass
        await nwws.stop()
        await poller.stop()
        await ipaws.stop()
        if weather_arbiter is not None:
            await weather_arbiter.drain()
        await tx.stop()
        db.close()


def create_app() -> FastAPI:
    app = FastAPI(title="WXDispatch", version=__version__, lifespan=lifespan)
    app.include_router(router)
    return app


app = create_app()


def main() -> None:
    import sys

    import uvicorn

    # On Windows, asyncio defaults to the ProactorEventLoop, but the MeshCore
    # serial layer (serial_asyncio) only works on the SelectorEventLoop. Without
    # this, MeshCore serial never connects on Windows ("no response").
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    cfg = load_bootstrap()
    uvicorn.run(app, host=cfg.http_host, port=cfg.http_port, log_config=None)


if __name__ == "__main__":
    main()
