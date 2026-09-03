"""NWS polling background task.

Owns the poll loop: fetch active alerts, filter, dedupe, format, and hand
transmissions to the TransmitManager. Errors never crash the loop; they are
logged and surfaced to the UI error log. Runtime status is exposed for the
dashboard.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from dataclasses import replace
from datetime import datetime, timezone

from .config import POLL_INTERVAL_MIN, POLL_HARD_TIMEOUT
from .dedupe import decide
from .filters import FilterRules
from .formatter import build_mesh_text, build_routed_mesh_text
from .models import Alert
from .nws import NWSClient, NWSError
from .routing import RoutedDestination, route_alert

logger = logging.getLogger("mesh_wx.poller")


def _delivery_hash(alert: Alert, matched_areas) -> str:
    """Material content for one routed endpoint, including its county scope."""
    basis = json.dumps({
        "event": alert.event,
        "headline": alert.headline,
        "message_type": alert.message_type,
        "onset": alert.onset,
        "ends": alert.ends,
        "expires": alert.expires,
        "detail": alert.detail,
        "matched_areas": sorted(
            {str(area).strip().casefold() for area in matched_areas if str(area).strip()}
        ),
    }, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()[:16]


class PollerStatus:
    def __init__(self):
        self.last_poll_time: str | None = None
        self.last_poll_success_time: str | None = None   # last poll that actually reached NWS
        self.last_poll_result: str = "not yet polled"
        self.last_raw_response: str = ""
        self.last_broadcast_failure: str | None = None    # last alert that failed on all radios
        self.last_broadcast_failure_text: str = ""
        self.clock_skew_seconds: float | None = None      # system clock vs NWS server time
        self.started_at: datetime = datetime.now(timezone.utc)

    @property
    def uptime_seconds(self) -> int:
        return int((datetime.now(timezone.utc) - self.started_at).total_seconds())


class WxPoller:
    def __init__(self, db, transmit_manager):
        self._db = db
        self._tx = transmit_manager
        self.status = PollerStatus()
        self._task: asyncio.Task | None = None
        self._wake = asyncio.Event()
        self._stopped = False
        self._pending_deliveries: dict[tuple[str, str, int, str, int], dict] = {}
        self._conditional_chains: set[tuple[str, int]] = set()

    # ---- lifecycle ------------------------------------------------------
    def start(self) -> None:
        self._stopped = False
        self._task = asyncio.create_task(self._run(), name="nws-poller")

    async def stop(self) -> None:
        self._stopped = True
        self._wake.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    def poke(self) -> None:
        """Wake the loop early (e.g. after a settings change)."""
        self._wake.set()

    # ---- loop -----------------------------------------------------------
    async def _run(self) -> None:
        while not self._stopped:
            try:
                # Hard watchdog: no single poll may hang the loop. If poll_once
                # wedges (network black-hole, DB lock, a bug), abort it and let the
                # next cycle run -- a stuck poller is a silent blind spot.
                await asyncio.wait_for(self.poll_once(), timeout=POLL_HARD_TIMEOUT)
            except asyncio.TimeoutError:
                self.status.last_poll_result = "error: poll timed out (aborted by watchdog)"
                self._db.add_error("poller", "poll hung and was aborted after %ds" % POLL_HARD_TIMEOUT)
                logger.error("poll_once exceeded %ds; aborted by watchdog", POLL_HARD_TIMEOUT)
            except Exception as exc:  # never let the loop die
                self.status.last_poll_result = f"error: {exc}"
                self._db.add_error("poller", str(exc))
                logger.exception("unexpected poll error")
            interval = max(POLL_INTERVAL_MIN, int(self._db.get_setting("poll_interval", 120)))
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                return

    async def poll_once(self) -> None:
        settings = self._db.all_settings()
        configured_zones = [
            value.strip().upper()
            for value in str(settings.get("zones", "SCZ050") or "").split(",")
            if value.strip()
        ]
        zones = ",".join(dict.fromkeys(configured_zones + self._db.enabled_route_zones()))
        contact = settings.get("nws_contact", "")
        client = NWSClient(contact=contact)
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        try:
            data, raw = await client.fetch_active(zones)
        except NWSError as exc:
            self.status.last_poll_time = now
            self.status.last_poll_result = f"error: {exc}"
            self._db.add_error("nws", str(exc))
            self._db.add_event("ERROR", f"NWS poll failed: {exc}")
            return

        self.status.last_poll_time = now
        self.status.last_poll_success_time = now
        self.status.last_raw_response = raw
        # Clock-skew check: compare our clock to the NWS server's Date header. A
        # skewed VM clock makes "until" times wrong and can break time-based dedup.
        srv = getattr(client, "last_server_date", None)
        if srv:
            try:
                from email.utils import parsedate_to_datetime
                server_dt = parsedate_to_datetime(srv)
                self.status.clock_skew_seconds = (
                    datetime.now(timezone.utc) - server_dt).total_seconds()
            except Exception:
                pass
        features = data.get("features", []) or []
        self.status.last_poll_result = f"ok: {len(features)} active alert(s)"

        self._db.purge_expired_state()
        self._db.prune_history()

        rules = FilterRules.from_settings(settings)
        tz_name = settings.get("display_timezone", "")
        channel = int(settings.get("channel_index", 0))
        dry_run = bool(settings.get("dry_run", True))

        for feature in features:
            try:
                await self._process(feature, rules, tz_name, channel, dry_run)
            except Exception as exc:
                logger.exception("error processing feature")
                self._db.add_error("poller", f"process error: {exc}")

    async def _process(self, feature, rules, tz_name, channel, dry_run) -> None:
        alert = Alert.from_feature(feature)
        if not alert.nws_id:
            return
        legacy_decision = decide(alert, rules, self._db.get_state)
        prior_rows = self._db.delivery_states_for_alerts([alert.nws_id] + alert.references)
        related_ids = {alert.nws_id, *alert.references}
        related_pending_items = [
            (key, pending) for key, pending in self._pending_deliveries.items()
            if pending["root_id"] in related_ids or pending["alert_id"] in related_ids
        ]
        related_pending = [pending for _key, pending in related_pending_items]
        accepted_prior_rows = [row for row in prior_rows
                               if row["disposition"] in ("sent", "update", "cleared", "cancelled")]
        is_referenced_change = alert.message_type in ("Update", "Cancel") or bool(alert.references)
        if not legacy_decision.transmit and not (
                is_referenced_change and (accepted_prior_rows or related_pending)):
            if not self._db.history_exists(alert.nws_id):
                self._db.add_history(alert.nws_id, alert.event, alert.area_desc,
                                     legacy_decision.disposition, "", legacy_decision.detail)
            return
        if is_referenced_change:
            cancel_queued = getattr(self._tx, "cancel_queued_correlation", None)
            if cancel_queued is not None:
                for chain_key in {pending["chain_key"] for pending in related_pending}:
                    cancel_queued(chain_key)
        # Supersession callbacks remove only the exact queued entries they cancel.
        # Any entry still present is the in-flight predecessor that a replacement
        # clear/cancel must remain conditional on.
        active_pending = [
            pending for key, pending in related_pending_items
            if self._pending_deliveries.get(key) is pending
        ]
        target_pending = active_pending if alert.message_type == "Cancel" else related_pending
        targets = self._delivery_targets(
            alert, prior_rows, target_pending, clearing_pending_rows=active_pending,
        )
        targets = [target for target in targets
                   if self._pending_key(alert, target[0]) not in self._pending_deliveries]
        if not targets:
            if not self._db.history_exists(alert.nws_id):
                disposition = "no_route" if legacy_decision.transmit else legacy_decision.disposition
                detail = "no matching route" if legacy_decision.transmit else legacy_decision.detail
                self._db.add_history(alert.nws_id, alert.event, alert.area_desc,
                                     disposition, "", detail)
            return

        routed = []
        for destination, root_id, disposition in targets:
            routed_alert = replace(alert, area_desc="; ".join(destination.matched_areas))
            text = build_routed_mesh_text(routed_alert, destination.matched_areas,
                                          tz_name, disposition=disposition)
            routed.append((destination, root_id, disposition, text))

        if dry_run:
            for destination, _root_id, _disposition, text in routed:
                self._db.add_event("INFO", "[DRY-RUN] would send via %s ch %d: %s" %
                                   (destination.transport, destination.channel, text))
            if not self._db.history_exists(alert.nws_id):
                self._db.add_history(
                    alert.nws_id, alert.event, alert.area_desc, "dry_run", routed[0][3],
                    "%d destination(s) simulated" % len(routed),
                )
            return

        if self._db.history_exists(alert.nws_id):
            self._db.update_history(
                alert.nws_id, "queued", routed[0][3],
                "%d destination(s) queued" % len(routed),
            )
        else:
            self._db.add_history(
                alert.nws_id, alert.event, alert.area_desc, "queued", routed[0][3],
                "%d destination(s) queued" % len(routed),
            )

        outcomes = {"remaining": len(routed), "accepted": 0, "errors": []}

        for destination, root_id, disposition, text in routed:
            fail_ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
            attempt_id = self._db.create_delivery_attempt(
                root_alert_id=root_id, alert_id=alert.nws_id,
                destination_id=destination.destination_id,
                destination_name=destination.name, transport=destination.transport,
                channel=destination.channel, matched_areas=list(destination.matched_areas),
                message_text=text, event=alert.event, headline=alert.headline,
                expires=alert.expires,
                msg_hash=_delivery_hash(alert, destination.matched_areas),
                disposition=disposition,
            )
            pending_key = self._pending_key(alert, destination)
            chain_key = (root_id, destination.destination_id)
            require_prior = (
                disposition in ("cancelled", "cleared") and
                any(p["chain_key"] == chain_key for p in active_pending) and
                not any(int(row["destination_id"]) == destination.destination_id
                        for row in accepted_prior_rows)
            )
            if require_prior:
                self._conditional_chains.add(chain_key)
            self._pending_deliveries[pending_key] = {
                "alert_id": alert.nws_id, "root_id": root_id,
                "destination": destination, "chain_key": chain_key,
                "disposition": disposition,
            }

            def _on_result(ok, err="", a=alert, dest=destination, root=root_id,
                           disp=disposition, t=text, ts=fail_ts, key=pending_key,
                           attempt=attempt_id, chain=chain_key,
                           conditional=require_prior, called=[False]):
                if called[0]:
                    return
                called[0] = True
                try:
                    if ok:
                        self._db.finalize_delivery_attempt(attempt, "accepted")
                        self._record_delivery(a, dest, root, disp)
                        outcomes["accepted"] += 1
                    else:
                        attempt_state = ("superseded" if err == "superseded" else
                                         "skipped" if err == "predecessor was not accepted" else
                                         "failed")
                        self._db.finalize_delivery_attempt(attempt, attempt_state, err)
                        outcomes["errors"].append(err or "not accepted")
                        self.status.last_broadcast_failure = ts
                        self.status.last_broadcast_failure_text = t
                        self._db.add_error("broadcast", "NOT SENT to %s ch %d (will retry): %s" %
                                           (dest.transport, dest.channel, t))
                        self._db.add_event("ALARM", "BROADCAST FAILED, will retry: %s" % a.event)
                finally:
                    self._pending_deliveries.pop(key, None)
                    if conditional:
                        self._conditional_chains.discard(chain)
                    if chain not in self._conditional_chains:
                        self._discard_chain_result(chain)
                    outcomes["remaining"] -= 1
                    if outcomes["remaining"] == 0:
                        accepted = outcomes["accepted"]
                        final = ("accepted" if accepted == len(routed) else
                                 "partial" if accepted else "failed")
                        detail = "%d/%d destination(s) accepted" % (accepted, len(routed))
                        if outcomes["errors"]:
                            detail += ": " + "; ".join(outcomes["errors"])
                        self._db.update_history(a.nws_id, final, detail=detail)

            try:
                chain_enqueue = getattr(self._tx, "enqueue_destination_chain", None)
                if chain_enqueue is not None:
                    chain_enqueue(
                        text, destination.transport, destination.channel,
                        destination.destination_id, chain_key,
                        require_prior_success=require_prior, on_result=_on_result,
                    )
                else:
                    self._tx.enqueue_destination(
                        text, destination.transport, destination.channel,
                        destination.destination_id, on_result=_on_result,
                    )
            except Exception as exc:
                _on_result(False, str(exc))
            self._db.add_event("INFO", "queued %s via %s ch %d: %s" %
                               (disposition, destination.transport, destination.channel, text))

    @staticmethod
    def _snapshot_destination(row, matched_areas=None) -> RoutedDestination:
        areas = (tuple(matched_areas) if matched_areas is not None else
                 tuple(json.loads(row["matched_areas"] or "[]")))
        return RoutedDestination(
            int(row["destination_id"]), str(row["transport"]), str(row["transport"]),
            int(row["channel"]), areas,
        )

    @staticmethod
    def _pending_key(alert: Alert, destination: RoutedDestination):
        return (alert.nws_id, _delivery_hash(alert, destination.matched_areas),
                destination.destination_id, destination.transport, destination.channel)

    def _destination_enabled(self, destination_id: int) -> bool:
        row = self._db.get_destination(destination_id)
        return row is not None and bool(row["enabled"])

    def _discard_chain_result(self, chain_key) -> None:
        """Bound transmitter chain bookkeeping without requiring a manager API."""
        for attribute in ("_chain_results", "chain_results"):
            results = getattr(self._tx, attribute, None)
            if isinstance(results, dict):
                results.pop(chain_key, None)

    def _delivery_targets(self, alert: Alert, prior_rows, pending_rows=(),
                          clearing_pending_rows=None
                          ) -> list[tuple[RoutedDestination, str, str]]:
        delivered = {"sent", "update", "cleared", "cancelled"}
        prior_by_destination = {
            int(row["destination_id"]): row
            for row in prior_rows if row["disposition"] in delivered
        }
        pending_by_destination = {
            pending["destination"].destination_id: pending for pending in pending_rows
        }
        clearing_pending = pending_rows if clearing_pending_rows is None else clearing_pending_rows
        clearing_pending_by_destination = {
            pending["destination"].destination_id: pending
            for pending in clearing_pending
        }
        current = {d.destination_id: d for d in route_alert(self._db, alert)}
        is_chain_change = alert.message_type in ("Update", "Cancel") or bool(alert.references)
        roots = ([row["root_alert_id"] for row in prior_by_destination.values()] +
                 [pending["root_id"] for pending in pending_rows])
        default_root = roots[0] if roots else alert.nws_id

        if alert.message_type == "Cancel":
            out = []
            for destination_id, row in prior_by_destination.items():
                if not self._destination_enabled(destination_id):
                    continue
                if row["last_alert_id"] == alert.nws_id and row["disposition"] == "cancelled":
                    continue
                out.append((self._snapshot_destination(row), row["root_alert_id"], "cancelled"))
            for destination_id, pending in pending_by_destination.items():
                if destination_id in prior_by_destination or not self._destination_enabled(destination_id):
                    continue
                out.append((pending["destination"], pending["root_id"], "cancelled"))
            return out

        out = []
        for destination_id, destination in current.items():
            prior = prior_by_destination.get(destination_id)
            if prior is None:
                pending = pending_by_destination.get(destination_id)
                disposition = "update" if is_chain_change else "sent"
                if pending is not None:
                    old = pending["destination"]
                    destination = RoutedDestination(
                        old.destination_id, old.name, old.transport, old.channel,
                        destination.matched_areas,
                    )
                out.append((destination, pending["root_id"] if pending else default_root,
                            disposition))
                continue

            # A prior recipient keeps its endpoint snapshot even if the reusable
            # destination is edited while this alert chain is active.
            snapshot = self._snapshot_destination(prior, destination.matched_areas)
            material_hash = _delivery_hash(alert, snapshot.matched_areas)
            if (prior["disposition"] in ("sent", "update") and
                    prior["msg_hash"] == material_hash):
                continue
            out.append((snapshot, prior["root_alert_id"], "update"))

        # A recipient whose route no longer matches gets one clearing using the
        # last successfully delivered county snapshot.
        for destination_id, prior in prior_by_destination.items():
            if destination_id in current or not self._destination_enabled(destination_id):
                continue
            if prior["disposition"] in ("cleared", "cancelled"):
                continue
            out.append((self._snapshot_destination(prior), prior["root_alert_id"], "cleared"))
        for destination_id, pending in clearing_pending_by_destination.items():
            if destination_id in current or destination_id in prior_by_destination:
                continue
            if self._destination_enabled(destination_id):
                out.append((pending["destination"], pending["root_id"], "cleared"))
        return out

    def _record_delivery(self, alert: Alert, destination: RoutedDestination,
                         root_id: str, disposition: str) -> None:
        self._db.upsert_delivery_state(
            root_id, alert.nws_id, destination.destination_id,
            destination.transport, destination.channel, list(destination.matched_areas),
            alert.event, alert.headline, alert.expires,
            _delivery_hash(alert, destination.matched_areas), disposition,
        )

    def _record_state(self, alert: Alert, decision) -> None:
        # Only called on the transmit path (sent / update / cancelled), so the
        # dedupe row always records a genuine broadcast.
        sent_ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
        self._db.upsert_state(
            nws_id=alert.nws_id,
            event=alert.event,
            headline=alert.headline,
            expires=alert.expires,
            msg_hash=alert.content_hash(),
            disposition=decision.disposition,
            sent_ts=sent_ts,
        )


def _format_cancel(alert: Alert, tz_name: str) -> str:
    from .formatter import PREFIX, _area_string
    from .config import MAX_PAYLOAD_BYTES

    area = _area_string(alert.area_desc)
    body = f"CANCELLED: {alert.event}"
    if area:
        body += f": {area}"
    msg = PREFIX + body
    if len(msg.encode()) <= MAX_PAYLOAD_BYTES:
        return msg
    return (PREFIX + f"CANCELLED: {alert.event}")[:MAX_PAYLOAD_BYTES]
