"""Persisted arbitration between immediate NOAA SAME and delayed NWS feeds."""
from __future__ import annotations

import asyncio
import copy
import json
import re
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from enum import Enum

from .models import Alert
from .noaa_same import SAME_COUNTY_FIPS, SAME_STATE_FIPS_TO_POSTAL
from .nwws import parse_vtec_parameter, vtec_correlation_key, vtec_event_year
from .routing import route_alert

_OFFICE = re.compile(r"[KPT][A-Z]{3}")
_SAME_SENDER = re.compile(r"([KPT][A-Z]{3})/NWS")
_SAME_WEATHER_EVENTS = {
    "AVA": "Avalanche Watch", "AVW": "Avalanche Warning",
    "BZW": "Blizzard Warning", "CFA": "Coastal Flood Watch",
    "CFW": "Coastal Flood Warning", "DSW": "Dust Storm Warning",
    "EWW": "Extreme Wind Warning", "FFA": "Flash Flood Watch",
    "FFW": "Flash Flood Warning", "FLA": "Flood Watch",
    "FLW": "Flood Warning", "FSW": "Flash Freeze Warning",
    "HUA": "Hurricane Watch", "HUW": "Hurricane Warning",
    "HWA": "High Wind Watch", "HWW": "High Wind Warning",
    "SMW": "Special Marine Warning", "SQW": "Snow Squall Warning",
    "SSA": "Storm Surge Watch", "SSW": "Storm Surge Warning",
    "SVA": "Severe Thunderstorm Watch", "SVR": "Severe Thunderstorm Warning",
    "TOA": "Tornado Watch", "TOR": "Tornado Warning",
    "TRA": "Tropical Storm Watch", "TRW": "Tropical Storm Warning",
    "TSA": "Tsunami Watch", "TSW": "Tsunami Warning",
    "WFW": "Wild Fire Warning", "WSA": "Winter Storm Watch",
    "WSW": "Winter Storm Warning",
}
_REST_WEATHER_EVENTS = frozenset(_SAME_WEATHER_EVENTS.values())
_REST_VTEC_EVENTS = {
    "Avalanche Watch": "AV.A", "Avalanche Warning": "AV.W",
    "Blizzard Warning": "BZ.W", "Coastal Flood Watch": "CF.A",
    "Coastal Flood Warning": "CF.W", "Dust Storm Warning": "DS.W",
    "Extreme Wind Warning": "EW.W", "Flash Flood Watch": "FF.A",
    "Flash Flood Warning": "FF.W", "Flood Watch": "FA.A",
    "Flood Warning": "FL.W", "Flash Freeze Warning": "FZ.W",
    "Hurricane Watch": "HU.A", "Hurricane Warning": "HU.W",
    "High Wind Watch": "HW.A", "High Wind Warning": "HW.W",
    "Special Marine Warning": "MA.W", "Snow Squall Warning": "SQ.W",
    "Storm Surge Watch": "SS.A", "Storm Surge Warning": "SS.W",
    "Severe Thunderstorm Watch": "SV.A",
    "Severe Thunderstorm Warning": "SV.W", "Tornado Watch": "TO.A",
    "Tornado Warning": "TO.W", "Tropical Storm Watch": "TR.A",
    "Tropical Storm Warning": "TR.W", "Tsunami Watch": "TS.A",
    "Tsunami Warning": "TS.W", "Wild Fire Warning": "FW.W",
    "Winter Storm Watch": "WS.A", "Winter Storm Warning": "WS.W",
}
_VALID_COUNTY_UGCS = frozenset(
    f"{SAME_STATE_FIPS_TO_POSTAL[state]}C{county}"
    for state, counties in SAME_COUNTY_FIPS.items()
    if state in SAME_STATE_FIPS_TO_POSTAL
    for county in counties
)


class ArbitrationOutcome(Enum):
    HANDLED = "handled"
    INELIGIBLE = "ineligible"
    REJECTED = "rejected"


class ArbitrationDeliveryOutcome(Enum):
    ACCEPTED = "accepted"
    RETRYABLE_FAILURE = "retryable_failure"
    PERMANENT_NO_TARGET = "permanent_no_target"


def classify_rest_feature(feature: dict) -> ArbitrationOutcome:
    """Classify whether a REST product belongs to SAME arbitration policy."""
    props = feature.get("properties", {}) if isinstance(feature, dict) else {}
    event = props.get("event") if isinstance(props, dict) else None
    if event not in _REST_WEATHER_EVENTS:
        return ArbitrationOutcome.INELIGIBLE
    metadata = _rest_metadata(feature)
    if metadata is None or metadata[0].action not in {
        "NEW", "CON", "EXT", "CAN", "EXP",
    }:
        return ArbitrationOutcome.REJECTED
    return ArbitrationOutcome.HANDLED


def _time(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)


def rest_product_timestamp(properties: object) -> datetime | None:
    """Return the first valid aware REST product timestamp in policy order."""
    if not isinstance(properties, dict):
        return None
    for name in ("sent", "effective", "onset"):
        parsed = _time(properties.get(name))
        if parsed is not None:
            return parsed
    return None


def _parameters(feature: dict) -> dict:
    props = feature.get("properties", {})
    value = props.get("parameters", {}) if isinstance(props, dict) else {}
    return value if isinstance(value, dict) else {}


def _one(parameters: dict, name: str) -> str | None:
    value = parameters.get(name)
    if not isinstance(value, list) or len(value) != 1 or not isinstance(value[0], str):
        return None
    return value[0]


def _rest_metadata(feature: dict):
    props = feature.get("properties", {}) if isinstance(feature, dict) else {}
    if not isinstance(props, dict) or props.get("status") != "Actual":
        return None
    issue = rest_product_timestamp(props)
    expires = _time(props.get("ends") or props.get("expires"))
    vtec_text = _one(_parameters(feature), "VTEC")
    if issue is None or expires is None or vtec_text is None:
        return None
    try:
        vtec = parse_vtec_parameter(vtec_text)
    except (TypeError, ValueError):
        return None
    if (
        vtec is None or _OFFICE.fullmatch(vtec.office) is None
        or _REST_VTEC_EVENTS.get(str(props.get("event")))
        != f"{vtec.phenomena}.{vtec.significance}"
    ):
        return None
    identity = vtec_correlation_key(vtec, vtec_event_year(vtec, issue))
    return vtec, identity, issue, expires


def _same_metadata(feature: dict):
    props = feature.get("properties", {}) if isinstance(feature, dict) else {}
    if not isinstance(props, dict) or props.get("status") != "Actual":
        return None
    parameters = _parameters(feature)
    originator = _one(parameters, "SAMEOriginator")
    event_code = _one(parameters, "SAMEEventCode")
    sender = _one(parameters, "SAMESender")
    sender_match = _SAME_SENDER.fullmatch(sender or "")
    event = _SAME_WEATHER_EVENTS.get(event_code or "")
    geocode = props.get("geocode", {})
    ugcs = geocode.get("UGC", []) if isinstance(geocode, dict) else []
    counties = {
        str(value).strip().upper() for value in ugcs
        if str(value).strip().upper() in _VALID_COUNTY_UGCS
    } if isinstance(ugcs, list) else set()
    issued = _time(props.get("effective") or props.get("onset"))
    expires = _time(props.get("ends") or props.get("expires"))
    if (
        originator != "WXR" or sender_match is None or event is None
        or props.get("event") != event or not counties
        or len(counties) != len(ugcs) or issued is None or expires is None
    ):
        return None
    return event, sender_match.group(1), issued, expires, counties


def _rest_counties(feature: dict) -> set[str]:
    props = feature.get("properties", {}) if isinstance(feature, dict) else {}
    geocode = props.get("geocode", {}) if isinstance(props, dict) else {}
    values = geocode.get("UGC", []) if isinstance(geocode, dict) else []
    return {str(value).strip().upper() for value in values if str(value).strip()}


def _contains_raw_same_header(value: object) -> bool:
    if isinstance(value, str):
        return "ZCZC-" in value
    if isinstance(value, dict):
        return any(_contains_raw_same_header(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_raw_same_header(item) for item in value)
    return False


def _without_raw_same_headers(value: dict) -> dict:
    def scrub(item: object):
        if isinstance(item, dict):
            return {
                key: scrub(child) for key, child in item.items()
                if not _contains_raw_same_header(child)
            }
        if isinstance(item, list):
            return [
                scrub(child) for child in item
                if not _contains_raw_same_header(child)
            ]
        return item

    return {
        key: scrub(item) for key, item in value.items()
        if not _contains_raw_same_header(item)
    }


class WeatherArbiter:
    """Serialize source races and persist every destination decision."""

    def __init__(
        self, db, deliver: Callable[..., Awaitable[None]], *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        grace_seconds: float = 45.0,
    ) -> None:
        self._db = db
        self._deliver = deliver
        self._clock = clock
        self._grace = timedelta(seconds=float(grace_seconds))
        self._lock = asyncio.Lock()
        self._tasks: set[asyncio.Task] = set()
        self._db.reconcile_successful_arbitration_transmits()
        self._db.recover_queued_arbitration_destinations()
        self._db.recover_queued_arbitration_followups()
        self._recoverable_followups = {
            (str(row["canonical_id"]), str(row["followup_id"]))
            for row in self._db.pending_arbitration_followups()
        }

    async def ingest(self, feature: dict, *, source: str) -> ArbitrationOutcome:
        if source not in {"rest", "nwws", "same"}:
            return ArbitrationOutcome.REJECTED
        if source == "same":
            return await self._ingest_same(feature)
        classification = classify_rest_feature(feature)
        if classification is not ArbitrationOutcome.HANDLED:
            return classification
        metadata = _rest_metadata(feature)
        assert metadata is not None
        vtec, identity, issued, expires = metadata
        if vtec.action != "NEW":
            return ArbitrationOutcome.REJECTED
        alert = Alert.from_feature(feature)
        if not alert.nws_id or alert.event not in _REST_WEATHER_EVENTS:
            return ArbitrationOutcome.REJECTED
        destinations = [item.destination_id for item in route_alert(self._db, alert)]
        if not destinations:
            return ArbitrationOutcome.REJECTED
        now = self._clock().astimezone(UTC)
        serialized = json.dumps(feature, sort_keys=True, separators=(",", ":"))
        async with self._lock:
            existing_vtec = self._db.get_arbitration_candidate_by_vtec(identity)
            if existing_vtec is not None:
                canonical_id = str(existing_vtec["canonical_id"])
                current_feature = str(existing_vtec["rest_feature"] or serialized)
                if not self._db.bind_arbitration_rest_new(
                    canonical_id, current_feature, identity, destinations,
                    expires_at=expires.isoformat(),
                    correlation_ugcs=_rest_counties(feature),
                ):
                    return ArbitrationOutcome.REJECTED
                return ArbitrationOutcome.HANDLED
            same_matches = []
            rest_counties = _rest_counties(feature)
            for row in self._db.list_arbitration_candidates():
                if not row["same_feature"] or row["rest_feature"]:
                    continue
                saved_same = json.loads(row["same_feature"])
                same_info = _same_metadata(saved_same)
                row_issued = _time(row["issued_at"])
                row_expires = _time(row["expires_at"])
                if (
                    same_info is not None and row["event"] == alert.event
                    and row["office"] == vtec.office and row_issued is not None
                    and row_expires is not None
                    and abs((row_issued - issued).total_seconds()) <= 600
                    and issued <= row_expires and row_issued <= expires
                    and same_info[4] & rest_counties
                ):
                    same_matches.append(row)
            if len(same_matches) == 1:
                canonical_id = str(same_matches[0]["canonical_id"])
                if not self._db.bind_arbitration_rest_new(
                    canonical_id, serialized, identity, destinations,
                    expires_at=expires.isoformat(),
                    correlation_ugcs=rest_counties,
                ):
                    return ArbitrationOutcome.REJECTED
                claimed = self._db.claim_arbitration_destinations(
                    canonical_id, destinations, "rest"
                )
                bound = True
            elif len(same_matches) > 1:
                return ArbitrationOutcome.REJECTED
            else:
                bound = False
                claimed = []
                canonical_id = alert.nws_id
            if not bound:
                try:
                    self._db.create_arbitration_candidate_with_destinations(
                        canonical_id=alert.nws_id, event=alert.event, office=vtec.office,
                        issued_at=issued.isoformat(), expires_at=expires.isoformat(),
                        deadline=(now + self._grace).isoformat(),
                        rest_feature=serialized, vtec_key=identity,
                        rest_destination_ids=destinations,
                        correlation_ugcs=rest_counties,
                    )
                except Exception as exc:
                    # Repeated feed observations are harmless; unexpected conflicts
                    # are handled by the persisted candidate already owning the key.
                    if "UNIQUE constraint failed" not in str(exc):
                        raise
                    existing_vtec = self._db.get_arbitration_candidate_by_vtec(identity)
                    if existing_vtec is None or not self._db.bind_arbitration_rest_new(
                        str(existing_vtec["canonical_id"]), serialized, identity,
                        destinations, expires_at=expires.isoformat(),
                        correlation_ugcs=rest_counties,
                    ):
                        return ArbitrationOutcome.REJECTED
        if claimed:
            rebound = copy.deepcopy(feature)
            rebound["id"] = canonical_id
            rebound.setdefault("properties", {})["id"] = canonical_id
            await self._deliver_claimed(rebound, claimed, "rest", canonical_id)
        return ArbitrationOutcome.HANDLED

    async def prepare_followup(self, feature: dict) -> dict | None:
        """Persist and re-key a bound REST update or cancellation."""
        metadata = _rest_metadata(feature)
        if metadata is None:
            return None
        vtec, identity, _issued, expires = metadata
        if vtec.action not in {"CON", "EXT", "CAN", "EXP"}:
            return None
        alert = Alert.from_feature(feature)
        destinations = (
            [item.destination_id for item in route_alert(self._db, alert)]
            if vtec.action in {"CON", "EXT"} else []
        )
        rebound = copy.deepcopy(feature)
        props = rebound.setdefault("properties", {})
        props["messageType"] = (
            "Update" if vtec.action in {"CON", "EXT"} else "Cancel"
        )
        serialized = json.dumps(rebound, sort_keys=True, separators=(",", ":"))
        async with self._lock:
            row = self._db.get_arbitration_candidate_by_vtec(identity)
            if row is None or not row["rest_feature"]:
                return None
            canonical_id = str(row["canonical_id"])
            destinations = sorted({
                *destinations,
                *self._db.active_arbitration_destinations(canonical_id),
                *(
                    int(item["destination_id"])
                    for item in self._db.delivery_states_for_alerts([canonical_id])
                ),
            })
            if not self._db.apply_arbitration_followup(
                canonical_id, identity, vtec.action, serialized,
                expires.isoformat(), destinations, _rest_counties(feature),
                now=self._clock().astimezone(UTC).isoformat(),
            ):
                return None
        rebound["id"] = canonical_id
        props["id"] = canonical_id
        props["references"] = [{"@id": canonical_id}]
        return rebound

    async def _ingest_same(self, feature: dict) -> ArbitrationOutcome:
        metadata = _same_metadata(feature)
        if metadata is None:
            return ArbitrationOutcome.REJECTED
        event, office, issued, expires, counties = metadata
        alert = Alert.from_feature(feature)
        same_destinations = [item.destination_id for item in route_alert(self._db, alert)]
        if not alert.nws_id or not same_destinations:
            return ArbitrationOutcome.REJECTED
        now = self._clock().astimezone(UTC)
        async with self._lock:
            overlapping = []
            for row in self._db.list_arbitration_candidates():
                row_issued = _time(row["issued_at"])
                row_expires = _time(row["expires_at"])
                if not (
                    row["event"] == event and row["office"] == office
                    and row_issued is not None and row_expires is not None
                    and abs((row_issued - issued).total_seconds()) <= 600
                    and issued <= row_expires and row_issued <= expires
                ):
                    continue
                try:
                    row_counties = set(json.loads(row["correlation_ugcs"] or "[]"))
                except (TypeError, ValueError):
                    row_counties = set()
                if counties & row_counties:
                    overlapping.append(row)
            if len(overlapping) > 1:
                return ArbitrationOutcome.REJECTED
            matches = overlapping
            canonical_id = str(matches[0]["canonical_id"]) if matches else alert.nws_id
            persisted = _without_raw_same_headers(copy.deepcopy(feature))
            persisted["id"] = canonical_id
            persisted.setdefault("properties", {})["id"] = canonical_id
            serialized = json.dumps(persisted, sort_keys=True, separators=(",", ":"))
            if matches:
                claimable = self._db.record_arbitration_same(
                    canonical_id, serialized, same_destinations, counties
                )
            else:
                existing = self._db.get_arbitration_candidate(canonical_id)
                if existing is not None:
                    claimable = self._db.record_arbitration_same(
                        canonical_id, serialized, same_destinations, counties
                    )
                else:
                    self._db.create_arbitration_candidate_with_destinations(
                        canonical_id=canonical_id, event=event, office=office,
                        issued_at=issued.isoformat(), expires_at=expires.isoformat(),
                        deadline=(now + self._grace).isoformat(), same_feature=serialized,
                        same_destination_ids=same_destinations,
                        correlation_ugcs=counties,
                    )
                    claimable = same_destinations
            claimed = self._db.claim_arbitration_destinations(
                canonical_id, claimable, "same"
            )
        if claimed:
            await self._deliver_claimed(persisted, claimed, "same", canonical_id)
        return ArbitrationOutcome.HANDLED

    async def release_due(self) -> int:
        delivered = 0
        now = self._clock().astimezone(UTC).isoformat()
        async with self._lock:
            self._db.prune_arbitration_candidates(now)
            rows = self._db.due_arbitration_candidates(now)
            work = []
            for row in rows:
                feature = json.loads(row["rest_feature"])
                claimed = self._db.claim_arbitration_destinations(
                    row["canonical_id"],
                    self._db.pending_arbitration_destinations(row["canonical_id"]),
                    "rest",
                )
                if claimed:
                    work.append((row, feature, claimed))
                    delivered += len(claimed)
        for index, (row, feature, destination_ids) in enumerate(work):
            try:
                await self._deliver_claimed(
                    copy.deepcopy(feature), destination_ids, "rest",
                    row["canonical_id"],
                )
            except Exception:
                for queued_row, _feature, queued_destinations in work[index + 1:]:
                    self._db.release_arbitration_destinations(
                        queued_row["canonical_id"], queued_destinations, "rest"
                    )
                raise
        return delivered + await self._release_pending_followups()

    async def deliver_prepared_followup(self, feature: dict) -> int:
        """Claim and enqueue a follow-up already committed to the durable outbox."""
        canonical_id = feature.get("id") if isinstance(feature, dict) else None
        if not isinstance(canonical_id, str) or not canonical_id:
            return 0
        return await self._release_pending_followups(canonical_id)

    async def _release_pending_followups(self,
                                         canonical_id: str | None = None) -> int:
        delivered = 0
        async with self._lock:
            work = []
            for row in self._db.pending_arbitration_followups(canonical_id):
                candidate_id = str(row["canonical_id"])
                followup_id = str(row["followup_id"])
                key = (candidate_id, followup_id)
                if canonical_id is None and key not in self._recoverable_followups:
                    continue
                claimed = self._db.claim_arbitration_followup(
                    candidate_id, followup_id
                )
                if not claimed:
                    continue
                feature = json.loads(row["feature"])
                feature["id"] = candidate_id
                props = feature.setdefault("properties", {})
                props["id"] = candidate_id
                props["references"] = [{"@id": candidate_id}]
                work.append((candidate_id, followup_id, feature, claimed))
                self._recoverable_followups.discard(key)
                delivered += len(claimed)
        for index, (candidate_id, followup_id, feature, destination_ids) in enumerate(work):
            try:
                await self._deliver(
                    feature, destination_ids, "followup", candidate_id,
                    self._followup_result_callback(candidate_id, followup_id),
                )
            except Exception:
                self._db.release_arbitration_followup(candidate_id, followup_id)
                self._recoverable_followups.add((candidate_id, followup_id))
                for queued_id, queued_followup, _feature, _destinations in work[index + 1:]:
                    self._db.release_arbitration_followup(queued_id, queued_followup)
                    self._recoverable_followups.add((queued_id, queued_followup))
                raise
        return delivered

    def _followup_result_callback(self, canonical_id: str, followup_id: str):
        def complete(destination_id: int, outcome: bool | ArbitrationDeliveryOutcome,
                     error: str = "") -> bool:
            changed = self._db.finish_arbitration_followup(
                canonical_id, followup_id, destination_id, outcome, error
            )
            value = getattr(outcome, "value", outcome)
            if changed and (value is False or value == "retryable_failure"):
                self._recoverable_followups.add((canonical_id, followup_id))
            return changed
        setattr(complete, "arbitration_followup_id", followup_id)
        return complete

    def _result_callback(self, canonical_id: str, source: str):
        def complete(destination_id: int, outcome: bool | ArbitrationDeliveryOutcome,
                     _error: str = "") -> bool:
            changed = self._db.finish_arbitration_destination(
                canonical_id, destination_id, source, outcome
            )
            accepted = outcome is True or outcome is ArbitrationDeliveryOutcome.ACCEPTED
            if changed and source == "same" and not accepted:
                task = asyncio.create_task(
                    self._release_failed_same(canonical_id, destination_id),
                    name=f"same-rest-fallback-{canonical_id}-{destination_id}",
                )
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)
            return changed
        return complete

    async def _release_failed_same(self, canonical_id: str,
                                   destination_id: int) -> None:
        fallback = None
        async with self._lock:
            row = self._db.get_arbitration_candidate(canonical_id)
            if row is not None and row["rest_feature"]:
                claimed = self._db.claim_arbitration_destinations(
                    canonical_id, [destination_id], "rest"
                )
                if claimed:
                    fallback = (json.loads(row["rest_feature"]), claimed)
        if fallback is not None:
            feature, claimed = fallback
            await self._deliver_claimed(feature, claimed, "rest", canonical_id)

    async def _deliver_claimed(self, feature: dict, destination_ids: list[int],
                               source: str, canonical_id: str) -> None:
        try:
            await self._deliver(
                feature, destination_ids, source, canonical_id,
                self._result_callback(canonical_id, source),
            )
        except Exception:
            self._db.release_arbitration_destinations(
                canonical_id, destination_ids, source
            )
            raise

    async def drain(self) -> None:
        """Wait for tracked failure-triggered fallback work to finish."""
        while self._tasks:
            await asyncio.gather(*tuple(self._tasks), return_exceptions=True)
