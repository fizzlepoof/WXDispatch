"""Validated decoding primitives for NOAA SAME messages."""

from __future__ import annotations

import calendar
import math
import re
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from types import MappingProxyType
from typing import Final


_HEADER_PATTERN = (
    r"ZCZC-(?P<originator>[A-Z]{3})-(?P<event>[A-Z0-9]{3})-"
    r"(?P<locations>\d{6}(?:-\d{6})*)\+(?P<duration>\d{4})-"
    r"(?P<issued>\d{7})-(?P<sender>[A-Z0-9/]{8})-"
)
_PREFIX_PATTERN = r"(?:[A-Za-z0-9 ._:/\[\]-]{1,128}EAS:\s*|EAS:\s*)?"
_HEADER_RE = re.compile(rf"{_PREFIX_PATTERN}(?P<header>{_HEADER_PATTERN})")
_EOM_RE = re.compile(rf"{_PREFIX_PATTERN}(?P<eom>NNNN)")
SAME_EVENT_NAMES: Final = MappingProxyType(
    {
        "ADR": "Administrative Message",
        "AVA": "Avalanche Watch",
        "AVW": "Avalanche Warning",
        "BHW": "Biological Hazard Warning",
        "BLU": "Blue Alert",
        "BOE": "Boil Water Warning",
        "BZW": "Blizzard Warning",
        "CAE": "Child Abduction Emergency",
        "CDW": "Civil Danger Warning",
        "CEM": "Civil Emergency Message",
        "CFA": "Coastal Flood Watch",
        "CFW": "Coastal Flood Warning",
        "CHW": "Chemical Hazard Warning",
        "DMO": "Practice/Demo Warning",
        "DSW": "Dust Storm Warning",
        "DWW": "Dam Break Warning",
        "EAN": "Emergency Action Notification",
        "EAT": "Emergency Action Termination",
        "EQW": "Earthquake Warning",
        "EVI": "Immediate Evacuation",
        "EWW": "Extreme Wind Warning",
        "FCW": "Food Contamination Warning",
        "FFA": "Flash Flood Watch",
        "FFS": "Flash Flood Statement",
        "FFW": "Flash Flood Warning",
        "FLA": "Flood Watch",
        "FLS": "Flood Statement",
        "FLW": "Flood Warning",
        "FRW": "Fire Warning",
        "FSW": "Flash Freeze Warning",
        "HLS": "Hurricane Local Statement",
        "HMW": "Hazardous Materials Warning",
        "HUA": "Hurricane Watch",
        "HUW": "Hurricane Warning",
        "HWA": "High Wind Watch",
        "HWW": "High Wind Warning",
        "IBW": "Iceberg Warning",
        "IFW": "Industrial Fire Warning",
        "LAE": "Local Area Emergency",
        "LEW": "Law Enforcement Warning",
        "LSW": "Landslide Warning",
        "NPT": "National Periodic Test",
        "NUW": "Nuclear Power Plant Warning",
        "RHW": "Radiological Hazard Warning",
        "RMT": "Required Monthly Test",
        "RWT": "Required Weekly Test",
        "SMW": "Special Marine Warning",
        "SPS": "Special Weather Statement",
        "SPW": "Shelter in Place Warning",
        "SQW": "Snow Squall Warning",
        "SSA": "Storm Surge Watch",
        "SSW": "Storm Surge Warning",
        "SVA": "Severe Thunderstorm Watch",
        "SVR": "Severe Thunderstorm Warning",
        "SVS": "Severe Weather Statement",
        "TOA": "Tornado Watch",
        "TOE": "911 Telephone Outage Emergency",
        "TOR": "Tornado Warning",
        "TRA": "Tropical Storm Watch",
        "TRW": "Tropical Storm Warning",
        "TSA": "Tsunami Watch",
        "TSW": "Tsunami Warning",
        "VOW": "Volcano Warning",
        "WFW": "Wild Fire Warning",
        "WSA": "Winter Storm Watch",
        "WSW": "Winter Storm Warning",
    }
)
_TEST_EVENT_CODES = frozenset({"DMO", "NPT", "RMT", "RWT"})
SAME_STATE_FIPS_TO_POSTAL: Final = MappingProxyType(
    {
        "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
        "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
        "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
        "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
        "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
        "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
        "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
        "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
        "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
        "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
        "56": "WY", "60": "AS", "66": "GU", "69": "MP", "72": "PR",
        "74": "UM", "78": "VI",
    }
)
_MAX_INPUT_LENGTH = 512
_MAX_HEADER_LENGTH = 268
_MAX_LOCATIONS = 31
_MAX_HEADER_AGE = timedelta(hours=24)
_MAX_FUTURE_SKEW = timedelta(minutes=15)


def _parse_duration(code: str) -> timedelta:
    hours = int(code[:2])
    minutes = int(code[2:])
    valid = (
        (hours == 0 and minutes in (15, 30, 45))
        or (1 <= hours <= 5 and minutes in (0, 30))
        or (hours == 6 and minutes == 0)
    )
    if not valid:
        raise ValueError("invalid SAME purge duration")
    return timedelta(hours=hours, minutes=minutes)


def _resolve_issued_at(code: str, now: datetime) -> datetime:
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be an aware datetime")
    now_utc = now.astimezone(timezone.utc)
    day = int(code[:3])
    hour = int(code[3:5])
    minute = int(code[5:])
    if day < 1 or day > 366 or hour > 23 or minute > 59:
        raise ValueError("invalid SAME issue timestamp")

    candidates: list[datetime] = []
    for year in (now_utc.year - 1, now_utc.year, now_utc.year + 1):
        days_in_year = 366 if calendar.isleap(year) else 365
        if day <= days_in_year:
            candidates.append(
                datetime(year, 1, 1, hour, minute, tzinfo=timezone.utc)
                + timedelta(days=day - 1)
            )
    if not candidates:
        raise ValueError("invalid SAME Julian day")
    issued = min(candidates, key=lambda candidate: abs(candidate - now_utc))
    age = now_utc - issued
    if age > _MAX_HEADER_AGE or age < -_MAX_FUTURE_SKEW:
        raise ValueError("implausible SAME issue timestamp")
    return issued


@dataclass(frozen=True, slots=True)
class SameEndMessage:
    """A validated SAME end-of-message marker."""

    raw_message: str = "NNNN"


@dataclass(frozen=True, slots=True)
class SameObservation:
    """An immutable, validated SAME start-header observation."""

    raw_header: str
    originator: str
    event_code: str
    event_name: str
    locations: tuple[str, ...]
    county_ugcs: tuple[str, ...]
    purge_duration: timedelta
    issued_at: datetime
    expires_at: datetime
    sender: str
    is_test: bool
    is_emergency: bool


class SameRepeatConfirmer:
    """Confirm repeated headers and suppress recently accepted duplicates."""

    def __init__(
        self,
        *,
        confirmation_window: float = 5.0,
        duplicate_suppression: float = 0.0,
        max_recent: int = 256,
    ) -> None:
        confirmation_window = float(confirmation_window)
        duplicate_suppression = float(duplicate_suppression)
        if not math.isfinite(confirmation_window) or confirmation_window <= 0:
            raise ValueError("confirmation_window must be positive and finite")
        if not math.isfinite(duplicate_suppression) or duplicate_suppression < 0:
            raise ValueError("duplicate_suppression must be non-negative and finite")
        if isinstance(max_recent, bool) or not isinstance(max_recent, int) or max_recent <= 0:
            raise ValueError("max_recent must be a positive integer")
        self.confirmation_window = confirmation_window
        self.duplicate_suppression = duplicate_suppression
        self.max_recent = max_recent
        self._candidate_header: str | None = None
        self._candidate_started = 0.0
        self._candidate_count = 0
        self._recent: OrderedDict[str, float] = OrderedDict()

    @property
    def recent_count(self) -> int:
        """Return the number of accepted headers retained for deduplication."""
        return len(self._recent)

    def _reset_candidate(self) -> None:
        self._candidate_header = None
        self._candidate_started = 0.0
        self._candidate_count = 0

    def process(
        self,
        result: SameObservation | SameEndMessage,
        *,
        monotonic_now: float | None = None,
    ) -> SameObservation | SameEndMessage | None:
        current = time.monotonic() if monotonic_now is None else float(monotonic_now)
        if not math.isfinite(current):
            raise ValueError("monotonic_now must be finite")
        if isinstance(result, SameEndMessage):
            self._reset_candidate()
            return result
        if not isinstance(result, SameObservation):
            raise TypeError("result must be a SAME observation or end marker")

        accepted_at = self._recent.get(result.raw_header)
        if (
            accepted_at is not None
            and 0 <= current - accepted_at < self.duplicate_suppression
        ):
            self._reset_candidate()
            return None
        if accepted_at is not None:
            del self._recent[result.raw_header]

        matches = (
            result.raw_header == self._candidate_header
            and 0 <= current - self._candidate_started <= self.confirmation_window
        )
        if not matches:
            self._candidate_header = result.raw_header
            self._candidate_started = current
            self._candidate_count = 1
            return None
        self._candidate_count += 1
        if self._candidate_count < 2:
            return None

        self._reset_candidate()
        self._recent[result.raw_header] = current
        self._recent.move_to_end(result.raw_header)
        while len(self._recent) > self.max_recent:
            self._recent.popitem(last=False)
        return result


def parse_same(
    text: str, *, now: datetime
) -> SameObservation | SameEndMessage:
    """Parse one bounded SAME start header or end marker."""
    if not isinstance(text, str):
        raise TypeError("SAME input must be text")
    if len(text) > _MAX_INPUT_LENGTH or not text.isascii():
        raise ValueError("SAME input must be bounded ASCII")
    line = text.strip()
    eom_match = _EOM_RE.fullmatch(line)
    if eom_match is not None:
        return SameEndMessage(raw_message=eom_match.group("eom"))
    match = _HEADER_RE.fullmatch(line)
    if match is None:
        raise ValueError("malformed SAME header")
    if len(match.group("header")) > _MAX_HEADER_LENGTH:
        raise ValueError("SAME header is too long")

    locations = tuple(match.group("locations").split("-"))
    if len(locations) > _MAX_LOCATIONS:
        raise ValueError("too many SAME locations")
    duration_code = match.group("duration")
    duration = _parse_duration(duration_code)
    issued_code = match.group("issued")
    issued = _resolve_issued_at(issued_code, now)
    county_ugcs = tuple(
        f"{SAME_STATE_FIPS_TO_POSTAL[location[1:3]]}C{location[3:]}"
        for location in locations
        if location[1:3] in SAME_STATE_FIPS_TO_POSTAL
    )
    event_code = match.group("event")
    is_test = event_code in _TEST_EVENT_CODES
    return SameObservation(
        raw_header=match.group("header"),
        originator=match.group("originator"),
        event_code=event_code,
        event_name=SAME_EVENT_NAMES.get(
            event_code, f"Unknown SAME event {event_code}"
        ),
        locations=locations,
        county_ugcs=county_ugcs,
        purge_duration=duration,
        issued_at=issued,
        expires_at=issued + duration,
        sender=match.group("sender"),
        is_test=is_test,
        is_emergency=not is_test,
    )
