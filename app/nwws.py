"""Bounded, hardened NWWS-OI product parsing helpers."""
from __future__ import annotations

import re
import unicodedata
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone

from defusedxml import ElementTree as ET

MAX_STANZA_BYTES = 256 * 1024
MAX_RAW_TEXT_BYTES = 200 * 1024
MAX_HISTORY_BYTES = 4096
MAX_LOCATIONS = 256
MAX_VTEC_LINE_CHARS = 256

_SAFE_OFFICE = re.compile(r"[A-Z]{4}")
_SAFE_WMO = re.compile(r"[A-Z]{4}[0-9]{2}")
_SAFE_AWIPS = re.compile(r"[A-Z0-9]{3,9}")
_SAFE_PROCESS = re.compile(r"[A-Za-z0-9_-]{1,64}")
_EXACT_OPERATIONAL_VTEC = re.compile(
    r"/O\.[A-Za-z]{3}\.[A-Z]{4}\.[A-Za-z]{2}\.[A-Za-z]\."
    r"[0-9]{4}\.[0-9]{6}T[0-9]{4}Z-[0-9]{6}T[0-9]{4}Z/"
)
_WMO_HEADER = re.compile(
    r"([A-Z]{4}[0-9]{2})[ \t]+([A-Z]{4})[ \t]+([0-9]{6})"
    r"(?:[ \t]+(?:COR|AMD|(?:AA|CC|RR)[A-X]))?"
)
_SEQUENCE_HEADER = re.compile(r"[0-9]{3}")
_VALID_UGC_AREAS = frozenset(
    "AL AK AM AN AS AZ AR CA CO CT DE DC FL GA GM GU HI ID IL IN IA KS KY LA LC LE "
    "LH LM LO LS ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND MP OH OK OR PA PK "
    "PM PH PR PZ RI SC SD SL TN TX UT VT VA VI WA WV WI WY".split()
)


class NWWSParseError(ValueError):
    """Raised when an NWWS stanza cannot be handled safely."""


@dataclass(frozen=True, slots=True)
class NWWSProduct:
    issuing_office: str
    wmo_id: str
    awips_id: str
    issue_time: datetime
    process_id: str
    sequence: int
    stream_id: str
    raw_text: str

    @property
    def actionable(self) -> bool:
        return True


@dataclass(frozen=True, slots=True)
class NWWSHistory:
    """A body-only history item, which lacks actionable product metadata."""

    body: str

    @property
    def actionable(self) -> bool:
        return False


@dataclass(frozen=True, slots=True)
class SequenceDecision:
    stream_id: str
    accepted: bool
    replay: bool = False
    gap: tuple[int, int] | None = None
    process_changed: bool = False


class SequenceTracker:
    """Track a bounded window of stream IDs for one NWWS process."""

    def __init__(self, max_seen: int = 256, max_retired: int = 64):
        if not 1 <= max_seen <= 4096:
            raise ValueError("max_seen must be between 1 and 4096")
        if not 1 <= max_retired <= 4096:
            raise ValueError("max_retired must be between 1 and 4096")
        self._max_seen = max_seen
        self._max_retired = max_retired
        self._process_id: str | None = None
        self._last_sequence: int | None = None
        self._seen_order: deque[str] = deque()
        self._seen: set[str] = set()
        self._retired_order: deque[str] = deque()
        self._retired: set[str] = set()

    @property
    def seen_count(self) -> int:
        return len(self._seen)

    @property
    def retired_count(self) -> int:
        return len(self._retired)

    def observe(self, item: NWWSProduct | str) -> SequenceDecision:
        stream_id = item.stream_id if isinstance(item, NWWSProduct) else item
        if not isinstance(stream_id, str) or len(stream_id) > 76:
            raise ValueError("invalid stream id")
        process_id, separator, sequence_text = stream_id.rpartition(".")
        if (
            not separator
            or _SAFE_PROCESS.fullmatch(process_id) is None
            or not sequence_text.isascii()
            or not sequence_text.isdecimal()
            or len(sequence_text) > 10
        ):
            raise ValueError("invalid stream id")
        sequence = int(sequence_text)
        if sequence > 2_147_483_647:
            raise ValueError("invalid stream id")

        if process_id in self._retired:
            return SequenceDecision(stream_id=stream_id, accepted=False, replay=True)

        changed = self._process_id is not None and process_id != self._process_id
        if process_id != self._process_id:
            if self._process_id is not None:
                self._retired.add(self._process_id)
                self._retired_order.append(self._process_id)
                while len(self._retired_order) > self._max_retired:
                    self._retired.discard(self._retired_order.popleft())
            self._process_id = process_id
            self._last_sequence = None
            self._seen_order.clear()
            self._seen.clear()
        elif stream_id in self._seen:
            return SequenceDecision(stream_id=stream_id, accepted=False, replay=True)

        if self._last_sequence is not None and sequence <= self._last_sequence:
            return SequenceDecision(stream_id=stream_id, accepted=False, replay=True)

        gap = None
        if self._last_sequence is not None and sequence > self._last_sequence + 1:
            gap = (self._last_sequence + 1, sequence - 1)
        if self._last_sequence is None or sequence > self._last_sequence:
            self._last_sequence = sequence

        self._seen.add(stream_id)
        self._seen_order.append(stream_id)
        while len(self._seen_order) > self._max_seen:
            self._seen.discard(self._seen_order.popleft())
        return SequenceDecision(
            stream_id=stream_id,
            accepted=True,
            gap=gap,
            process_changed=changed,
        )


@dataclass(frozen=True, slots=True)
class VTEC:
    action: str
    office: str
    phenomena: str
    significance: str
    etn: int
    start: datetime | None
    end: datetime | None


@dataclass(frozen=True, slots=True)
class ProductMetadata:
    ugc: tuple[str, ...]
    vtec: VTEC | None
    event: str


_EVENT_NAMES = {
    "TO.W": "Tornado Warning",
    "SV.W": "Severe Thunderstorm Warning",
    "FF.W": "Flash Flood Warning",
    "FL.W": "Flood Warning",
    "EH.W": "Excessive Heat Warning",
    "HT.Y": "Heat Advisory",
    "WS.W": "Winter Storm Warning",
    "WW.Y": "Winter Weather Advisory",
    "FA.Y": "Flood Advisory",
}


def friendly_event_name(phenomena: str, significance: str) -> str:
    """Return a bounded friendly VTEC event name."""
    code = f"{phenomena}.{significance}"
    if (
        len(phenomena) != 2
        or not phenomena.isascii()
        or not phenomena.isalpha()
        or len(significance) != 1
        or not significance.isascii()
        or not significance.isalpha()
    ):
        return "Unknown NWS Event"
    code = code.upper()
    return _EVENT_NAMES.get(code, f"Unknown NWS Event ({code})")


def _has_forbidden_control(text: str) -> bool:
    return any(
        char not in "\t\n\r" and unicodedata.category(char) == "Cc"
        for char in text
    )


def _bounded_product_text(text: str) -> str:
    if not isinstance(text, str):
        raise TypeError("product text must be str")
    try:
        encoded_size = len(text.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise NWWSParseError("invalid product text encoding") from exc
    if encoded_size > MAX_RAW_TEXT_BYTES:
        raise NWWSParseError("product text too large")
    if _has_forbidden_control(text):
        raise NWWSParseError("control character in product text")
    return text


def _ugc_piece(piece: str, prefix: str | None) -> tuple[str | None, int, int] | None:
    """Return (prefix, low, high) for one fixed-size UGC piece."""
    left, separator, right = piece.partition(">")
    if separator and ">" in right:
        return None
    if len(left) == 6:
        candidate_prefix = left[:3]
        number = left[3:]
        if (
            candidate_prefix[:2] not in _VALID_UGC_AREAS
            or candidate_prefix[2] not in "CZ"
        ):
            return None
        prefix = candidate_prefix.upper()
    elif len(left) == 3 and prefix is not None:
        number = left
    else:
        return None
    if not number.isascii() or not number.isdecimal():
        return None
    low = int(number)
    high = low
    if separator:
        if len(right) != 3 or not right.isascii() or not right.isdecimal():
            return None
        high = int(right)
    if low < 0 or high > 999 or high < low:
        return None
    return prefix, low, high


def _valid_ugc_expiration(piece: str) -> bool:
    """Return whether a UGC purge time has the required DDHHMM shape."""
    return (
        len(piece) == 6
        and piece.isascii()
        and piece.isdecimal()
        and 1 <= int(piece[:2]) <= 31
        and int(piece[2:4]) <= 23
        and int(piece[4:]) <= 59
    )


def parse_ugc(text: str) -> tuple[str, ...]:
    """Collect bounded county/forecast-zone UGC codes from a complete UGC block."""
    text = _bounded_product_text(text)
    output: list[str] = []
    seen: set[str] = set()
    prefix: str | None = None
    collecting = False
    complete = False
    for raw_line in text.splitlines():
        line = raw_line.strip().upper()
        if not line or len(line) > 4096:
            continue
        for piece in line.split("-")[: MAX_LOCATIONS + 2]:
            if not piece:
                continue
            if collecting and _valid_ugc_expiration(piece):
                complete = True
                break
            parsed = _ugc_piece(piece, prefix)
            if parsed is None:
                continue
            prefix, low, high = parsed
            collecting = True
            for number in range(max(1, low), high + 1):
                code = f"{prefix}{number:03d}"
                if code not in seen and len(output) < MAX_LOCATIONS:
                    seen.add(code)
                    output.append(code)
        if complete:
            break
    return tuple(output) if complete else ()


def _vtec_time(value: str) -> datetime | None:
    if value == "000000T0000Z":
        return None
    if (
        len(value) != 12
        or value[6] != "T"
        or value[-1] != "Z"
        or not (value[:6] + value[7:11]).isascii()
        or not (value[:6] + value[7:11]).isdecimal()
    ):
        raise ValueError("invalid VTEC time")
    try:
        return datetime(
            2000 + int(value[:2]),
            int(value[2:4]),
            int(value[4:6]),
            int(value[7:9]),
            int(value[9:11]),
            tzinfo=timezone.utc,
        )
    except ValueError as exc:
        raise ValueError("invalid VTEC time") from exc


def parse_vtec(text: str) -> VTEC | None:
    """Parse the first valid primary operational VTEC line in bounded text."""
    text = _bounded_product_text(text)
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if len(line) > MAX_VTEC_LINE_CHARS or not line.startswith("/O.") or not line.endswith("/"):
            continue
        fields = line[1:-1].split(".")
        if len(fields) != 7:
            continue
        product_class, action, office, phenomena, significance, etn_text, times = fields
        if product_class != "O":
            continue
        if len(action) != 3 or not action.isascii() or not action.isalpha():
            continue
        if _SAFE_OFFICE.fullmatch(office) is None:
            continue
        if len(phenomena) != 2 or not phenomena.isascii() or not phenomena.isalpha():
            continue
        if len(significance) != 1 or not significance.isascii() or not significance.isalpha():
            continue
        if len(etn_text) != 4 or not etn_text.isascii() or not etn_text.isdecimal():
            continue
        start_text, separator, end_text = times.partition("-")
        if not separator or "-" in end_text:
            continue
        try:
            start = _vtec_time(start_text)
            end = _vtec_time(end_text)
        except ValueError:
            continue
        return VTEC(
            action=action.upper(),
            office=office,
            phenomena=phenomena.upper(),
            significance=significance.upper(),
            etn=int(etn_text),
            start=start,
            end=end,
        )
    return None


def parse_vtec_parameter(value: object) -> VTEC | None:
    """Parse one exact operational VTEC parameter value, without wrappers."""
    if not isinstance(value, str) or _EXACT_OPERATIONAL_VTEC.fullmatch(value) is None:
        return None
    return parse_vtec(value)


def parse_product_metadata(text: str) -> ProductMetadata:
    text = _bounded_product_text(text)
    actionable_segments = 0
    for segment in re.split(r"(?m)^[ \t]*\$\$[ \t]*$", text):
        segment_vtec = parse_vtec(segment)
        if (
            segment_vtec is not None
            and segment_vtec.action in _ACTION_MESSAGE_TYPES
            and parse_ugc(segment)
        ):
            actionable_segments += 1
            if actionable_segments > 1:
                raise NWWSParseError("multiple actionable operational segments")
    vtec = parse_vtec(text)
    event = friendly_event_name(vtec.phenomena, vtec.significance) if vtec else ""
    return ProductMetadata(ugc=parse_ugc(text), vtec=vtec, event=event)


def vtec_correlation_key(vtec: VTEC, year: int) -> str:
    """Return a source-neutral identity for one yearly VTEC event."""
    if (
        not isinstance(vtec, VTEC)
        or not isinstance(year, int)
        or isinstance(year, bool)
        or not 1 <= year <= 9999
        or _SAFE_OFFICE.fullmatch(vtec.office) is None
        or len(vtec.phenomena) != 2
        or not vtec.phenomena.isascii()
        or not vtec.phenomena.isalpha()
        or len(vtec.significance) != 1
        or not vtec.significance.isascii()
        or not vtec.significance.isalpha()
        or not isinstance(vtec.etn, int)
        or isinstance(vtec.etn, bool)
        or not 0 <= vtec.etn <= 9999
    ):
        raise ValueError("invalid VTEC correlation fields")
    return (
        f"urn:nws:vtec:{year:04d}:{vtec.office}:"
        f"{vtec.phenomena.upper()}:{vtec.significance.upper()}:{vtec.etn:04d}"
    )


def _vtec_wire_time(value: datetime | None) -> str:
    if value is None:
        return "000000T0000Z"
    if _iso_z(value) is None:
        raise ValueError("invalid VTEC time")
    return value.astimezone(timezone.utc).strftime("%y%m%dT%H%MZ")


def _canonical_vtec(vtec: VTEC) -> str:
    return (
        f"/O.{vtec.action}.{vtec.office}.{vtec.phenomena}.{vtec.significance}."
        f"{vtec.etn:04d}.{_vtec_wire_time(vtec.start)}-{_vtec_wire_time(vtec.end)}/"
    )


_ACTION_MESSAGE_TYPES = {
    "NEW": "Alert",
    "CAN": "Cancel",
    "EXP": "Cancel",
    "CON": "Update",
    "EXT": "Update",
    "EXA": "Update",
    "EXB": "Update",
}
_ZERO_TIME_FOLLOWUP_ACTIONS = frozenset({"CON", "CAN", "EXP", "EXT"})
_PREVIOUS_YEAR_GRACE_DAYS = 7


def _iso_z(value: datetime) -> str | None:
    offset = value.utcoffset()
    if offset is None or offset.total_seconds() != 0:
        return None
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def vtec_event_year(vtec: VTEC, issue: datetime) -> int:
    """Resolve the yearly ETN namespace using deterministic VTEC evidence."""
    if vtec.start is not None:
        return vtec.start.year
    if vtec.end is not None:
        return vtec.end.year
    if (
        vtec.action in _ZERO_TIME_FOLLOWUP_ACTIONS
        and issue.month == 1
        and issue.day <= _PREVIOUS_YEAR_GRACE_DAYS
        and issue.year > 1
    ):
        return issue.year - 1
    return issue.year


def project_to_feature(product: NWWSProduct) -> dict | None:
    """Project a complete product into a sanitized NWS-like alert feature.

    The raw WMO product is deliberately excluded. Incomplete or unsafe metadata
    fails closed with ``None``.
    """
    if not isinstance(product, NWWSProduct):
        return None
    if (
        not isinstance(product.issuing_office, str)
        or not isinstance(product.wmo_id, str)
        or not isinstance(product.awips_id, str)
        or not isinstance(product.issue_time, datetime)
        or not isinstance(product.process_id, str)
        or not isinstance(product.sequence, int)
        or isinstance(product.sequence, bool)
        or not isinstance(product.stream_id, str)
        or not isinstance(product.raw_text, str)
    ):
        return None
    try:
        if _SAFE_OFFICE.fullmatch(product.issuing_office) is None:
            return None
        if _SAFE_WMO.fullmatch(product.wmo_id) is None:
            return None
        if _SAFE_AWIPS.fullmatch(product.awips_id) is None:
            return None
        process_id, separator, sequence_text = product.stream_id.rpartition(".")
        if (
            not separator
            or process_id != product.process_id
            or _SAFE_PROCESS.fullmatch(process_id) is None
            or not sequence_text.isascii()
            or not sequence_text.isdecimal()
            or int(sequence_text) != product.sequence
            or product.sequence < 0
            or product.sequence > 2_147_483_647
        ):
            return None
        issued = _iso_z(product.issue_time)
        metadata = parse_product_metadata(product.raw_text)
    except (NWWSParseError, TypeError, ValueError):
        return None
    vtec = metadata.vtec
    if vtec is None or not metadata.event or not metadata.ugc:
        return None
    message_type = _ACTION_MESSAGE_TYPES.get(vtec.action)
    if message_type is None:
        return None
    expires = _iso_z(vtec.end) if vtec.end is not None else issued
    onset = _iso_z(vtec.start) if vtec.start is not None else issued
    if issued is None or expires is None or onset is None:
        return None
    if vtec.end is None and vtec.action not in _ZERO_TIME_FOLLOWUP_ACTIONS:
        return None

    stream_id = product.stream_id
    event_year = vtec_event_year(vtec, product.issue_time)
    try:
        correlation_key = vtec_correlation_key(vtec, event_year)
        canonical_vtec = _canonical_vtec(vtec)
    except (TypeError, ValueError):
        return None
    event = metadata.event[:80]
    area_desc = "; ".join(metadata.ugc)[:2048]
    headline = f"{event} issued by NWS {product.issuing_office}"[:160]
    if not area_desc:
        return None
    return {
        "id": correlation_key,
        "type": "Feature",
        "geometry": None,
        "properties": {
            "event": event,
            "headline": headline,
            "areaDesc": area_desc,
            "effective": issued,
            "onset": onset,
            "expires": expires,
            "ends": expires,
            "messageType": message_type,
            "senderName": f"NWS {product.issuing_office}",
            "geocode": {"UGC": list(metadata.ugc)},
            "parameters": {
                "NWSSource": ["NWWS-OI"],
                "NWWSStreamID": [stream_id],
                "NWWSProcessID": [product.process_id],
                "NWWSSequence": [str(product.sequence)],
                "WMOIdentifier": [product.wmo_id],
                "AWIPSIdentifier": [product.awips_id],
                "VTEC": [canonical_vtec],
                "VTECCorrelationKey": [correlation_key],
                "VTECAction": [vtec.action],
                "VTECOffice": [vtec.office],
                "VTECPhenomena": [vtec.phenomena],
                "VTECSignificance": [vtec.significance],
                "VTECEventTrackingNumber": [f"{vtec.etn:04d}"],
                "VTECStartTime": [_iso_z(vtec.start) if vtec.start is not None else ""],
                "VTECEndTime": [_iso_z(vtec.end) if vtec.end is not None else ""],
            },
        },
    }


def _bounded_xml(stanza: bytes | str) -> bytes:
    if isinstance(stanza, str):
        try:
            raw = stanza.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise NWWSParseError("invalid XML encoding") from exc
    elif isinstance(stanza, bytes):
        raw = stanza
    else:
        raise NWWSParseError("stanza must be bytes or text")
    if len(raw) > MAX_STANZA_BYTES:
        raise NWWSParseError("stanza too large")
    try:
        decoded = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise NWWSParseError("invalid XML encoding") from exc
    if _has_forbidden_control(decoded):
        raise NWWSParseError("control character in stanza")
    upper = raw.upper()
    if b"<!DOCTYPE" in upper or b"<!ENTITY" in upper:
        raise NWWSParseError("XML declarations are not allowed")
    return raw


def _required(attrs: dict[str, str], name: str) -> str:
    value = attrs.get(name)
    if value is None:
        raise NWWSParseError(f"missing {name}")
    return value


def _validated(value: str, pattern: re.Pattern[str], name: str) -> str:
    if pattern.fullmatch(value) is None:
        raise NWWSParseError(f"invalid {name}")
    return value


def _utc_datetime(value: str) -> datetime:
    if len(value) > 40:
        raise NWWSParseError("invalid issue time")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)
    except ValueError as exc:
        raise NWWSParseError("invalid issue time") from exc
    offset = parsed.utcoffset()
    if offset is None or offset.total_seconds() != 0:
        raise NWWSParseError("issue time must be UTC")
    return parsed.astimezone(timezone.utc)


def _validate_product_headers(
    text: str, *, ttaaii: str, cccc: str, awipsid: str, issue: datetime
) -> None:
    """Validate the authoritative WMO and AWIPS lines at the product start."""
    lines = text.splitlines()
    index = 0
    if lines and _SEQUENCE_HEADER.fullmatch(lines[0].strip()) is not None:
        index = 1
    if len(lines) < index + 2:
        raise NWWSParseError("invalid product headers")

    match = _WMO_HEADER.fullmatch(lines[index].strip())
    product_awips = lines[index + 1].strip()
    if match is None or _SAFE_AWIPS.fullmatch(product_awips) is None:
        raise NWWSParseError("invalid product headers")
    product_ttaaii, product_cccc, ddhhmm = match.groups()
    if (
        not 1 <= int(ddhhmm[:2]) <= 31
        or int(ddhhmm[2:4]) > 23
        or int(ddhhmm[4:]) > 59
    ):
        raise NWWSParseError("invalid product headers")
    if (product_ttaaii, product_cccc, product_awips) != (ttaaii, cccc, awipsid):
        raise NWWSParseError("product header mismatch")
    if ddhhmm != issue.strftime("%d%H%M"):
        raise NWWSParseError("product header timestamp mismatch")


def parse_nwws_stanza(stanza: bytes | str) -> NWWSProduct | NWWSHistory:
    """Parse exactly one bounded NWWS-OI XMPP message stanza.

    Body-only messages emitted by history services are returned as non-actionable
    ``NWWSHistory`` values; malformed or unsafe data raises ``NWWSParseError``.
    """
    raw_xml = _bounded_xml(stanza)
    try:
        root = ET.fromstring(raw_xml)
    except (ET.ParseError, ValueError) as exc:
        raise NWWSParseError("malformed XML") from exc
    if root.tag.rsplit("}", 1)[-1] != "message":
        raise NWWSParseError("expected message stanza")
    if root.attrib.get("type") != "groupchat":
        raise NWWSParseError("expected groupchat message")

    if any(
        child.tag in {"{urn:xmpp:delay}delay", "{jabber:x:delay}x"}
        for child in root
    ):
        return NWWSHistory(body="delayed XMPP history")

    x_children = [child for child in root if child.tag.rsplit("}", 1)[-1] == "x"]
    if any(child.tag != "{nwws-oi}x" for child in x_children):
        raise NWWSParseError("invalid x namespace")
    payloads = [child for child in x_children if child.tag == "{nwws-oi}x"]
    if not payloads:
        bodies = [child for child in root if child.tag.rsplit("}", 1)[-1] == "body"]
        if len(bodies) == 1 and not list(bodies[0]):
            body = (bodies[0].text or "").strip()
            if len(body.encode("utf-8")) > MAX_HISTORY_BYTES:
                raise NWWSParseError("history body too large")
            return NWWSHistory(body=body)
        raise NWWSParseError("missing nwws-oi payload")
    if len(payloads) != 1 or list(payloads[0]):
        raise NWWSParseError("invalid nwws-oi payload")

    payload = payloads[0]
    attrs = payload.attrib
    office = _validated(_required(attrs, "cccc"), _SAFE_OFFICE, "cccc")
    wmo_id = _validated(_required(attrs, "ttaaii"), _SAFE_WMO, "ttaaii")
    awips_id = _validated(_required(attrs, "awipsid"), _SAFE_AWIPS, "awipsid")
    issue = _utc_datetime(_required(attrs, "issue"))

    stream_id = _required(attrs, "id")
    if len(stream_id) > 76:
        raise NWWSParseError("invalid stream id")
    process_id, separator, sequence_text = stream_id.rpartition(".")
    if (
        not separator
        or _SAFE_PROCESS.fullmatch(process_id) is None
        or not sequence_text.isascii()
        or not sequence_text.isdecimal()
        or len(sequence_text) > 10
    ):
        raise NWWSParseError("invalid stream id")
    sequence = int(sequence_text)
    if sequence > 2_147_483_647:
        raise NWWSParseError("invalid stream id")

    raw_text = (payload.text or "").strip()
    if not raw_text:
        raise NWWSParseError("empty product text")
    raw_text = _bounded_product_text(raw_text)
    _validate_product_headers(
        raw_text, ttaaii=wmo_id, cccc=office, awipsid=awips_id, issue=issue
    )

    return NWWSProduct(
        issuing_office=office,
        wmo_id=wmo_id,
        awips_id=awips_id,
        issue_time=issue,
        process_id=process_id,
        sequence=sequence,
        stream_id=stream_id,
        raw_text=raw_text,
    )
