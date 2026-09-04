"""Minimal MeshWX v4 warning encoder for compatible rich-card clients.

Implements the public digitaino/meshwx 0x20/0x21 wire formats without
changing WXDispatch's existing plain-text alert delivery.
"""
from __future__ import annotations

import math
import struct
from datetime import datetime

from .models import Alert

_STATE_CODES = (
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC", "PR", "VI", "GU", "AS", "MP", "MH", "FM", "PW",
    "AN", "AM", "GM", "PK", "PZ", "PH", "CI", "CN", "US", "MX",
    "PM", "LE", "LO", "LH", "LM", "LS", "LC", "SL", "PS",
)
_STATE_INDEX = {state: index for index, state in enumerate(_STATE_CODES)}

_WARNING_OTHER = 0xF
_EVENT_TYPES = {
    "tornado warning": 0x1,
    "severe thunderstorm warning": 0x2,
    "snow squall warning": 0x2,
    "extreme wind warning": 0x2,
    "flash flood warning": 0x3,
    "flood warning": 0x4,
    "flood advisory": 0x4,
    "winter storm warning": 0x5,
    "winter storm watch": 0x5,
    "winter weather advisory": 0x5,
    "blizzard warning": 0x5,
    "high wind warning": 0x6,
    "high wind watch": 0x6,
    "wind advisory": 0x6,
    "red flag warning": 0x7,
    "fire weather watch": 0x7,
    "special marine warning": 0x8,
    "marine weather statement": 0x8,
    "special weather statement": 0x9,
}


def cobs_encode(data: bytes) -> bytes:
    output = bytearray([0])
    block_start = 0
    run_length = 1
    for byte in data:
        if byte == 0:
            output[block_start] = run_length
            block_start = len(output)
            output.append(0)
            run_length = 1
        else:
            output.append(byte)
            run_length += 1
            if run_length == 0xFF:
                output[block_start] = run_length
                block_start = len(output)
                output.append(0)
                run_length = 1
    output[block_start] = run_length
    return bytes(output)


def cobs_decode(data: bytes) -> bytes:
    output = bytearray()
    offset = 0
    while offset < len(data):
        code = data[offset]
        if code == 0:
            raise ValueError("invalid COBS data")
        offset += 1
        end = offset + code - 1
        if end > len(data):
            raise ValueError("truncated COBS data")
        output.extend(data[offset:end])
        offset = end
        if code < 0xFF and offset < len(data):
            output.append(0)
    return bytes(output)


def _unix_minutes(value: str) -> int:
    if not value:
        return 0
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    minutes = int(parsed.timestamp() // 60)
    if not 0 < minutes <= 0xFFFFFFFF:
        raise ValueError("timestamp is outside the wire-format range")
    return minutes


def _severity(alert: Alert) -> int:
    event = alert.event.lower()
    if "emergency" in event:
        return 0x4
    if "warning" in event:
        return 0x3
    if "watch" in event:
        return 0x2
    if "advisory" in event or "statement" in event:
        return 0x1
    cap = str((alert.raw.get("properties", {}) or {}).get("severity", "")).lower()
    return {"extreme": 0x4, "severe": 0x3, "moderate": 0x2}.get(cap, 0x1)


def _headline(alert: Alert) -> str:
    return (alert.headline or f"{alert.event} for {alert.area_desc}").strip()


def _fit_headline(text: str, room: int) -> bytes:
    encoded = text.encode("utf-8")
    if len(encoded) <= room:
        return encoded
    if room <= 3:
        return encoded[:room].decode("utf-8", errors="ignore").encode("utf-8")
    prefix = encoded[: room - 3].decode("utf-8", errors="ignore").rstrip()
    split = prefix.rfind(" ")
    if split > room // 2:
        prefix = prefix[:split].rstrip()
    return (prefix + "...").encode("utf-8")


def _polygon_vertices(alert: Alert) -> list[tuple[float, float]]:
    geometry = alert.raw.get("geometry") or {}
    if not isinstance(geometry, dict):
        return []
    coordinates = geometry.get("coordinates") or []
    if (
        geometry.get("type") != "Polygon"
        or not isinstance(coordinates, list)
        or not coordinates
    ):
        return []
    ring = coordinates[0]
    if not isinstance(ring, list):
        return []
    vertices = []
    for point in ring:
        if not isinstance(point, (list, tuple)) or len(point) < 2:
            return []
        lon, lat = float(point[0]), float(point[1])
        if not math.isfinite(lat) or not math.isfinite(lon):
            return []
        if not -90 <= lat <= 90 or not -180 <= lon <= 180:
            return []
        vertices.append((lat, lon))
    if len(vertices) > 1 and vertices[0] == vertices[-1]:
        vertices.pop()
    return vertices if 3 <= len(vertices) <= 20 else []


def _pack_polygon(alert: Alert, expires: int, onset: int) -> bytes | None:
    vertices = _polygon_vertices(alert)
    if not vertices:
        return None
    message = bytearray([
        0x20,
        ((_EVENT_TYPES.get(alert.event.lower(), _WARNING_OTHER) & 0xF) << 4)
        | (_severity(alert) & 0xF),
    ])
    message.extend(struct.pack(">II", expires, onset))
    message.append(len(vertices))
    lat0, lon0 = vertices[0]
    message.extend(int(lat0 * 10000).to_bytes(3, "big", signed=True))
    message.extend(int(lon0 * 10000).to_bytes(3, "big", signed=True))
    for lat, lon in vertices[1:]:
        dlat = round((lat - lat0) / 0.001)
        dlon = round((lon - lon0) / 0.001)
        if not -32768 <= dlat <= 32767 or not -32768 <= dlon <= 32767:
            return None
        message.extend(struct.pack(">hh", dlat, dlon))
    message.extend(_fit_headline(_headline(alert), max(0, 136 - len(message))))
    return bytes(message)


def _pack_zones(alert: Alert, expires: int, onset: int) -> bytes | None:
    props = alert.raw.get("properties", {}) or {}
    geocode = props.get("geocode", {}) or {}
    zones = []
    seen = set()
    for raw in geocode.get("UGC", []) or []:
        code = str(raw).strip().upper()
        if (len(code) == 6 and code[2] == "Z" and code[:2] in _STATE_INDEX
                and code[3:].isdigit() and code not in seen):
            zones.append(code)
            seen.add(code)
    zones = zones[:28]
    if not zones:
        return None
    message = bytearray([
        0x21,
        ((_EVENT_TYPES.get(alert.event.lower(), _WARNING_OTHER) & 0xF) << 4)
        | (_severity(alert) & 0xF),
    ])
    message.extend(struct.pack(">II", expires, onset))
    message.append(len(zones))
    for code in zones:
        message.append(_STATE_INDEX[code[:2]])
        message.extend(struct.pack(">H", int(code[3:])))
    message.extend(_fit_headline(_headline(alert), max(0, 136 - len(message))))
    return bytes(message)


def encode_alert(alert: Alert, *, sequence: int) -> bytes | None:
    """Encode one NWS alert as a COBS-wrapped MeshWX v4 warning frame."""
    if (
        isinstance(sequence, bool)
        or not isinstance(sequence, int)
        or not 0 <= sequence <= 0xFFFF
    ):
        return None
    try:
        expires = _unix_minutes(alert.ends or alert.expires)
        onset = _unix_minutes(alert.onset)
    except (TypeError, ValueError, OverflowError):
        return None
    if not expires:
        return None
    try:
        inner = _pack_zones(alert, expires, onset) or _pack_polygon(
            alert, expires, onset
        )
    except (
        AttributeError, IndexError, TypeError, ValueError, OverflowError, struct.error
    ):
        return None
    if inner is None:
        return None
    wrapped = struct.pack(">BBBBH", 0x04, inner[0], 0, 0, sequence) + inner[1:]
    encoded = cobs_encode(wrapped)
    return encoded if len(encoded) <= 163 else None
