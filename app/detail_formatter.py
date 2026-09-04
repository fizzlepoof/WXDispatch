"""Build one concise, source-grounded follow-up from an NWS alert."""
from __future__ import annotations

import re

from .config import MAX_PAYLOAD_BYTES

PREFIX = "DETAIL: "
_URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
_SECTION_RE = re.compile(
    r"(?:^|\n)\s*\*?\s*([A-Z][A-Z ]{1,30})\s*(?:\.\.\.|:)\s*(.*?)"
    r"(?=(?:\n\s*\n?\s*\*?\s*[A-Z][A-Z ]{1,30}\s*(?:\.\.\.|:))|\Z)",
    re.DOTALL,
)


def _clean(value: object) -> str:
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]+", " ", str(value or ""))
    text = _URL_RE.sub("", text)
    text = text.replace("…", "...")
    text = re.sub(r"\s+", " ", text).strip(" -\n\t")
    return re.sub(r"\s+([,.;:!?])", r"\1", text)


def _sentences(value: str) -> list[str]:
    text = _clean(value)
    if not text:
        return []
    return [part.strip() for part in re.split(r"(?<=[.!?])\s+", text) if part.strip()]


def _sections(description: str) -> dict[str, str]:
    return {
        key.strip().upper(): _clean(value)
        for key, value in _SECTION_RE.findall(description or "")
        if _clean(value)
    }


def _parameter(props: dict, name: str) -> str:
    values = (props.get("parameters", {}) or {}).get(name) or []
    return _clean(values[0]) if values else ""


def _format_hail(value: str) -> str:
    match = re.search(r"(?:UP TO\s*)?(\d*\.?\d+)", value, re.IGNORECASE)
    if not match:
        return ""
    try:
        numeric = float(match.group(1))
    except ValueError:
        return ""
    if numeric <= 0:
        return ""
    number = f"{numeric:g}"
    if number.startswith("."):
        number = "0" + number
    return number + " in hail"


def _structured_fact(props: dict) -> str:
    wind = _parameter(props, "maxWindGust")
    hail = _format_hail(_parameter(props, "maxHailSize"))
    if not wind and not hail:
        return ""
    pieces = []
    if wind:
        pieces.append(wind.lower() + " winds")
    if hail:
        pieces.append(hail)
    source = _parameter(props, "windThreat") or _parameter(props, "hailThreat")
    lead = "Radar indicated " if "RADAR" in source.upper() else ""
    return lead + " and ".join(pieces) + "."


def _fact(alert) -> str:
    props = (getattr(alert, "raw", {}) or {}).get("properties", {}) or {}
    description = str(props.get("description") or "")
    sections = _sections(description)
    event = str(getattr(alert, "event", "")).casefold()
    structured = _structured_fact(props)
    if structured:
        return structured
    if "flood" in event:
        for sentence in _sentences(description):
            lowered = sentence.casefold()
            if "inch" in lowered and "rain" in lowered and "fallen" in lowered:
                return sentence
    if "red flag" in event or "fire weather" in event:
        wind = (_sentences(sections.get("WIND", "")) or [""])[0]
        humidity = (_sentences(sections.get("HUMIDITY", "")) or [""])[0]
        if wind and humidity:
            return f"{wind.rstrip('.')}; humidity {humidity[0].lower() + humidity[1:]}"
    for label in ("WHAT", "HAZARD", "IMPACTS", "IMPACT"):
        values = _sentences(sections.get(label, ""))
        if values:
            return values[0]
    candidates = _sentences(description)
    for sentence in candidates:
        upper = sentence.upper()
        if upper.startswith(("WHERE", "WHEN", "LOCATIONS IMPACTED", "ADDITIONAL DETAILS")):
            continue
        if re.match(r"THE NATIONAL WEATHER SERVICE\b.*\bHAS ISSUED\b", upper):
            continue
        if "FOR MORE INFORMATION" in upper:
            continue
        return sentence
    return ""


def _action(alert) -> str:
    props = (getattr(alert, "raw", {}) or {}).get("properties", {}) or {}
    instruction = _clean(props.get("instruction"))
    if not instruction:
        return ""
    lower = instruction.casefold()
    if "interior room" in lower and "lowest floor" in lower:
        return "Move to an interior room on the lowest floor."
    if "turn around" in lower and "don't drown" in lower:
        return "Turn around, don't drown."
    if ("drink plenty of fluids" in lower and "air-conditioned" in lower
            and "stay out of the sun" in lower
            and ("check up on" in lower or "check on" in lower)):
        return "Drink fluids, stay in A/C, avoid sun, and check on others."
    if ("slow down" in lower and "low-beam" in lower
            and "leave plenty of distance" in lower):
        return "Slow down, use low beams, and leave extra distance."
    values = _sentences(instruction)
    return values[0] if values else ""


def _fit_sentence(value: str, max_bytes: int) -> str:
    value = _clean(value)
    if len(value.encode("utf-8")) <= max_bytes:
        return value
    words = value.split()
    kept: list[str] = []
    for word in words:
        candidate = " ".join([*kept, word]).rstrip(".,;:") + "…"
        if len(candidate.encode("utf-8")) > max_bytes:
            break
        kept.append(word)
    return (" ".join(kept).rstrip(".,;:") + "…") if kept else ""


def build_alert_detail(alert, max_bytes: int = MAX_PAYLOAD_BYTES) -> str:
    """Return one <=max_bytes detail message, or empty when NWS supplied no detail."""
    event = str(getattr(alert, "event", "")).strip().casefold()
    props = (getattr(alert, "raw", {}) or {}).get("properties", {}) or {}
    status = _clean(props.get("status")).casefold()
    if status and status != "actual":
        return ""
    if event == "test message":
        return ""
    source_description = _clean(props.get("description")).upper()
    if (event.endswith("watch") and "THIS WATCH INCLUDES" in source_description
            and ("REMAINS VALID" in source_description or "HAS ISSUED" in source_description)):
        return ""
    fact = _fact(alert)
    action = _action(alert)
    upper_fact = fact.upper()
    if re.match(r"^[A-Z]{6}\.\.\.", upper_fact) or upper_fact == "LOCATIONS IMPACTED...":
        fact = ""
    if (event.endswith("watch") and "REMAINS VALID" in upper_fact
            and "THIS WATCH INCLUDES" in upper_fact):
        fact = ""
    if fact and action and _clean(fact).casefold() == _clean(action).casefold():
        action = ""
    if not fact and not action:
        return ""
    parts = [part for part in (fact, action) if part]
    full = PREFIX + " ".join(parts)
    if len(full.encode("utf-8")) <= max_bytes:
        return full

    budget = max_bytes - len(PREFIX.encode("utf-8"))
    if budget <= 0:
        return ""
    if fact and action:
        action_bytes = min(len(action.encode("utf-8")), budget // 2)
        short_action = _fit_sentence(action, action_bytes)
        fact_budget = budget - len(short_action.encode("utf-8")) - (1 if short_action else 0)
        short_fact = _fit_sentence(fact, max(0, fact_budget))
        combined = " ".join(part for part in (short_fact, short_action) if part)
    else:
        combined = _fit_sentence(parts[0], budget)
    return PREFIX + combined if combined else ""
