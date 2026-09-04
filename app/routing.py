"""County/event routing from NWS alerts to reusable radio destinations."""
from __future__ import annotations

from dataclasses import dataclass


_STATE_FIPS = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06",
    "CO": "08", "CT": "09", "DE": "10", "DC": "11", "FL": "12",
    "GA": "13", "HI": "15", "ID": "16", "IL": "17", "IN": "18",
    "IA": "19", "KS": "20", "KY": "21", "LA": "22", "ME": "23",
    "MD": "24", "MA": "25", "MI": "26", "MN": "27", "MS": "28",
    "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
    "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38",
    "OH": "39", "OK": "40", "OR": "41", "PA": "42", "RI": "44",
    "SC": "45", "SD": "46", "TN": "47", "TX": "48", "UT": "49",
    "VT": "50", "VA": "51", "WA": "53", "WV": "54", "WI": "55",
    "WY": "56", "AS": "60", "GU": "66", "MP": "69", "PR": "72",
    "UM": "74", "VI": "78",
}


@dataclass(frozen=True)
class RoutedDestination:
    destination_id: int
    name: str
    transport: str
    channel: int
    matched_areas: tuple[str, ...]


def _county_same_key(zone_code: str) -> str:
    """Convert a county UGC (KYC047) to SAME's state/county portion (21047)."""
    code = zone_code.strip().upper()
    state_fips = _STATE_FIPS.get(code[:2], "")
    if len(code) != 6 or code[2] != "C" or not code[3:].isdigit() or not state_fips:
        return ""
    return state_fips + code[3:]


def route_alert(db, alert) -> list[RoutedDestination]:
    props = alert.raw.get("properties", {}) or {}
    geocode = props.get("geocode", {}) or {}
    has_ugc = "UGC" in geocode
    ugc_values = [str(v).strip().upper() for v in geocode.get("UGC", []) or []]
    affected_codes = set(ugc_values)
    # SAME locations are PSSCCC: partition, state FIPS, county FIPS. A county
    # route should match any partition within that same state/county.
    same_locations = {
        value[-5:] for raw in geocode.get("SAME", []) or []
        if (value := str(raw).strip()).isdigit() and len(value) == 6
    }
    affected_names = {v.strip().casefold() for v in alert.area_desc.split(";") if v.strip()}
    matches: dict[tuple[str, int], dict] = {}
    for row in db.routing_rows():
        if not row["rule_enabled"] or not row["destination_enabled"]:
            continue
        event_ok = bool(row["all_warnings"] and alert.event.casefold().endswith("warning"))
        event_ok = event_ok or row["event"] == alert.event
        if not event_ok:
            continue
        zone_code = row["zone_code"].upper()
        county_ok = ((zone_code in affected_codes or
                      _county_same_key(zone_code) in same_locations) if has_ugc
                     else row["county_name"].strip().casefold() in affected_names)
        if not county_ok:
            continue
        key = (row["transport"], int(row["channel"]))
        entry = matches.setdefault(key, {
            "destination_id": int(row["destination_id"]),
            "name": row["destination_name"], "transport": row["transport"],
            "channel": int(row["channel"]), "areas": [],
        })
        if row["county_name"] not in entry["areas"]:
            entry["areas"].append(row["county_name"])
    return [RoutedDestination(v["destination_id"], v["name"], v["transport"],
                              v["channel"], tuple(v["areas"]))
            for v in matches.values()]