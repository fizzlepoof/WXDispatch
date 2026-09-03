"""County/event routing from NWS alerts to reusable radio destinations."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RoutedDestination:
    destination_id: int
    name: str
    transport: str
    channel: int
    matched_areas: tuple[str, ...]


def route_alert(db, alert) -> list[RoutedDestination]:
    props = alert.raw.get("properties", {}) or {}
    geocode = props.get("geocode", {}) or {}
    has_ugc = "UGC" in geocode
    affected_codes = {str(v).strip().upper() for v in geocode.get("UGC", []) or []}
    affected_names = {v.strip().casefold() for v in alert.area_desc.split(";") if v.strip()}
    matches: dict[tuple[str, int], dict] = {}
    for row in db.routing_rows():
        if not row["rule_enabled"] or not row["destination_enabled"]:
            continue
        event_ok = bool(row["all_warnings"] and alert.event.casefold().endswith("warning"))
        event_ok = event_ok or row["event"] == alert.event
        if not event_ok:
            continue
        county_ok = (row["zone_code"].upper() in affected_codes if has_ugc
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