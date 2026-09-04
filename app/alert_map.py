"""Safe local-area map data from county-scoped NWS alert queries."""
from __future__ import annotations

import asyncio
import json
import math
import re
import time
from datetime import datetime, timezone
from typing import Any

import httpx

from . import __version__


def _text(value: Any, limit: int = 500) -> str:
    return str(value or "").strip()[:limit]


def _county_key(value: Any) -> str:
    name = _text(value, 100).casefold()
    return name[:-7].strip() if name.endswith(" county") else name


_MAX_GEOMETRY_POINTS = 20_000
_MAX_COLLECTION_GEOMETRY_POINTS = 100_000
_MAX_RESPONSE_BYTES = 2_000_000
_MAX_ALERT_FEATURES_PER_COUNTY = 100
_MAX_MAP_COUNTIES = 100
_FAILURE_RETRY_SECONDS = 30
_COUNTY_CODE = re.compile(r"^[A-Z]{2}C\d{3}$")


def configured_counties(db) -> list[dict]:
    """Return the union of legacy county zones and every manual route county."""
    counties: dict[str, str] = {}
    for raw_code in _text(db.get_setting("zones", ""), 10_000).split(","):
        code = raw_code.strip().upper()
        if _COUNTY_CODE.fullmatch(code) and code not in counties:
            counties[code] = code
    list_routes = getattr(db, "list_routes", None)
    routes = list_routes() if callable(list_routes) else []
    if not isinstance(routes, (list, tuple)):
        routes = []
    for route in routes:
        if not isinstance(route, dict):
            continue
        route_counties = route.get("counties", [])
        if not isinstance(route_counties, list):
            continue
        for county in route_counties:
            if not isinstance(county, dict):
                continue
            code = _text(county.get("zone_code"), 16).upper()
            if _COUNTY_CODE.fullmatch(code):
                counties[code] = _text(county.get("county_name"), 100) or code
    return [{"code": code, "name": name} for code, name in counties.items()]


def _position(value: Any) -> list[float] | None:
    if not isinstance(value, (list, tuple)) or len(value) < 2:
        return None
    lon, lat = value[0], value[1]
    if (not isinstance(lon, (int, float)) or isinstance(lon, bool)
            or not isinstance(lat, (int, float)) or isinstance(lat, bool)):
        return None
    lon, lat = float(lon), float(lat)
    if not math.isfinite(lon) or not math.isfinite(lat):
        return None
    if not -180 <= lon <= 180 or not -90 <= lat <= 90:
        return None
    return [lon, lat]


def _polygon(value: Any, count: list[int]) -> list | None:
    if not isinstance(value, list) or not value:
        return None
    polygon = []
    for raw_ring in value:
        if not isinstance(raw_ring, list) or len(raw_ring) < 4:
            return None
        ring = []
        for raw_position in raw_ring:
            position = _position(raw_position)
            if position is None:
                return None
            count[0] += 1
            if count[0] > _MAX_GEOMETRY_POINTS:
                return None
            ring.append(position)
        if ring[0] != ring[-1]:
            return None
        polygon.append(ring)
    return polygon


def safe_geometry(value: Any, budget: list[int] | None = None) -> dict | None:
    """Copy a bounded, valid Polygon/MultiPolygon without retaining other fields."""
    if not isinstance(value, dict):
        return None
    geometry_type = value.get("type")
    coordinates = value.get("coordinates")
    count = [0]
    if geometry_type == "Polygon":
        safe = _polygon(coordinates, count)
    elif geometry_type == "MultiPolygon" and isinstance(coordinates, list):
        safe = []
        for raw_polygon in coordinates:
            polygon = _polygon(raw_polygon, count)
            if polygon is None:
                return None
            safe.append(polygon)
        if not safe:
            return None
    else:
        return None
    if safe is None:
        return None
    if budget is not None:
        if budget[0] + count[0] > _MAX_COLLECTION_GEOMETRY_POINTS:
            return None
        budget[0] += count[0]
    return {"type": geometry_type, "coordinates": safe}


async def _fetch_json(url: str, headers: dict, timeout: float) -> dict:
    async with httpx.AsyncClient(timeout=timeout) as client:
        async with client.stream("GET", url, headers=headers) as response:
            response.raise_for_status()
            chunks = []
            size = 0
            async for chunk in response.aiter_bytes():
                size += len(chunk)
                if size > _MAX_RESPONSE_BYTES:
                    raise ValueError("NWS response exceeded the size limit")
                chunks.append(chunk)
    data = json.loads(b"".join(chunks))
    if not isinstance(data, dict):
        raise ValueError("NWS response was not an object")
    return data


class ZoneGeometryCache:
    """Bounded in-memory cache for sanitized NWS county-zone boundaries."""

    def __init__(self, fetch_json=None, ttl_seconds: float = 21_600,
                 clock=time.monotonic):
        self._fetch_json = fetch_json or _fetch_json
        self._ttl_seconds = ttl_seconds
        self._clock = clock
        self._cache: dict[str, tuple[float, dict, str]] = {}
        self._retry_after: dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def get_many(self, counties: list[dict], contact: str) -> tuple[list[dict], list[str]]:
        async with self._lock:
            return await self._get_many(counties, contact)

    async def _get_many(self, counties: list[dict], contact: str) -> tuple[list[dict], list[str]]:
        features = []
        selected = counties[:_MAX_MAP_COUNTIES]
        overflow = counties[_MAX_MAP_COUNTIES:]
        errors = [
            code for county in overflow if isinstance(county, dict)
            and _COUNTY_CODE.fullmatch(code := _text(county.get("code"), 16).upper())
        ]
        selected_codes = {
            code for county in selected if isinstance(county, dict)
            and _COUNTY_CODE.fullmatch(code := _text(county.get("code"), 16).upper())
        }
        self._cache = {code: value for code, value in self._cache.items()
                       if code in selected_codes}
        self._retry_after = {code: value for code, value in self._retry_after.items()
                             if code in selected_codes}
        now = self._clock()
        safe_contact = _text(contact, 320) or "https://github.com/fizzlepoof/WXDispatch"
        headers = {
            "User-Agent": f"WXDispatch/{__version__} ({safe_contact})",
            "Accept": "application/geo+json",
        }
        source_budget = [0]

        async def resolve(county):
            if not isinstance(county, dict):
                return None
            code = _text(county.get("code"), 16).upper()
            if not _COUNTY_CODE.fullmatch(code):
                return None
            cached = self._cache.get(code)
            if cached and now < cached[0]:
                geometry, nws_name = cached[1], cached[2]
                return code, county, geometry, nws_name, False
            if now < self._retry_after.get(code, 0):
                if cached:
                    return code, county, cached[1], cached[2], True
                return code, county, None, "", True
            else:
                try:
                    data = await self._fetch_json(
                        "https://api.weather.gov/zones/county/" + code,
                        headers, 20.0,
                    )
                    geometry = safe_geometry(data.get("geometry"), budget=source_budget)
                    props = data.get("properties")
                    nws_name = _text(
                        props.get("name") if isinstance(props, dict) else "", 100,
                    )
                    if geometry is None:
                        raise ValueError("NWS county response had no valid geometry")
                except Exception:
                    self._retry_after[code] = now + _FAILURE_RETRY_SECONDS
                    if cached:
                        return code, county, cached[1], cached[2], True
                    return code, county, None, "", True
                self._cache[code] = (now + self._ttl_seconds, geometry, nws_name)
                self._retry_after.pop(code, None)
            return code, county, geometry, nws_name, False

        results = await asyncio.gather(*(resolve(county) for county in selected))
        budget = [0]
        for result in results:
            if result is None:
                continue
            code, county, geometry, nws_name, had_error = result
            if had_error and code not in errors:
                errors.append(code)
            if geometry is None:
                if code not in errors:
                    errors.append(code)
                continue
            geometry = safe_geometry(geometry, budget=budget)
            if geometry is None:
                errors.append(code)
                continue
            configured_name = _text(county.get("name"), 100)
            name = configured_name if configured_name and configured_name != code else (
                (nws_name + " County") if nws_name else code
            )
            features.append({
                "type": "Feature",
                "geometry": geometry,
                "properties": {"code": code, "name": name},
            })
        return features, errors


def _project_alert(feature: Any, local_codes: list[str],
                   code_to_name: dict[str, str], current_time: datetime,
                   geometry_budget: list[int] | None = None) -> dict | None:
    if not isinstance(feature, dict):
        return None
    props = feature.get("properties")
    if not isinstance(props, dict):
        return None
    end_value = _text(props.get("ends") or props.get("expires"), 64)
    if end_value:
        try:
            end_time = datetime.fromisoformat(end_value.replace("Z", "+00:00"))
            if end_time.tzinfo is None:
                end_time = end_time.replace(tzinfo=timezone.utc)
            if end_time <= current_time:
                return None
        except ValueError:
            pass
    return {
        "id": _text(feature.get("id") or props.get("id") or props.get("@id"), 500),
        "event": _text(props.get("event"), 160),
        "headline": _text(props.get("headline"), 500),
        "area": _text(props.get("areaDesc"), 1000),
        "severity": _text(props.get("severity"), 32),
        "urgency": _text(props.get("urgency"), 32),
        "certainty": _text(props.get("certainty"), 32),
        "onset": _text(props.get("onset") or props.get("effective"), 64),
        "ends": _text(props.get("ends"), 64),
        "expires": _text(props.get("expires"), 64),
        "local_zones": local_codes,
        "local_counties": [code_to_name[code] for code in local_codes],
        "geometry": safe_geometry(feature.get("geometry"), budget=geometry_budget),
    }


class CountyAlertCache:
    """Short-lived, county-tagged NWS alert cache for the local map only."""

    def __init__(self, fetch_json=None, ttl_seconds: float = 120,
                 clock=time.monotonic, wall_clock=lambda: datetime.now(timezone.utc)):
        self._fetch_json = fetch_json or _fetch_json
        self._ttl_seconds = ttl_seconds
        self._clock = clock
        self._wall_clock = wall_clock
        self._cache: dict[str, tuple[float, list[dict], str]] = {}
        self._retry_after: dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def get_many(self, counties: list[dict], contact: str) -> tuple[list[dict], list[str], bool, str]:
        async with self._lock:
            return await self._get_many(counties, contact)

    async def _get_many(self, counties: list[dict], contact: str) -> tuple[list[dict], list[str], bool, str]:
        now = self._clock()
        safe_contact = _text(contact, 320) or "https://github.com/fizzlepoof/WXDispatch"
        headers = {
            "User-Agent": f"WXDispatch/{__version__} ({safe_contact})",
            "Accept": "application/geo+json",
        }
        selected = counties[:_MAX_MAP_COUNTIES]
        overflow = counties[_MAX_MAP_COUNTIES:]
        overflow_errors = [
            code for county in overflow if isinstance(county, dict)
            and _COUNTY_CODE.fullmatch(code := _text(county.get("code"), 16).upper())
        ]
        code_to_name = {
            _text(county.get("code"), 16).upper(): _text(county.get("name"), 100)
            for county in selected if isinstance(county, dict)
            and _COUNTY_CODE.fullmatch(_text(county.get("code"), 16).upper())
        }
        self._cache = {code: value for code, value in self._cache.items()
                       if code in code_to_name}
        self._retry_after = {code: value for code, value in self._retry_after.items()
                             if code in code_to_name}
        per_county: dict[str, list[dict]] = {}
        county_updated: dict[str, str] = {}
        errors: list[str] = overflow_errors
        stale = False
        updated_times: list[str] = []

        source_geometry_budget = [0]

        async def resolve(code):
            cached = self._cache.get(code)
            if cached and now < cached[0]:
                return code, cached[1], cached[2], "", False
            if now < self._retry_after.get(code, 0):
                if cached:
                    return code, cached[1], cached[2], code, True
                return code, [], "", code, False
            try:
                data = await self._fetch_json(
                    "https://api.weather.gov/alerts/active?zone=" + code,
                    headers, 20.0,
                )
                features = data.get("features")
                if not isinstance(features, list):
                    raise ValueError("NWS alert response had no feature list")
                if len(features) > _MAX_ALERT_FEATURES_PER_COUNTY:
                    raise ValueError("NWS alert response exceeded the feature limit")
                fetched_at = self._wall_clock()
                if fetched_at.tzinfo is None:
                    fetched_at = fetched_at.replace(tzinfo=timezone.utc)
                alerts = []
                for feature in features:
                    alert = _project_alert(
                        feature, [code], code_to_name, fetched_at,
                        geometry_budget=source_geometry_budget,
                    )
                    if alert is not None:
                        alerts.append(alert)
                updated_at = fetched_at.isoformat()
                self._cache[code] = (now + self._ttl_seconds, alerts, updated_at)
                self._retry_after.pop(code, None)
                return code, alerts, updated_at, "", False
            except Exception:
                self._retry_after[code] = now + _FAILURE_RETRY_SECONDS
                if cached:
                    return code, cached[1], cached[2], code, True
                return code, [], "", code, False

        results = await asyncio.gather(*(resolve(code) for code in code_to_name))
        for code, alerts, updated_at, error, is_stale in results:
            per_county[code] = alerts
            if updated_at:
                county_updated[code] = updated_at
                updated_times.append(updated_at)
            if error:
                errors.append(error)
            stale = stale or is_stale

        merged: dict[str, dict] = {}
        merged_updated: dict[str, str] = {}
        for code in code_to_name:
            for alert in per_county.get(code, []):
                key = alert["id"] or "\x1f".join(
                    str(alert[field]) for field in ("event", "headline", "onset", "expires")
                )
                existing = merged.get(key)
                if existing is None:
                    merged[key] = {**alert, "local_zones": [code],
                                   "local_counties": [code_to_name[code]]}
                    merged_updated[key] = county_updated.get(code, "")
                elif code not in existing["local_zones"]:
                    zones = existing["local_zones"] + [code]
                    names = existing["local_counties"] + [code_to_name[code]]
                    source_updated = county_updated.get(code, "")
                    if source_updated > merged_updated[key]:
                        geometry = alert["geometry"] or existing["geometry"]
                        merged[key] = {**alert, "geometry": geometry,
                                       "local_zones": zones, "local_counties": names}
                        merged_updated[key] = source_updated
                    else:
                        existing["local_zones"] = zones
                        existing["local_counties"] = names
                        if existing["geometry"] is None and alert["geometry"] is not None:
                            existing["geometry"] = alert["geometry"]
        updated_at = min(updated_times) if updated_times else ""
        current_time = self._wall_clock()
        if current_time.tzinfo is None:
            current_time = current_time.replace(tzinfo=timezone.utc)
        current_alerts = []
        geometry_budget = [0]
        for alert in merged.values():
            end_value = alert.get("ends") or alert.get("expires")
            if end_value:
                try:
                    end_time = datetime.fromisoformat(str(end_value).replace("Z", "+00:00"))
                    if end_time.tzinfo is None:
                        end_time = end_time.replace(tzinfo=timezone.utc)
                    if end_time <= current_time:
                        continue
                except ValueError:
                    pass
            alert["geometry"] = safe_geometry(alert["geometry"], budget=geometry_budget)
            current_alerts.append(alert)
        return current_alerts, errors, stale, updated_at


def build_current_alerts(raw: str, counties: list[dict],
                         now: datetime | None = None) -> tuple[list[dict], str]:
    """Return current alerts intersecting configured counties, with safe fields only."""
    if not raw:
        return [], "Current NWS alert data is not available yet."
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return [], "Current NWS alert data is unavailable."
    features = data.get("features") if isinstance(data, dict) else None
    if not isinstance(features, list):
        return [], "Current NWS alert data is unavailable."

    code_to_name = {
        _text(county.get("code"), 16).upper(): _text(county.get("name"), 100)
        for county in counties if isinstance(county, dict) and county.get("code")
    }
    current_time = now or datetime.now(timezone.utc)
    if current_time.tzinfo is None:
        current_time = current_time.replace(tzinfo=timezone.utc)
    alerts = []
    for feature in features:
        if not isinstance(feature, dict):
            continue
        props = feature.get("properties")
        if not isinstance(props, dict):
            continue
        geocode = props.get("geocode")
        if isinstance(geocode, dict) and "UGC" in geocode:
            ugc = geocode.get("UGC") or []
            affected = {_text(code, 16).upper() for code in ugc if _text(code, 16)}
            local_codes = [code for code in code_to_name if code in affected]
        elif isinstance(props.get("affectedZones"), list):
            affected = {
                _text(url, 500).rstrip("/").rsplit("/", 1)[-1].upper()
                for url in props["affectedZones"] if _text(url, 500)
            }
            local_codes = [code for code in code_to_name if code in affected]
        else:
            names = {_county_key(name) for name in _text(
                props.get("areaDesc"), 2000,
            ).split(";") if _county_key(name)}
            local_codes = [
                code for code, name in code_to_name.items()
                if _county_key(name) in names
            ]
        if not local_codes:
            continue
        alert = _project_alert(feature, local_codes, code_to_name, current_time)
        if alert is not None:
            alerts.append(alert)
    return alerts, ""