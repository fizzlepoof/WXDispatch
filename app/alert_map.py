"""Safe local-area map data from bounded regional NWS queries."""
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
from .routing import county_same_key


def _text(value: Any, limit: int = 500) -> str:
    return str(value or "").strip()[:limit]


def _county_key(value: Any) -> str:
    name = _text(value, 100).casefold()
    return name[:-7].strip() if name.endswith(" county") else name


_MAX_GEOMETRY_POINTS = 20_000
_MAX_COLLECTION_GEOMETRY_POINTS = 100_000
_MAX_RESPONSE_BYTES = 2_000_000
_MAX_ALERT_FEATURES_PER_COUNTY = 100
_MAX_ALERT_FEATURES_PER_AREA = 500
_MAX_MAP_COUNTIES = 100
_MAX_MAP_AREAS = 10
_MAX_ALERT_ZONES = 100
_MAX_REGIONAL_MAP_ZONES = 300
_MAX_ZONE_FEATURES_PER_AREA = 500
_FAILURE_RETRY_SECONDS = 30
_COUNTY_CODE = re.compile(r"^[A-Z]{2}C\d{3}$")
_ZONE_CODE = re.compile(r"^[A-Z]{2}[CZ]\d{3}$")


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
    """Bounded cache for sanitized NWS county and forecast-zone boundaries."""

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
            and _ZONE_CODE.fullmatch(code := _text(county.get("code"), 16).upper())
        ]
        selected_codes = {
            code for county in selected if isinstance(county, dict)
            and _ZONE_CODE.fullmatch(code := _text(county.get("code"), 16).upper())
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
            if not _ZONE_CODE.fullmatch(code):
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
                    zone_kind = "county" if code[2] == "C" else "forecast"
                    data = await self._fetch_json(
                        f"https://api.weather.gov/zones/{zone_kind}/" + code,
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
                (nws_name + " County") if nws_name and code[2] == "C" else (nws_name or code)
            )
            properties: dict[str, Any] = {"code": code, "name": name}
            if "watched" in county:
                properties["watched"] = bool(county.get("watched"))
            features.append({
                "type": "Feature",
                "geometry": geometry,
                "properties": properties,
            })
        return features, errors


class RegionalZoneGeometryCache:
    """Cache bounded state zone collections for the regional alert map."""

    def __init__(self, fetch_json=None, ttl_seconds: float = 21_600,
                 clock=time.monotonic):
        self._fetch_json = fetch_json or _fetch_json
        self._ttl_seconds = ttl_seconds
        self._clock = clock
        self._cache: dict[
            tuple[str, str], tuple[float, dict[str, tuple[str, frozenset[str]]]]
        ] = {}
        self._retry_after: dict[tuple[str, str], float] = {}
        self._geometry_cache = ZoneGeometryCache(
            fetch_json=self._fetch_json, ttl_seconds=ttl_seconds, clock=clock,
        )
        self._lock = asyncio.Lock()

    async def get_many(self, zones: list[dict], contact: str) -> tuple[list[dict], list[str], list[str]]:
        async with self._lock:
            return await self._get_many(zones, contact)

    async def _get_many(self, zones: list[dict], contact: str) -> tuple[list[dict], list[str], list[str]]:
        candidates = []
        seen_codes = set()
        for zone in zones:
            if not isinstance(zone, dict):
                continue
            code = _text(zone.get("code"), 16).upper()
            if _ZONE_CODE.fullmatch(code) and code not in seen_codes:
                candidates.append({**zone, "code": code})
                seen_codes.add(code)
        watched_states = {
            zone["code"][:2] for zone in candidates if zone.get("watched")
        }
        requested = [
            zone for zone in candidates if zone["code"][:2] in watched_states
        ]
        selected = requested[:_MAX_REGIONAL_MAP_ZONES]
        errors = [zone["code"] for zone in requested[_MAX_REGIONAL_MAP_ZONES:]]
        groups = list(dict.fromkeys(
            (zone["code"][:2], "county" if zone["code"][2] == "C" else "forecast")
            for zone in selected
        ))
        self._cache = {key: value for key, value in self._cache.items() if key in groups}
        self._retry_after = {
            key: value for key, value in self._retry_after.items() if key in groups
        }
        now = self._clock()
        safe_contact = _text(contact, 320) or "https://github.com/fizzlepoof/WXDispatch"
        headers = {
            "User-Agent": f"WXDispatch/{__version__} ({safe_contact})",
            "Accept": "application/geo+json",
        }

        async def resolve(key):
            cached = self._cache.get(key)
            if cached and now < cached[0]:
                return key, cached[1], False
            if now < self._retry_after.get(key, 0):
                return key, cached[1] if cached else {}, True
            area, zone_kind = key
            try:
                data = await self._fetch_json(
                    f"https://api.weather.gov/zones?type={zone_kind}&area={area}",
                    headers, 20.0,
                )
                raw_features = data.get("features")
                if not isinstance(raw_features, list):
                    raise ValueError("NWS zone collection had no feature list")
                if len(raw_features) > _MAX_ZONE_FEATURES_PER_AREA:
                    raise ValueError("NWS zone collection exceeded the feature limit")
                collection: dict[str, tuple[str, frozenset[str]]] = {}
                expected_type = "C" if zone_kind == "county" else "Z"
                for raw_feature in raw_features:
                    if not isinstance(raw_feature, dict):
                        continue
                    props = raw_feature.get("properties")
                    if not isinstance(props, dict):
                        continue
                    code = _text(props.get("id"), 16).upper()
                    if (not _ZONE_CODE.fullmatch(code) or code[:2] != area
                            or code[2] != expected_type):
                        continue
                    cwas = frozenset(
                        _text(value, 8).upper() for value in (props.get("cwa") or [])
                        if re.fullmatch(r"[A-Z0-9]{3}", _text(value, 8).upper())
                    ) if isinstance(props.get("cwa"), list) else frozenset()
                    collection[code] = (_text(props.get("name"), 100), cwas)
                self._cache[key] = (now + self._ttl_seconds, collection)
                self._retry_after.pop(key, None)
                return key, collection, False
            except Exception:
                self._retry_after[key] = now + _FAILURE_RETRY_SECONDS
                return key, cached[1] if cached else {}, True

        results = await asyncio.gather(*(resolve(key) for key in groups))
        collections = {key: collection for key, collection, _error in results}
        failed_groups = {key for key, _collection, error in results if error}
        watched_cwas: set[str] = set()
        for zone in selected:
            if not zone.get("watched"):
                continue
            code = zone["code"]
            key = (code[:2], "county" if code[2] == "C" else "forecast")
            entry = collections.get(key, {}).get(code)
            if entry is not None:
                watched_cwas.update(entry[1])

        scoped = []
        for zone in selected:
            code = zone["code"]
            key = (code[:2], "county" if code[2] == "C" else "forecast")
            entry = collections.get(key, {}).get(code)
            if key in failed_groups and code not in errors:
                errors.append(code)
            if entry is None:
                if zone.get("watched"):
                    scoped.append(zone)
                continue
            nws_name, cwas = entry
            if zone.get("watched") or watched_cwas.intersection(cwas):
                configured_name = _text(zone.get("name"), 100)
                name = configured_name if configured_name and configured_name != code else (
                    (nws_name + " County")
                    if nws_name and code[2] == "C" else (nws_name or code)
                )
                scoped.append({**zone, "name": name})

        features, geometry_errors = await self._geometry_cache.get_many(scoped, contact)
        for code in geometry_errors:
            if code not in errors:
                errors.append(code)
        return features, errors, [zone["code"] for zone in scoped]


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


def _watched_codes(feature: dict, code_to_name: dict[str, str]) -> list[str]:
    props = feature.get("properties")
    if not isinstance(props, dict):
        return []
    geocode = props.get("geocode")
    if isinstance(geocode, dict) and "UGC" in geocode:
        ugc_values = geocode.get("UGC")
        same_values = geocode.get("SAME")
        affected = {
            _text(code, 16).upper()
            for code in (ugc_values if isinstance(ugc_values, list) else [])
            if _text(code, 16)
        }
        same_locations = {
            value[-5:]
            for raw in (same_values if isinstance(same_values, list) else [])
            if (value := _text(raw, 16)).isdigit() and len(value) == 6
        }
        return [
            code for code in code_to_name
            if code in affected or county_same_key(code) in same_locations
        ]
    if isinstance(props.get("affectedZones"), list):
        affected = {
            _text(url, 500).rstrip("/").rsplit("/", 1)[-1].upper()
            for url in props["affectedZones"] if _text(url, 500)
        }
        return [code for code in code_to_name if code in affected]
    names = {
        _county_key(name) for name in _text(props.get("areaDesc"), 2000).split(";")
        if _county_key(name)
    }
    return [
        code for code, name in code_to_name.items() if _county_key(name) in names
    ]


def _has_valid_alert_locations(feature: dict) -> bool:
    props = feature.get("properties")
    if not isinstance(props, dict):
        return False
    geocode = props.get("geocode")
    if geocode is not None and not isinstance(geocode, dict):
        return False
    if isinstance(geocode, dict):
        for key in ("UGC", "SAME"):
            value = geocode.get(key)
            if value is not None and not isinstance(value, list):
                return False
    affected_zones = props.get("affectedZones")
    return affected_zones is None or isinstance(affected_zones, list)


def _alert_zone_codes(feature: dict) -> list[str]:
    props = feature.get("properties")
    if not isinstance(props, dict):
        return []
    geocode = props.get("geocode")
    raw_codes = geocode.get("UGC") if isinstance(geocode, dict) else None
    if not isinstance(raw_codes, list):
        raw_codes = [
            _text(url, 500).rstrip("/").rsplit("/", 1)[-1]
            for url in (props.get("affectedZones") or [])
            if _text(url, 500)
        ] if isinstance(props.get("affectedZones"), list) else []
    codes = []
    for raw_code in raw_codes:
        code = _text(raw_code, 16).upper()
        if _ZONE_CODE.fullmatch(code) and code not in codes:
            codes.append(code)
        if len(codes) >= _MAX_ALERT_ZONES:
            break
    return codes


class AreaAlertCache:
    """Short-lived state-area alert cache for regional map awareness."""

    def __init__(self, fetch_json=None, ttl_seconds: float = 120,
                 clock=time.monotonic, wall_clock=lambda: datetime.now(timezone.utc)):
        self._fetch_json = fetch_json or _fetch_json
        self._ttl_seconds = ttl_seconds
        self._clock = clock
        self._wall_clock = wall_clock
        self._cache: dict[str, tuple[float, list[dict], str, tuple]] = {}
        self._retry_after: dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def get_many(self, counties: list[dict], contact: str) -> tuple[list[dict], list[str], bool, str]:
        async with self._lock:
            return await self._get_many(counties, contact)

    async def _get_many(self, counties: list[dict], contact: str) -> tuple[list[dict], list[str], bool, str]:
        code_to_name = {
            _text(county.get("code"), 16).upper(): _text(county.get("name"), 100)
            for county in counties if isinstance(county, dict)
            and _COUNTY_CODE.fullmatch(_text(county.get("code"), 16).upper())
        }
        areas = list(dict.fromkeys(code[:2] for code in code_to_name))
        selected = areas[:_MAX_MAP_AREAS]
        errors = areas[_MAX_MAP_AREAS:]
        signature = tuple(code_to_name.items())
        now = self._clock()
        safe_contact = _text(contact, 320) or "https://github.com/fizzlepoof/WXDispatch"
        headers = {
            "User-Agent": f"WXDispatch/{__version__} ({safe_contact})",
            "Accept": "application/geo+json",
        }
        self._cache = {area: value for area, value in self._cache.items() if area in selected}
        self._retry_after = {
            area: value for area, value in self._retry_after.items() if area in selected
        }
        source_geometry_budget = [0]

        async def resolve(area):
            cached = self._cache.get(area)
            cache_matches = cached is not None and cached[3] == signature
            if cache_matches and now < cached[0]:
                return area, cached[1], cached[2], "", False
            if now < self._retry_after.get(area, 0):
                if cache_matches:
                    return area, cached[1], cached[2], area, True
                return area, [], "", area, False
            try:
                data = await self._fetch_json(
                    "https://api.weather.gov/alerts/active?area=" + area,
                    headers, 20.0,
                )
                features = data.get("features")
                if not isinstance(features, list):
                    raise ValueError("NWS alert response had no feature list")
                if len(features) > _MAX_ALERT_FEATURES_PER_AREA:
                    raise ValueError("NWS area alert response exceeded the feature limit")
                fetched_at = self._wall_clock()
                if fetched_at.tzinfo is None:
                    fetched_at = fetched_at.replace(tzinfo=timezone.utc)
                alerts = []
                for feature in features:
                    if not isinstance(feature, dict) or not _has_valid_alert_locations(feature):
                        continue
                    watched_codes = _watched_codes(feature, code_to_name)
                    alert = _project_alert(
                        feature, watched_codes, code_to_name, fetched_at,
                        geometry_budget=source_geometry_budget,
                    )
                    if alert is not None:
                        alert["watched"] = bool(watched_codes)
                        alert["affected_zones"] = _alert_zone_codes(feature)
                        alerts.append(alert)
                updated_at = fetched_at.isoformat()
                self._cache[area] = (
                    now + self._ttl_seconds, alerts, updated_at, signature,
                )
                self._retry_after.pop(area, None)
                return area, alerts, updated_at, "", False
            except Exception:
                self._retry_after[area] = now + _FAILURE_RETRY_SECONDS
                if cache_matches:
                    return area, cached[1], cached[2], area, True
                return area, [], "", area, False

        results = await asyncio.gather(*(resolve(area) for area in selected))
        merged: dict[str, dict] = {}
        updated_times = []
        stale = False
        for _area, alerts, updated_at, error, is_stale in results:
            if updated_at:
                updated_times.append(updated_at)
            if error:
                errors.append(error)
            stale = stale or is_stale
            for alert in alerts:
                key = alert["id"] or "\x1f".join(
                    str(alert[field]) for field in ("event", "headline", "onset", "expires")
                )
                existing = merged.get(key)
                if existing is None:
                    merged[key] = dict(alert)
                    continue
                for code, name in zip(alert["local_zones"], alert["local_counties"]):
                    if code not in existing["local_zones"]:
                        existing["local_zones"].append(code)
                        existing["local_counties"].append(name)
                existing["watched"] = bool(existing["local_zones"])
                for code in alert["affected_zones"]:
                    if code not in existing["affected_zones"]:
                        existing["affected_zones"].append(code)
                if existing["geometry"] is None and alert["geometry"] is not None:
                    existing["geometry"] = alert["geometry"]

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
        current_alerts.sort(key=lambda alert: not alert["watched"])
        return current_alerts, errors, stale, min(updated_times) if updated_times else ""


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