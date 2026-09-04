from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

import pytest

from app.db import Database


def _feature(alert_id: str, event: str, ugc, *, area="", geometry=None):
    geocode = {} if ugc is None else {"UGC": ugc}
    return {
        "id": alert_id,
        "type": "Feature",
        "geometry": geometry,
        "properties": {
            "event": event,
            "headline": event + " headline",
            "areaDesc": area,
            "severity": "Severe",
            "urgency": "Immediate",
            "certainty": "Observed",
            "onset": "2099-01-01T00:00:00+00:00",
            "ends": "2099-01-01T01:00:00+00:00",
            "expires": "2099-01-01T01:00:00+00:00",
            "geocode": geocode,
        },
    }


def test_current_alert_map_payload_uses_ugc_and_keeps_safe_local_fields_only():
    from app.alert_map import build_current_alerts

    counties = [
        {"code": "TNC125", "name": "Montgomery County"},
        {"code": "KYC047", "name": "Christian County"},
    ]
    geometry = {
        "type": "Polygon",
        "coordinates": [[
            [-87.0, 36.5], [-86.5, 36.5], [-86.5, 36.0], [-87.0, 36.5],
        ]],
    }
    raw = json.dumps({"features": [
        _feature("local", "Tornado Warning", ["TNC125", "TNC999"], geometry=geometry),
        _feature("outside", "Flood Warning", ["TNC001"]),
    ]})

    alerts, error = build_current_alerts(raw, counties)

    assert error == ""
    assert alerts == [{
        "id": "local",
        "event": "Tornado Warning",
        "headline": "Tornado Warning headline",
        "area": "",
        "severity": "Severe",
        "urgency": "Immediate",
        "certainty": "Observed",
        "onset": "2099-01-01T00:00:00+00:00",
        "ends": "2099-01-01T01:00:00+00:00",
        "expires": "2099-01-01T01:00:00+00:00",
        "local_zones": ["TNC125"],
        "local_counties": ["Montgomery County"],
        "geometry": geometry,
    }]
    assert "description" not in alerts[0]
    assert "instruction" not in alerts[0]


def test_current_alert_map_uses_name_fallback_only_when_ugc_is_absent():
    from app.alert_map import build_current_alerts

    counties = [{"code": "TNC021", "name": "Cheatham County"}]
    raw = json.dumps({"features": [
        _feature("authoritative-empty", "Flood Warning", [], area="Cheatham"),
        _feature("fallback", "Wind Advisory", None, area="Cheatham; Davidson"),
    ]})

    alerts, error = build_current_alerts(raw, counties)

    assert error == ""
    assert [alert["id"] for alert in alerts] == ["fallback"]
    assert alerts[0]["local_zones"] == ["TNC021"]


def test_current_alert_map_uses_affected_zone_urls_when_ugc_is_absent():
    from app.alert_map import build_current_alerts

    counties = [{"code": "KYC047", "name": "Christian County"}]
    feature = _feature("zone-url", "Flood Warning", None, area="Regional area")
    feature["properties"]["affectedZones"] = [
        "https://api.weather.gov/zones/county/KYC047",
        "https://api.weather.gov/zones/forecast/KYZ017",
    ]

    alerts, error = build_current_alerts(json.dumps({"features": [feature]}), counties)

    assert error == ""
    assert [alert["id"] for alert in alerts] == ["zone-url"]
    assert alerts[0]["local_zones"] == ["KYC047"]


def test_current_alert_map_omits_invalid_or_out_of_bounds_geometry():
    from app.alert_map import build_current_alerts

    counties = [{"code": "TNC125", "name": "Montgomery County"}]
    invalid_geometry = {
        "type": "Polygon",
        "coordinates": [[[999, 36.5], [-86.5, 36.5], [-86.5, 36.0], [999, 36.5]]],
    }
    raw = json.dumps({"features": [
        _feature("local", "Tornado Warning", ["TNC125"], geometry=invalid_geometry),
    ]})

    alerts, error = build_current_alerts(raw, counties)

    assert error == ""
    assert alerts[0]["geometry"] is None


def test_configured_map_counties_unions_legacy_zones_and_all_manual_rules():
    from app.alert_map import configured_counties

    db = Database(":memory:")
    db.set_setting("zones", "TNC125,TNZ125,KYC047,TNC125")
    first = db.create_route("first", 10, True)
    db.replace_route_counties(first, [
        ("TNC125", "Montgomery County"),
        ("TNC021", "Cheatham County"),
    ])
    disabled = db.create_route("disabled", 20, False)
    db.replace_route_counties(disabled, [("TNC147", "Robertson County")])

    counties = configured_counties(db)

    assert counties == [
        {"code": "TNC125", "name": "Montgomery County"},
        {"code": "KYC047", "name": "KYC047"},
        {"code": "TNC021", "name": "Cheatham County"},
        {"code": "TNC147", "name": "Robertson County"},
    ]
    db.close()


async def test_zone_geometry_cache_fetches_validated_county_once_and_returns_safe_feature():
    from app.alert_map import ZoneGeometryCache

    calls = []
    geometry = {
        "type": "Polygon",
        "coordinates": [[
            [-87.0, 36.5], [-86.5, 36.5], [-86.5, 36.0], [-87.0, 36.5],
        ]],
    }

    async def fetch_json(url, headers, timeout):
        calls.append((url, headers, timeout))
        return {
            "type": "Feature",
            "geometry": geometry,
            "properties": {"name": "Montgomery", "state": "TN", "secret": "discard"},
        }

    cache = ZoneGeometryCache(fetch_json=fetch_json, clock=lambda: 100.0)
    counties = [{"code": "TNC125", "name": "TNC125"}]

    first, first_errors = await cache.get_many(counties, "operator@example.com")
    second, second_errors = await cache.get_many(counties, "operator@example.com")

    expected = [{
        "type": "Feature",
        "geometry": geometry,
        "properties": {"code": "TNC125", "name": "Montgomery County"},
    }]
    assert first == second == expected
    assert first_errors == second_errors == []
    assert len(calls) == 1
    assert calls[0][0] == "https://api.weather.gov/zones/county/TNC125"
    assert calls[0][1]["Accept"] == "application/geo+json"
    assert "operator@example.com" in calls[0][1]["User-Agent"]
    assert "secret" not in repr(first).lower()


async def test_zone_geometry_cache_fetches_counties_concurrently():
    from app.alert_map import ZoneGeometryCache

    active = [0]
    maximum = [0]
    geometry = {
        "type": "Polygon",
        "coordinates": [[[-87, 36], [-86, 36], [-86, 35], [-87, 36]]],
    }

    async def fetch_json(url, headers, timeout):
        active[0] += 1
        maximum[0] = max(maximum[0], active[0])
        await asyncio.sleep(0)
        active[0] -= 1
        code = url.rsplit("/", 1)[-1]
        return {"geometry": geometry, "properties": {"name": code}}

    cache = ZoneGeometryCache(fetch_json=fetch_json)
    await cache.get_many([
        {"code": "TNC125", "name": "Montgomery County"},
        {"code": "TNC147", "name": "Robertson County"},
    ], "operator@example.com")

    assert maximum[0] == 2


async def test_nws_fetch_rejects_response_over_byte_limit(monkeypatch):
    import app.alert_map as alert_map

    monkeypatch.setattr(alert_map, "_MAX_RESPONSE_BYTES", 4)

    class FakeResponse:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        def raise_for_status(self):
            return None

        async def aiter_bytes(self):
            yield b'{"features":[]}'

    class FakeClient:
        def __init__(self, timeout):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        def stream(self, method, url, headers):
            return FakeResponse()

    monkeypatch.setattr(alert_map.httpx, "AsyncClient", FakeClient)

    with pytest.raises(ValueError, match="size limit"):
        await alert_map._fetch_json("https://example.invalid", {}, 1.0)


async def test_county_alert_cache_maps_forecast_zone_alerts_to_each_queried_county():
    from app.alert_map import CountyAlertCache

    calls = []
    feature = _feature("heat", "Heat Advisory", ["TNZ026"], area="Montgomery; Robertson")

    async def fetch_json(url, headers, timeout):
        calls.append(url)
        return {"features": [feature]}

    now = datetime(2026, 9, 3, 23, 0, tzinfo=timezone.utc)
    cache = CountyAlertCache(
        fetch_json=fetch_json, clock=lambda: 100.0, wall_clock=lambda: now,
    )
    counties = [
        {"code": "TNC125", "name": "Montgomery County"},
        {"code": "TNC147", "name": "Robertson County"},
    ]

    first = await cache.get_many(counties, "operator@example.com")
    second = await cache.get_many(counties, "operator@example.com")

    alerts, errors, stale, updated_at = first
    assert second == first
    assert errors == []
    assert stale is False
    assert updated_at == "2026-09-03T23:00:00+00:00"
    assert len(alerts) == 1
    assert alerts[0]["id"] == "heat"
    assert alerts[0]["local_zones"] == ["TNC125", "TNC147"]
    assert alerts[0]["local_counties"] == ["Montgomery County", "Robertson County"]
    assert calls == [
        "https://api.weather.gov/alerts/active?zone=TNC125",
        "https://api.weather.gov/alerts/active?zone=TNC147",
    ]


async def test_county_alert_cache_drops_expired_alert_from_stale_response():
    from app.alert_map import CountyAlertCache

    feature = _feature("short", "Severe Thunderstorm Warning", ["TNZ026"])
    feature["properties"]["ends"] = "2026-09-03T23:00:30+00:00"
    feature["properties"]["expires"] = "2026-09-03T23:00:30+00:00"
    monotonic = [100.0]
    wall = [datetime(2026, 9, 3, 23, 0, tzinfo=timezone.utc)]
    attempts = [0]

    async def fetch_json(url, headers, timeout):
        attempts[0] += 1
        if attempts[0] > 1:
            raise RuntimeError("temporary NWS failure")
        return {"features": [feature]}

    cache = CountyAlertCache(
        fetch_json=fetch_json, ttl_seconds=10,
        clock=lambda: monotonic[0], wall_clock=lambda: wall[0],
    )
    counties = [{"code": "TNC125", "name": "Montgomery County"}]

    first, errors, stale, updated_at = await cache.get_many(counties, "operator@example.com")
    assert len(first) == 1
    assert errors == []
    assert stale is False
    assert updated_at == "2026-09-03T23:00:00+00:00"

    monotonic[0] = 111.0
    wall[0] = datetime(2026, 9, 3, 23, 1, tzinfo=timezone.utc)
    second, errors, stale, updated_at = await cache.get_many(counties, "operator@example.com")

    assert second == []
    assert errors == ["TNC125"]
    assert stale is True
    assert updated_at == "2026-09-03T23:00:00+00:00"


async def test_county_alert_cache_prefers_fresh_duplicate_over_stale_copy():
    from app.alert_map import CountyAlertCache

    monotonic = [100.0]
    wall = [datetime(2026, 9, 3, 23, 0, tzinfo=timezone.utc)]
    round_number = [0]

    async def fetch_json(url, headers, timeout):
        code = url.rsplit("=", 1)[-1]
        feature = _feature("shared", "Heat Advisory", ["TNZ026"])
        if round_number[0] == 0:
            feature["properties"]["headline"] = "old headline"
            return {"features": [feature]}
        if code == "TNC125":
            raise RuntimeError("temporary failure")
        feature["properties"]["headline"] = "updated headline"
        feature["properties"]["severity"] = "Extreme"
        return {"features": [feature]}

    cache = CountyAlertCache(
        fetch_json=fetch_json, ttl_seconds=10,
        clock=lambda: monotonic[0], wall_clock=lambda: wall[0],
    )
    counties = [
        {"code": "TNC125", "name": "Montgomery County"},
        {"code": "TNC147", "name": "Robertson County"},
    ]
    await cache.get_many(counties, "operator@example.com")

    round_number[0] = 1
    monotonic[0] = 111.0
    wall[0] = datetime(2026, 9, 3, 23, 1, tzinfo=timezone.utc)
    alerts, errors, stale, _updated_at = await cache.get_many(counties, "operator@example.com")

    assert errors == ["TNC125"]
    assert stale is True
    assert alerts[0]["headline"] == "updated headline"
    assert alerts[0]["severity"] == "Extreme"
    assert alerts[0]["local_zones"] == ["TNC125", "TNC147"]


async def test_county_alert_cache_checks_expiry_again_after_slow_fetches():
    from app.alert_map import CountyAlertCache

    wall = [datetime(2026, 9, 3, 23, 0, tzinfo=timezone.utc)]
    feature = _feature("short", "Flood Warning", ["TNZ026"])
    feature["properties"]["ends"] = "2026-09-03T23:01:00+00:00"
    feature["properties"]["expires"] = "2026-09-03T23:01:00+00:00"

    async def fetch_json(url, headers, timeout):
        wall[0] = datetime(2026, 9, 3, 23, 2, tzinfo=timezone.utc)
        return {"features": [feature]}

    cache = CountyAlertCache(fetch_json=fetch_json, wall_clock=lambda: wall[0])
    alerts, _errors, _stale, _updated_at = await cache.get_many(
        [{"code": "TNC125", "name": "Montgomery County"}], "operator@example.com",
    )

    assert alerts == []


async def test_county_alert_cache_fetches_counties_concurrently():
    from app.alert_map import CountyAlertCache

    active = [0]
    maximum = [0]

    async def fetch_json(url, headers, timeout):
        active[0] += 1
        maximum[0] = max(maximum[0], active[0])
        await asyncio.sleep(0)
        active[0] -= 1
        return {"features": []}

    cache = CountyAlertCache(fetch_json=fetch_json)
    await cache.get_many([
        {"code": "TNC125", "name": "Montgomery County"},
        {"code": "TNC147", "name": "Robertson County"},
    ], "operator@example.com")

    assert maximum[0] == 2


async def test_county_alert_cache_coalesces_overlapping_refreshes():
    from app.alert_map import CountyAlertCache

    calls = [0]

    async def fetch_json(url, headers, timeout):
        calls[0] += 1
        await asyncio.sleep(0)
        return {"features": []}

    cache = CountyAlertCache(fetch_json=fetch_json)
    counties = [{"code": "TNC125", "name": "Montgomery County"}]

    first, second = await asyncio.gather(
        cache.get_many(counties, "operator@example.com"),
        cache.get_many(counties, "operator@example.com"),
    )

    assert first == second
    assert calls[0] == 1


async def test_county_alert_cache_negative_caches_overlapping_failures():
    from app.alert_map import CountyAlertCache

    calls = [0]

    async def fetch_json(url, headers, timeout):
        calls[0] += 1
        await asyncio.sleep(0)
        raise RuntimeError("NWS unavailable")

    cache = CountyAlertCache(fetch_json=fetch_json)
    counties = [{"code": "TNC125", "name": "Montgomery County"}]

    first, second = await asyncio.gather(
        cache.get_many(counties, "operator@example.com"),
        cache.get_many(counties, "operator@example.com"),
    )

    assert first == second == ([], ["TNC125"], False, "")
    assert calls[0] == 1


async def test_county_alert_cache_rejects_oversized_feature_collection(monkeypatch):
    import app.alert_map as alert_map

    monkeypatch.setattr(alert_map, "_MAX_ALERT_FEATURES_PER_COUNTY", 1)

    async def fetch_json(url, headers, timeout):
        return {"features": [
            _feature("one", "Flood Warning", ["TNZ026"]),
            _feature("two", "Flood Warning", ["TNZ026"]),
        ]}

    cache = alert_map.CountyAlertCache(fetch_json=fetch_json)
    alerts, errors, stale, updated_at = await cache.get_many(
        [{"code": "TNC125", "name": "Montgomery County"}], "operator@example.com",
    )

    assert alerts == []
    assert errors == ["TNC125"]
    assert stale is False
    assert updated_at == ""


async def test_county_alert_cache_bounds_county_query_fanout(monkeypatch):
    import app.alert_map as alert_map

    monkeypatch.setattr(alert_map, "_MAX_MAP_COUNTIES", 1)
    calls = []

    async def fetch_json(url, headers, timeout):
        calls.append(url)
        return {"features": []}

    cache = alert_map.CountyAlertCache(fetch_json=fetch_json)
    alerts, errors, stale, _updated_at = await cache.get_many([
        {"code": "TNC125", "name": "Montgomery County"},
        {"code": "TNC147", "name": "Robertson County"},
    ], "operator@example.com")

    assert alerts == []
    assert errors == ["TNC147"]
    assert stale is False
    assert calls == ["https://api.weather.gov/alerts/active?zone=TNC125"]


def test_safe_geometry_honors_shared_collection_point_budget(monkeypatch):
    import app.alert_map as alert_map

    monkeypatch.setattr(alert_map, "_MAX_COLLECTION_GEOMETRY_POINTS", 7)
    geometry = {
        "type": "Polygon",
        "coordinates": [[[-87, 36], [-86, 36], [-86, 35], [-87, 36]]],
    }
    budget = [0]

    assert alert_map.safe_geometry(geometry, budget=budget) == geometry
    assert alert_map.safe_geometry(geometry, budget=budget) is None


def test_current_alert_map_drops_alerts_that_expired_since_the_last_poll():
    from app.alert_map import build_current_alerts

    counties = [{"code": "TNC125", "name": "Montgomery County"}]
    feature = _feature("expired", "Tornado Warning", ["TNC125"])
    feature["properties"]["ends"] = "2026-09-03T22:00:00+00:00"
    feature["properties"]["expires"] = "2026-09-03T22:00:00+00:00"

    alerts, error = build_current_alerts(
        json.dumps({"features": [feature]}), counties,
        now=datetime(2026, 9, 3, 23, 0, tzinfo=timezone.utc),
    )

    assert error == ""
    assert alerts == []
