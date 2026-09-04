from datetime import datetime, timezone

import pytest

from app.db import Database
from app.meshwx_v4 import cobs_decode, encode_alert
from app.models import Alert


def _alert(*, geometry=None, ugc=None, event="Extreme Heat Warning") -> Alert:
    feature = {
        "id": "urn:oid:test-warning",
        "geometry": geometry,
        "properties": {
            "event": event,
            "headline": f"{event} for the test area",
            "areaDesc": "Test County",
            "severity": "Severe",
            "effective": "2026-09-04T17:00:00+00:00",
            "onset": "2026-09-04T17:00:00+00:00",
            "expires": "2026-09-04T20:00:00+00:00",
            "ends": "2026-09-04T20:00:00+00:00",
            "messageType": "Alert",
            "geocode": {"UGC": ugc or []},
        },
    }
    return Alert.from_feature(feature)


def test_zone_warning_encodes_as_cobs_wrapped_meshwx_v4() -> None:
    encoded = encode_alert(_alert(ugc=["TNZ050"]), sequence=0x3EF3)

    assert encoded is not None
    assert encoded.hex() == (
        "030421010e3ef3f301c6da3001c6d97c0129283245787472656d652048656174205761"
        "726e696e6720666f722074686520746573742061726561"
    )
    assert b"\x00" not in encoded
    raw = cobs_decode(encoded)
    assert raw[:4] == bytes([0x04, 0x21, 0x00, 0x00])
    assert raw[6] >> 4 == 0x0F  # Extreme Heat maps to protocol's Other warning type.
    assert raw[6] & 0x0F == 0x03
    assert int.from_bytes(raw[7:11], "big") == int(
        datetime(2026, 9, 4, 20, tzinfo=timezone.utc).timestamp() // 60
    )
    assert raw[15] == 1


def test_county_warning_uses_geojson_polygon_when_available() -> None:
    geometry = {
        "type": "Polygon",
        "coordinates": [[[-87.5, 36.4], [-87.1, 36.4], [-87.1, 36.7], [-87.5, 36.4]]],
    }

    encoded = encode_alert(
        _alert(geometry=geometry, ugc=["TNC125"], event="Tornado Warning"),
        sequence=1,
    )

    assert encoded is not None
    raw = cobs_decode(encoded)
    assert raw[1] == 0x20
    assert raw[6] >> 4 == 0x01
    assert raw[15] == 3  # duplicate closing polygon vertex is removed


def test_county_warning_without_polygon_is_not_encoded() -> None:
    assert encode_alert(_alert(ugc=["TNC125"]), sequence=1) is None


def test_zone_warning_filters_invalid_and_duplicate_ugc_values() -> None:
    encoded = encode_alert(
        _alert(ugc=["TNZ050", "TNZ050", "TNC125", "bad"]), sequence=1
    )
    assert encoded is not None
    raw = cobs_decode(encoded)
    assert raw[15] == 1
    assert raw[16:19] == bytes([41, 0, 50])


def test_supplied_sequence_is_encoded_and_payload_fits_meshcore_limit() -> None:
    alert = _alert(ugc=["TNZ050"])

    first = encode_alert(alert, sequence=0x1234)
    second = encode_alert(alert, sequence=0x1235)

    assert first is not None
    assert second is not None
    assert cobs_decode(first)[4:6] == b"\x12\x34"
    assert cobs_decode(second)[4:6] == b"\x12\x35"
    assert len(first) <= 163


@pytest.mark.parametrize(
    "geometry",
    [
        {"type": "MultiPolygon", "coordinates": [[[[-87.5, 36.4], [-87.1, 36.4]]]]},
        {"type": "Polygon", "coordinates": [[[200, 36.4], [-87.1, 36.4], [200, 36.4]]]},
        {
            "type": "Polygon",
            "coordinates": [[[-87.5, 36.4], [-80.0, 36.4], [-87.5, 36.4]]],
        },
        {"type": "Polygon", "coordinates": [[[-87.5, 36.4]] * 22]},
        {
            "type": "Polygon",
            "coordinates": [[[-87.5, "bad"], [-87.1, 36.4], [-87.5, "bad"]]],
        },
    ],
)
def test_unsafe_polygon_geometry_fails_closed(geometry) -> None:
    assert encode_alert(
        _alert(geometry=geometry, ugc=["TNC125"], event="Tornado Warning"),
        sequence=1,
    ) is None


def test_naive_timestamp_fails_closed() -> None:
    alert = _alert(ugc=["TNZ050"])
    alert = Alert.from_feature({
        **alert.raw,
        "properties": {**alert.raw["properties"], "ends": "2026-09-04T20:00:00"},
    })
    assert encode_alert(alert, sequence=1) is None


def test_long_unicode_headline_remains_valid_utf8() -> None:
    alert = _alert(ugc=["TNZ050"])
    alert = Alert.from_feature({
        **alert.raw,
        "properties": {**alert.raw["properties"], "headline": "🔥" * 200},
    })
    encoded = encode_alert(alert, sequence=1)
    assert encoded is not None
    cobs_decode(encoded)[19:].decode("utf-8")


def test_meshwx_delivery_state_round_trip_and_startup_recovery() -> None:
    db = Database(":memory:")
    db.set_meshwx_delivery("alert-1", "hash-1", 7, "queued")
    assert db.get_meshwx_delivery("alert-1")["state"] == "queued"

    assert db.recover_queued_meshwx_deliveries() == 1
    assert db.get_meshwx_delivery("alert-1")["state"] == "failed"

    db.set_meshwx_delivery("alert-1", "hash-1", 7, "accepted")
    assert db.supersede_meshwx_deliveries(["alert-1"]) == 1
    assert db.get_meshwx_delivery("alert-1")["state"] == "superseded"
