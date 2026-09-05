"""Tests for strict NOAA Weather Radio SAME decoding."""

from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone

import pytest

from app.noaa_same import (
    SAME_STATE_FIPS_TO_POSTAL,
    SameEndMessage,
    SameObservation,
    SameRepeatConfirmer,
    parse_same,
)


UTC = timezone.utc


def test_parses_montgomery_tennessee_tornado_header() -> None:
    now = datetime(2026, 9, 6, 18, 35, tzinfo=UTC)

    result = parse_same(
        "  EAS: ZCZC-WXR-TOR-047125+0030-2491830-KOHX/NWS-  ",
        now=now,
    )

    assert isinstance(result, SameObservation)
    assert result.raw_header == "ZCZC-WXR-TOR-047125+0030-2491830-KOHX/NWS-"
    assert result.originator == "WXR"
    assert result.event_code == "TOR"
    assert result.event_name == "Tornado Warning"
    assert result.locations == ("047125",)
    assert result.county_ugcs == ("TNC125",)
    assert result.purge_duration == timedelta(minutes=30)
    assert result.issued_at == datetime(2026, 9, 6, 18, 30, tzinfo=UTC)
    assert result.expires_at == datetime(2026, 9, 6, 19, 0, tzinfo=UTC)
    assert result.sender == "KOHX/NWS"
    assert result.is_test is False
    assert result.is_emergency is True


def test_parses_multiple_locations_and_routes_only_known_fips() -> None:
    result = parse_same(
        "EAS: ZCZC-WXR-SVR-047125-001001-099999+0015-2491830-KOHX/NWS-",
        now=datetime(2026, 9, 6, 18, 31, tzinfo=UTC),
    )

    assert result.locations == ("047125", "001001", "099999")
    assert result.county_ugcs == ("TNC125", "ALC001")


def test_state_fips_map_is_complete_and_read_only() -> None:
    assert len(SAME_STATE_FIPS_TO_POSTAL) == 57
    assert SAME_STATE_FIPS_TO_POSTAL["11"] == "DC"
    assert SAME_STATE_FIPS_TO_POSTAL["60"] == "AS"
    assert SAME_STATE_FIPS_TO_POSTAL["69"] == "MP"
    assert SAME_STATE_FIPS_TO_POSTAL["72"] == "PR"
    assert SAME_STATE_FIPS_TO_POSTAL["74"] == "UM"
    assert SAME_STATE_FIPS_TO_POSTAL["78"] == "VI"


def test_accepts_a_bounded_decoder_prefix_and_line_whitespace() -> None:
    result = parse_same(
        "\n  [2026-09-06 18:30:02] multimon-ng: EAS: "
        "ZCZC-WXR-FFW-147125+0015-2491830-KOHX/NWS-  \n",
        now=datetime(2026, 9, 6, 18, 31, tzinfo=UTC),
    )

    assert isinstance(result, SameObservation)
    assert result.locations == ("147125",)
    assert result.county_ugcs == ("TNC125",)
    assert result.raw_header.startswith("ZCZC-")


def test_observation_is_immutable() -> None:
    result = parse_same(
        "ZCZC-WXR-TOR-047125+0030-2491830-KOHX/NWS-",
        now=datetime(2026, 9, 6, 18, 31, tzinfo=UTC),
    )

    with pytest.raises(FrozenInstanceError):
        result.event_code = "SVR"  # type: ignore[misc]


@pytest.mark.parametrize(
    "text",
    [
        "noise ZCZC-WXR-TOR-047125+0030-2491830-KOHX/NWS-",
        "EAS: ZCZC-WXR-TOR-047125+0030-2491830-KOHX/NWS- trailing",
        "EAS: ZCZC-WxR-TOR-047125+0030-2491830-KOHX/NWS-",
        "EAS: ZCZC-WXR-TO!-047125+0030-2491830-KOHX/NWS-",
        "EAS: ZCZC-WXR-TOR-47125+0030-2491830-KOHX/NWS-",
        "EAS: ZCZC-WXR-TOR-047125+0030-2491830-SHORT-",
        "EAS: ZCZC-WXR-TOR-047125+0030-2491830-KOHX/NWS-\nNNNN",
        "é EAS: ZCZC-WXR-TOR-047125+0030-2491830-KOHX/NWS-",
        ("x" * 129) + " EAS: ZCZC-WXR-TOR-047125+0030-2491830-KOHX/NWS-",
        "EAS: ZCZC-WXR-TOR-"
        + "-".join(f"047{i:03d}" for i in range(32))
        + "+0030-2491830-KOHX/NWS-",
    ],
)
def test_rejects_malformed_injected_and_oversize_input(text: str) -> None:
    with pytest.raises(ValueError):
        parse_same(text, now=datetime(2026, 9, 6, 18, 31, tzinfo=UTC))


@pytest.mark.parametrize("duration", ["0000", "0010", "0115", "0160", "0601", "0630", "9900"])
def test_rejects_invalid_purge_durations(duration: str) -> None:
    with pytest.raises(ValueError):
        parse_same(
            f"ZCZC-WXR-TOR-047125+{duration}-2491830-KOHX/NWS-",
            now=datetime(2026, 9, 6, 18, 31, tzinfo=UTC),
        )


@pytest.mark.parametrize("issued", ["0001830", "3671830", "2492430", "2491860"])
def test_rejects_invalid_julian_dates_and_clock_times(issued: str) -> None:
    with pytest.raises(ValueError):
        parse_same(
            f"ZCZC-WXR-TOR-047125+0030-{issued}-KOHX/NWS-",
            now=datetime(2026, 9, 6, 18, 31, tzinfo=UTC),
        )


def test_resolves_previous_year_at_new_year_boundary() -> None:
    result = parse_same(
        "ZCZC-WXR-TOR-047125+0030-3652359-KOHX/NWS-",
        now=datetime(2027, 1, 1, 0, 5, tzinfo=UTC),
    )

    assert result.issued_at == datetime(2026, 12, 31, 23, 59, tzinfo=UTC)
    assert result.expires_at == datetime(2027, 1, 1, 0, 29, tzinfo=UTC)


def test_resolves_small_future_skew_into_next_year() -> None:
    result = parse_same(
        "ZCZC-WXR-TOR-047125+0030-0010002-KOHX/NWS-",
        now=datetime(2026, 12, 31, 23, 58, tzinfo=UTC),
    )

    assert result.issued_at == datetime(2027, 1, 1, 0, 2, tzinfo=UTC)


def test_accepts_leap_day() -> None:
    result = parse_same(
        "ZCZC-WXR-TOR-047125+0030-0601200-KOHX/NWS-",
        now=datetime(2024, 2, 29, 12, 5, tzinfo=UTC),
    )

    assert result.issued_at == datetime(2024, 2, 29, 12, 0, tzinfo=UTC)


@pytest.mark.parametrize("issued", ["2471800", "2491900", "3661200"])
def test_rejects_implausibly_old_future_or_nonleap_headers(issued: str) -> None:
    with pytest.raises(ValueError):
        parse_same(
            f"ZCZC-WXR-TOR-047125+0030-{issued}-KOHX/NWS-",
            now=datetime(2026, 9, 6, 18, 31, tzinfo=UTC),
        )


def test_requires_an_aware_now() -> None:
    with pytest.raises(ValueError, match="aware"):
        parse_same(
            "ZCZC-WXR-TOR-047125+0030-2491830-KOHX/NWS-",
            now=datetime(2026, 9, 6, 18, 31),
        )


@pytest.mark.parametrize(
    ("code", "name"),
    [
        ("SVR", "Severe Thunderstorm Warning"),
        ("FFW", "Flash Flood Warning"),
        ("RMT", "Required Monthly Test"),
        ("NPT", "National Periodic Test"),
        ("HUW", "Hurricane Warning"),
        ("TSW", "Tsunami Warning"),
        ("CAE", "Child Abduction Emergency"),
        ("EWW", "Extreme Wind Warning"),
        ("SQW", "Snow Squall Warning"),
    ],
)
def test_maps_broad_official_event_codes(code: str, name: str) -> None:
    result = parse_same(
        f"ZCZC-WXR-{code}-047125+0030-2491830-KOHX/NWS-",
        now=datetime(2026, 9, 6, 18, 31, tzinfo=UTC),
    )

    assert result.event_name == name


def test_preserves_unknown_valid_event_code() -> None:
    result = parse_same(
        "ZCZC-WXR-XYZ-047125+0030-2491830-KOHX/NWS-",
        now=datetime(2026, 9, 6, 18, 31, tzinfo=UTC),
    )

    assert result.event_code == "XYZ"
    assert result.event_name == "Unknown SAME event XYZ"


def test_required_weekly_test_flags_test_not_emergency() -> None:
    result = parse_same(
        "ZCZC-WXR-RWT-047125+0030-2491830-KOHX/NWS-",
        now=datetime(2026, 9, 6, 18, 31, tzinfo=UTC),
    )

    assert result.event_name == "Required Weekly Test"
    assert result.is_test is True
    assert result.is_emergency is False


def test_parses_end_of_message_as_a_distinct_result() -> None:
    result = parse_same(
        "  multimon-ng: EAS: NNNN  ",
        now=datetime(2026, 9, 6, 18, 31, tzinfo=UTC),
    )

    assert isinstance(result, SameEndMessage)
    assert not isinstance(result, SameObservation)
    assert result.raw_message == "NNNN"


def test_repeat_confirmer_accepts_second_matching_header_in_window() -> None:
    observation = parse_same(
        "ZCZC-WXR-TOR-047125+0030-2491830-KOHX/NWS-",
        now=datetime(2026, 9, 6, 18, 31, tzinfo=UTC),
    )
    assert isinstance(observation, SameObservation)
    confirmer = SameRepeatConfirmer(confirmation_window=5.0)

    assert confirmer.process(observation, monotonic_now=10.0) is None
    assert confirmer.process(observation, monotonic_now=14.0) == observation


def _observation(event_code: str = "TOR") -> SameObservation:
    result = parse_same(
        f"ZCZC-WXR-{event_code}-047125+0030-2491830-KOHX/NWS-",
        now=datetime(2026, 9, 6, 18, 31, tzinfo=UTC),
    )
    assert isinstance(result, SameObservation)
    return result


def test_repeat_mismatches_do_not_combine() -> None:
    confirmer = SameRepeatConfirmer(confirmation_window=5.0)
    tornado = _observation("TOR")
    severe = _observation("SVR")

    assert confirmer.process(tornado, monotonic_now=0.0) is None
    assert confirmer.process(severe, monotonic_now=1.0) is None
    assert confirmer.process(tornado, monotonic_now=2.0) is None
    assert confirmer.process(tornado, monotonic_now=3.0) == tornado


def test_repeat_confirmation_expires_outside_window() -> None:
    confirmer = SameRepeatConfirmer(confirmation_window=5.0)
    observation = _observation()

    assert confirmer.process(observation, monotonic_now=0.0) is None
    assert confirmer.process(observation, monotonic_now=6.0) is None
    assert confirmer.process(observation, monotonic_now=7.0) == observation


def test_accepted_duplicates_are_suppressed_then_require_reconfirmation() -> None:
    confirmer = SameRepeatConfirmer(
        confirmation_window=5.0, duplicate_suppression=10.0
    )
    observation = _observation()

    assert confirmer.process(observation, monotonic_now=0.0) is None
    assert confirmer.process(observation, monotonic_now=1.0) == observation
    assert confirmer.process(observation, monotonic_now=2.0) is None
    assert confirmer.process(observation, monotonic_now=3.0) is None
    assert confirmer.process(observation, monotonic_now=12.0) is None
    assert confirmer.process(observation, monotonic_now=13.0) == observation


def test_end_of_message_resets_pending_confirmation() -> None:
    confirmer = SameRepeatConfirmer(confirmation_window=5.0)
    observation = _observation()
    eom = SameEndMessage()

    assert confirmer.process(observation, monotonic_now=0.0) is None
    assert confirmer.process(eom, monotonic_now=1.0) == eom
    assert confirmer.process(observation, monotonic_now=2.0) is None
    assert confirmer.process(observation, monotonic_now=3.0) == observation


def test_repeat_confirmer_bounds_duplicate_memory() -> None:
    confirmer = SameRepeatConfirmer(max_recent=2, duplicate_suppression=60.0)

    for offset, event_code in enumerate(("TOR", "SVR", "FFW")):
        observation = _observation(event_code)
        assert confirmer.process(observation, monotonic_now=float(offset * 2)) is None
        assert confirmer.process(observation, monotonic_now=float(offset * 2 + 1)) == observation

    assert confirmer.recent_count == 2
