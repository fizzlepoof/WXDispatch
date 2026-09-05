from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

import pytest

from app.nwws import (
    MAX_RAW_TEXT_BYTES,
    MAX_STANZA_BYTES,
    NWWSHistory,
    NWWSParseError,
    NWWSProduct,
    SequenceTracker,
    friendly_event_name,
    parse_nwws_stanza,
    parse_product_metadata,
    parse_ugc,
    parse_vtec,
    project_to_feature,
    vtec_correlation_key,
)
from app.models import Alert


OFFICIAL_STANZA = b"""<message type='groupchat' from='nwws@host/resource' to='client@example'>
  <x xmlns='nwws-oi' cccc='KOHX' ttaaii='WFUS54' issue='2026-09-05T01:00:00Z'
     awipsid='TOROHX' id='654321.42'>WFUS54 KOHX 050100
TOROHX

TNC021-037-043-147-165-051900-
/O.NEW.KOHX.TO.W.0042.260905T0100Z-260905T0200Z/
Tornado Warning text.</x>
</message>"""


def test_parses_official_style_nwws_oi_stanza() -> None:
    parsed = parse_nwws_stanza(OFFICIAL_STANZA)

    assert isinstance(parsed, NWWSProduct)
    assert parsed.issuing_office == "KOHX"
    assert parsed.wmo_id == "WFUS54"
    assert parsed.awips_id == "TOROHX"
    assert parsed.issue_time == datetime(2026, 9, 5, 1, 0, tzinfo=timezone.utc)
    assert parsed.process_id == "654321"
    assert parsed.sequence == 42
    assert parsed.stream_id == "654321.42"
    assert parsed.raw_text.endswith("Tornado Warning text.")
    assert parsed.actionable is True


def test_body_only_history_stanza_is_explicitly_incomplete() -> None:
    parsed = parse_nwws_stanza(
        "<message type='groupchat'><body>NWWS history is not replayable product metadata</body></message>"
    )

    assert isinstance(parsed, NWWSHistory)
    assert parsed.actionable is False
    assert parsed.body == "NWWS history is not replayable product metadata"


@pytest.mark.parametrize("message_type", [None, "chat"])
def test_rejects_non_groupchat_message_stanzas(message_type: str | None) -> None:
    stanza = OFFICIAL_STANZA
    if message_type is None:
        stanza = stanza.replace(b" type='groupchat'", b"")
    else:
        stanza = stanza.replace(b"type='groupchat'", f"type='{message_type}'".encode())

    with pytest.raises(NWWSParseError, match="expected groupchat message"):
        parse_nwws_stanza(stanza)


def test_wrong_namespace_x_is_not_downgraded_to_history() -> None:
    stanza = b"""<message type='groupchat'>
      <x xmlns='urn:not-nwws'>untrusted payload</x>
      <body>history-looking body</body>
    </message>"""

    with pytest.raises(NWWSParseError, match="invalid x namespace"):
        parse_nwws_stanza(stanza)


@pytest.mark.parametrize(
    "stanza, error",
    [
        (b"<message><x", "malformed XML"),
        (b"<message type='groupchat'/>", "missing nwws-oi payload"),
        (
            b"<message type='groupchat'><x xmlns='wrong' cccc='KOHX' ttaaii='WFUS54' "
            b"issue='2026-09-05T01:00:00Z' awipsid='TOROHX' id='1.1'>x</x></message>",
            "invalid x namespace",
        ),
        (
            OFFICIAL_STANZA.replace(b" issue='2026-09-05T01:00:00Z'", b""),
            "missing issue",
        ),
        (
            OFFICIAL_STANZA.replace(b"2026-09-05T01:00:00Z", b"2026-09-05T01:00:00"),
            "issue time must be UTC",
        ),
        (
            OFFICIAL_STANZA.replace(b"2026-09-05T01:00:00Z", b"2026-09-05T02:00:00+01:00"),
            "issue time must be UTC",
        ),
        (OFFICIAL_STANZA.replace(b"KOHX", b"KO HX", 1), "invalid cccc"),
        (OFFICIAL_STANZA.replace(b"WFUS54", b"BAD", 1), "invalid ttaaii"),
        (OFFICIAL_STANZA.replace(b"TOROHX", b"TO/R", 1), "invalid awipsid"),
        (OFFICIAL_STANZA.replace(b"654321.42", b"bad id"), "invalid stream id"),
        (OFFICIAL_STANZA.replace(b"Tornado Warning text.", b"bad\x01text"), "control character"),
        (OFFICIAL_STANZA.replace(b"Tornado Warning text.", b"bad\x7ftext"), "control character"),
    ],
)
def test_rejects_malformed_or_unsafe_product_stanzas(stanza: bytes, error: str) -> None:
    with pytest.raises(NWWSParseError, match=error):
        parse_nwws_stanza(stanza)


def test_rejects_oversized_xml_and_raw_product() -> None:
    with pytest.raises(NWWSParseError, match="stanza too large"):
        parse_nwws_stanza(b" " * (MAX_STANZA_BYTES + 1))

    oversized = OFFICIAL_STANZA.replace(
        b"Tornado Warning text.", b"X" * (MAX_RAW_TEXT_BYTES + 1)
    )
    with pytest.raises(NWWSParseError, match="product text too large"):
        parse_nwws_stanza(oversized)


def test_official_product_header_may_follow_sequence_line() -> None:
    parsed = parse_nwws_stanza(
        OFFICIAL_STANZA.replace(b">WFUS54 KOHX 050100", b">111\nWFUS54 KOHX 050100")
    )

    assert isinstance(parsed, NWWSProduct)
    assert parsed.raw_text.startswith("111\nWFUS54 KOHX 050100\nTOROHX")


@pytest.mark.parametrize("bbb", ["COR", "AMD", "RRA", "RRX", "CCA", "AAX"])
def test_accepts_valid_optional_wmo_bbb_indicator(bbb: str) -> None:
    parsed = parse_nwws_stanza(
        OFFICIAL_STANZA.replace(b"WFUS54 KOHX 050100", f"WFUS54 KOHX 050100 {bbb}".encode())
    )

    assert isinstance(parsed, NWWSProduct)


@pytest.mark.parametrize("bbb", ["XYZ", "RR", "RRZ", "cor", "COR EXTRA"])
def test_rejects_invalid_wmo_bbb_indicator(bbb: str) -> None:
    stanza = OFFICIAL_STANZA.replace(
        b"WFUS54 KOHX 050100", f"WFUS54 KOHX 050100 {bbb}".encode()
    )

    with pytest.raises(NWWSParseError, match="invalid product headers"):
        parse_nwws_stanza(stanza)


def test_wmo_bbb_does_not_bypass_core_attribute_validation() -> None:
    stanza = OFFICIAL_STANZA.replace(
        b"WFUS54 KOHX 050100", b"WFUS54 KOHX 050100 COR"
    ).replace(b"cccc='KOHX'", b"cccc='KOUN'")

    with pytest.raises(NWWSParseError, match="product header mismatch"):
        parse_nwws_stanza(stanza)


def test_wmo_header_timestamp_must_match_stanza_issue_timestamp() -> None:
    stanza = OFFICIAL_STANZA.replace(b"WFUS54 KOHX 050100", b"WFUS54 KOHX 050101")

    with pytest.raises(NWWSParseError, match="product header timestamp mismatch"):
        parse_nwws_stanza(stanza)


@pytest.mark.parametrize(
    "header, error",
    [
        (b"WFUS55 KOHX 050100\nTOROHX", "product header mismatch"),
        (b"WFUS54 KOUN 050100\nTOROHX", "product header mismatch"),
        (b"WFUS54 KOHX 050100\nSVROHX", "product header mismatch"),
        (b"WFUS54 KOHX 052460\nTOROHX", "invalid product headers"),
        (b"TOROHX", "invalid product headers"),
        (b"WFUS54 KOHX 050100", "invalid product headers"),
    ],
)
def test_rejects_missing_malformed_or_mismatched_product_headers(
    header: bytes, error: str
) -> None:
    stanza = OFFICIAL_STANZA.replace(
        b"WFUS54 KOHX 050100\nTOROHX", header
    )

    with pytest.raises(NWWSParseError, match=error):
        parse_nwws_stanza(stanza)


def test_sequence_tracker_detects_same_process_gap() -> None:
    tracker = SequenceTracker(max_seen=8)

    first = tracker.observe("workerA.10")
    gap = tracker.observe("workerA.13")

    assert first.accepted is True
    assert first.gap is None
    assert gap.accepted is True
    assert gap.gap == (11, 12)
    assert gap.process_changed is False


def test_sequence_tracker_resets_on_process_restart_and_suppresses_replay() -> None:
    tracker = SequenceTracker(max_seen=3)

    tracker.observe("old.90")
    replay = tracker.observe("old.90")
    restart = tracker.observe("new.2")
    after_restart = tracker.observe("new.3")

    assert replay.accepted is False
    assert replay.replay is True
    assert restart.accepted is True
    assert restart.process_changed is True
    assert restart.gap is None
    assert after_restart.process_changed is False
    assert tracker.seen_count <= 3


def test_sequence_tracker_permanently_rejects_delayed_retired_process() -> None:
    tracker = SequenceTracker(max_seen=3)

    tracker.observe("old.90")
    tracker.observe("new.2")
    delayed = tracker.observe("old.91")
    current = tracker.observe("new.3")

    assert delayed.accepted is False
    assert delayed.replay is True
    assert delayed.process_changed is False
    assert current.accepted is True
    assert current.process_changed is False
    assert current.gap is None


def test_sequence_tracker_alternation_cannot_erase_retired_process_state() -> None:
    tracker = SequenceTracker(max_seen=3, max_retired=3)

    tracker.observe("first.1")
    tracker.observe("second.1")
    assert tracker.observe("first.2").accepted is False
    tracker.observe("third.1")
    assert tracker.observe("second.2").accepted is False
    assert tracker.observe("first.3").accepted is False
    assert tracker.observe("third.2").accepted is True
    assert tracker.retired_count == 2


def test_sequence_tracker_retired_process_memory_is_bounded() -> None:
    tracker = SequenceTracker(max_seen=2, max_retired=2)

    for index in range(5):
        assert tracker.observe(f"worker{index}.1").accepted is True

    assert tracker.retired_count == 2


def test_sequence_tracker_accepts_product_objects() -> None:
    product = parse_nwws_stanza(OFFICIAL_STANZA)
    assert isinstance(product, NWWSProduct)

    result = SequenceTracker().observe(product)

    assert result.accepted is True
    assert result.stream_id == "654321.42"


def test_parses_ugc_continuations_and_compressed_ranges() -> None:
    text = """WFUS54 KOHX 050100
TNZ005>007-009-
011-013>014-051900-
/O.NEW.KOHX.WS.W.0012.260905T0100Z-260905T1900Z/
"""

    assert parse_ugc(text) == (
        "TNZ005", "TNZ006", "TNZ007", "TNZ009",
        "TNZ011", "TNZ013", "TNZ014",
    )


def test_ugc_parser_keeps_only_valid_county_and_zone_codes() -> None:
    text = "TNC000-001-TNP003-ABC123-TNZ999-051900-"

    assert parse_ugc(text) == ("TNC001", "TNZ999")


@pytest.mark.parametrize(
    "prefix",
    [
        "AMZ", "ANZ", "GMZ", "LCZ", "LEZ", "LHZ", "LMZ",
        "LOZ", "LSZ", "PHZ", "PKZ", "PMZ", "PZZ", "SLZ",
    ],
)
def test_ugc_parser_accepts_nws_marine_and_pseudo_state_prefixes(prefix: str) -> None:
    assert parse_ugc(f"{prefix}001-051900-") == (f"{prefix}001",)


@pytest.mark.parametrize("expiration", ["000000", "320000", "012400", "010060", "999999"])
def test_ugc_parser_requires_valid_ddhhmm_expiration(expiration: str) -> None:
    assert parse_ugc(f"TNC021-{expiration}-") == ()


def test_parses_primary_vtec_and_product_metadata() -> None:
    metadata = parse_product_metadata(OFFICIAL_STANZA.decode().split(">", 2)[2])
    vtec = metadata.vtec

    assert metadata.ugc == ("TNC021", "TNC037", "TNC043", "TNC147", "TNC165")
    assert vtec is not None
    assert vtec.action == "NEW"
    assert vtec.office == "KOHX"
    assert vtec.phenomena == "TO"
    assert vtec.significance == "W"
    assert vtec.etn == 42
    assert vtec.start == datetime(2026, 9, 5, 1, 0, tzinfo=timezone.utc)
    assert vtec.end == datetime(2026, 9, 5, 2, 0, tzinfo=timezone.utc)
    assert metadata.event == "Tornado Warning"


@pytest.mark.parametrize(
    "code, expected",
    [
        ("TO.W", "Tornado Warning"),
        ("SV.W", "Severe Thunderstorm Warning"),
        ("FF.W", "Flash Flood Warning"),
        ("FL.W", "Flood Warning"),
        ("EH.W", "Excessive Heat Warning"),
        ("HT.Y", "Heat Advisory"),
        ("WS.W", "Winter Storm Warning"),
        ("WW.Y", "Winter Weather Advisory"),
        ("FA.Y", "Flood Advisory"),
        ("XY.Z", "Unknown NWS Event (XY.Z)"),
    ],
)
def test_maps_common_events_with_safe_unknown_fallback(code: str, expected: str) -> None:
    phenomena, significance = code.split(".")
    assert friendly_event_name(phenomena, significance) == expected


def test_unknown_or_malformed_products_have_no_vtec() -> None:
    assert parse_vtec("ordinary text /not-vtec/") is None
    assert parse_product_metadata("ordinary text").event == ""


def test_multiple_actionable_operational_segments_fail_closed() -> None:
    product = parse_nwws_stanza(
        OFFICIAL_STANZA.replace(b".NEW.", b".CAN.").replace(
            b"Tornado Warning text.",
            b"Tornado Warning cancellation.\n$$\n"
            b"TNZ005-051930-\n"
            b"/O.CON.KOHX.SV.W.0043.260905T0100Z-260905T0230Z/\n"
            b"Severe Thunderstorm Warning continuation.",
        )
    )
    assert isinstance(product, NWWSProduct)

    with pytest.raises(NWWSParseError, match="multiple actionable operational segments"):
        parse_product_metadata(product.raw_text)
    assert project_to_feature(product) is None


def test_single_actionable_segment_with_product_terminator_still_projects() -> None:
    product = parse_nwws_stanza(
        OFFICIAL_STANZA.replace(b"Tornado Warning text.", b"Tornado Warning text.\n$$\nNNNN")
    )
    assert isinstance(product, NWWSProduct)

    assert project_to_feature(product) is not None


def test_product_text_helpers_reject_unencodable_unicode() -> None:
    with pytest.raises(NWWSParseError, match="invalid product text encoding"):
        parse_ugc("\ud800")


def test_projects_sanitized_nws_like_feature_for_alert_model() -> None:
    product = parse_nwws_stanza(
        OFFICIAL_STANZA.replace(b"Tornado Warning text.", b"SECRET RAW BODY")
    )
    assert isinstance(product, NWWSProduct)

    feature = project_to_feature(product)

    assert feature is not None
    assert feature["id"] == "urn:nws:vtec:2026:KOHX:TO:W:0042"
    assert feature["properties"]["messageType"] == "Alert"
    assert feature["properties"]["event"] == "Tornado Warning"
    assert feature["properties"]["geocode"]["UGC"] == [
        "TNC021", "TNC037", "TNC043", "TNC147", "TNC165"
    ]
    assert feature["properties"]["expires"] == "2026-09-05T02:00:00Z"
    assert feature["properties"]["parameters"]["NWWSStreamID"] == ["654321.42"]
    assert feature["properties"]["parameters"]["VTEC"] == [
        "/O.NEW.KOHX.TO.W.0042.260905T0100Z-260905T0200Z/"
    ]
    assert feature["properties"]["parameters"]["VTECPhenomena"] == ["TO"]
    assert feature["properties"]["parameters"]["VTECSignificance"] == ["W"]
    assert feature["properties"]["parameters"]["VTECStartTime"] == [
        "2026-09-05T01:00:00Z"
    ]
    assert feature["properties"]["parameters"]["VTECEndTime"] == [
        "2026-09-05T02:00:00Z"
    ]
    assert "SECRET RAW BODY" not in repr(feature)
    alert = Alert.from_feature(feature)
    assert alert.nws_id == "urn:nws:vtec:2026:KOHX:TO:W:0042"
    assert alert.event == "Tornado Warning"


def test_vtec_identity_is_stable_across_nwws_process_sequences() -> None:
    first = parse_nwws_stanza(OFFICIAL_STANZA)
    replayed_elsewhere = parse_nwws_stanza(
        OFFICIAL_STANZA.replace(b"654321.42", b"worker_b.987")
    )
    assert isinstance(first, NWWSProduct)
    assert isinstance(replayed_elsewhere, NWWSProduct)

    metadata = parse_product_metadata(first.raw_text)
    assert metadata.vtec is not None
    expected = vtec_correlation_key(metadata.vtec, 2026)

    assert expected == "urn:nws:vtec:2026:KOHX:TO:W:0042"
    first_feature = project_to_feature(first)
    replayed_feature = project_to_feature(replayed_elsewhere)
    assert first_feature is not None
    assert replayed_feature is not None
    assert first_feature["id"] == expected
    assert replayed_feature["id"] == expected


def test_vtec_identity_uses_start_then_end_year_when_available() -> None:
    product = parse_nwws_stanza(
        OFFICIAL_STANZA.replace(
            b"260905T0100Z-260905T0200Z", b"000000T0000Z-251231T2359Z"
        )
    )
    assert isinstance(product, NWWSProduct)

    feature = project_to_feature(product)

    assert feature is not None
    assert feature["id"] == "urn:nws:vtec:2025:KOHX:TO:W:0042"


@pytest.mark.parametrize("action", ["CON", "CAN", "EXP", "EXT"])
def test_early_january_zero_time_followup_keeps_previous_year_identity(action: str) -> None:
    initial = parse_nwws_stanza(
        OFFICIAL_STANZA.replace(b"2026-09-05T01:00:00Z", b"2025-12-31T23:55:00Z")
        .replace(b"WFUS54 KOHX 050100", b"WFUS54 KOHX 312355")
        .replace(b"260905T0100Z-260905T0200Z", b"251231T2355Z-260101T0200Z")
    )
    followup = parse_nwws_stanza(
        OFFICIAL_STANZA.replace(b"2026-09-05T01:00:00Z", b"2026-01-01T00:05:00Z")
        .replace(b"WFUS54 KOHX 050100", b"WFUS54 KOHX 010005")
        .replace(b".NEW.", f".{action}.".encode())
        .replace(b"260905T0100Z-260905T0200Z", b"000000T0000Z-000000T0000Z")
    )
    assert isinstance(initial, NWWSProduct)
    assert isinstance(followup, NWWSProduct)

    initial_feature = project_to_feature(initial)
    followup_feature = project_to_feature(followup)

    assert initial_feature is not None
    assert followup_feature is not None
    assert initial_feature["id"] == "urn:nws:vtec:2025:KOHX:TO:W:0042"
    assert followup_feature["id"] == initial_feature["id"]
    assert followup_feature["properties"]["expires"] == "2026-01-01T00:05:00Z"


@pytest.mark.parametrize("action", ["CON", "CAN", "EXP", "EXT"])
def test_zero_time_followup_uses_current_year_after_january_grace_window(action: str) -> None:
    product = parse_nwws_stanza(
        OFFICIAL_STANZA.replace(b"2026-09-05T01:00:00Z", b"2026-01-08T00:05:00Z")
        .replace(b"WFUS54 KOHX 050100", b"WFUS54 KOHX 080005")
        .replace(b".NEW.", f".{action}.".encode())
        .replace(b"260905T0100Z-260905T0200Z", b"000000T0000Z-000000T0000Z")
    )
    assert isinstance(product, NWWSProduct)

    feature = project_to_feature(product)

    assert feature is not None
    assert feature["id"] == "urn:nws:vtec:2026:KOHX:TO:W:0042"


@pytest.mark.parametrize(
    "action, message_type",
    [
        ("NEW", "Alert"),
        ("CAN", "Cancel"),
        ("EXP", "Cancel"),
        ("CON", "Update"),
        ("EXT", "Update"),
        ("EXA", "Update"),
        ("EXB", "Update"),
    ],
)
def test_projects_vtec_actions(action: str, message_type: str) -> None:
    stanza = OFFICIAL_STANZA.replace(b".NEW.", f".{action}.".encode())
    product = parse_nwws_stanza(stanza)
    assert isinstance(product, NWWSProduct)

    feature = project_to_feature(product)

    assert feature is not None
    assert feature["properties"]["messageType"] == message_type


def test_projection_fails_closed_without_complete_actionable_metadata() -> None:
    product = parse_nwws_stanza(OFFICIAL_STANZA)
    assert isinstance(product, NWWSProduct)

    assert project_to_feature(replace(product, raw_text="ordinary text")) is None
    assert project_to_feature(
        replace(product, raw_text=product.raw_text.replace("TNC021-037-043-147-165-051900-", ""))
    ) is None
    assert project_to_feature(
        replace(product, raw_text=product.raw_text.replace("260905T0200Z", "000000T0000Z"))
    ) is None
    assert project_to_feature(replace(product, issue_time=datetime(2026, 9, 5, 1, 0))) is None
    assert project_to_feature(
        replace(product, raw_text=product.raw_text.replace(".NEW.", ".UPG."))
    ) is None


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("issuing_office", b"KOHX"),
        ("wmo_id", None),
        ("awips_id", ["TOROHX"]),
        ("stream_id", 42),
        ("issue_time", "2026-09-05T01:00:00Z"),
        ("process_id", None),
        ("sequence", "42"),
        ("sequence", True),
        ("raw_text", b"product"),
    ],
)
def test_projection_fails_closed_for_wrong_runtime_types(field: str, value: object) -> None:
    product = parse_nwws_stanza(OFFICIAL_STANZA)
    assert isinstance(product, NWWSProduct)

    assert project_to_feature(replace(product, **{field: value})) is None


def test_ugc_output_is_bounded() -> None:
    codes = parse_ugc("TNZ001>999-051900-")

    assert len(codes) == 256
    assert codes[0] == "TNZ001"
    assert codes[-1] == "TNZ256"
