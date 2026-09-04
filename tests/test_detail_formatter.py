from app.detail_formatter import build_alert_detail
from app.models import Alert


def _alert(event, description="", instruction=None, parameters=None):
    return Alert.from_feature({
        "id": "detail-test",
        "properties": {
            "event": event,
            "headline": f"{event} issued by NWS",
            "areaDesc": "Montgomery County",
            "messageType": "Alert",
            "effective": "2099-01-01T00:00:00+00:00",
            "expires": "2099-01-01T01:00:00+00:00",
            "description": description,
            "instruction": instruction,
            "parameters": parameters or {},
        },
    })


def test_severe_thunderstorm_detail_uses_structured_hazards_and_action():
    alert = _alert(
        "Severe Thunderstorm Warning",
        description="HAZARD...60 mph wind gusts.\n\nSOURCE...Radar indicated.",
        instruction="For your protection move to an interior room on the lowest floor of a building.",
        parameters={"maxWindGust": ["60 MPH"], "maxHailSize": ["Up to .75"]},
    )

    detail = build_alert_detail(alert)

    assert detail.startswith("DETAIL: ")
    assert "60 mph" in detail.lower()
    assert "0.75 in hail" in detail.lower()
    assert "interior room" in detail.lower()
    assert len(detail.encode("utf-8")) <= 195


def test_zero_hail_parameter_is_omitted():
    alert = _alert(
        "Special Weather Statement",
        description="HAZARD...Wind gusts up to 45 mph.",
        parameters={"maxWindGust": ["45 MPH"], "maxHailSize": ["0.00"]},
    )

    detail = build_alert_detail(alert)

    assert "45 mph winds" in detail.lower()
    assert "hail" not in detail.lower()


def test_heat_detail_uses_what_and_condenses_protective_instruction():
    alert = _alert(
        "Heat Advisory",
        description=(
            "* WHAT...Heat index values around 105 early this evening.\n\n"
            "* WHERE...Portions of Kentucky.\n\n"
            "* IMPACTS...Hot temperatures and humidity may cause heat illness."
        ),
        instruction=(
            "Drink plenty of fluids, stay in an air-conditioned room, stay out of "
            "the sun, and check up on relatives and neighbors."
        ),
    )

    detail = build_alert_detail(alert)

    assert "Heat index values around 105" in detail
    assert "Drink fluids" in detail
    assert "WHERE" not in detail


def test_action_compaction_never_adds_missing_advice():
    heat = _alert(
        "Heat Advisory",
        description="* WHAT...Heat index near 105.",
        instruction="Drink plenty of fluids and stay in an air-conditioned room.",
    )
    fog = _alert(
        "Dense Fog Advisory",
        description="* WHAT...Visibility below one quarter mile.",
        instruction="Slow down and use your low-beam headlights.",
    )

    heat_detail = build_alert_detail(heat)
    fog_detail = build_alert_detail(fog)

    assert "avoid sun" not in heat_detail
    assert "check on others" not in heat_detail
    assert "leave extra distance" not in fog_detail


def test_flash_flood_detail_keeps_rainfall_and_turn_around_action():
    alert = _alert(
        "Flash Flood Warning",
        description=(
            "Between 1.5 and 2.5 inches of rain has fallen. Flash flooding is "
            "ongoing or expected to begin shortly."
        ),
        instruction="Turn around, don't drown when encountering flooded roads. Most flood deaths occur in vehicles.",
    )

    detail = build_alert_detail(alert)

    assert "1.5 and 2.5 inches" in detail
    assert "Turn around, don't drown." in detail


def test_flash_flood_prefers_measured_rainfall_over_issuance_boilerplate():
    alert = _alert(
        "Flash Flood Warning",
        description=(
            "At 450 PM, Doppler radar indicated prior thunderstorms had produced "
            "heavy rain. Between 1.5 and 2.5 inches of rain has fallen. Flash "
            "flooding is ongoing or expected to begin shortly.\n\n"
            "HAZARD...Flash flooding caused by prior thunderstorms."
        ),
        instruction="Turn around, don't drown when encountering flooded roads.",
    )

    detail = build_alert_detail(alert)

    assert "1.5 and 2.5 inches of rain" in detail
    assert "At 450 PM" not in detail


def test_red_flag_detail_combines_wind_and_humidity_sections():
    alert = _alert(
        "Red Flag Warning",
        description=(
            "* IMPACTS: Strong gusty winds could cause erratic fire behavior.\n\n"
            "* WIND: Southwest 15 to 25 mph with gusts up to 35 mph.\n\n"
            "* HUMIDITY: As low as 9 to 15 percent."
        ),
        instruction="Avoid outdoor burning and activities that may produce sparks.",
    )

    detail = build_alert_detail(alert)

    assert "Southwest 15 to 25 mph" in detail
    assert "humidity as low as 9 to 15 percent" in detail.lower()
    assert "Avoid outdoor burning" in detail


def test_air_quality_detail_can_use_fact_when_instruction_is_missing():
    alert = _alert(
        "Air Quality Alert",
        description=(
            "A Code Orange Air Quality Action Day is in effect for ground level ozone. "
            "Sensitive groups may experience health effects. For more information visit https://example.test."
        ),
    )

    detail = build_alert_detail(alert)

    assert detail.startswith("DETAIL: A Code Orange")
    assert "https://" not in detail


def test_uncommon_alert_uses_labeled_what_and_first_action_sentence():
    alert = _alert(
        "Ashfall Advisory",
        description="* WHAT...Ash accumulation up to one quarter inch.\n\n* WHERE...The warned area.",
        instruction="Remain indoors if possible. Wear a mask if you must go outside.",
    )

    assert build_alert_detail(alert) == (
        "DETAIL: Ash accumulation up to one quarter inch. Remain indoors if possible."
    )


def test_detail_stays_silent_without_useful_nws_content():
    alert = _alert("Test Message", description="", instruction=None)

    assert build_alert_detail(alert) == ""


def test_test_messages_and_bulletin_headers_are_not_forwarded_as_details():
    test_alert = _alert(
        "Test Message",
        description="Monitoring message only.",
        instruction="Monitoring message only.",
    )
    watch = _alert(
        "Severe Thunderstorm Watch",
        description=(
            "SEVERE THUNDERSTORM WATCH 653 REMAINS VALID UNTIL 9 PM FOR THE "
            "FOLLOWING AREAS IN TENNESSEE THIS WATCH INCLUDES 4 COUNTIES."
        ),
    )
    issued_watch = _alert(
        "Severe Thunderstorm Watch",
        description=(
            "THE NATIONAL WEATHER SERVICE HAS ISSUED SEVERE THUNDERSTORM WATCH "
            "654 IN EFFECT UNTIL 10 PM FOR THE FOLLOWING AREAS. THIS WATCH "
            "INCLUDES 25 COUNTIES."
        ),
    )
    coded_outlook = _alert(
        "Hydrologic Outlook", description="ESFAFG...Locations Impacted..."
    )
    issuance_only = _alert(
        "Tornado Warning",
        description="The National Weather Service in Nashville has issued a Tornado Warning.",
    )
    status_test = _alert(
        "Tornado Warning",
        description="HAZARD...Tornado.",
        instruction="Take shelter now.",
    )
    status_test.raw["properties"]["status"] = "Test"

    assert build_alert_detail(test_alert) == ""
    assert build_alert_detail(watch) == ""
    assert build_alert_detail(issued_watch) == ""
    assert build_alert_detail(coded_outlook) == ""
    assert build_alert_detail(issuance_only) == ""
    assert build_alert_detail(status_test) == ""


def test_duplicate_fact_and_instruction_are_emitted_only_once():
    alert = _alert(
        "Local Weather Message",
        description="Monitoring message only.",
        instruction="Monitoring message only.",
    )

    assert build_alert_detail(alert) == "DETAIL: Monitoring message only."


def test_detail_never_splits_utf8_and_drops_lower_priority_text_to_fit():
    alert = _alert(
        "Unusual Warning",
        description="* WHAT..." + ("Dangerous café conditions " * 20) + ".",
        instruction="Remain indoors until conditions improve.",
    )

    detail = build_alert_detail(alert, max_bytes=120)

    assert len(detail.encode("utf-8")) <= 120
    assert "�" not in detail
    assert detail.startswith("DETAIL: ")


def test_detail_honors_every_small_byte_limit():
    alert = _alert(
        "Tornado Warning",
        description="HAZARD...Tornado observed near town.",
        instruction="Take shelter now.",
    )

    for max_bytes in range(50):
        detail = build_alert_detail(alert, max_bytes=max_bytes)
        assert len(detail.encode("utf-8")) <= max_bytes
        assert "�" not in detail
