"""BridgeTransmitter: HTTP transport to a mesh-ai-bridge send API.

Radio-free like the rest of the suite: respx intercepts httpx, so these tests
pin the wire contract (paths, payload shapes, token header) and the mapping
from bridge HTTP responses to TxUnsent categories the retry loop acts on.
"""
import httpx
import pytest
import respx

from app.config import MAX_PAYLOAD_BYTES
from app.transmit import BridgeTransmitter, TxUnsent, _build_transports

URL = "http://bridge:8700"
TOKEN = "sekrit"
TEST_DM = "!1d1ea7ae"


def _health(node="RAZOREDG Base"):
    return respx.get(f"{URL}/api/health").mock(
        return_value=httpx.Response(200, json={"ok": True, "node": node}))


async def _connected(**kw):
    tx = BridgeTransmitter(URL, TOKEN, kw.pop("test_dm", TEST_DM))
    _health()
    await tx.connect()
    return tx


# ---- connect / info ------------------------------------------------------

@respx.mock
async def test_connect_probes_health_and_reads_node_name():
    tx = await _connected()
    assert tx.connected
    info = await tx.read_info()
    assert info["model"] == "Meridian bridge via RAZOREDG Base"
    await tx.close()
    assert not tx.connected


@respx.mock
async def test_connect_failure_raises_and_stays_disconnected():
    respx.get(f"{URL}/api/health").mock(return_value=httpx.Response(500))
    tx = BridgeTransmitter(URL, TOKEN)
    with pytest.raises(Exception):
        await tx.connect()
    assert not tx.connected


async def test_send_before_connect_is_a_runtime_error():
    tx = BridgeTransmitter(URL, TOKEN)
    with pytest.raises(RuntimeError):
        await tx.send_text("x", 0)


# ---- wire contract -------------------------------------------------------

@respx.mock
async def test_broadcast_posts_channel_and_token():
    tx = await _connected()
    route = respx.post(f"{URL}/api/send").mock(
        return_value=httpx.Response(200, json={"ok": True}))
    await tx.send_text("[WX] Tornado Warning: Broward until 8:45 PM EDT", 0)
    req = route.calls.last.request
    assert req.headers["X-Send-Token"] == TOKEN
    import json
    body = json.loads(req.content)
    assert body["channel"] == 0
    assert "to" not in body


@respx.mock
async def test_test_sentinel_sends_dm_not_channel():
    tx = await _connected()
    route = respx.post(f"{URL}/api/send").mock(
        return_value=httpx.Response(200, json={"ok": True}))
    await tx.send_text("[WX] mesh-wx test", -1)
    import json
    body = json.loads(route.calls.last.request.content)
    assert body["to"] == TEST_DM
    assert "channel" not in body


@respx.mock
async def test_test_sentinel_without_dm_target_fails_fast():
    tx = await _connected(test_dm="")
    route = respx.post(f"{URL}/api/send")
    with pytest.raises(TxUnsent) as e:
        await tx.send_text("x", -1)
    assert e.value.category == "no_channel"
    assert not route.called   # config error: never keys the API


# ---- HTTP status -> TxUnsent category mapping ----------------------------

async def _send_expect(status, body, category, text="x", detail_has=""):
    tx = await _connected()
    respx.post(f"{URL}/api/send").mock(
        return_value=httpx.Response(status, json=body))
    with pytest.raises(TxUnsent) as e:
        await tx.send_text(text, 0)
    assert e.value.category == category
    if detail_has:
        assert detail_has in e.value.detail


@respx.mock
async def test_429_rate_limited_maps_to_duty_cycle():
    await _send_expect(429, {"error": "rate limited"}, "duty_cycle")


@respx.mock
async def test_503_radio_down_maps_to_link():
    await _send_expect(503, {"error": "radio not connected"}, "link")


@respx.mock
async def test_502_radio_send_failed_maps_to_unsent():
    await _send_expect(502, {"error": "radio send failed"}, "unsent")


@respx.mock
async def test_401_unauthorized_fails_fast_with_token_hint():
    await _send_expect(401, {"error": "unauthorized"}, "no_channel",
                       detail_has="token")


@respx.mock
async def test_400_over_bytes_maps_to_too_large():
    await _send_expect(400, {"error": "text over 190 bytes"}, "too_large")


@respx.mock
async def test_400_bad_destination_maps_to_no_channel():
    await _send_expect(400, {"error": "bad destination"}, "no_channel")


@respx.mock
async def test_network_error_maps_to_link():
    tx = await _connected()
    respx.post(f"{URL}/api/send").mock(side_effect=httpx.ConnectError("boom"))
    with pytest.raises(TxUnsent) as e:
        await tx.send_text("x", 0)
    assert e.value.category == "link"


# ---- integration points --------------------------------------------------

def test_payload_cap_matches_bridge_chunk_bytes():
    # the live bridge rejects text over CHUNK_BYTES=190; the formatter must
    # never produce a message the transport cannot send
    assert MAX_PAYLOAD_BYTES <= 190


def test_build_transports_includes_bridge():
    class Db:
        def __init__(self, d):
            self.d = d

        def get_setting(self, k, default=None):
            return self.d.get(k, default)

    db = Db({"bridge_enabled": True, "bridge_url": "http://bridge:8700/",
             "bridge_token": "t", "bridge_test_dm": TEST_DM})
    t = _build_transports(db)["bridge"]
    assert t.enabled and t.conn == "http"
    assert t.channel == 0 and t.test_channel == -1
    tx = t.make()
    assert isinstance(tx, BridgeTransmitter)
    assert tx.url == "http://bridge:8700"   # trailing slash normalized
    assert tx.test_dm == TEST_DM

    off = _build_transports(Db({}))["bridge"]
    assert not off.enabled   # disabled by default


@respx.mock
async def test_read_channels_reflects_live_and_test_semantics():
    tx = await _connected()
    chans = await tx.read_channels()
    assert [c["index"] for c in chans] == [0, -1]
    assert TEST_DM in chans[1]["name"]
