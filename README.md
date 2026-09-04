<h1 align="center">WXDispatch</h1>

<p align="center">
  <b>Off-grid weather warnings.</b> WXDispatch watches the National Weather Service and
  broadcasts the alerts that matter over your <a href="https://meshtastic.org/">Meshtastic</a>
  and/or <a href="https://meshcore.co.uk/">MeshCore</a> radios, so your mesh keeps getting
  tornado, flood, and severe-storm warnings when the cell network and internet are gone.
</p>

<p align="center">
  Self-hosted · one small web app · runs on a Raspberry Pi, a Windows PC, or Docker · no account, no cloud.
</p>

<p align="center">
  <b>Status: v2.3.0.</b> Verified on a Heltec V3 for Meshtastic and MeshCore, over USB and over the network.
</p>

---


> [!NOTE]
> **Built for the whole mesh ecosystem.** WXDispatch speaks the standard Meshtastic and MeshCore
> serial and network protocols, so it is designed to run on the full range of boards those
> firmwares support (Heltec, LILYGO, RAK, Station G2, Seeed, and more). It is verified today
> on the Heltec V3 for both Meshtastic and MeshCore, over USB and over the network. As it is
> confirmed on more hardware the list will grow, so please report how yours does via
> [Issues](../../issues).

> [!WARNING]
> **WXDispatch is a supplemental tool, not a certified warning system.** It depends on your
> internet connection to reach the NWS API, on your hardware, and on LoRa propagation.
> Do **not** rely on it as your only source of life-safety alerts. Always keep an
> official channel: a NOAA Weather Radio, wireless emergency alerts, or local sirens.
> Test it in **dry-run mode** before you trust it, and review the settings for your area.

> [!IMPORTANT]
> **Routing starts empty on both first install and upgrade.** Upgrades intentionally create no
> automatic destinations or routing rules. Automated delivery remains inactive until you open
> **Routing**, create and enable a destination and routing rule before disabling dry-run. This
> prevents an upgrade from silently choosing a radio, channel, county, or audience for you.

## Why it exists

When a hurricane or flood takes out the towers, a LoRa mesh often keeps working, but the
mesh has no way to *know* a warning was issued. WXDispatch bridges that gap: it polls the
[NWS alerts API](https://www.weather.gov/documentation/services-web-api), decides what's
worth sending, formats it to fit a LoRa packet, and transmits it to everyone on your
channel. Built after living through Hurricane Helene's comms blackout.

## Features

- **Dual radio, side by side.** Run Meshtastic, MeshCore, or **both at once**: every
  alert goes to each enabled radio on its own channel.
- **NOAA weather + FEMA IPAWS.** Broadcast NWS weather warnings and, optionally, non-weather
  public safety alerts from IPAWS (AMBER, civil emergency, evacuation, and more), scoped to
  your counties.
- **Dead-simple setup.** Pick your state and check your counties; the NWS zones populate
  automatically. Choose which alerts to send from checklists, not cryptic codes to type.
- **Smart filtering.** Broadcast all Warnings plus Tornado Watch by default; add other
  Watches/Advisories à la carte.
- **No spam.** Never rebroadcasts the same alert; sends one concise *update* when a warning
  materially changes and a *cancellation* when it clears. Old state auto-expires.
- **Fits a LoRa packet.** Alerts are trimmed to ≤195 bytes, e.g.
  `[WX] Tornado Warning: Charleston +2 more until 8:45 PM EDT`.
- **Optional rich warning cards.** WXDispatch can dual-cast active warnings as
  COBS-encoded MeshWX v4 `0x20`/`0x21` binary frames on a dedicated non-Public
  MeshCore hash channel. Compatible clients render structured warning cards while
  the existing county text channels continue unchanged.
- **Dry-run by default.** Automated alerts are logged, not transmitted, until you flip it on.
- **A real dashboard.** Live radio status, recent alerts, 7-day activity, transmit log,
  and a per-radio **Send test** button to key up each radio on the bench.
- **Local alert map.** Shows every configured county and its current county-scoped NWS
  alerts, including multi-county events, partial lookup failures, and stale-data warnings.

## Install

Pick the one that matches your box. All three run the exact same app.

### 🐳 Docker (any Linux host, incl. 64-bit Raspberry Pi)

```bash
docker run -d --name meshwx \
  -p 8110:8000 \
  -e MESHWX_ADMIN_PASSWORD='choose-a-long-random-password' \
  -v meshwx-data:/data \
  --device-cgroup-rule='c 188:* rmw' \
  --device-cgroup-rule='c 166:* rmw' \
  -v /dev:/dev \
  --restart unless-stopped \
  ghcr.io/fizzlepoof/wxdispatch:latest
```

The legacy `ghcr.io/fizzlepoof/meshwx` image name remains published as a compatibility alias
so existing deployments can continue upgrading without changing their compose files immediately.

Or clone the repo and `docker compose up -d`. Then open `http://<host>:8110`.
The `--device` rules + `/dev` mount let the container reach any USB serial radio without
pinning a device path (they renumber on replug). See the compose file for details.

### 🥧 Raspberry Pi / Linux (native, no Docker)

64-bit Raspberry Pi OS, or any Debian / Ubuntu. Plug in your radio, then run **one command**:

```bash
curl -fsSL https://raw.githubusercontent.com/fizzlepoof/WXDispatch/main/install.sh | sudo bash
```

That is the whole install. It installs everything it needs (Python, venv, pip, git), puts
WXDispatch in the legacy-compatible `/opt/MeshWX` path, sets up a virtualenv, adds you to the `dialout` group for serial
access, and starts a `systemd` service on boot. Open `http://<this-host>:8110`.

**To update, or if an install ever goes wrong, run the exact same command again.** It pulls
the latest, repairs a stale or broken copy, and reinstalls. There is nothing else to
remember and no folder to clean up.

<details><summary>Advanced: clone and run the installer manually</summary>

```bash
git clone https://github.com/fizzlepoof/WXDispatch.git
cd WXDispatch && sudo ./packaging/install-linux.sh
```

The installer still handles all prerequisites and self-updates on each run.
</details>

Tested on 64-bit Raspberry Pi OS Bookworm and Ubuntu (Python 3.11 through 3.14): every
dependency installs as a prebuilt wheel, so no compiler or Rust toolchain is needed.
Manage with `sudo systemctl restart mesh-wx` and `journalctl -u mesh-wx -f`.

### 🪟 Windows

1. Download `WXDispatch-windows-*.zip` from the [Releases](../../releases) page.
2. Unzip anywhere and run **WXDispatch.exe**.
3. Your browser opens to the dashboard automatically. Keep the console window open;
   close it to stop WXDispatch.

No Python install required. Windows may warn about an unrecognized app the first time:
"More info → Run anyway" (the build is unsigned).

## First run

1. Open the dashboard, go to **Settings**.
2. **NOAA → NWS contact**: set this to your email address. The NWS API
   [requires a contact string](https://www.weather.gov/documentation/services-web-api)
   in every request; leaving the default placeholder can get you rate-limited or blocked.
3. **Coverage**: pick your state, check your counties.
4. **What to broadcast**: leave *All Warnings* on; add any watches/advisories you want.
5. **Radios**: enable Meshtastic and/or MeshCore. For each, choose **USB** or **network (IP/TCP)**, click **Connect and load channels**, then pick which channel carries live alerts and which carries test messages.
6. Save, then open **Routing**. Create and enable at least one destination, then create and
   enable a county/event rule that uses it. WXDispatch never creates routing rules automatically.
7. Go to **Troubleshoot → Send test**. A success response means the request was accepted by
   Meshtastic node software or accepted by MeshCore companion software; it does **not** prove
   over-air delivery. Confirm reception on a separate listening node.
8. Only after routing and reception are verified, turn **dry-run off** on the dashboard. WXDispatch
   blocks LIVE mode when no enabled rule has an enabled destination.

Upgrading an existing installation uses the same safety rule: **Upgrades intentionally create
no automatic destinations or routing rules.** Review **Routing**, create and enable a destination
and routing rule before disabling dry-run. Existing radio settings do not become routes by
themselves.


### Radio notes

- **Meshtastic**: any Meshtastic node, over **USB serial** or over the **network (TCP/IP)**,
  for example a WiFi connected node at its IP address (optionally `host:port`). On USB the
  board can renumber its port on replug; leave the port blank to auto-discover, or set it.
- **MeshCore**: flash the board with the **USB (companion)** firmware, *not* repeater
  firmware. Repeater firmware exposes no serial API, so WXDispatch can't drive it.
### Confirmed radios

Hardware verified working with WXDispatch, and over which connection. WXDispatch speaks the standard
Meshtastic and MeshCore protocols, so other boards on those firmwares are expected to work;
this list grows as they are confirmed.

| Radio | Protocol | USB | WiFi (TCP/IP) |
|---|---|:---:|:---:|
| Heltec V3 | Meshtastic | ✅ | ✅ |
| Heltec V3 | MeshCore | ✅ | ⬜ |
| Thinknode M7 | Meshtastic | ✅ | ⚠️ |
| RAK WisMesh Pocket V2 | Meshtastic | ✅ | ⬜ |
| Seeed Studio T1000-E | Meshtastic | ✅ | ⬜ |
| Seeed Studio MeshTracker X1 | Meshtastic | ✅ | ⬜ |
| Seeed Studio MeshTracker X1 | MeshCore | ✅ | ⬜ |

Key: ✅ confirmed reliable, ⚠️ works but the link is unreliable, ⬜ not tested yet.

### Channels: live vs. test

WXDispatch keeps testing off the air people are actually watching:

- **Channel 0 is the live channel**: real NWS alerts broadcast there (each radio's
  alert channel, index 0 by default).
- **Channel 1 is the test channel**: the per-radio **Test** buttons submit there, keeping tests
  separate from the live alert channel. The manual-send page submits to the **live** channel
  instead, since those are real messages you compose for people.

Both are set in **Settings** (per-radio alert channel, plus a per-radio **Test channel**).
Point your listening node at channel 0 for real alerts, or channel 1 to watch tests.

### Interface acceptance is not RF delivery

LoRa channel broadcasts are unacknowledged. For MeshCore, a successful WebUI result means the
request was **accepted by MeshCore companion**; it does not prove that the radio transmitted or
that another node received it. For Meshtastic, success means the request was **accepted by
Meshtastic node** software, likewise without proof of over-air delivery. The transmit log records
interface outcomes and failures, not RF delivery receipts. Verify tests on a separate receiving
node before relying on the system.

### Structured MeshWX v4 warnings

Enable **Structured MeshWX v4 warning cards** under Settings → Radios and select a dedicated
non-Public channel index containing the same hash channel on the sender and receiver, for example
`#middle-tn-meshwx-v4`. WXDispatch emits the public MeshWX v4 warning-zone (`0x21`) format when an
NWS forecast-zone UGC is available, or the warning-polygon (`0x20`) format when usable GeoJSON
geometry is present. County text routes remain the compatibility, update, and cancellation path;
binary output is an additional initial-warning feed and is disabled by default. It uses MeshCore's
experimental `0xFFFF` application-data namespace until MeshWX receives a registered data type. The
encoding is compatible with
[`digitaino/meshwx`](https://github.com/digitaino/meshwx) and clients that implement its v4 format.

### IPAWS alerts (FEMA)

Alongside NOAA weather, WXDispatch can broadcast **non-weather public safety alerts** from FEMA's
IPAWS feed: AMBER Alerts, civil emergencies, evacuations, shelter-in-place, law enforcement,
911/utility outages, hazardous materials, and local emergency management messages. Enable it
and pick which types to broadcast in **Settings**; the **IPAWS History** page shows what has
come through.

- **Scoped to your area** automatically, using the same counties you selected for weather.
- **Real alerts go on your live channel**; optional test/exercise messages go on the test
  channel (tagged `[IPAWS TEST]`) so you can confirm the mesh during quiet stretches.
- Weather alerts already carried by NOAA are not duplicated, and cancellations are broadcast
  so people know when an order is lifted.

## Configuration

Only bootstrap and channel-administration settings come from the environment. Everything else
lives in the UI and the database.

| Env var        | Default (native)                         | Purpose            |
| -------------- | ---------------------------------------- | ------------------ |
| `MESH_WX_PORT` | `8000` (`8110` for the systemd service)  | HTTP port          |
| `MESH_WX_HOST` | `0.0.0.0`                                | HTTP bind address  |
|| `MESH_WX_DB`   | per-OS data dir (see below)              | SQLite file path   |
|| `MESHWX_ADMIN_PASSWORD` | unset | Enables MeshCore companion channel set/clear and all-slot inventory; HTTP Basic username is `admin` |

When `MESHWX_ADMIN_PASSWORD` is unset, channel set/clear fail closed with HTTP 503 and the UI
marks channel administration disabled. The password is read from the process environment and is
never stored in the database or rendered in the WebUI. Read-only sanitized channel inspection
remains available. WXDispatch channel creation is hash-channel-only: names must begin with `#`, and
MeshCore derives the deterministic 16-byte channel key from the exact UTF-8 channel name. No
separate channel secret is accepted or stored. Anyone who knows the exact hash-channel name can
derive the same key. **Load all channels** reads every device-reported slot, including empty slots,
in one operation. That inventory requires administrator authentication because it reveals all exact
hash-channel names; channel keys are never returned or displayed.

The default database location when `MESH_WX_DB` is unset deliberately retains the original
MeshWX paths so upgrades reuse existing settings and history:
`/data` in Docker · `%LOCALAPPDATA%\MeshWX` on Windows ·
`~/Library/Application Support/MeshWX` on macOS · `~/.local/share/mesh-wx` on Linux.

## Development

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
pytest                 # unit tests (filter / formatter / dedupe / poller)
python -m app.main     # http://localhost:8000
```

Stack: FastAPI + Uvicorn, server-rendered Jinja templates + htmx, SQLite. No build step.
The filter, formatter (byte-cap), and dedupe logic are covered by unit tests backed by
captured NWS alert JSON fixtures under `tests/fixtures/`, with no serial/network deps.

## Keep it running when the grid goes down

WXDispatch only helps if it is still up when the weather turns bad, which is exactly
when grid power and internet tend to fail. The mesh side keeps relaying on its own,
but WXDispatch itself needs two things to *know* an alert was issued and push it out:
power, and a path to the National Weather Service. Plan for both.

- **Power: run it on battery, solar, or a UPS.** A Raspberry Pi and a LoRa radio
  draw very little, so a small solar panel with a battery, or even a modest UPS, can
  keep WXDispatch broadcasting for hours or days after the power drops. Put your listening
  nodes on backup power too, since a warning nobody's radio can receive helps no one.
- **Internet: use a resilient link like Starlink.** WXDispatch polls the NWS over the
  internet, so if your cable or fiber dies with the grid, it goes quiet. A satellite
  link such as Starlink, on its own battery or solar, keeps alerts flowing when
  terrestrial service is down.
- **Know the limit.** With no internet and no backup path, WXDispatch cannot fetch new
  alerts. It is a bridge from the NWS to your mesh, not a weather source of its own.
  Keep a NOAA Weather Radio as the offline fallback.

## Credits

Thanks to the people helping make WXDispatch real:

- **Matthew Crook (W1MRC)**: testing and outreach.

More hands are welcome. If you test WXDispatch, help spread the word, or run it on your own
mesh, open an [issue](../../issues) or pull request and you'll be added here.

## License

[MIT](LICENSE). Free to use, modify, and share. Contributions welcome.

<p align="center"><sub>WXDispatch is maintained by fizzlepoof and preserves the history, license, and attribution of <a href="https://brokensignal.tv/MeshWX/">MeshWX by BrokenSignal</a>.</sub></p>
