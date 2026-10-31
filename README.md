# pyrdmnet

**A self-contained RDMnet (ANSI E1.33) broker and controller in pure Python – no external dependencies.**

pyrdmnet discovers RDM devices behind RDMnet gateways, reads device info, personalities and sensors, and can set
DMX personality and start address under strict safeguards. It runs standalone and can be embedded in other programs
through a synchronous interface.

> **Coming from 0.4 (German names)?** Version 0.5 is a breaking change: all names (modules, classes, functions, CLI commands and
> options, database tables and JSON keys) are now English. See [CHANGELOG.md](CHANGELOG.md) and the step-by-step
> guide [docs/MIGRATING_TO_0.5.md](docs/MIGRATING_TO_0.5.md). Existing SQLite databases are migrated automatically
> when opened (back them up first – the migration is one-way).

## Status

| | |
|---|---|
| Software tests | 235/235 passing on Linux, macOS and Windows (Python 3.10–3.13, standard library only) |
| Real-world test | with an **LSS MasterPort 2** (firmware 2.13) as RDMnet gateway and an **ETC ColorSource PAR**: broker discovery via mDNS, connection, finding devices, reading, setting mode/start address and reading back (tested with 0.4) |
| Other gateways/devices | not tested – feedback welcome |

No E1.33 conformance testing, no certification. Use at your own risk (see license).

## Features

- **Broker** per ANSI E1.33 (asyncio): connect with scope check, dynamic UIDs, client list, RPT forwarding
  (including broadcast), heartbeat, packet capture as JSONL, Client Entry Update (change an entry without reconnecting)
- **Fair queuing** (E1.33 9.2.10): the broker keeps at most `device_window` requests (default 4) in flight per device
  and sends waiting requests of several controllers round-robin, so a busy controller cannot starve the others
- **EPT** (E1.33 section 8): the broker accepts EPT clients, keeps their client list and forwards EPT data between
  them, including one-to-many messages packed with PDU inheritance; `EptClient` for own tools and tests
- **Broker RDM responder**: controllers can query the broker itself (`COMPONENT_SCOPE`, `SEARCH_DOMAIN`,
  `BROKER_STATUS`, device info). Scope and state cannot be changed via RDM.
- **mDNS registration** `_rdmnet._tcp` with scope subtype – via Bonjour (`dns-sd`, available on macOS), otherwise
  optionally via the `zeroconf` package
- **One broker per scope** (E1.33 9.1.4): before announcing itself the broker looks for other brokers on its scope;
  if there is one it stays in STANDBY (reported via `BROKER_STATUS`) until the other one is gone, and while running it
  warns when another broker appears. `brokers` lists all brokers of a scope.
- **Controller**: RDM GET, ACK_TIMER, ACK_OVERFLOW, NACK, status messages; `read_tcp_comms_status()` for the broker
  address and unhealthy-connection count of a device or controller (`TCP_COMMS_STATUS`)
- **Default responders of controllers and devices** (E1.33 7.2.1, 7.4.8): the controller answers RDM requests to its
  own UID (required E1.20 PIDs, `COMPONENT_SCOPE`, `SEARCH_DOMAIN`, `TCP_COMMS_STATUS`); a SET of `COMPONENT_SCOPE`
  moves a client to the new scope (and static broker). Unhealthy TCP connections are counted per scope.
- **Automatic reconnection** (E1.33 6.2.5) for controllers, devices and EPT clients: after a broker restart or a
  lost connection they reconnect by themselves with the back-off of the standard; Client Redirect (IPv4 and IPv6) is
  followed, and the broker can redirect a client itself (`redirect_client`).
- **IPv6**: the broker listens on IPv4 and/or IPv6 (`BrokerConfig.extra_hosts`, CLI `broker --ipv6 ::`), clients
  connect to either (`--broker [fd00::5]:8888`); static IPv6 broker addresses in `COMPONENT_SCOPE`.
  The facade waits for the reconnection instead of failing.
- **Reading** via gateway endpoints or native RDMnet devices: manufacturer, model, firmware, DMX personalities with
  footprint, start address, sensors, device hours
- **Settings check** of the device setting against an expected setting (mode, footprint, start address, universe)
- **Restricted RDM SET** (see below)
- **Sensor snapshots** before/after an action, stored in SQLite (tables `rdm_*`)
- **RDM over Art-Net** (ArtTod, ArtRdm)
- **LLRP** (E1.33 section 5): find RDMnet components without a broker, read and – after confirmation – set scope,
  static broker address and search domain. The broker can respond as an LLRP target itself (`--llrp`). IPv4 and IPv6.
- **Simulator** for tests without hardware; with `--llrp` also an LLRP target, e.g. started with a wrong scope and then
  fixed with the `llrp` command

## Write safety

The module only reads by default. Which RDM SET commands may be sent is set by a `WritePolicy` (controller,
Art-Net controller, LLRP manager, `RdmConfig.write_policy`):

- default `WritePolicy()`: any PID, but only after explicit write permission per session (`write_enabled = True`,
  facade: `set_write_enabled(True)`)
- `WritePolicy.lab()`: measurement-lab profile, only `DMX_PERSONALITY`, `DMX_START_ADDRESS` and `IDENTIFY_DEVICE`;
  `WritePolicy.llrp_lab()`: over LLRP only `COMPONENT_SCOPE`, `SEARCH_DOMAIN` and `IDENTIFY_DEVICE`
- own lists: `WritePolicy({C.PID_DEVICE_LABEL, ...})`; for scripts without the per-session permission:
  `WritePolicy(require_write_enabled=False)`
- the settings workflow reads every change back and logs it as old → new
- the module never sends discovery commands (`DISC_*`), whatever the policy; discovery is done by the gateway

```python
from pyrdmnet.controller import WritePolicy
module = RdmModule(RdmConfig(broker_host=adapter_ip, write_policy=WritePolicy.lab()))
```

## Quick start

Requires Python 3.10 or later. No further packages needed.

```bash
git clone https://github.com/Schlagi-lx/pyrdmnet.git
cd pyrdmnet
python3 -m unittest discover -s tests -t tests   # tests
python3 -m pyrdmnet demo                        # offline run with the simulator
```

Or install as a package: `pip install .` (optionally with mDNS via zeroconf: `pip install ".[zeroconf]"`).

### Commands

| Command | Purpose |
|---|---|
| `broker` | start the broker (runs until Ctrl+C) |
| `simulator` | connect a simulated RDMnet device to a broker |
| `read` | find and read all devices on the broker (read only) |
| `livetest` | test with a real gateway, read only |
| `livetest --checks all` | additionally `TCP_COMMS_STATUS`, two controllers at once, LLRP (read only), broker restart |
| `writetest` | set mode/start address on a real device, verify and restore (with on-site confirmation) |
| `llrp` | find components via LLRP (no broker needed); set with `--uid` and `--scope`/`--broker`/`--domain` |
| `brokers` | list the brokers announced via mDNS for a scope; warns if there is more than one |
| `demo` | complete offline run (simulation) |

Example with separate processes:

```bash
python3 -m pyrdmnet broker --host 127.0.0.1 --port 8888 --log events.jsonl
python3 -m pyrdmnet simulator --broker 127.0.0.1:8888
python3 -m pyrdmnet read --broker 127.0.0.1:8888 --json devices.json
```

Gateway with the wrong scope or without broker discovery? Find and reconfigure it via LLRP:

```bash
python3 -m pyrdmnet llrp --adapter 192.168.1.10                       # search, show scope
python3 -m pyrdmnet llrp --adapter 192.168.1.10 --uid 7ff0:00000100 \
        --scope default --broker 192.168.1.10:8888                    # set scope + static broker
python3 -m pyrdmnet llrp --adapter 192.168.1.10 --interface6 eth0     # IPv6 on a named interface
```

Try it without hardware: a simulator with the wrong scope keeps trying to connect and answers LLRP; fixing its scope
connects it at once.

```bash
python3 -m pyrdmnet broker --port 8888
python3 -m pyrdmnet simulator --broker 127.0.0.1:8888 --scope wrong --llrp
python3 -m pyrdmnet llrp                                                # finds the simulator, scope "wrong"
python3 -m pyrdmnet llrp --uid 7ff0:00000100 --scope default --yes      # it connects to the broker
```

LLRP runs over IPv4 and IPv6 at the same time. `--interface6` selects the IPv6 interface (name such as `eth0`/`en0`,
or the index; on Windows see `netsh interface ipv6 show interfaces`); without it the system default is used. If the
computer has no IPv6, LLRP continues over IPv4 with a warning.

On a real network, set `--host` to the IP of the network adapter and register via mDNS with `--dnssd`. On the
gateway, set broker discovery to mDNS and enter the same scope (default: `default`, case-sensitive). Avoid competing
brokers in the same scope.

### Windows

pyrdmnet runs on Windows with the standard Python from python.org (tested in CI with Python 3.10–3.13). Points to
note:

- **mDNS:** broker registration (`--dnssd`, default for `livetest`/`writetest`) needs either Apple's Bonjour
  service (provides `dns-sd`, e.g. "Bonjour Print Services for Windows") or the `zeroconf` package:
  `pip install ".[zeroconf]"`. Without either, use `--no-mdns` and enter the broker address in the gateway.
- **Firewall:** on the first broker start, Windows Defender Firewall asks whether Python may accept connections.
  Allow it for the network the gateway is on. If that network is classified as "Public", either change it to
  "Private" or allow inbound TCP 8888 (broker), UDP 5569 (LLRP) and UDP 6454 (Art-Net) for Python.
- **Adapter selection:** `livetest`/`writetest` without `--host` list the adapters from `ipconfig`.

## Embedding in your own programs

```python
from pyrdmnet.facade import RdmModule, RdmConfig
from pyrdmnet.settings_check import ExpectedSetting

module = RdmModule(RdmConfig(broker_host=adapter_ip, db="rdm.sqlite"))
module.start()
profile = module.read_device(module.find_devices()[0])
expected = ExpectedSetting(gdtf_mode, footprint, start_address, universe)
check = module.check_setting(profile, expected, reference="session-1")
if not check.measurement_allowed:
    module.set_write_enabled(True)          # only after on-site confirmation
    application, profile, check = module.apply_setting(profile, expected, reference="session-1")
with module.accompany_measurement(profile, measurement_ref="1"):
    ...  # your own action; sensors are stored before and after
module.stop()
```

A mode change requires a confirmed mapping RDM personality ↔ mode (per model and firmware); nothing is guessed.

## Structure

| File | Content |
|---|---|
| `constants.py` | protocol constants E1.20, E1.33, E1.37-7 |
| `rdm.py`, `rdm_params.py` | RDM messages, UID, checksum, parameter data |
| `pdu.py`, `connection.py` | ACN TCP preamble, root/broker/RPT PDUs, TCP connection |
| `broker.py`, `broker_responder.py` | broker, the broker's RDM responder |
| `client.py`, `client_responder.py`, `controller.py` | RPT client, default responder of clients, controller |
| `readout.py`, `settings_check.py` | reading devices, settings check, applying with read-back |
| `storage.py` | SQLite tables `rdm_*`, migration of 0.4 databases |
| `facade.py` | synchronous interface (asyncio in its own thread) |
| `dnssd.py` | mDNS registration |
| `artnet_rdm.py` | RDM over Art-Net |
| `llrp.py` | LLRP manager and target |
| `ept.py` | EPT PDUs (with inheritance), EPT client |
| `simulator.py`, `live_test.py`, `write_test.py`, `__main__.py` | simulator, tools, command line |

## Notes

- The protocol constants were checked against ETC's public reference implementations
  ([ETCLabs/RDMnet](https://github.com/ETCLabs/RDMnet), [ETCLabs/RDM](https://github.com/ETCLabs/RDM)); no code was
  taken from them.
- By default the broker uses the ESTA prototype manufacturer ID `0x7FF0` (intended for development). If you use the
  module in a product, set your own ESTA manufacturer ID in `RdmConfig.broker_uid`.
- The ANSI E1.20 and E1.33 standards are available from ESTA (tsp.esta.org).
- `COMPONENT_SCOPE`: E1.33-2019 states a data length of 0x54, but the fields add up to 88 bytes (0x58). pyrdmnet sends
  and expects 88 bytes, like ETC's reference implementation.
- "RDMnet" refers to the ESTA standard ANSI E1.33. This project is not affiliated with ESTA, ETC or LSS.

## License

MIT – see [LICENSE](LICENSE).
