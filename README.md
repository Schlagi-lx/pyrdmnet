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
| Software tests | 149/149 passing (Python 3.10–3.13, standard library only) |
| Real-world test | with an **LSS MasterPort 2** (firmware 2.13) as RDMnet gateway and an **ETC ColorSource PAR**: broker discovery via mDNS, connection, finding devices, reading, setting mode/start address and reading back (tested with 0.4) |
| Other gateways/devices | not tested – feedback welcome |

No E1.33 conformance testing, no certification. Use at your own risk (see license).

## Features

- **Broker** per ANSI E1.33 (asyncio): connect with scope check, dynamic UIDs, client list, RPT forwarding
  (including broadcast), heartbeat, packet capture as JSONL, Client Entry Update (change an entry without reconnecting)
- **EPT** (E1.33 section 8): the broker accepts EPT clients, keeps their client list and forwards EPT data between
  them, including one-to-many messages packed with PDU inheritance; `EptClient` for own tools and tests
- **Broker RDM responder**: controllers can query the broker itself (`COMPONENT_SCOPE`, `SEARCH_DOMAIN`,
  `BROKER_STATUS`, device info). Scope and state cannot be changed via RDM.
- **mDNS registration** `_rdmnet._tcp` with scope subtype – via Bonjour (`dns-sd`, available on macOS), otherwise
  optionally via the `zeroconf` package
- **Controller**: RDM GET, ACK_TIMER, ACK_OVERFLOW, NACK, status messages
- **Reading** via gateway endpoints or native RDMnet devices: manufacturer, model, firmware, DMX personalities with
  footprint, start address, sensors, device hours
- **Settings check** of the device setting against an expected setting (mode, footprint, start address, universe)
- **Restricted RDM SET** (see below)
- **Sensor snapshots** before/after an action, stored in SQLite (tables `rdm_*`)
- **RDM over Art-Net** (ArtTod, ArtRdm)
- **LLRP** (E1.33 section 5): find RDMnet components without a broker, read and – after confirmation – set scope,
  static broker address and search domain. The broker can respond as an LLRP target itself (`--llrp`). IPv4 only.
- **Simulator** for tests without hardware

## Write safety

The module only reads by default. Writing is deliberately narrow:

- only `DMX_PERSONALITY`, `DMX_START_ADDRESS` and `IDENTIFY_DEVICE` (over LLRP: `COMPONENT_SCOPE`, `SEARCH_DOMAIN`,
  `IDENTIFY_DEVICE`)
- only after explicit write permission per session
- every change is read back and logged as old → new
- the module never sends discovery commands (`DISC_*`); discovery is done by the gateway

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
| `writetest` | set mode/start address on a real device, verify and restore (with on-site confirmation) |
| `llrp` | find components via LLRP (no broker needed); set with `--uid` and `--scope`/`--broker`/`--domain` |
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
```

On a real network, set `--host` to the IP of the network adapter and register via mDNS with `--dnssd`. On the
gateway, set broker discovery to mDNS and enter the same scope (default: `default`, case-sensitive). Avoid competing
brokers in the same scope.

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
| `client.py`, `controller.py` | RPT client, controller |
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
