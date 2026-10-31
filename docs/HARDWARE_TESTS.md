# Hardware tests

Test plan for the open hardware items of [TODO.md](../TODO.md):

- **3c** LLRP with the MasterPort
- **8** several gateways at once

It also checks on real hardware what has so far only been tested against the simulator: `TCP_COMMS_STATUS`, fair
queuing with two controllers, and reconnection after a broker restart.

Everything except T3 and T6 only reads. Every run writes a result folder (`live_test_<date>_<time>`) – **send back
the whole folder** for each test, plus the notes asked for below.

## Before the appointment

**Hardware**

- [ ] LSS MasterPort 2. Note the firmware version (web interface).
- [ ] For T4: a second RDMnet gateway or native RDMnet device. A second MasterPort is fine.
- [ ] At least one RDM fixture on a MasterPort port, with RDM enabled on the fixture. Note manufacturer, model,
      mode and start address.
- [ ] A switch, network cables and a DMX cable with terminator.

**Computer**

- [ ] pyrdmnet from `main` installed. `python3 -m pyrdmnet demo` runs without errors.
- [ ] A fixed IPv4 address on the network adapter facing the gateway, e.g. `192.168.1.10/24`, with the MasterPort
      in the same subnet. Note both addresses.
- [ ] Firewall:
  - Windows: network profile "Private", or allow inbound TCP 8888 and UDP 5569 for Python.
  - macOS: allow incoming connections for Python.
- [ ] mDNS:
  - macOS has Bonjour built in.
  - Windows: Bonjour or `pip install zeroconf`.
  - Without mDNS, use `--no-mdns` and enter the broker statically in the MasterPort.
- [ ] No other RDMnet broker on the network, or a different scope for the test.

**MasterPort**

- [ ] RDMnet enabled, scope `default`, broker discovery "mDNS" (or static: computer IP, port 8888).
- [ ] The fixture's port configured as DMX output with RDM enabled, and the universe active (sACN).
- [ ] Take a screenshot of the RDMnet settings page – it will be needed again in T3.

## T1 – Live test with all read-only checks

Covers reading, `TCP_COMMS_STATUS` (E1.33 7.6.3) and fair queuing with two controllers (9.2.10).

```bash
python3 -m pyrdmnet livetest --host 192.168.1.10 --checks comms,controllers,llrp
```

| Expected | If not |
|---|---|
| MasterPort connects within a few seconds ("CONNECTED: Device ...") | Read the hints at the end; usually scope, firewall or mDNS. Retry with `--no-mdns` and a static broker in the MasterPort. |
| Gateway, ports and fixture are read; report "SUCCESSFUL" | Note what is missing ("unavailable: ..."); the folder has the details |
| `comms: OK` – scope `default`, broker = computer IP:8888 | `not supported` is a finding, not an error: the MasterPort does not implement the PID required for devices |
| `controllers: OK` – B done long before all of A's 20 requests | Note `requests_per_s` and `quiet_done_s` from the report. `inconclusive`: the device answered too fast to see any queuing (fine); `failed` only if B waited behind all of A's requests although they took 0.5 s or more |
| `llrp: OK` – MasterPort found via LLRP with scope `default` | `not supported`: no LLRP answer – check UDP 5569 in the firewall; retry with T3 |

## T2 – Broker restart

Covers automatic reconnection of the gateway (E1.33 6.2.5).

```bash
python3 -m pyrdmnet livetest --host 192.168.1.10 --checks restart
```

Expected: `restart: OK`, MasterPort back after a few seconds (`reconnect_s` in the report). If it does not come back
within 60 s, note what the MasterPort web interface shows for the RDMnet connection.

## T3 – LLRP: fix a wrong scope (item 3c)

Changes the MasterPort configuration via LLRP and puts it back. This is the only test besides T6 that writes.

1. In the MasterPort, set the RDMnet scope to `wrong` (take a screenshot of the original settings first).
2. Start a broker that shows connection attempts:
   ```bash
   python3 -m pyrdmnet broker --host 192.168.1.10 --dnssd --log t3_events.jsonl
   ```
   The MasterPort is rejected because of the scope (or does not find the broker via mDNS).
3. In a second terminal, search via LLRP:
   ```bash
   python3 -m pyrdmnet llrp --adapter 192.168.1.10 --json t3_before.json
   ```
   Expected: the MasterPort is listed with scope `wrong`. Note its UID.
4. Set the scope via LLRP (with a static broker, so mDNS does not matter):
   ```bash
   python3 -m pyrdmnet llrp --adapter 192.168.1.10 --uid <UID> --scope default --broker 192.168.1.10:8888
   ```
   Confirm with `y`. Expected: "read back" shows scope `default` and the broker `192.168.1.10:8888`. In the broker
   terminal the MasterPort connects.
5. Search again with `--all` (it is connected now):
   ```bash
   python3 -m pyrdmnet llrp --adapter 192.168.1.10 --all --json t3_after.json
   ```
6. Restore the original MasterPort settings (screenshot) in its web interface.

Send back: `t3_events.jsonl`, `t3_before.json`, `t3_after.json` and the console output. Also note whether the
MasterPort web interface showed the new scope and static broker.

Optional, if the computer has IPv6 on that adapter: repeat step 3 with `--interface6 <name or index>` and check
whether the MasterPort answers over IPv6 too (the output says "LLRP discovery via IPv4 + IPv6").

## T4 – Several gateways at once (item 8)

Connect the second gateway or device to the same network, scope `default`, ideally with an RDM fixture as well.

```bash
python3 -m pyrdmnet livetest --host 192.168.1.10 --checks all
```

Expected:

- Both gateways connect and are listed with their ports and fixtures.
- `comms` lists both.
- `llrp` finds both.
- `restart` brings both back.

Note which devices were connected and with which firmware.

## T5 – IPv6 broker connection (optional)

Only if the MasterPort supports RDMnet over IPv6 (see its web interface).

```bash
python3 -m pyrdmnet broker --host 192.168.1.10 --ipv6 :: --dnssd --log t5_events.jsonl
python3 -m pyrdmnet read --broker 192.168.1.10:8888 --json t5_devices.json
```

Check in `t5_events.jsonl` whether the MasterPort connected over IPv6: the peer address appears in brackets, e.g.
`[fe80::...]`.

## T6 – Write test (regression, optional)

Same as before the 0.5 changes: set mode and start address, read back, restore. Only after the on-site checklist.

```bash
python3 -m pyrdmnet writetest --host 192.168.1.10 --target-mode 2 --target-address 101
```

## What to send back

- [ ] All result folders and files of T1–T6
- [ ] MasterPort firmware version, model and firmware of the fixture(s)
- [ ] Screenshots of the MasterPort RDMnet settings (original, after T3)
- [ ] Notes: anything unexpected, with the time it happened (the event logs have timestamps)

## Rehearsal without hardware

The checks of T1, T2 and T4 run against two simulated gateways in `tests/test_live_checks.py`. The T3 flow can be
tried with the simulator, see the LLRP walkthrough in the README.

## Findings 2026-10-31

Setup: MacBook Pro (macOS 27, Python 3.13), LSS MasterPort 2 firmware v2.13.0028, ETC ColorSource Par CPU3 (firmware
4.0.1.34) on port 1, mDNS discovery, scope `default`.

| Test | Result |
|---|---|
| T1 | Successful. Gateway and fixture read; `TCP_COMMS_STATUS` supported (0 unhealthy events); two controllers: B done after 9 of A's 20 requests, about 34 RDM requests/s to the fixture; LLRP discovery and reading over IPv4 |
| T2 | Successful. The MasterPort reconnects by itself about 16 s after the broker restart (within E1.33 6.2.5) |
| T3 | LLRP discovery and reading work (also without broker connection). **SET COMPONENT_SCOPE is acknowledged but not applied** – over LLRP the MasterPort shows "portconfig changed" and restarts its network part (no LLRP answer for a few seconds, ACK carries two unexpected data bytes `0x0046`), but keeps the old scope, also after a power cycle; over RDMnet it acknowledges without any effect. SET SEARCH_DOMAIN is acknowledged; the device stores the domain without the final dot. `SUPPORTED_PARAMETERS` is not available via LLRP. The scope has to be changed in the MasterPort's menu. |
| T4 | Not possible: no second RDMnet device. Covered against two simulated gateways. |
| LLRP over IPv6 | On macOS the IPv6 interface must be given explicitly (now derived from `--adapter`). With only a link-local address (fe80::) on the adapter, sending to the LLRP groups `ff18::…` fails with "No route to host": they are not link-local, so the interface needs a global or unique-local IPv6 address (e.g. fd00::10/64). |

Changes in pyrdmnet from these findings: IPv6 interface from the IPv4 adapter; `llrp` reports "not applied" instead
of a traceback and keeps reading back for up to 15 s (devices that restart after a configuration change); search
domains are compared without the final dot; brokers stopped with SIGTERM remove their mDNS registration; the
two-controller check rates very fast devices as `inconclusive`.
