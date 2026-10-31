# Changelog

## Unreleased

- Live test: additional checks with `--checks` (default `comms,controllers`; `all`, `none`): `TCP_COMMS_STATUS` of
  every gateway, two controllers at once on one fixture (fair queuing), LLRP discovery with scope and search domain
  (read only), broker restart with the reconnection time of every gateway. Results in `result.json` (`checks`) and
  a section of `report.md`. The `controllers` check reports `inconclusive` when the device answers too fast for
  any queuing to show.
- Test plan for the hardware tests: [docs/HARDWARE_TESTS.md](docs/HARDWARE_TESTS.md).
- The simulator can be an LLRP target (`SimulatedGateway.start_llrp()`, CLI `simulator --llrp` with `--adapter` /
  `--interface6`): same CID and default responder as the RPT component, connected devices only answer probes
  without the "inactive" filter. Scope, static broker and search domain can be changed via LLRP; a scope change
  disconnects with `DISCONNECT_LLRP_RECONFIGURE`.
- `connect(..., keep_trying=True)`: a failed first connect does not raise; the client keeps trying with the
  reconnection back-off (a device with the wrong scope waiting to be fixed via LLRP). A scope change while waiting
  ends the back-off pause at once.
- The broker logs the reason of a client's Disconnect (`client_disconnect` event, `C.DISCONNECT_TEXT`).
- Fair queuing in the broker (E1.33 9.2.10): per RPT client at most `BrokerConfig.device_window` (4) requests are in
  flight; further requests wait in one queue per controller and are sent round-robin, so each of N busy controllers
  gets at least 1/N of a device's bandwidth and an idle controller's share goes to the others. A request without an
  answer frees its slot after `request_timeout_s` (5 s); at most `max_queued_per_controller` (100) requests wait per
  controller and device (more are dropped, counter `request_queue_full`). When a device leaves, its waiting and
  in-flight requests are answered with Unknown RPT UID at once; when a controller leaves, its requests are removed.
  `device_window=0` forwards at once as before. The broker status shows `queued_requests`.
- IPv6 for broker connections: `BrokerConfig.host` may be an IPv6 address, `BrokerConfig.extra_hosts` adds further
  listen addresses on the same port (e.g. `("::",)` next to an IPv4 host; CLI `broker --ipv6 ::`,
  `RdmConfig.broker_extra_hosts`); `RdmnetBroker.addresses`. Clients connect over IPv4 or IPv6.
- Client Redirect IPv6 (E1.33 6.3.1.6) is followed during connect and while connected, like IPv4.
  `RdmnetBroker.redirect_client(uid, host, port)` sends a redirect (IPv6 only over IPv6 connections).
  `build_redirect_v6` / `read_redirect_v6`, `format_address` in `pdu`.
- IPv6 addresses appear as `[addr]:port` in logs, events and the broker status. mDNS registration via `zeroconf`
  accepts an IPv6 address.
- `TCP_COMMS_STATUS` (E1.33 7.6.3): RPT clients count unhealthy TCP connections (heartbeat timeout) per scope and
  report scope, broker address and count; SET resets the count (NACK `UNKNOWN_SCOPE` for another scope).
  `RdmnetController.read_tcp_comms_status()` / `reset_tcp_comms_status()`; `TcpCommsEntry` in `rdm_params`.
- Default responder of controllers (E1.33 7.4.8, 7.4.9) in the new module `client_responder.py`: the controller
  answers RDM requests addressed to it (IDENTIFY_DEVICE, SUPPORTED_PARAMETERS, PARAMETER_DESCRIPTION, labels,
  DEVICE_INFO, COMPONENT_SCOPE, SEARCH_DOMAIN, TCP_COMMS_STATUS). The broker now forwards requests to controllers too
  (9.2.7); before, it answered them with Unknown RPT UID.
- The simulator's default responder adds COMPONENT_SCOPE, SEARCH_DOMAIN, TCP_COMMS_STATUS and PARAMETER_DESCRIPTION.
  SET COMPONENT_SCOPE via RDMnet changes the scope (and static broker) of a client; it sends Disconnect with
  `DISCONNECT_RPT_RECONFIGURE` and, with automatic reconnection, joins the new scope. The client uses its search
  domain in Client Connect.
- Fix: the broker's SUPPORTED_PARAMETERS now lists the required PIDs too (E1.33 7.2.1 differs from E1.20 here), it
  answers PARAMETER_DESCRIPTION, and its product category is DATA_DISTRIBUTION (0x0801, was 0x0700).
- New constants: NACK reason codes of E1.33 table A-16, disconnect reason codes of table A-24.
- **Changed: RDM SET is governed by a `WritePolicy`** instead of the hard-wired `ALLOWED_SET_PIDS` (removed).
  Default `WritePolicy()`: any PID may be set, still only after the per-session write permission.
  `WritePolicy.lab()` restores the previous rule (only `DMX_PERSONALITY`, `DMX_START_ADDRESS`, `IDENTIFY_DEVICE`),
  `WritePolicy.llrp_lab()` the previous LLRP rule (`COMPONENT_SCOPE`, `SEARCH_DOMAIN`, `IDENTIFY_DEVICE`).
  Own lists via `WritePolicy({...})`; `require_write_enabled=False` drops the per-session permission. Discovery
  commands stay blocked under every policy. Set via `write_policy=` on `RdmnetController`, `ArtnetRdmController`,
  `LlrpManager` and `RdmConfig`. GelMatch should use `WritePolicy.lab()` (see the migration guide).
- Simulator fixture accepts SET `DEVICE_LABEL`.
- LLRP over IPv6 (E1.33 5.3): the manager listens on and sends to the IPv4 and IPv6 groups (`ff18::85:0:0:85` /
  `ff18::85:0:0:86`); targets (also the broker with `--llrp`) listen on both and answer over the family a request
  came in on. A request received over both families is handled only once, so an RDM SET is never executed twice.
  A family the host does not offer is skipped with a warning. New `LlrpNetwork` fields `ipv4`, `ipv6`,
  `interface6`, `hop_limit` (and `request_addr6`, `response_addr6`, `adapter6` for unicast); `LlrpManager.families`.
  CLI `llrp`: `--interface6`, `--hop-limit`, `--ipv4-only`, `--ipv6-only`.
- `set_scope(..., static_broker=...)` and `llrp --broker` also accept an IPv6 broker address (`[fd00::5]:8888`).
- EPT support in the broker (E1.33 section 8, 9.3): EPT clients connect with CLIENT_PROTOCOL_EPT (zero UIDs in
  the Connect Reply), get their own client list with incremental updates and Client Entry Update, and exchange
  EPT data. Blocks with PDU inheritance (e.g. one message to several CIDs, Appendix D.9) are split per destination
  and forwarded complete; unknown vectors and CIDs are answered with EPT status messages. Sender CIDs are checked.
- New module `ept.py` with EPT encoding and `EptClient`.
- Automatic reconnection (E1.33 6.2.5): `connect(..., auto_reconnect=True)` for all clients (controller, device,
  EPT). First two attempts immediate, then a back-off growing by a random 1-5 s after an established but failed
  connect (max. 30 s); refused TCP connects are retried every `reconnect_min_s`. Client Redirect IPv4 is followed
  during connect and while connected. New: `wait_connected()`, `auto_reconnecting`, `reconnect_count`,
  `on_connection_change`. The facade connects its controller with automatic reconnection and waits up to
  `RdmConfig.reconnect_wait_s` (10 s) for it. Leftover tasks of a previous connection are cancelled.
- Read-only facade calls (`find_devices`, `read_device`, `sensor_snapshot`) are repeated once after an automatic
  reconnection if the connection dropped during the call; writing calls are never repeated automatically.
- Fix: `RdmnetBroker.stop()` now stops accepting connections before disconnecting the clients, so reconnecting
  clients no longer land on the stopping broker (showed up on Windows); late connections are closed at once.
- One broker per scope (E1.33 9.1.4): `BrokerAnnouncer` looks for other brokers on the scope before registering via
  mDNS, keeps the broker in STANDBY while there is one (re-checks every 15 s) and warns when another broker appears
  later (check every 60 s). Used by `broker --dnssd`, `RdmConfig.dnssd` and the live test. `BROKER_STATUS` reports
  ACTIVE/STANDBY; SET BROKER_STATUS is answered with NACK ACTION_NOT_SUPPORTED (7.6.4).
- `find_brokers()` / `conflicting_brokers()` in `dnssd.py` (dns-sd or zeroconf) and the CLI command `brokers`.
- Windows support: tests run on Windows in CI; adapter list via `ipconfig` (Windows) and `ip` (Linux without
  `ifconfig`); `dns-sd` started via its resolved path; firewall hints per platform; README section for Windows.
- Pauses of the lab flows are configurable, defaults unchanged: `LiveConfig.settle_s`, `poll_s`, `search_retry_s`,
  `mdns_self_test_s`; `WriteConfig.readback_pause_s`; `ArtnetRdmController.flush_wait_s`. The test suite uses short
  values and runs in about 30 s instead of about 100 s.
- `ClientEntry` carries EPT sub-protocols (`ept_protocols`, `ClientEntry.ept()`); `type_text` is "EPT" for them.

## 0.5.0

**Breaking change: the whole project is now in English.** Code that uses pyrdmnet 0.4 has to be adapted; there
are no German aliases.

### Renamed

- Modules: `konstanten` → `constants`, `rdm_parameter` → `rdm_params`, `verbindung` → `connection`,
  `klient` → `client`, `auslesen` → `readout`, `abgleich` → `settings_check`, `speicher` → `storage`,
  `fassade` → `facade`, `livetest` → `live_test`, `schreibtest` → `write_test`
- Classes, functions, attributes and constants, e.g. `RdmModul` → `RdmModule`, `RdmKonfig` → `RdmConfig`,
  `EinstellungsErwartung` → `ExpectedSetting`, `RdmNachricht` → `RdmMessage`, `RptKlient` → `RptClient`,
  `BrokerKonfig` → `BrokerConfig`, `geraete_finden()` → `find_devices()`, `geraet_auslesen()` → `read_device()`,
  `einstellung_pruefen()` → `check_setting()`, `einstellung_anwenden()` → `apply_setting()`,
  `schreibfreigabe_setzen()` → `set_write_enabled()`, `messung_begleiten()` → `accompany_measurement()`,
  `starten()`/`stoppen()` → `start()`/`stop()`
- Settings check status values: `OK_UNBESTAETIGT` → `OK_UNCONFIRMED`, `ABWEICHUNG` → `DEVIATION`,
  `NICHT_PRUEFBAR` → `NOT_CHECKABLE`
- CLI: `auslesen` → `read`, `schreibtest` → `writetest`; options e.g. `--ohne-mdns` → `--no-mdns`,
  `--warten` → `--wait`, `--suche` → `--search`, `--ordner` → `--folder`, `--ziel-modus` → `--target-mode`,
  `--ziel-adresse` → `--target-address`, `--zurueck-modus` → `--return-mode`, `--zurueck-adresse` → `--return-address`,
  `--nativ` → `--native`, `--weiter` → `--keep`, `llrp --alle`/`--ja` → `--all`/`--yes`, `-v/--ausfuehrlich` → `--verbose`
- Confirmation prompts accept `y`/`yes` (previously `j`/`ja`)
- JSON exports, reports and event logs use English keys and event names (e.g. `client_verbunden` → `client_connected`)

### Database

- Schema version 2 with English table and column names (`rdm_geraet` → `rdm_device`, `rdm_schnappschuss` →
  `rdm_snapshot`, `rdm_sensorwert` → `rdm_sensor_value`, `rdm_abgleich` → `rdm_check`, `rdm_aenderung` →
  `rdm_change`, `rdm_personality_zuordnung` → `rdm_personality_mapping`).
- Databases written by 0.4 (schema 1) are migrated automatically when opened with `RdmStore`: tables, columns,
  phase and status values, and the keys inside the JSON columns (`profile_json`, `details_json`, `error_json`).
  Free text inside JSON (hints, error messages) stays in the language it was written in.

## 0.4.1 and earlier

- Broker RDM responder (E1.33 6.2), Client Entry Update (6.3.1.4), LLRP manager and target (section 5) – merged
  in pull request #1 before the translation
- First public version: broker, controller, readout, settings check, restricted SET, Art-Net RDM, simulator
