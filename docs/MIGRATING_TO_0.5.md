# Migrating from pyrdmnet 0.4 to 0.5

Handover for code written against the original German version of this module (pyrdmnet 0.4) – in particular
**GelMatch**, where the module was originally developed.

**GelMatch:** it does not use pyrdmnet as a dependency yet. The plan is to integrate pyrdmnet 0.5 there only after the
open items of [TODO.md](../TODO.md) in this repository are done – directly in its English form, not via 0.4. The
tables below map the German names that GelMatch code (or code copied from the original module) may still use. The
facade quick reference is the usual entry point (`pyrdmnet.facade`, formerly `pyrdmnet.fassade`).

## What changed

Version 0.5 translates the whole project to English. Behaviour and protocol handling are unchanged except for **one
safety rule: which RDM SET commands are allowed** (see step 4a). **Every German name is gone and there are no
compatibility aliases**:

- module names, classes, functions, methods, attributes, constants and dataclass fields
- status values of the settings check (`OK_UNBESTAETIGT` → `OK_UNCONFIRMED`, `ABWEICHUNG` → `DEVIATION`,
  `NICHT_PRUEFBAR` → `NOT_CHECKABLE`; `OK` stays)
- sensor snapshot status (`teilweise` → `partial`, `fehlgeschlagen` → `failed`; `ok` stays) and phases
  (`vor`/`nach`/`einzeln` → `before`/`after`/`single`)
- keys of every `to_dict()` / JSON export, event names and counters of the broker
- SQLite schema (version 2, English tables and columns)
- command-line commands and options, confirmation answers (`y`/`yes` instead of `j`/`ja`)
- file names written by `livetest` / `writetest`
- all messages, hints and exceptions texts – code that matches on message text has to be checked

## Steps

1. **Add pyrdmnet 0.5 as a dependency**, pinned so later changes arrive only when wanted: `pyrdmnet==0.5.0` from
   PyPI (`pip install pyrdmnet==0.5.0`), or from GitHub `pip install git+https://github.com/Schlagi-lx/pyrdmnet@v0.5.0`.
2. **Back up existing databases.** If a database already contains `rdm_*` tables written by the original German
   module, pyrdmnet 0.5 migrates it to schema 2 in place the first time it is opened (`RdmStore`, `RdmConfig.db`).
   The migration is one-way: **the German version cannot open a migrated database.** Copy such files first.
3. **Update the imports** using the module table below.
4. **Rename the API calls.** Start with the facade (quick reference below), then the less common names from the
   full tables. Searching GelMatch for `pyrdmnet`, `RdmModul`, `RdmKonfig`, `EinstellungsErwartung` and `.fassade`
   finds most places.
4a. **Keep the lab write restriction.** In 0.4, SET was hard-wired to DMX mode, start address and identify. In 0.5
   the allowed PIDs are a `WritePolicy`, and the default allows any PID (still only after
   `set_write_enabled(True)`). GelMatch should keep the old rule explicitly:
   `RdmConfig(..., write_policy=WritePolicy.lab())` with `from pyrdmnet.controller import WritePolicy`
   (for an `LlrpManager`: `write_policy=WritePolicy.llrp_lab()`).
5. **Status values and dict keys.** Replace comparisons with German status strings by the constants in
   `pyrdmnet.settings_check` (`OK`, `OK_UNCONFIRMED`, `DEVIATION`, `NOT_CHECKABLE`) and update every access to
   result dicts / JSON (`profil["hersteller"]` → `profile["manufacturer"]` …). The key names equal the new field
   names in the tables below.
6. **Direct SQL.** If GelMatch reads the `rdm_*` tables itself, use the new table and column names (see "Database").
7. **Scripts and docs.** Update calls of the command line (`auslesen` → `read`, `schreibtest` → `writetest`, options
   below) and anything that reads the live/write test output files.
8. **Message texts.** Search GelMatch for checks on pyrdmnet message text (e.g. `"Schreibfreigabe" in str(e)`) and
   switch to exception types (`WriteLockError`, `RdmNackError`, `RdmTimeoutError`, `RptStatusError`,
   `RdmnetConnectionError`) where possible.
9. **Run GelMatch's tests and one offline run** (`python3 -m pyrdmnet demo` checks pyrdmnet itself).

## Quick reference: facade

| 0.4 | 0.5 |
|---|---|
| `from pyrdmnet.fassade import RdmModul, RdmKonfig` | `from pyrdmnet.facade import RdmModule, RdmConfig` |
| `from pyrdmnet.abgleich import EinstellungsErwartung` | `from pyrdmnet.settings_check import ExpectedSetting` |
| `RdmKonfig(broker_intern=…, ereignis_datei=…, alle_sensoren=…, strikt=…)` | `RdmConfig(internal_broker=…, event_file=…, all_sensors=…, strict=…)` |
| `RdmKonfig(artnet_ziel_ip=…, artnet_port_adressen=…, artnet_tod_warte_s=…)` | `RdmConfig(artnet_target_ip=…, artnet_port_addresses=…, artnet_tod_wait_s=…)` |
| `modul.starten()` / `modul.stoppen()` | `module.start()` / `module.stop()` |
| `modul.geraete_finden()` | `module.find_devices()` |
| `modul.geraet_auslesen(adresse, speichern=True)` | `module.read_device(address, save=True)` |
| `modul.zuordnung_bestaetigen(profil, gdtf_modus, personality_nr, …)` | `module.confirm_mapping(profile, gdtf_mode, personality_num, …)` |
| `modul.einstellung_pruefen(profil, erwartung, bezug=…)` | `module.check_setting(profile, expected, reference=…)` |
| `modul.schreibfreigabe_setzen(True)` | `module.set_write_enabled(True)` |
| `modul.einstellung_anwenden(profil, erwartung, bezug=…)` | `module.apply_setting(profile, expected, reference=…)` |
| `modul.sensor_schnappschuss(profil, messung_ref=…)` | `module.sensor_snapshot(profile, measurement_ref=…)` |
| `with modul.messung_begleiten(profil, messung_ref=…)` | `with module.accompany_measurement(profile, measurement_ref=…)` |
| `EinstellungsErwartung(gdtf_modus, footprint, startadresse, universe)` | `ExpectedSetting(gdtf_mode, footprint, start_address, universe)` |
| `abgleich.status`, `.messung_erlaubt`, `.abweichungen`, `.hinweise`, `.soll_personality`, `.ist`, `.zeit_utc` | `check.status`, `.measurement_allowed`, `.deviations`, `.hints`, `.target_personality`, `.actual`, `.time_utc` |
| `anwendung.erfolgreich`, `.aenderungen` (`{parameter, alt, neu}`), `.fehler`, `.hinweise` | `application.success`, `.changes` (`{parameter, old, new}`), `.error`, `.hints` |
| `profil.adresse`, `.hersteller`, `.modell`, `.geraete_label`, `.sensoren`, `.geraete_stunden`, `.lampen_stunden` | `profile.address`, `.manufacturer`, `.model`, `.device_label`, `.sensors`, `.device_hours`, `.lamp_hours` |
| `GeraeteAdresse(rpt_uid, endpoint, rdm_uid)` | `DeviceAddress(rpt_uid, endpoint, rdm_uid)` |

## Modules

| 0.4 | 0.5 |
|---|---|
| `pyrdmnet.konstanten` | `pyrdmnet.constants` |
| `pyrdmnet.rdm_parameter` | `pyrdmnet.rdm_params` |
| `pyrdmnet.verbindung` | `pyrdmnet.connection` |
| `pyrdmnet.klient` | `pyrdmnet.client` |
| `pyrdmnet.auslesen` | `pyrdmnet.readout` |
| `pyrdmnet.abgleich` | `pyrdmnet.settings_check` |
| `pyrdmnet.speicher` | `pyrdmnet.storage` |
| `pyrdmnet.fassade` | `pyrdmnet.facade` |
| `pyrdmnet.livetest` | `pyrdmnet.live_test` |
| `pyrdmnet.schreibtest` | `pyrdmnet.write_test` |

Unchanged: `broker`, `broker_responder`, `controller`, `dnssd`, `artnet_rdm`, `llrp`, `pdu`, `rdm`, `simulator`.
Logger names follow the module names (`pyrdmnet.facade`, `pyrdmnet.client` …).

## Database

| 0.4 table | 0.5 table |
|---|---|
| `rdm_geraet` | `rdm_device` |
| `rdm_sensor_definition` | `rdm_sensor_definition` (columns renamed) |
| `rdm_schnappschuss` | `rdm_snapshot` |
| `rdm_sensorwert` | `rdm_sensor_value` |
| `rdm_abgleich` | `rdm_check` |
| `rdm_aenderung` | `rdm_change` |
| `rdm_personality_zuordnung` | `rdm_personality_mapping` |

Column names follow the field names (`geraet_id` → `device_id`, `messung_ref` → `measurement_ref`,
`zeit_utc` → `time_utc`, `profil_json` → `profile_json`, `fehler_json` → `error_json`, `sensor_nr` → `sensor_num`,
`bezug` → `reference`, `alt`/`neu`/`erfolgreich` → `old`/`new`/`success` …). The exact list is in
`pyrdmnet/storage.py` (`_SCHEMA` and `_MIGRATION_V1`). `RdmStore(...).migrated_from` is `1` when a database was just
migrated. JSON keys inside migrated rows are translated as well; free text (hints, error messages) keeps its
original language.

## Command line

| 0.4 | 0.5 |
|---|---|
| `auslesen` | `read` |
| `schreibtest` | `writetest` |
| `-v` / `--ausfuehrlich` | `-v` / `--verbose` |
| `--nativ` | `--native` |
| `--ohne-mdns` | `--no-mdns` |
| `--warten` | `--wait` |
| `--suche` | `--search` |
| `--ordner` | `--folder` |
| `--modus`, `--startadresse` | `--mode`, `--start-address` |
| `--ziel-modus`, `--ziel-adresse` | `--target-mode`, `--target-address` |
| `--zurueck-modus`, `--zurueck-adresse` | `--return-mode`, `--return-address` |
| `llrp --alle`, `--ja` | `llrp --all`, `--yes` |
| `demo --weiter` | `demo --keep` |

Output files of `livetest` / `writetest`: `bericht.md` → `report.md`, `ergebnis.json` → `result.json`,
`broker_ereignisse.jsonl` → `broker_events.jsonl`, `paketmitschnitt.jsonl` → `packet_capture.jsonl`
(keys `direction`/`length`, values `in`/`out`), `livetest.sqlite` → `live_test.sqlite`,
`schreibtest.sqlite` → `write_test.sqlite`; result folders start with `live_test_` / `write_test_`.

## Full rename tables

Generated from the rename dictionary used for 0.5 (public names only).

### Classes and exceptions

| 0.4 | 0.5 |
|---|---|
| `AbgleichErgebnis` | `CheckResult` |
| `AnwendungsErgebnis` | `ApplyResult` |
| `ArtnetFormatFehler` | `ArtnetFormatError` |
| `Auslesung` | `DeviceReader` |
| `BrokerKonfig` | `BrokerConfig` |
| `BrokerResponderDaten` | `BrokerResponderInfo` |
| `ClientEintrag` | `ClientEntry` |
| `ConnectDaten` | `ConnectData` |
| `DnssdAnmeldung` | `DnssdRegistration` |
| `DynUidAnfrage` | `DynUidRequest` |
| `EinstellungsErwartung` | `ExpectedSetting` |
| `GeraeteAdresse` | `DeviceAddress` |
| `GeraeteProfil` | `DeviceProfile` |
| `KomponentenResponder` | `ComponentResponder` |
| `LiveKonfig` | `LiveConfig` |
| `LlrpNetz` | `LlrpNetwork` |
| `LlrpPaket` | `LlrpPacket` |
| `LlrpZiel` | `DiscoveredTarget` |
| `PersonalityZuordnung` | `PersonalityMapping` |
| `ProbeAnfrage` | `ProbeRequest` |
| `ProbeAntwort` | `ProbeReply` |
| `ProtokollFehler` | `ProtocolError` |
| `RdmAntwortFehler` | `RdmResponseError` |
| `RdmFehler` | `RdmError` |
| `RdmFormatFehler` | `RdmFormatError` |
| `RdmKonfig` | `RdmConfig` |
| `RdmModul` | `RdmModule` |
| `RdmNachricht` | `RdmMessage` |
| `RdmNackFehler` | `RdmNackError` |
| `RdmSpeicher` | `RdmStore` |
| `RdmZeitFehler` | `RdmTimeoutError` |
| `RptKlient` | `RptClient` |
| `RptKopf` | `RptHeader` |
| `RptStatusFehler` | `RptStatusError` |
| `SchreibKonfig` | `WriteConfig` |
| `SchreibSperreFehler` | `WriteLockError` |
| `SchreibTest` | `WriteTest` |
| `SensorMesswert` | `SensorReading` |
| `SensorRohwert` | `SensorRawValue` |
| `SensorSchnappschuss` | `SensorSnapshot` |
| `SimScheinwerfer` | `SimFixture` |
| `SimulierterArtnetNode` | `SimulatedArtnetNode` |
| `SimulierterGateway` | `SimulatedGateway` |
| `TransaktionsZaehler` | `TransactionCounter` |
| `Verbindung` | `TcpConnection` |
| `VerbindungsFehler` | `RdmnetConnectionError` |

### Functions and methods

| 0.4 | 0.5 |
|---|---|
| `adapter_waehlen` | `choose_adapter` |
| `als_bytes` | `to_bytes` |
| `als_dict` | `to_dict` |
| `als_int` | `to_int` |
| `antwort` | `response` |
| `antworttyp` | `response_type` |
| `anwenden` | `apply` |
| `aus_int` | `from_int` |
| `aus_text` | `from_text` |
| `ausfuehren` | `run` |
| `baue_broker` | `build_broker` |
| `baue_client_entry_update` | `build_client_entry_update` |
| `baue_component_scope` | `build_component_scope` |
| `baue_device_info` | `build_device_info` |
| `baue_dyn_uid_zuordnungen` | `build_dyn_uid_assignments` |
| `baue_endpoint_liste` | `build_endpoint_list` |
| `baue_endpoint_responder` | `build_endpoint_responders` |
| `baue_llrp` | `build_llrp` |
| `baue_notification` | `build_notification` |
| `baue_paket` | `build_packet` |
| `baue_personality_beschreibung` | `build_personality_description` |
| `baue_probe_anfrage` | `build_probe_request` |
| `baue_probe_antwort` | `build_probe_reply` |
| `baue_rdm_cmd_pdu` | `build_rdm_cmd_pdu` |
| `baue_request` | `build_request` |
| `baue_rpt` | `build_rpt` |
| `baue_sensor_definition` | `build_sensor_definition` |
| `baue_sensor_wert` | `build_sensor_value` |
| `baue_status` | `build_status` |
| `baue_text` | `build_text` |
| `beantworten` | `respond` |
| `beenden` | `stop` |
| `clientliste` | `client_list` |
| `dekodieren` | `decode` |
| `dienstname` | `service_name` |
| `dns_sd_befehl` | `dns_sd_command` |
| `einheit_text` | `unit_text` |
| `einstellung_anwenden` | `apply_setting` |
| `einstellung_lesen` | `read_setting` |
| `einstellung_pruefen` | `check_setting` |
| `eintrag_aktualisieren` | `update_entry` |
| `entdecken` | `discover` |
| `faktor` | `factor` |
| `flags_laenge` | `flags_length` |
| `geraet_auslesen` | `read_device` |
| `geraete` | `devices` |
| `geraete_finden` | `find_devices` |
| `hardware_adresse` | `hardware_address` |
| `hat_min_max` | `has_min_max` |
| `hat_recorded` | `has_recorded` |
| `hersteller_aus_mid_broadcast` | `manufacturer_from_mid_broadcast` |
| `ip_ist_lokal` | `ip_is_local` |
| `ist_antwort` | `is_response` |
| `ist_broadcast` | `is_broadcast` |
| `ist_controller` | `is_controller` |
| `ist_dynamisch` | `is_dynamic` |
| `ist_dynamisch_angefragt` | `is_dynamic_request` |
| `ist_geraete_broadcast` | `is_device_broadcast` |
| `ist_gueltig_fuer_client` | `is_valid_for_client` |
| `jetzt_utc` | `now_utc` |
| `kandidaten` | `candidates` |
| `kodieren` | `encode` |
| `lese` | `read` |
| `lese_broker` | `read_broker` |
| `lese_client_eintraege` | `read_client_entries` |
| `lese_client_entry_update` | `read_client_entry_update` |
| `lese_component_scope` | `read_component_scope` |
| `lese_connect` | `read_connect` |
| `lese_connect_reply` | `read_connect_reply` |
| `lese_device_info` | `read_device_info` |
| `lese_dyn_uid_anfragen` | `read_dyn_uid_requests` |
| `lese_dyn_uid_zuordnungen` | `read_dyn_uid_assignments` |
| `lese_endpoint_liste` | `read_endpoint_list` |
| `lese_endpoint_responder` | `read_endpoint_responders` |
| `lese_endpoint_universe` | `read_endpoint_universe` |
| `lese_flags_laenge` | `read_flags_length` |
| `lese_llrp` | `read_llrp` |
| `lese_personality` | `read_personality` |
| `lese_personality_beschreibung` | `read_personality_description` |
| `lese_pid_liste` | `read_pid_list` |
| `lese_probe_anfrage` | `read_probe_request` |
| `lese_probe_antwort` | `read_probe_reply` |
| `lese_rdm_cmd` | `read_rdm_cmd` |
| `lese_request_oder_notification` | `read_request_or_notification` |
| `lese_root_pdus` | `read_root_pdus` |
| `lese_rpt` | `read_rpt` |
| `lese_sensor_definition` | `read_sensor_definition` |
| `lese_sensor_wert` | `read_sensor_value` |
| `lese_status` | `read_status` |
| `lese_suchdomain` | `read_search_domain` |
| `lese_tcp_block` | `read_tcp_block` |
| `lese_u16` | `read_u16` |
| `lese_u32` | `read_u32` |
| `lese_uid_liste` | `read_uid_list` |
| `messrelevant` | `measurement_relevant` |
| `messung_begleiten` | `accompany_measurement` |
| `naechste` | `next_tn` |
| `netzwerkadapter` | `network_adapters` |
| `pdus_aufteilen` | `split_pdus` |
| `personality_aktuell` | `personality_current` |
| `port_adresse` | `port_address` |
| `praefix_bekannt` | `prefix_known` |
| `produktkategorie_text` | `product_category_text` |
| `profil` | `profile` |
| `pruefen` | `check` |
| `pruefsumme` | `checksum` |
| `rohwert` | `raw_value` |
| `schliessen` | `close` |
| `schreibfreigabe_setzen` | `set_write_enabled` |
| `scope_lesen` | `read_scope` |
| `scope_setzen` | `set_scope` |
| `selbsttest` | `self_test` |
| `sende_broker` | `send_broker` |
| `sende_rpt` | `send_rpt` |
| `senden_roh` | `send_raw` |
| `sensor_schnappschuss` | `sensor_snapshot` |
| `sensorwerte_zu_messung` | `sensor_values_for_measurement` |
| `skaliert` | `scaled` |
| `speichere_abgleich` | `save_check` |
| `speichere_anwendung` | `save_application` |
| `speichere_profil` | `save_profile` |
| `speichere_schnappschuss` | `save_snapshot` |
| `startadresse` | `start_address` |
| `starten` | `start` |
| `statische_adresse` | `static_address` |
| `stoppen` | `stop` |
| `suchdomain_setzen` | `set_search_domain` |
| `tod_abfragen` | `query_tod` |
| `trennen` | `disconnect` |
| `txt_eintraege` | `txt_entries` |
| `typ_text` | `type_text` |
| `verbinden` | `connect` |
| `verbunden` | `connected` |
| `versuch` | `attempt` |
| `verzoegert` | `delayed` |
| `zerlegen` | `parse_packet` |
| `zuordnung_bestaetigen` | `confirm_mapping` |
| `zuordnung_finden` | `find_mapping` |
| `zuordnung_passt` | `mapping_matches` |

### Constants

| 0.4 | 0.5 |
|---|---|
| `ABWEICHUNG` | `DEVIATION` |
| `ACN_TCP_PREAMBEL` | `ACN_TCP_PREAMBLE` |
| `ACN_UDP_PREAMBEL` | `ACN_UDP_PREAMBLE` |
| `ALLE_CONTROLLER` | `ALL_CONTROLLERS` |
| `ALLE_GERAETE` | `ALL_DEVICES` |
| `ANTWORT_ACK` | `RESPONSE_ACK` |
| `ANTWORT_ACK_OVERFLOW` | `RESPONSE_ACK_OVERFLOW` |
| `ANTWORT_ACK_TIMER` | `RESPONSE_ACK_TIMER` |
| `ANTWORT_NACK` | `RESPONSE_NACK` |
| `BROKER_CONNECT_FLAG_INKREMENTELL` | `BROKER_CONNECT_FLAG_INCREMENTAL` |
| `CHECKLISTE` | `CHECKLIST` |
| `COMPONENT_SCOPE_LAENGE` | `COMPONENT_SCOPE_LENGTH` |
| `DIENSTTYP` | `SERVICE_TYPE` |
| `E133_BROKER_AKTIV` | `E133_BROKER_ACTIVE` |
| `E133_BROKER_DEAKTIVIERT` | `E133_BROKER_DISABLED` |
| `E133_CLIENT_PROTOKOLL_EPT` | `E133_CLIENT_PROTOCOL_EPT` |
| `E133_CLIENT_PROTOKOLL_RPT` | `E133_CLIENT_PROTOCOL_RPT` |
| `E133_DNSSD_TYP` | `E133_DNSSD_TYPE` |
| `E133_DOMAIN_LAENGE` | `E133_DOMAIN_LENGTH` |
| `E133_DYN_UID_DOPPELTE_RID` | `E133_DYN_UID_DUPLICATE_RID` |
| `E133_DYN_UID_KAPAZITAET` | `E133_DYN_UID_CAPACITY_EXCEEDED` |
| `E133_DYN_UID_NICHT_GEFUNDEN` | `E133_DYN_UID_NOT_FOUND` |
| `E133_DYN_UID_UNGUELTIG` | `E133_DYN_UID_INVALID` |
| `E133_HEARTBEAT_INTERVALL_S` | `E133_HEARTBEAT_INTERVAL_S` |
| `E133_KEINE_STATISCHE_KONFIG` | `E133_NO_STATIC_CONFIG` |
| `E133_RPT_ALLE_CONTROLLER` | `E133_RPT_ALL_CONTROLLERS` |
| `E133_RPT_ALLE_GERAETE` | `E133_RPT_ALL_DEVICES` |
| `E133_SCOPE_LAENGE` | `E133_SCOPE_LENGTH` |
| `E133_STANDARD_DOMAIN` | `E133_DEFAULT_DOMAIN` |
| `E133_STANDARD_SCOPE` | `E133_DEFAULT_SCOPE` |
| `E133_STATISCHE_KONFIG_IPV4` | `E133_STATIC_CONFIG_IPV4` |
| `E133_STATISCHE_KONFIG_IPV6` | `E133_STATIC_CONFIG_IPV6` |
| `EINHEIT_TEXT` | `UNIT_TEXT` |
| `ENDPOINT_MODUS_TEXT` | `ENDPOINT_MODE_TEXT` |
| `ENDPOINT_TYP_PHYSISCH` | `ENDPOINT_TYPE_PHYSICAL` |
| `ENDPOINT_TYP_VIRTUELL` | `ENDPOINT_TYPE_VIRTUAL` |
| `ERLAUBTE_SET_PIDS` | `LAB_SET_PIDS` (use `WritePolicy.lab()`) |
| `ESTA_PROTOTYP_HERSTELLER` | `ESTA_PROTOTYPE_MANUFACTURER` |
| `LLRP_FILTER_NUR_BROKER` | `LLRP_FILTER_BROKERS_ONLY` |
| `LLRP_FILTER_NUR_UNVERBUNDEN` | `LLRP_FILTER_CLIENT_CONN_INACTIVE` |
| `LLRP_IPV4_ANFRAGE` | `LLRP_IPV4_REQUEST` |
| `LLRP_IPV4_ANTWORT` | `LLRP_IPV4_RESPONSE` |
| `LLRP_TYP_BROKER` | `LLRP_TYPE_BROKER` |
| `LLRP_TYP_CONTROLLER` | `LLRP_TYPE_CONTROLLER` |
| `LLRP_TYP_GERAET` | `LLRP_TYPE_DEVICE` |
| `LLRP_TYP_KEIN_RDMNET` | `LLRP_TYPE_NON_RDMNET` |
| `LLRP_TYP_TEXT` | `LLRP_TYPE_TEXT` |
| `MODULNAME` | `MODULE_NAME` |
| `NICHT_PRUEFBAR` | `NOT_CHECKABLE` |
| `OK_UNBESTAETIGT` | `OK_UNCONFIRMED` |
| `PID_NAMEN` | `PID_NAMES` |
| `PRAEFIX_EXPONENT` | `PREFIX_EXPONENT` |
| `PRODUKTKATEGORIE_DATENVERTEILUNG` | `PRODUCT_CATEGORY_DATA_DISTRIBUTION` |
| `PRODUKTKATEGORIE_GRUPPE` | `PRODUCT_CATEGORY_GROUP` |
| `RDM_ALLE_SUBDEVICES` | `RDM_ALL_SUBDEVICES` |
| `RDM_KOPF_LAENGE` | `RDM_HEADER_LENGTH` |
| `RPT_CLIENT_TYP_CONTROLLER` | `RPT_CLIENT_TYPE_CONTROLLER` |
| `RPT_CLIENT_TYP_GERAET` | `RPT_CLIENT_TYPE_DEVICE` |
| `SENSORTYPEN_MESSRELEVANT` | `MEASUREMENT_RELEVANT_SENSOR_TYPES` |
| `SENSORTYP_TEXT` | `SENSOR_TYPE_TEXT` |
| `UNTERSTUETZTE_PIDS` | `SUPPORTED_PIDS` |

### Dataclass and config fields

| 0.4 | 0.5 |
|---|---|
| `abweichungen` | `deviations` |
| `ack_timer_einmal` | `ack_timer_once` |
| `adresse` | `address` |
| `aenderungen` | `changes` |
| `aktuell` | `current` |
| `alle_sensoren` | `all_sensors` |
| `anfrage_port` | `request_port` |
| `anfrage_ziel` | `request_addr` |
| `anstieg_je_s` | `rise_per_s` |
| `antwort_port` | `response_port` |
| `antwort_ziel` | `response_addr` |
| `artnet_port_adressen` | `artnet_port_addresses` |
| `artnet_tod_warte_s` | `artnet_tod_wait_s` |
| `artnet_ziel_ip` | `artnet_target_ip` |
| `artnet_ziel_port` | `artnet_target_port` |
| `aufgezeichnet` | `recorded` |
| `ausgelesen_utc` | `read_utc` |
| `ausserhalb_normal` | `outside_normal` |
| `bekannte` | `known` |
| `bereich_max` | `range_max` |
| `bereich_min` | `range_min` |
| `beschreibung` | `description` |
| `bestaetigt_utc` | `confirmed_utc` |
| `bestaetigt_von` | `confirmed_by` |
| `broker_intern` | `internal_broker` |
| `daten` | `data` |
| `dauer_s` | `duration_s` |
| `einheit` | `unit` |
| `eintrag` | `entry` |
| `endwert` | `end_value` |
| `ereignis_datei` | `event_file` |
| `erfolgreich` | `success` |
| `erwartung` | `expected` |
| `fehler` | `error` |
| `gdtf_modus` | `gdtf_mode` |
| `geraet` | `device` |
| `geraete_label` | `device_label` |
| `geraete_stunden` | `device_hours` |
| `heartbeat_intervall_s` | `heartbeat_interval_s` |
| `hersteller` | `manufacturer` |
| `hersteller_id` | `manufacturer_id` |
| `hersteller_label` | `manufacturer_label` |
| `hinweise` | `hints` |
| `hoechster` | `highest` |
| `inkrementell` | `incremental` |
| `ist` | `actual` |
| `kopf` | `header` |
| `lampen_stunden` | `lamp_hours` |
| `llrp_netz` | `llrp_network` |
| `messung_erlaubt` | `measurement_allowed` |
| `mitschnitt_datei` | `capture_file` |
| `mitschnitt_max_zeilen` | `capture_max_lines` |
| `modell` | `model` |
| `modell_id` | `model_id` |
| `nachrichtenzaehler` | `message_count` |
| `nicht_verfuegbar` | `unavailable` |
| `niedrigster` | `lowest` |
| `nummer` | `number` |
| `obere` | `upper` |
| `ordner` | `folder` |
| `pause_nach_set_s` | `pause_after_set_s` |
| `personality_anzahl` | `personality_count` |
| `personality_nr` | `personality_num` |
| `port_oder_antworttyp` | `port_or_response_type` |
| `praefix` | `prefix` |
| `produktkategorie` | `product_category` |
| `protokoll` | `protocol` |
| `quelle` | `source` |
| `quelle_endpoint` | `source_endpoint` |
| `rdm_protokoll` | `rdm_protocol` |
| `recorded_unterstuetzung` | `recorded_support` |
| `roh` | `raw` |
| `rueckleseversuche` | `readback_attempts` |
| `schreibschutz` | `write_protect` |
| `sensor_anzahl` | `sensor_count` |
| `sensoren` | `sensors` |
| `sequenz` | `sequence` |
| `set_protokoll` | `set_log` |
| `soll_personality` | `target_personality` |
| `startwert` | `start_value` |
| `statisch_ipv4` | `static_ipv4` |
| `statisch_ipv6` | `static_ipv6` |
| `statisch_port` | `static_port` |
| `statisch_typ` | `static_type` |
| `strikt` | `strict` |
| `subdevice_anzahl` | `subdevice_count` |
| `suchdomain` | `search_domain` |
| `suche_responder_s` | `find_responders_s` |
| `transaktion` | `transaction` |
| `typ` | `type` |
| `untere` | `lower` |
| `unterstuetzte_pids` | `supported_pids` |
| `vektor` | `vector` |
| `verbindung` | `connection` |
| `verbunden_seit` | `connected_since` |
| `warte_auf_geraete_s` | `wait_for_devices_s` |
| `wert` | `value` |
| `werte` | `values` |
| `zeit_utc` | `time_utc` |
| `ziel` | `dest` |
| `ziel_adresse` | `target_address` |
| `ziel_cid` | `dest_cid` |
| `ziel_endpoint` | `dest_endpoint` |
| `ziel_modus` | `target_mode` |
| `zurueck_adresse` | `return_address` |
| `zurueck_modus` | `return_mode` |
| `zusatz_pids` | `extra_pids` |

### Other public attributes

| 0.4 | 0.5 |
|---|---|
| `ausgabe` | `output` |
| `bei_aenderung` | `on_change` |
| `discovery_noetig` | `discovery_needed` |
| `eigene_cid` | `own_cid` |
| `eingabe` | `input_fn` |
| `empfang` | `on_receive` |
| `empfangen` | `received` |
| `ereignisse` | `events` |
| `ergebnis` | `result` |
| `fremde_pakete` | `foreign_packets` |
| `gegenstelle` | `peer` |
| `geschlossen` | `closed` |
| `getrennt` | `disconnected` |
| `grund` | `reason` |
| `identifizieren` | `identifying` |
| `konfig` | `config` |
| `letzter_empfang` | `last_received` |
| `letztes_senden` | `last_sent` |
| `mitschnitt` | `capture` |
| `nativ` | `native` |
| `netz` | `network` |
| `scheinwerfer` | `fixture` |
| `schreibfreigabe` | `write_enabled` |
| `sk` | `wc` |
| `speicher` | `store` |
| `stumm` | `mute` |
| `trenngrund` | `disconnect_reason` |
| `uid_wunsch` | `requested_uid` |
| `unaufgeforderte_meldungen` | `unsolicited_messages` |
| `verzoegerung` | `delay` |
| `weg` | `method` |
| `wiederholungen` | `retries` |
| `zaehler` | `counters` |
| `zeitlimit_s` | `timeout_window_s` |
| `ziel_ip` | `target_ip` |
| `ziel_port` | `target_port` |

