# Changelog

## Unreleased

- EPT support in the broker (E1.33 section 8, 9.3): EPT clients connect with CLIENT_PROTOCOL_EPT (zero UIDs in
  the Connect Reply), get their own client list with incremental updates and Client Entry Update, and exchange
  EPT data. Blocks with PDU inheritance (e.g. one message to several CIDs, Appendix D.9) are split per destination
  and forwarded complete; unknown vectors and CIDs are answered with EPT status messages. Sender CIDs are checked.
- New module `ept.py` with EPT encoding and `EptClient`.
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
