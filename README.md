# pyrdmnet

**Eigener RDMnet-Broker (ANSI E1.33) und Controller in reinem Python – ohne externe Abhängigkeiten.**

*English: A self-contained RDMnet (ANSI E1.33) broker and controller written in pure Python (standard library only). It discovers RDM devices behind RDMnet gateways, reads device info, personalities and sensors, and can set DMX personality / start address under strict safeguards. Documentation is in German.*

Entstanden im Projekt GelMatch (Vermessung von Scheinwerfern der Veranstaltungstechnik). Das Modul läuft eigenständig und lässt sich über eine synchrone Schnittstelle in andere Programme einbinden.

## Status

| | |
|---|---|
| Softwaretests | 117/117 bestanden (Python 3.10–3.13, nur Standardbibliothek) |
| Realtest | mit **LSS MasterPort 2** (Firmware 2.13) als RDMnet-Gateway und einem **ETC ColorSource PAR**: Broker-Suche per mDNS, Verbindung, Geräte finden, auslesen, Modus/Startadresse setzen und zurücklesen |
| Andere Gateways/Geräte | nicht getestet – Rückmeldungen willkommen |

Kein Konformitätstest nach E1.33, keine Zertifizierung. Nutzung auf eigene Verantwortung (siehe Lizenz).

## Was das Modul kann

- **Broker** nach ANSI E1.33 (asyncio): Connect mit Scope-Prüfung, dynamische UIDs, Clientliste, RPT-Weiterleitung (auch Broadcast), Heartbeat, Paketmitschnitt als JSONL, Client Entry Update (Eintrag ändern ohne Neuverbindung)
- **Broker-eigener RDM-Responder**: Controller können den Broker selbst abfragen (`COMPONENT_SCOPE`, `SEARCH_DOMAIN`, `BROKER_STATUS`, Gerätedaten). Scope und Zustand sind per RDM nicht änderbar.
- **mDNS-Anmeldung** `_rdmnet._tcp` mit Scope-Subtyp – über Bonjour (`dns-sd`, auf macOS vorhanden), sonst optional über das Paket `zeroconf`
- **Controller**: RDM GET, ACK_TIMER, ACK_OVERFLOW, NACK, Status-Meldungen
- **Auslesen** über Gateway-Endpoints oder native RDMnet-Geräte: Hersteller, Modell, Firmware, DMX-Personalities mit Footprint, Startadresse, Sensoren, Betriebsstunden
- **Abgleich** der Geräteeinstellung mit einer Soll-Einstellung (Modus, Footprint, Startadresse, Universum)
- **Eingeschränktes RDM SET** (siehe unten)
- **Sensor-Schnappschüsse** vor/nach einer Aktion, gespeichert in SQLite (Tabellen `rdm_*`)
- **RDM über Art-Net** (ArtTod, ArtRdm)
- **Simulator** für Tests ohne Hardware

## Sicherheit beim Schreiben

Das Modul liest standardmäßig nur. Schreiben ist bewusst eng begrenzt:

- nur `DMX_PERSONALITY`, `DMX_START_ADDRESS` und `IDENTIFY_DEVICE`
- nur nach ausdrücklicher Schreibfreigabe je Sitzung
- jede Änderung wird zurückgelesen und mit alt → neu protokolliert
- Discovery-Befehle (`DISC_*`) sendet das Modul nie; die Discovery macht das Gateway

## Schnellstart

Voraussetzung: Python ab 3.10. Keine Installation weiterer Pakete nötig.

```bash
git clone https://github.com/Schlagi-lx/pyrdmnet.git
cd pyrdmnet
python3 -m unittest discover -s tests -t tests   # Tests
python3 -m pyrdmnet demo                        # Offline-Durchlauf mit Simulator
```

Oder als Paket installieren: `pip install .` (optional mit mDNS über zeroconf: `pip install ".[zeroconf]"`).

### Befehle

| Befehl | Zweck |
|---|---|
| `broker` | Broker starten (läuft bis Strg+C) |
| `simulator` | simuliertes RDMnet-Gerät mit einem Broker verbinden |
| `auslesen` | alle Geräte am Broker finden und auslesen (nur lesen) |
| `livetest` | Test mit realem Gateway, nur lesend |
| `schreibtest` | Modus/Startadresse am realen Gerät setzen, prüfen und zurückstellen (mit Bestätigung vor Ort) |
| `demo` | kompletter Offline-Durchlauf (Simulation) |

Beispiel mit getrennten Prozessen:

```bash
python3 -m pyrdmnet broker --host 127.0.0.1 --port 8888 --log ereignisse.jsonl
python3 -m pyrdmnet simulator --broker 127.0.0.1:8888
python3 -m pyrdmnet auslesen --broker 127.0.0.1:8888 --json geraete.json
```

Im echten Netz `--host` auf die IP des Netzwerkadapters setzen und mit `--dnssd` per mDNS anmelden. Am Gateway die Broker-Suche auf mDNS stellen und denselben Scope eintragen (Standard: `default`, Groß-/Kleinschreibung beachten). Konkurrierende Broker im selben Scope vermeiden.

## Einbinden in eigene Programme

```python
from pyrdmnet.fassade import RdmModul, RdmKonfig
from pyrdmnet.abgleich import EinstellungsErwartung

modul = RdmModul(RdmKonfig(broker_host=adapter_ip, db="rdm.sqlite"))
modul.starten()
profil = modul.geraet_auslesen(modul.geraete_finden()[0])
erwartung = EinstellungsErwartung(modus_name, footprint, startadresse, universe)
abgleich = modul.einstellung_pruefen(profil, erwartung, bezug="sitzung-1")
if not abgleich.messung_erlaubt:
    modul.schreibfreigabe_setzen(True)          # nur nach Bestätigung vor Ort
    anwendung, profil, abgleich = modul.einstellung_anwenden(profil, erwartung, bezug="sitzung-1")
with modul.messung_begleiten(profil, messung_ref="1"):
    ...  # eigene Aktion; Sensoren werden davor und danach gespeichert
modul.stoppen()
```

Ein Moduswechsel braucht eine bestätigte Zuordnung RDM-Personality ↔ Modus (je Modell und Firmware); geraten wird nie.

## Aufbau

| Datei | Inhalt |
|---|---|
| `konstanten.py` | Protokollkonstanten E1.20, E1.33, E1.37-7 |
| `rdm.py`, `rdm_parameter.py` | RDM-Nachrichten, UID, Prüfsumme, Parameterdaten |
| `pdu.py`, `verbindung.py` | ACN-TCP-Präambel, Root/Broker/RPT-PDUs, TCP-Verbindung |
| `broker.py`, `broker_responder.py` | Broker, RDM-Responder des Brokers |
| `klient.py`, `controller.py` | RPT-Client, Controller |
| `auslesen.py`, `abgleich.py` | Geräte auslesen, Soll-Abgleich, Anwenden mit Rücklesen |
| `speicher.py` | SQLite-Tabellen `rdm_*` |
| `fassade.py` | synchrone Schnittstelle (asyncio im eigenen Thread) |
| `dnssd.py` | mDNS-Anmeldung |
| `artnet_rdm.py` | RDM über Art-Net |
| `simulator.py`, `livetest.py`, `schreibtest.py`, `__main__.py` | Simulator, Werkzeuge, Kommandozeile |

## Hinweise

- Die Protokollkonstanten wurden mit den öffentlichen Referenzimplementierungen von ETC ([ETCLabs/RDMnet](https://github.com/ETCLabs/RDMnet), [ETCLabs/RDM](https://github.com/ETCLabs/RDM)) abgeglichen; Code daraus wurde nicht übernommen.
- Der Broker meldet sich standardmäßig mit der ESTA-Prototyp-Herstellerkennung `0x7FF0` an (für Entwicklung vorgesehen). Wer das Modul in einem Produkt einsetzt, trägt in `RdmKonfig.broker_uid` seine eigene ESTA-Herstellerkennung ein.
- Die Normen ANSI E1.20 und E1.33 gibt es bei ESTA (tsp.esta.org).
- „RDMnet“ bezeichnet den ESTA-Standard ANSI E1.33. Dieses Projekt steht in keiner Verbindung zu ESTA, ETC oder LSS.

## Lizenz

MIT – siehe [LICENSE](LICENSE).
