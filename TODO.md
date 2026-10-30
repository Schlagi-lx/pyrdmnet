# Todo

## Protokoll / Normnähe (E1.33)

- [x] 1. Broker als RDM-Responder: Anfragen an die eigene Broker-UID beantworten
      (`COMPONENT_SCOPE`, `SEARCH_DOMAIN`, `BROKER_STATUS`, Basis-PIDs nach E1.20)
- [ ] 2. `CLIENT_ENTRY_UPDATE` verarbeiten (bisher nur gezählt und ignoriert)
- [ ] 3. LLRP: Gateways ohne Broker finden, Scope/Broker-Adresse setzen
- [ ] 4. PDU-Vererbung (Flags) in `pdu.py` unterstützen
- [ ] 5. EPT-Clients sauber durchreichen oder ablehnen
- [ ] 6. Mehrere Broker im selben Scope per mDNS erkennen und warnen

## Robustheit im Realbetrieb

- [ ] 7. Controller verbindet sich nach Broker-/Gateway-Neustart selbst wieder
- [ ] 8. Mehrere Gateways gleichzeitig testen (zweites Gerät neben dem MasterPort)
- [ ] 9. Weitere Gateways/Geräte testen, Kompatibilitätsliste im README

## Tests / Projektpflege

- [ ] 10. Testlaufzeit senken (derzeit ca. 90 s): feste Wartezeiten durch Ereignisse ersetzen
- [x] 11. README: Platzhalter `<konto>` im Clone-Befehl ersetzen
- [ ] 12. Release 0.5 mit Changelog, ggf. PyPI
