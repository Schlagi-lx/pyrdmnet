# To do

## Protocol / standard compliance (E1.33)

- [x] 1. Broker as RDM responder: answer requests to the broker's own UID
      (`COMPONENT_SCOPE`, `SEARCH_DOMAIN`, `BROKER_STATUS`, base PIDs per E1.20)
- [x] 2. Handle `CLIENT_ENTRY_UPDATE` (previously only counted and ignored)
- [x] 3. LLRP: find gateways without a broker, set scope/broker address
      - [ ] 3a. LLRP over IPv6 (E1.33 requires IPv4 and IPv6 for the manager)
      - [ ] 3b. Simulator gateway as LLRP target; real-world test with the MasterPort
- [ ] 4. Support PDU inheritance (flags) in `pdu.py`
- [ ] 5. Pass EPT clients through properly or reject them
- [ ] 6. Detect multiple brokers in the same scope via mDNS and warn

## Robustness in real-world use

- [ ] 7. Controller reconnects by itself after a broker/gateway restart
- [ ] 8. Test several gateways at once (a second device next to the MasterPort)
- [ ] 9. Test more gateways/devices, compatibility list in the README

## Tests / maintenance

- [ ] 10. Reduce test run time (currently about 100 s): replace fixed waits with events
- [x] 11. README: replace the `<konto>` placeholder in the clone command
- [x] 12a. Translate the project to English (0.5.0)
- [ ] 12. Release 0.5 with changelog, possibly PyPI

## Afterwards

- [ ] 13. Switch GelMatch to 0.5 – only after the items above are done; guide: [docs/MIGRATING_TO_0.5.md](docs/MIGRATING_TO_0.5.md)
