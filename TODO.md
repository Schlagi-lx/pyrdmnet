# To do

## Protocol / standard compliance (E1.33)

- [x] 1. Broker as RDM responder: answer requests to the broker's own UID
      (`COMPONENT_SCOPE`, `SEARCH_DOMAIN`, `BROKER_STATUS`, base PIDs per E1.20)
- [x] 2. Handle `CLIENT_ENTRY_UPDATE` (previously only counted and ignored)
- [x] 3. LLRP: find gateways without a broker, set scope/broker address
      - [x] 3a. LLRP over IPv6 (E1.33 requires IPv4 and IPv6 for the manager)
      - [ ] 3b. Simulator gateway as LLRP target; real-world test with the MasterPort
- [x] 4. PDU inheritance – per E1.33 4.1.1 only allowed for EPT, done as part of 5
- [x] 5. EPT in the broker: EPT clients, client list, forwarding (incl. inheritance), Unknown CID/Vector status
- [x] 6. One broker per scope (9.1.4): STANDBY at start-up while another broker holds the scope, monitoring,
      `brokers` command
- [x] 14. `TCP_COMMS_STATUS` (7.6.3, required for controllers and devices): unhealthy-TCP-event counter per scope,
      GET/SET in the controller (as RDM responder) and the simulator device; controller helper to read it
- [ ] 15. IPv6 for broker connections: broker listens on IPv6, clients connect via IPv6, Client Redirect IPv6
      (6.3.1.6), static IPv6 broker configuration
- [ ] 16. Fair queuing in the broker (9.2.10): each of N controllers gets at least 1/N of the bandwidth to a device

## Robustness in real-world use

- [x] 7. Automatic reconnection after a broker restart (6.2.5 back-off, Client Redirect IPv4), facade waits
- [ ] 8. Test several gateways at once (a second device next to the MasterPort)
- [ ] 9. Test more gateways/devices, compatibility list in the README
- [x] 9b. Revise the restrictive SET settings: `WritePolicy` – default any PID after the per-session permission,
      `WritePolicy.lab()` / `llrp_lab()` keep the old lists, permission can be switched off, discovery stays blocked

## Tests / maintenance

- [x] 9a. Windows support: tests on Windows in CI, adapter list (ipconfig/ip), portable mDNS test, docs
- [x] 10. Reduce test run time: about 100 s → about 30 s (lab pauses configurable, tests use short ones)
- [x] 11. README: replace the `<konto>` placeholder in the clone command
- [x] 12a. Translate the project to English (0.5.0)
- [ ] 12. Release 0.5 with changelog, possibly PyPI

## Afterwards

- [ ] 13. Integrate pyrdmnet 0.5 into GelMatch (not using it yet) – only after the items above are done; guide: [docs/MIGRATING_TO_0.5.md](docs/MIGRATING_TO_0.5.md)
