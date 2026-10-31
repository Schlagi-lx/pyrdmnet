# Development

How pyrdmnet is developed, tested and released. The open items are in [TODO.md](../TODO.md); every change is listed in
[CHANGELOG.md](../CHANGELOG.md).

## Principles

- **Standard library only.** `zeroconf` is the only optional extra (mDNS on systems without Bonjour).
- **English** for code, comments, docs, commit messages, CLI output and data formats.
- **Python 3.10 or later** on Linux, macOS and Windows.
- **Reference:** the ANSI E1.33 (RDMnet) and E1.20 (RDM) standards. Cite the section in code and docs (e.g.
  "E1.33 9.2.10"). The standards are not in the repository (copyright); get them from ESTA (tsp.esta.org).
- **Safety when writing:** RDM SET only after the per-session write permission; the allowed PIDs are a `WritePolicy`.
  Discovery commands are never sent.
- **Simulator first:** every protocol feature gets a test against the simulator (`pyrdmnet.simulator`) before it is
  tried on hardware.

## Workflow

1. **Branch:** create one branch per change from an up-to-date `main` (`git switch -c <topic>`).
2. **Implement and test locally:**
   ```bash
   python3 -m unittest discover -s tests -t tests      # about 1 minute
   python3 -m pyrdmnet demo                            # quick end-to-end check
   ```
   New behaviour needs tests. Keep timing-dependent tests robust: Windows has a coarse timer and refuses localhost
   connections slowly (about 2 s), so wait for a condition instead of sleeping a fixed time.
3. **Update docs:** add an entry to the top section of `CHANGELOG.md`, and update the README or TODO.md if affected.
4. **Pull request:** push the branch and open a pull request against `main`. CI (`.github/workflows/tests.yml`)
   runs all tests on Ubuntu, macOS and Windows with Python 3.10–3.13.
5. **Merge** only when CI is green on all three systems, then delete the branch.

Tests that need IPv6 on loopback or IPv6 multicast skip themselves where it is not available. Locally they may
skip; CI runs most of them.

## Releasing

Versions follow `MAJOR.MINOR.PATCH`. A version on PyPI cannot be replaced, only yanked, so fixes become a new patch
version.

1. **Bump the version** in a release branch:
   - `version` in `pyproject.toml`,
   - `__version__` in `pyrdmnet/__init__.py`.
2. **CHANGELOG:** turn the top section into `## <version> – <date>`.
3. **Check the package:**
   ```bash
   python3 -m pip install build twine
   python3 -m build && python3 -m twine check dist/*
   ```
4. **Merge** the release branch through a pull request with green CI.
5. **Publish a GitHub release:** on GitHub, Releases → Draft a new release, new tag `v<version>` on `main`, then
   Publish. This starts `.github/workflows/publish.yml`, which builds the package, checks that the tag matches
   the version and uploads it to PyPI.
6. **Verify** in a fresh virtual environment: `pip install pyrdmnet==<version>`, then `pyrdmnet --version` and
   `pyrdmnet demo`.

PyPI uses **trusted publishing**, so no token is stored anywhere. On pypi.org the project `pyrdmnet` trusts the
repository `Schlagi-lx/pyrdmnet`, the workflow `publish.yml` and the GitHub environment `pypi`. If the workflow file
or environment is renamed, update the publisher on pypi.org too.

## Working with Claude

Claude can do most of this, with some exceptions.

**Claude can:** create branches, open and merge pull requests, wait for CI and read its logs.

**The repository owner does:**
- creating GitHub releases and tags (step 5 above),
- deleting branches on GitHub,
- account settings on GitHub and PyPI.

When a session needs the exact wording of E1.33 or E1.20, attach the standards (PDF) to the conversation again.

## Hardware tests

The test plan, checklist and the results so far are in [HARDWARE_TESTS.md](HARDWARE_TESTS.md).

- **Equipment used:** LSS MasterPort 2 with an ETC ColorSource PAR, run from a Mac with a fixed IPv4 address on the
  Ethernet adapter.
- **Main tool:** `python3 -m pyrdmnet livetest --host <adapter IP> --checks all` runs the read-only checks and
  writes a result folder for analysis.
- **Rehearsal:** `tests/test_live_checks.py` runs the same checks against two simulated gateways.
- **Before a hardware session:** check the test computer for Python version, Bonjour, firewall, free ports and
  other brokers on the scope. On macOS, allow incoming connections for the Python binary in the firewall.

Known device behaviour:

| Device / firmware | Behaviour |
|---|---|
| LSS MasterPort 2, 2.13.0028 | acknowledges SET `COMPONENT_SCOPE` (LLRP and RDMnet) but does not apply it; scope only via its menu |
| LSS MasterPort 2, 2.13.0028 | reconnects about 16 s after a broker restart |
| LSS MasterPort 2, 2.13.0028 | stores the search domain without the final dot |

## Code map

See the "Structure" table in the [README](../README.md#structure). Entry points:

| Area | Files |
|---|---|
| Protocol encoding | `pdu.py`, `rdm.py`, `rdm_params.py`, `constants.py`, `ept.py`, `llrp.py` |
| Broker | `broker.py`, `broker_responder.py`, `dnssd.py` |
| Clients | `client.py` (connection, reconnection, scope), `client_responder.py`, `controller.py` |
| Application layer | `readout.py`, `settings_check.py`, `storage.py`, `facade.py` |
| Tools | `__main__.py` (CLI), `simulator.py`, `live_test.py`, `write_test.py`, `netif.py` |
