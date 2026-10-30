"""pyrdmnet – a standalone RDMnet broker (ANSI E1.33) and controller.

Originated in the "GelMatch" project (measurement of fixtures). The module runs
standalone; :mod:`pyrdmnet.facade` provides a synchronous interface for embedding.

Principles:
* RDM SET only with per-session write permission; which PIDs may be set is a
  :class:`~pyrdmnet.controller.WritePolicy` (default: any; ``WritePolicy.lab()``:
  DMX mode, start address and identify only). Discovery commands are never sent.
  The settings workflow reads every change back.
* RDM is supplementary information: an RDM failure does not block a measurement,
  a detected settings deviation does.
* No external dependencies (Python standard library only).
  mDNS via Bonjour (dns-sd, available on macOS), otherwise optionally ``zeroconf``.
"""

__version__ = "0.5.0"
MODULE_NAME = "pyrdmnet"
