"""pyrdmnet – eigener RDMnet-Broker (ANSI E1.33) und Controller.

Entstanden im Projekt „GelMatch“ (Vermessung von Scheinwerfern). Das Modul ist eigenständig
lauffähig; :mod:`pyrdmnet.fassade` bietet eine synchrone Schnittstelle zum Einbinden.

Grundsätze:
* RDM SET nur für DMX-Modus, Startadresse und Identify, nur mit
  Schreibfreigabe je Sitzung; jede Änderung wird zurückgelesen.
* RDM ist eine Begleitinformation: Ein RDM-Ausfall blockiert keine Messung,
  eine festgestellte Einstellungsabweichung schon.
* Keine externen Abhängigkeiten (nur Python-Standardbibliothek).
  mDNS über Bonjour (dns-sd, auf macOS vorhanden), sonst optional ``zeroconf``.
"""

__version__ = "0.4.1"
MODULNAME = "pyrdmnet"
