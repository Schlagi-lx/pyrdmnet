"""Network adapters of this computer: names and IPv4 addresses (macOS, Linux, Windows).

Standard library only: parses ``ifconfig`` (macOS, older Linux), ``ip -4 -o addr show``
(Linux) or ``ipconfig`` (Windows).
"""

from __future__ import annotations

import re
import socket
import subprocess
import sys

_IPV4 = r"(\d+\.\d+\.\d+\.\d+)"


def _run_text(command: list[str]) -> str | None:
    try:
        p = subprocess.run(command, capture_output=True, timeout=5)
    except (OSError, subprocess.SubprocessError):
        return None
    # ipconfig writes in the console code page; only names may suffer, addresses are ASCII
    return p.stdout.decode("utf-8", errors="replace") if p.returncode == 0 else None


def parse_ifconfig(text: str) -> list[tuple[str, str]]:
    """``ifconfig`` output (macOS, older Linux)."""
    adapters, name = [], None
    for line in text.splitlines():
        m = re.match(r"^([A-Za-z0-9_.\-]+):? ", line)
        if m and not line.startswith((" ", "\t")):
            name = m.group(1).rstrip(":")
            continue
        m = re.search(r"\binet (?:addr:)?" + _IPV4, line)
        if m and name:
            adapters.append((name, m.group(1)))
    return adapters


def parse_ip_addr(text: str) -> list[tuple[str, str]]:
    """``ip -4 -o addr show`` output (Linux without ifconfig)."""
    adapters = []
    for line in text.splitlines():
        m = re.match(r"^\d+:\s+(\S+)\s+inet\s+" + _IPV4, line)
        if m:
            adapters.append((m.group(1), m.group(2)))
    return adapters


def parse_ipconfig(text: str) -> list[tuple[str, str]]:
    """``ipconfig`` output (Windows, any display language: matches the "IPv4" line of each adapter)."""
    adapters, name = [], None
    for line in text.splitlines():
        if line and not line[0].isspace() and line.rstrip().endswith(":"):
            name = line.rstrip()[:-1].strip()
            continue
        m = re.search(r"IPv4[^:]*:\s*" + _IPV4, line)
        if m and name:
            adapters.append((name, m.group(1)))
    return adapters


def network_adapters(text: str | None = None) -> list[tuple[str, str]]:
    """(name, IPv4) of all adapters that have an address.

    Windows: ``ipconfig``; macOS/Linux: ``ifconfig``, falling back to ``ip -4 -o addr show``.
    ``text`` (ifconfig format) is only used by tests.
    """
    if text is not None:
        return parse_ifconfig(text)
    if sys.platform == "win32":
        out = _run_text(["ipconfig"])
        return parse_ipconfig(out) if out else []
    out = _run_text(["ifconfig"])
    if out and (adapters := parse_ifconfig(out)):
        return adapters
    out = _run_text(["ip", "-4", "-o", "addr", "show"])
    return parse_ip_addr(out) if out else []


def interface_for_ipv4(ip: str) -> str | None:
    """Name of the adapter that has this IPv4 address (e.g. ``en5``), or None."""
    for name, address in network_adapters():
        if address == ip:
            return name
    return None


def interface_index_for_ipv4(ip: str) -> int:
    """IPv6 interface index of the adapter with this IPv4 address; 0 if unknown.

    Used for IPv6 multicast, which on macOS needs an explicit interface. On Windows the
    adapter names of ``ipconfig`` are not interface names, so the result is usually 0 there.
    """
    if ip in ("", "0.0.0.0") or ip.startswith("127."):
        return 0
    name = interface_for_ipv4(ip)
    if not name:
        return 0
    try:
        return socket.if_nametoindex(name)
    except (OSError, AttributeError):
        return 0
