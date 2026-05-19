"""ICMP reachability checks — ping one host or a batch concurrently.

Used by the L3-router tool to confirm routers are still pingable on
their WAN IPs after being rescheduled between L3 agents. Pure standard
library: the pinging is delegated to the system `ping` binary in a
subprocess, so no raw sockets and no privileges beyond what the `ping`
binary itself carries.

Design notes:
  * Every IP is validated with `ipaddress` before it reaches the
    subprocess — together with `shell=False` and an argument list, that
    rules out command injection. Callers can only ever ping a literal
    IP address.
  * Three outcomes are kept distinct per host: reachable, not reachable,
    and *could not check* (the `ping` binary is missing or lacks
    permission). The third must never be reported as "down" — a broken
    checker is not a dead router.
"""

from __future__ import annotations

import ipaddress
import re
import shutil
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

# `rtt min/avg/max/mdev = 0.123/0.456/0.789/0.111 ms` -> capture avg.
_RTT_RE = re.compile(r"=\s*[\d.]+/([\d.]+)/")
# Fallback: an individual reply line `... time=0.456 ms`.
_TIME_RE = re.compile(r"time[=<]([\d.]+)\s*ms")


def _result(
    ip: str,
    reachable: Optional[bool],
    latency_ms: Optional[float] = None,
    note: str = "",
    error: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "ip": ip,
        "reachable": reachable,
        "latency_ms": latency_ms,
        "note": note,
        "error": error,
    }


def ping_host(ip: str, *, count: int = 2, timeout: int = 1) -> Dict[str, Any]:
    """Ping one host. Never raises — an unreachable host is a normal
    result. `error` is set only when the check itself could not run (a
    malformed IP, or a missing / unprivileged `ping` binary)."""
    try:
        ipaddress.ip_address(ip)
    except ValueError:
        return _result(ip, False, error=f"not a valid IP address: {ip!r}")

    ping_bin = shutil.which("ping")
    if not ping_bin:
        return _result(ip, False, error="ping binary unavailable on the server")

    # iputils flags: -c count, -W per-reply wait (s), -i interval between
    # packets (0.3 is the floor for an unprivileged caller), -n no rDNS,
    # -q quiet (still prints the rtt summary we parse).
    cmd = [ping_bin, "-c", str(count), "-W", str(timeout),
           "-i", "0.3", "-n", "-q", ip]
    # Bound the whole call: `count` packets, each waited up to `timeout`,
    # 0.3s apart, plus slack.
    deadline = count * (0.3 + timeout) + 3
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True,
            timeout=deadline, check=False,
        )
    except subprocess.TimeoutExpired:
        return _result(ip, False, note="timed out")
    except OSError as exc:
        return _result(ip, False, error=f"could not run ping: {exc}")

    if proc.returncode == 0:
        out = proc.stdout or ""
        match = _RTT_RE.search(out) or _TIME_RE.search(out)
        latency = float(match.group(1)) if match else None
        return _result(ip, True, latency_ms=latency)
    if proc.returncode == 1:
        # Ran fine, no echo reply came back.
        return _result(ip, False, note="no reply")
    # returncode 2 (or anything else) — tell "host down" apart from
    # "ping itself is broken".
    stderr = (proc.stderr or "").strip()
    low = stderr.lower()
    if (
        "operation not permitted" in low
        or "lacking privilege" in low
        or "socket:" in low
    ):
        return _result(
            ip, False,
            error="ping is not permitted on the server "
                  "(missing NET_RAW capability)",
        )
    return _result(ip, False, error=stderr[:200] or "ping failed")


def ping_hosts(
    ips: List[str],
    *,
    count: int = 2,
    timeout: int = 1,
    attempts: int = 2,
    retry_gap: float = 2.0,
    max_workers: int = 16,
) -> Dict[str, Any]:
    """Ping many hosts concurrently, with a round-based retry.

    Round 1 pings every host; later rounds re-ping only the hosts that
    were *down* (not the ones that errored), after `retry_gap` seconds —
    giving a router still settling after a reschedule a second chance
    without per-host bookkeeping.

    Returns `{results: {ip: ping_host-dict}, summary, ping_available,
    error}`. A binary-level failure (`ping` missing / not permitted)
    short-circuits the retries and is reported in `error`.
    """
    unique = list(dict.fromkeys(ip for ip in ips if ip))
    results: Dict[str, Dict[str, Any]] = {}
    batch_error: Optional[str] = None

    pending = unique
    for round_no in range(max(1, attempts)):
        if not pending:
            break
        if round_no > 0:
            time.sleep(retry_gap)
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            round_results = list(pool.map(
                lambda ip: ping_host(ip, count=count, timeout=timeout),
                pending,
            ))
        for res in round_results:
            results[res["ip"]] = res
        # A hard error means the checker itself is broken — stop here
        # rather than retrying every host pointlessly.
        hard = next((r for r in round_results if r["error"]), None)
        if hard is not None:
            batch_error = hard["error"]
            break
        pending = [r["ip"] for r in round_results if not r["reachable"]]

    reachable = sum(1 for r in results.values() if r["reachable"])
    errored = sum(1 for r in results.values() if r["error"])
    unreachable = len(results) - reachable - errored
    return {
        "results": results,
        "summary": {
            "total": len(unique),
            "reachable": reachable,
            "unreachable": unreachable,
            "unknown": errored,
        },
        "ping_available": batch_error is None,
        "error": batch_error,
    }
