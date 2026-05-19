"""ICMP reachability helpers — ping_host / ping_hosts."""

from __future__ import annotations

import subprocess
from unittest.mock import MagicMock

from openstack_bi import netcheck


def _completed(returncode, stdout="", stderr=""):
    cp = MagicMock()
    cp.returncode = returncode
    cp.stdout = stdout
    cp.stderr = stderr
    return cp


_RTT_OUT = (
    "--- 203.0.113.5 ping statistics ---\n"
    "2 packets transmitted, 2 received, 0% packet loss, time 1002ms\n"
    "rtt min/avg/max/mdev = 10.100/12.300/14.500/1.200 ms\n"
)


def test_ping_host_rejects_invalid_ip(monkeypatch):
    # A malformed IP must never reach the subprocess.
    called = []
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: called.append(a))
    res = netcheck.ping_host("not-an-ip; rm -rf /")
    assert res["reachable"] is False
    assert res["error"]
    assert called == []


def test_ping_host_reachable_parses_latency(monkeypatch):
    monkeypatch.setattr(netcheck.shutil, "which", lambda n: "/usr/bin/ping")
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: _completed(0, _RTT_OUT))
    res = netcheck.ping_host("203.0.113.5")
    assert res["reachable"] is True
    assert res["latency_ms"] == 12.3
    assert res["error"] is None


def test_ping_host_no_reply(monkeypatch):
    monkeypatch.setattr(netcheck.shutil, "which", lambda n: "/usr/bin/ping")
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: _completed(1))
    res = netcheck.ping_host("203.0.113.9")
    assert res["reachable"] is False
    assert res["error"] is None
    assert "no reply" in res["note"]


def test_ping_host_not_permitted_is_an_error(monkeypatch):
    monkeypatch.setattr(netcheck.shutil, "which", lambda n: "/usr/bin/ping")
    monkeypatch.setattr(
        subprocess, "run",
        lambda *a, **k: _completed(2, "", "ping: socket: Operation not permitted"),
    )
    res = netcheck.ping_host("203.0.113.9")
    assert res["reachable"] is False
    assert "not permitted" in res["error"].lower()


def test_ping_host_timeout_is_unreachable_not_error(monkeypatch):
    monkeypatch.setattr(netcheck.shutil, "which", lambda n: "/usr/bin/ping")

    def _raise(*a, **k):
        raise subprocess.TimeoutExpired(cmd="ping", timeout=5)

    monkeypatch.setattr(subprocess, "run", _raise)
    res = netcheck.ping_host("203.0.113.9")
    assert res["reachable"] is False
    assert res["error"] is None
    assert "timed out" in res["note"]


def test_ping_host_binary_missing(monkeypatch):
    monkeypatch.setattr(netcheck.shutil, "which", lambda n: None)
    res = netcheck.ping_host("203.0.113.9")
    assert res["reachable"] is False
    assert "unavailable" in res["error"]


def test_ping_hosts_retries_down_host_then_succeeds(monkeypatch):
    # 203.0.113.9 is down on the first round, up on the retry.
    seen: dict = {}

    def fake_ping_host(ip, **kw):
        seen[ip] = seen.get(ip, 0) + 1
        if ip == "203.0.113.9" and seen[ip] == 1:
            return {"ip": ip, "reachable": False, "latency_ms": None,
                    "note": "no reply", "error": None}
        return {"ip": ip, "reachable": True, "latency_ms": 1.0,
                "note": "", "error": None}

    monkeypatch.setattr(netcheck, "ping_host", fake_ping_host)
    out = netcheck.ping_hosts(["203.0.113.5", "203.0.113.9"], retry_gap=0)
    assert out["results"]["203.0.113.9"]["reachable"] is True
    assert seen["203.0.113.9"] == 2   # retried
    assert seen["203.0.113.5"] == 1   # up first time — not retried
    assert out["summary"]["reachable"] == 2
    assert out["ping_available"] is True


def test_ping_hosts_hard_error_short_circuits(monkeypatch):
    calls = []

    def fake_ping_host(ip, **kw):
        calls.append(ip)
        return {"ip": ip, "reachable": False, "latency_ms": None,
                "note": "", "error": "ping is not permitted on the server"}

    monkeypatch.setattr(netcheck, "ping_host", fake_ping_host)
    out = netcheck.ping_hosts(["203.0.113.5", "203.0.113.9"], retry_gap=0)
    assert out["ping_available"] is False
    assert "permitted" in out["error"]
    assert sorted(calls) == ["203.0.113.5", "203.0.113.9"]  # one round only
    assert out["summary"]["unknown"] == 2
