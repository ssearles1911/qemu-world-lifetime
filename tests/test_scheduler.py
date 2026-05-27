"""Internal scheduler — interval handling, error tolerance, lifecycle."""

from __future__ import annotations

import pytest

from openstack_bi import scheduler


def test_disabled_via_env(monkeypatch):
    monkeypatch.setenv("OPSBI_DISABLE_SCHEDULER", "1")
    assert scheduler._disabled() is True
    monkeypatch.setenv("OPSBI_DISABLE_SCHEDULER", "yes")
    assert scheduler._disabled() is True
    monkeypatch.delenv("OPSBI_DISABLE_SCHEDULER", raising=False)
    assert scheduler._disabled() is False


def test_env_int_validation(monkeypatch):
    monkeypatch.setenv("X", "42")
    assert scheduler._env_int("X", 2, 0, 100) == 42
    monkeypatch.setenv("X", "999")
    assert scheduler._env_int("X", 2, 0, 100) == 2   # out of range
    monkeypatch.setenv("X", "not-a-number")
    assert scheduler._env_int("X", 2, 0, 100) == 2
    monkeypatch.delenv("X", raising=False)
    assert scheduler._env_int("X", 2, 0, 100) == 2


def test_interval_minutes_default(monkeypatch):
    monkeypatch.delenv("OPSBI_COLLECTOR_INTERVAL_MINUTES", raising=False)
    assert scheduler._interval_minutes() == 15


def test_interval_minutes_env_override(monkeypatch):
    monkeypatch.setenv("OPSBI_COLLECTOR_INTERVAL_MINUTES", "5")
    assert scheduler._interval_minutes() == 5
    monkeypatch.setenv("OPSBI_COLLECTOR_INTERVAL_MINUTES", "0")
    # Below band -> falls back to default.
    assert scheduler._interval_minutes() == 15


def test_run_collector_swallows_errors(monkeypatch):
    from openstack_bi import dashboard_metrics as dm

    def _boom(*a, **k):
        raise RuntimeError("MariaDB unreachable")

    monkeypatch.setattr(dm, "collect_snapshot", _boom)
    # Must not raise.
    assert scheduler._run_collector() == 0


def test_run_collector_writes_rows_on_success(monkeypatch):
    from openstack_bi import dashboard_metrics as dm

    fake_rows = [{
        "snapshot_date": "2026-05-27", "snapshot_at": "x",
        "region": "_combined", "metric": "instances_total", "value": 7,
    }]
    written: list = []
    monkeypatch.setattr(dm, "collect_snapshot", lambda: fake_rows)
    monkeypatch.setattr(dm, "write_snapshot", lambda rows: written.extend(rows))

    assert scheduler._run_collector() == 1
    assert written == fake_rows


def test_start_disabled_returns_false(monkeypatch):
    monkeypatch.setenv("OPSBI_DISABLE_SCHEDULER", "1")
    assert scheduler.start() is False
    assert scheduler._thread is None


def test_start_then_stop_lifecycle(monkeypatch, tmp_config_db):
    monkeypatch.delenv("OPSBI_DISABLE_SCHEDULER", raising=False)
    # The first thing _loop does is call _run_collector — neutralise it
    # so the thread immediately settles into the wait state.
    monkeypatch.setattr(scheduler, "_run_collector", lambda: 0)
    # Force a long interval; the stop event preempts the wait.
    monkeypatch.setenv("OPSBI_COLLECTOR_INTERVAL_MINUTES", "1440")
    assert scheduler.start() is True
    # Second start() is a no-op while the first is alive.
    assert scheduler.start() is False
    scheduler.stop(timeout=2.0)
    assert scheduler._thread is None


def test_loop_fires_collector_at_least_once_on_start(monkeypatch):
    """The fresh-deploy contract: an immediate run at boot, then the
    interval wait — so the dashboard is never empty for the first
    full interval after a deploy."""
    calls = {"n": 0}

    def _fake_run():
        calls["n"] += 1
        # Signal exit so the loop returns without sleeping its interval.
        scheduler._stop.set()
        return 1

    monkeypatch.setattr(scheduler, "_run_collector", _fake_run)
    scheduler._stop.clear()
    scheduler._loop(interval_minutes=15)
    assert calls["n"] == 1
