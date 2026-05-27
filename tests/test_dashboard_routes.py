"""Dashboard page + JSON endpoint + CSV download."""

from __future__ import annotations

from typing import Dict, Tuple

import pytest


@pytest.fixture
def client(tmp_config_db):
    from openstack_bi import config_db
    from openstack_bi.auth import local as local_auth
    from openstack_bi.web import create_app

    config_db.upsert_region(
        name="dtw", host="127.0.0.1", port=3306, db_user="r", db_password="",
        is_keystone_region=True,
    )
    config_db.upsert_region(
        name="cvg", host="127.0.0.1", port=3306, db_user="r", db_password="",
    )
    local_auth.create_admin("admin", "password123")
    app = create_app()
    app.config["WTF_CSRF_ENABLED"] = False
    return app.test_client()


def _login_local(client):
    """Drop a local-admin session payload into the test client."""
    from openstack_bi.auth.session import SESSION_KEY
    with client.session_transaction() as sess:
        sess[SESSION_KEY] = {
            "kind": "local", "user_id": "1", "username": "admin",
            "is_admin": True, "project_ids": [], "domain_id": None,
            "roles": [],
        }


def _patch_metrics(monkeypatch, current=None, history=None):
    """Inject in-memory current+history data so the view doesn't hit
    SQLite. The dashboard view reads `_current_from_history` and
    `_history_bulk` — patch both."""
    from openstack_bi.web import dashboard_routes as dr

    snap: Dict[Tuple[str, str], int] = current if current is not None else {
        ("dtw", "instances_total"): 100,
        ("cvg", "instances_total"): 120,
        ("_combined", "instances_total"): 220,
        ("dtw", "instances_error"): 0,
        ("cvg", "instances_error"): 0,
        ("_combined", "instances_error"): 0,
        ("dtw", "ports_build"): 0,
        ("cvg", "ports_build"): 0,
        ("_combined", "ports_build"): 0,
        ("_combined", "snapshots_autobackup_today"): 312,
        ("dtw", "snapshots_autobackup_today"): 168,
        ("cvg", "snapshots_autobackup_today"): 144,
    }
    monkeypatch.setattr(
        dr, "_current_from_history",
        lambda: (snap, "2026-05-27T14:00:00+00:00"),
    )
    hist = history if history is not None else {}
    monkeypatch.setattr(dr, "_history_bulk", lambda days: hist)
    return snap, hist


def test_dashboard_requires_login(client):
    r = client.get("/dashboard")
    assert r.status_code == 302
    assert "/login" in r.headers["Location"]


def test_dashboard_renders_with_zero_anomalies_clean_banner(client, monkeypatch):
    _login_local(client)
    _patch_metrics(monkeypatch)
    r = client.get("/dashboard")
    assert r.status_code == 200
    # The OK banner appears when nothing is wrong.
    assert b"No anomalies" in r.data
    assert b"steady state" in r.data


def test_dashboard_renders_anomalies_when_present(client, monkeypatch):
    _login_local(client)
    _patch_metrics(monkeypatch, current={
        ("dtw", "instances_error"): 2,
        ("cvg", "instances_error"): 0,
        ("_combined", "instances_error"): 2,
        ("dtw", "ports_build"): 5,
        ("cvg", "ports_build"): 0,
        ("_combined", "ports_build"): 5,
        ("_combined", "snapshots_autobackup_today"): 1,
    })
    r = client.get("/dashboard")
    assert r.status_code == 200
    assert b"anomaly-tile" in r.data
    assert b"Instances in ERROR" in r.data
    assert b"Ports in BUILD" in r.data


def test_dashboard_region_filter_only_shows_one_region(client, monkeypatch):
    _login_local(client)
    _patch_metrics(monkeypatch)
    r = client.get("/dashboard?region=dtw")
    assert r.status_code == 200
    # Per-region table is hidden when filtered to one region.
    assert b"Per-region breakdown" not in r.data


def test_dashboard_range_chip_changes_history_window(client, monkeypatch):
    _login_local(client)
    called = {}

    def _fake_bulk(days):
        called["days"] = days
        return {}

    from openstack_bi.web import dashboard_routes as dr
    monkeypatch.setattr(dr, "_history_bulk", _fake_bulk)
    from openstack_bi import dashboard_metrics as dm
    monkeypatch.setattr(dm, "current_snapshot", lambda *a, **k: {})

    client.get("/dashboard?range=90d")
    assert called["days"] == 90


def test_dashboard_json_returns_json(client, monkeypatch):
    _login_local(client)
    _patch_metrics(monkeypatch)
    r = client.get("/dashboard.json?region=all&range=30d")
    assert r.status_code == 200
    d = r.get_json()
    assert d["region_filter"] == "all"
    assert d["range"] == "30d"
    assert "tiles" in d and "anomalies" in d
    assert "backups" in d and "charts" in d


def test_backups_csv_endpoint_rejects_unknown_region(client):
    _login_local(client)
    r = client.get("/dashboard/backups.csv?region=bogus")
    assert r.status_code == 404


def test_backups_csv_endpoint_returns_text_csv(client, monkeypatch):
    _login_local(client)
    from openstack_bi import dashboard_metrics as dm
    monkeypatch.setattr(
        dm, "today_autobackups_csv",
        lambda region, date: "id,display_description\nsnap-1,autobackup-acme\n",
    )
    r = client.get("/dashboard/backups.csv?region=dtw")
    assert r.status_code == 200
    assert r.headers["Content-Type"].startswith("text/csv")
    assert b"snap-1" in r.data


def test_dashboard_reads_current_values_from_history_table(client, monkeypatch):
    """The view reads `_current_from_history` (not the live MariaDB).
    Sanity-check that a value written to the history table appears as
    the tile value on the page."""
    _login_local(client)
    from openstack_bi.web import dashboard_routes as dr
    monkeypatch.setattr(dr, "_history_bulk", lambda d: {})
    from openstack_bi import config_db
    with config_db.cursor() as cur:
        cur.execute(
            "INSERT OR REPLACE INTO dashboard_metric_history "
            "(snapshot_date, snapshot_at, region, metric, value) VALUES "
            "(date('now'), datetime('now'), '_combined', 'instances_total', 999)"
        )

    r = client.get("/dashboard")
    assert r.status_code == 200
    assert b"999" in r.data


def test_dashboard_shows_empty_history_banner_when_collector_has_not_run(client, monkeypatch):
    _login_local(client)
    from openstack_bi.web import dashboard_routes as dr
    monkeypatch.setattr(dr, "_history_bulk", lambda d: {})
    monkeypatch.setattr(dr, "_current_from_history", lambda: ({}, None))

    r = client.get("/dashboard")
    assert r.status_code == 200
    assert b"no metric history yet" in r.data
    assert b"opsbi snapshot-metrics" in r.data


def test_dashboard_json_fresh_runs_collector(client, monkeypatch):
    _login_local(client)
    from openstack_bi import dashboard_metrics as dm
    from openstack_bi.web import dashboard_routes as dr

    calls = {"collect": 0, "write": 0}
    monkeypatch.setattr(
        dm, "collect_snapshot",
        lambda *a, **k: (calls.__setitem__("collect", calls["collect"] + 1) or []),
    )
    monkeypatch.setattr(
        dm, "write_snapshot",
        lambda *a, **k: calls.__setitem__("write", calls["write"] + 1),
    )
    _patch_metrics(monkeypatch)

    r = client.get("/dashboard.json?fresh=1")
    assert r.status_code == 200
    assert calls["collect"] == 1 and calls["write"] == 1

    # Without ?fresh, neither runs again.
    monkeypatch.setattr(dr, "_current_from_history",
                        lambda: ({}, "2026-05-27T14:00:00+00:00"))
    r = client.get("/dashboard.json")
    assert r.status_code == 200
    assert calls["collect"] == 1 and calls["write"] == 1
