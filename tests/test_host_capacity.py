"""Host-capacity report — math, heat-map shading, rollups, region fan-out.

The report's only outside contact is `query` (one MariaDB call per
region/cell), `openstack.list_cells`, and `parse_regions`. We stub all
three so these tests exercise the report's logic without a live database.
"""

from __future__ import annotations

import types

from openstack_bi.config import Region
from openstack_bi.reports import host_capacity


def _region(name: str) -> Region:
    return Region(name=name, host=f"{name.lower()}.db", port=3306,
                  user="ssreports", password="x")


class FakeBackend:
    """Stands in for `openstack_bi.db.query`; returns each region's raw
    `compute_nodes` rows verbatim (the report does the math)."""

    def __init__(self, host_rows):
        self.host_rows = host_rows  # region -> list of compute_node dicts
        self.queries = []

    def query(self, region, database, sql, args=()):
        self.queries.append((getattr(region, "name", region), database, sql, tuple(args)))
        return [dict(r) for r in self.host_rows.get(getattr(region, "name", region), [])]


def _install(monkeypatch, backend, regions=None):
    if regions is None:
        regions = [_region("DTW"), _region("CVG")]
    monkeypatch.setattr(host_capacity, "query", backend.query)
    monkeypatch.setattr(
        host_capacity, "openstack",
        types.SimpleNamespace(list_cells=lambda region: ["nova_cell1"]),
    )
    monkeypatch.setattr(host_capacity, "parse_regions", lambda: list(regions))


def _node(hostname, *, vcpus=64, vcpus_used=32, memory_mb=262144,
          memory_mb_used=131072, running_vms=10, disabled=0, host=None):
    return {
        "hostname": hostname, "host": host or hostname,
        "vcpus": vcpus, "vcpus_used": vcpus_used,
        "memory_mb": memory_mb, "memory_mb_used": memory_mb_used,
        "running_vms": running_vms, "disabled": disabled,
    }


def _by_host(result):
    return {r["hostname"]: r for r in result.rows}


def test_report_is_registered():
    from openstack_bi.reports import by_id
    assert by_id("host_capacity").id == "host_capacity"


def test_basic_capacity_math(monkeypatch):
    backend = FakeBackend(host_rows={
        "DTW": [_node("dtw-1", vcpus=64, vcpus_used=32,
                      memory_mb=1024 * 100, memory_mb_used=1024 * 50,
                      running_vms=12)],
        "CVG": [],
    })
    _install(monkeypatch, backend)
    row = _by_host(host_capacity.REPORT.run())["dtw-1"]
    assert row["vcpus"] == 64
    assert row["vcpus_used"] == 32
    assert row["cpu_pct"] == 50
    assert row["memory_gb"] == 100
    assert row["memory_used_gb"] == 50
    assert row["mem_pct"] == 50
    assert row["instances"] == 12
    assert row["oversub"] == 0.5  # max(0.5 cpu, 0.5 mem)


def test_oversubscription_takes_worse_dimension(monkeypatch):
    # CPU 4x oversubscribed, memory at 60% -> oversub follows CPU = 4.0.
    backend = FakeBackend(host_rows={
        "DTW": [_node("dtw-1", vcpus=10, vcpus_used=40,
                      memory_mb=1000, memory_mb_used=600)],
        "CVG": [],
    })
    _install(monkeypatch, backend)
    row = _by_host(host_capacity.REPORT.run())["dtw-1"]
    assert row["cpu_pct"] == 400
    assert row["oversub"] == 4.0


def test_cpu_shading_thresholds(monkeypatch):
    # cpu_pct: 100 -> green (<150 warn), 200 -> orange, 350 -> red.
    backend = FakeBackend(host_rows={
        "DTW": [
            _node("green", vcpus=10, vcpus_used=10),    # 100%
            _node("orange", vcpus=10, vcpus_used=20),   # 200%
            _node("red", vcpus=10, vcpus_used=35),      # 350%
        ],
        "CVG": [],
    })
    _install(monkeypatch, backend)
    rows = _by_host(host_capacity.REPORT.run())
    assert rows["green"]["cpu_pct_shade"] == "green"
    assert rows["orange"]["cpu_pct_shade"] == "orange"
    assert rows["red"]["cpu_pct_shade"] == "red"


def test_memory_shading_thresholds(monkeypatch):
    # mem_pct defaults: warn 80, crit 90.
    backend = FakeBackend(host_rows={
        "DTW": [
            _node("ok", memory_mb=1000, memory_mb_used=700),     # 70%
            _node("warn", memory_mb=1000, memory_mb_used=850),   # 85%
            _node("crit", memory_mb=1000, memory_mb_used=950),   # 95%
        ],
        "CVG": [],
    })
    _install(monkeypatch, backend)
    rows = _by_host(host_capacity.REPORT.run())
    assert rows["ok"]["mem_pct_shade"] == "green"
    assert rows["warn"]["mem_pct_shade"] == "orange"
    assert rows["crit"]["mem_pct_shade"] == "red"


def test_custom_thresholds_applied(monkeypatch):
    backend = FakeBackend(host_rows={
        "DTW": [_node("h", vcpus=10, vcpus_used=12)],  # 120%
        "CVG": [],
    })
    _install(monkeypatch, backend)
    # Default: 120% < 150 warn -> green.
    assert _by_host(host_capacity.REPORT.run())["h"]["cpu_pct_shade"] == "green"
    # Tighten warn to 110 -> now orange.
    tightened = host_capacity.REPORT.run(cpu_warn_pct=110, cpu_crit_pct=200)
    assert _by_host(tightened)["h"]["cpu_pct_shade"] == "orange"


def test_enabled_disabled_status_and_shade(monkeypatch):
    backend = FakeBackend(host_rows={
        "DTW": [_node("on", disabled=0), _node("off", disabled=1)],
        "CVG": [],
    })
    _install(monkeypatch, backend)
    rows = _by_host(host_capacity.REPORT.run())
    assert rows["on"]["service"] == "Enabled"
    assert rows["on"]["service_shade"] == "green"
    assert rows["off"]["service"] == "Disabled"
    assert rows["off"]["service_shade"] == "red"


def test_missing_service_is_unknown(monkeypatch):
    # LEFT JOIN can yield disabled=None when no nova-compute service matched.
    backend = FakeBackend(host_rows={
        "DTW": [_node("orphan", disabled=None)],
        "CVG": [],
    })
    _install(monkeypatch, backend)
    row = _by_host(host_capacity.REPORT.run())["orphan"]
    assert row["service"] == "Unknown"
    assert row["service_shade"] == "gray"


def test_zero_capacity_host_is_gray_not_green(monkeypatch):
    # A node reporting 0 vCPU/0 memory must not read as "healthy green".
    backend = FakeBackend(host_rows={
        "DTW": [_node("empty", vcpus=0, vcpus_used=0,
                      memory_mb=0, memory_mb_used=0)],
        "CVG": [],
    })
    _install(monkeypatch, backend)
    row = _by_host(host_capacity.REPORT.run())["empty"]
    assert row["cpu_pct"] is None
    assert row["mem_pct"] is None
    assert row["oversub"] is None
    assert row["cpu_pct_shade"] == "gray"
    assert row["mem_pct_shade"] == "gray"
    assert row["oversub_shade"] == "gray"


def test_region_fan_out_and_rollups(monkeypatch):
    backend = FakeBackend(host_rows={
        "DTW": [_node("dtw-1", vcpus=64, vcpus_used=32, running_vms=5, disabled=0),
                _node("dtw-2", vcpus=64, vcpus_used=64, running_vms=8, disabled=1)],
        "CVG": [_node("cvg-1", vcpus=32, vcpus_used=16, running_vms=3, disabled=0)],
    })
    _install(monkeypatch, backend)
    result = host_capacity.REPORT.run()
    assert result.metadata["total_hosts"] == 3
    assert result.metadata["enabled_hosts"] == 2
    assert result.metadata["disabled_hosts"] == 1
    assert result.metadata["total_instances"] == 16
    assert result.metadata["vcpu_allocated_vs_capacity"] == "112 / 160"
    assert "region_DTW" in result.metadata
    assert "region_CVG" in result.metadata
    assert result.metadata["region_errors"] == "(none)"
    # Rows are sorted by (region, hostname).
    assert [r["hostname"] for r in result.rows] == ["cvg-1", "dtw-1", "dtw-2"]


def test_region_selection_limits_fan_out(monkeypatch):
    backend = FakeBackend(host_rows={
        "DTW": [_node("dtw-1")], "CVG": [_node("cvg-1")],
    })
    _install(monkeypatch, backend)
    result = host_capacity.REPORT.run(regions=["CVG"])
    assert {r["region"] for r in result.rows} == {"CVG"}
    assert {q[0] for q in backend.queries} == {"CVG"}


def test_columns_have_no_shade_companions(monkeypatch):
    # The `_shade` companions must not leak in as visible columns.
    backend = FakeBackend(host_rows={"DTW": [_node("dtw-1")], "CVG": []})
    _install(monkeypatch, backend)
    col_keys = {k for k, _ in host_capacity.REPORT.run().columns}
    assert "service_shade" not in col_keys
    assert "cpu_pct_shade" not in col_keys
    assert {"region", "hostname", "service", "instances", "vcpus",
            "vcpus_used", "cpu_pct", "memory_gb", "memory_used_gb",
            "mem_pct", "oversub"} == col_keys


# --- end-to-end web render: the template must emit the shade classes ---------

def test_report_page_renders_cell_shading(monkeypatch, tmp_config_db):
    from openstack_bi import config_db
    from openstack_bi.auth import local as local_auth
    from openstack_bi.auth.session import SESSION_KEY
    from openstack_bi.web import create_app

    config_db.upsert_region(
        name="dfw", host="127.0.0.1", port=3306, db_user="r", db_password="",
        is_keystone_region=True,
    )
    local_auth.create_admin("admin", "password123")

    backend = FakeBackend(host_rows={
        "dfw": [
            _node("healthy", vcpus=10, vcpus_used=5,
                  memory_mb=1000, memory_mb_used=500, disabled=0),   # green
            _node("hot", vcpus=10, vcpus_used=20,
                  memory_mb=1000, memory_mb_used=850, disabled=0),   # orange
            _node("down", vcpus=10, vcpus_used=35, disabled=1),      # red
        ],
    })
    _install(monkeypatch, backend, regions=[_region("dfw")])

    app = create_app()
    app.config["WTF_CSRF_ENABLED"] = False
    client = app.test_client()
    with client.session_transaction() as sess:
        sess[SESSION_KEY] = {
            "kind": "local", "user_id": "1", "username": "admin",
            "is_admin": True,
        }

    resp = client.get("/report/host_capacity?regions=dfw")
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    # All three shade bands made it into the rendered table.
    assert "cell-shade-green" in html
    assert "cell-shade-orange" in html
    assert "cell-shade-red" in html
    # And the enabled/disabled text rendered.
    assert "Disabled" in html and "Enabled" in html
