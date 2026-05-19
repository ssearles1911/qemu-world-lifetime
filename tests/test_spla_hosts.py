"""SPLA-licensed hosts report — trait resolution, cores math, filters.

The report's only outside contact is `query` (one MariaDB call per
region/cell), `openstack.list_cells`, and the region/schema config
accessors. We stub all four so these tests exercise the report's logic
without a live database.
"""

from __future__ import annotations

import json
import types
from datetime import datetime

from openstack_bi.config import Region
from openstack_bi.reports import spla_hosts


def _region(name: str) -> Region:
    return Region(name=name, host=f"{name.lower()}.db", port=3306,
                  user="ssreports", password="x")


class FakeBackend:
    """Stands in for `openstack_bi.db.query`.

    `trait_ids` maps region -> {trait_name: id}; `host_rows` maps region
    -> the raw compute-node rows that region's cell would return. Each
    host row carries a private `_trait_id` so the fake can mimic the
    trait filter, and the fake honours the `s.disabled = 0` clause when
    the report includes it.
    """

    def __init__(self, trait_ids, host_rows):
        self.trait_ids = trait_ids
        self.host_rows = host_rows
        self.queries = []  # (region, database, sql, args)

    def query(self, region, database, sql, args=()):
        self.queries.append((region.name, database, sql, tuple(args)))
        if "FROM traits" in sql:
            tid = self.trait_ids.get(region.name, {}).get(args[0])
            return [{"id": tid}] if tid is not None else []
        if "FROM compute_nodes" in sql:
            tid = args[0]
            rows = [
                dict(r) for r in self.host_rows.get(region.name, [])
                if r.get("_trait_id") == tid
            ]
            if "AND s.disabled = 0" in sql:
                rows = [r for r in rows if not r.get("disabled")]
            return rows
        raise AssertionError(f"unexpected query: {sql}")


def _install(monkeypatch, backend, regions=None):
    if regions is None:
        regions = [_region("DTW"), _region("CVG")]
    monkeypatch.setattr(spla_hosts, "query", backend.query)
    monkeypatch.setattr(spla_hosts, "placement_db", lambda: "placement")
    monkeypatch.setattr(
        spla_hosts, "openstack",
        types.SimpleNamespace(list_cells=lambda region: ["nova_cell1"]),
    )
    monkeypatch.setattr(spla_hosts, "parse_regions", lambda: list(regions))


def test_report_is_registered():
    from openstack_bi.reports import by_id
    assert by_id("spla_hosts").id == "spla_hosts"


def test_resolves_trait_name_per_region(monkeypatch):
    # Same trait name, different ids per region — the report must
    # resolve each region's Placement DB independently.
    backend = FakeBackend(
        trait_ids={"DTW": {"CUSTOM_MS_SPLA": 289}, "CVG": {"CUSTOM_MS_SPLA": 305}},
        host_rows={
            "DTW": [{
                "hostname": "dtw-cmp-1", "vcpus": 64,
                "cpu_info": json.dumps({"model": "Cascadelake"}),
                "created_at": datetime(2021, 3, 1), "disabled": 0,
                "_trait_id": 289,
            }],
            "CVG": [{
                "hostname": "cvg-cmp-1", "vcpus": 48,
                "cpu_info": json.dumps({"vendor": "AMD"}),
                "created_at": datetime(2022, 6, 15), "disabled": 0,
                "_trait_id": 305,
            }],
        },
    )
    _install(monkeypatch, backend)

    result = spla_hosts.REPORT.run()

    assert result.metadata["region_errors"] == "(none)"
    assert result.metadata["total_hosts"] == 2
    assert result.metadata["total_vcpus"] == 112
    assert result.metadata["total_cores"] == 56  # 64/2 + 48/2
    assert "DTW=289" in result.metadata["trait_ids"]
    assert "CVG=305" in result.metadata["trait_ids"]
    assert result.groupings == ["region"]

    by_host = {r["hostname"]: r for r in result.rows}
    assert by_host["dtw-cmp-1"]["cores"] == 32
    assert by_host["dtw-cmp-1"]["cpu_model"] == "Cascadelake"
    assert by_host["dtw-cmp-1"]["in_service"] == "2021-03-01"
    assert by_host["cvg-cmp-1"]["cpu_model"] == "AMD"  # vendor fallback


def test_trait_id_override_skips_name_resolution(monkeypatch):
    # Name resolution would find nothing (trait_ids empty); the override
    # must be applied directly and `placement.traits` left untouched.
    backend = FakeBackend(
        trait_ids={},
        host_rows={
            "DTW": [{"hostname": "dtw-1", "vcpus": 16, "cpu_info": None,
                     "created_at": datetime(2020, 1, 1), "disabled": 0,
                     "_trait_id": 289}],
            "CVG": [{"hostname": "cvg-1", "vcpus": 16, "cpu_info": None,
                     "created_at": datetime(2020, 1, 1), "disabled": 0,
                     "_trait_id": 289}],
        },
    )
    _install(monkeypatch, backend)

    result = spla_hosts.REPORT.run(trait_id=289)

    assert result.metadata["total_hosts"] == 2
    assert "289 (override" in result.metadata["trait_ids"]
    assert not any("FROM traits" in q[2] for q in backend.queries)


def test_disabled_services_excluded_by_default(monkeypatch):
    backend = FakeBackend(
        trait_ids={"DTW": {"CUSTOM_MS_SPLA": 1}, "CVG": {"CUSTOM_MS_SPLA": 1}},
        host_rows={
            "DTW": [
                {"hostname": "dtw-on", "vcpus": 8, "cpu_info": None,
                 "created_at": None, "disabled": 0, "_trait_id": 1},
                {"hostname": "dtw-off", "vcpus": 8, "cpu_info": None,
                 "created_at": None, "disabled": 1, "_trait_id": 1},
            ],
            "CVG": [],
        },
    )
    _install(monkeypatch, backend)

    default = spla_hosts.REPORT.run()
    assert {r["hostname"] for r in default.rows} == {"dtw-on"}

    with_disabled = spla_hosts.REPORT.run(include_disabled=True)
    assert {r["hostname"] for r in with_disabled.rows} == {"dtw-on", "dtw-off"}
    states = {r["hostname"]: r["service_state"] for r in with_disabled.rows}
    assert states["dtw-off"] == "disabled"
    assert states["dtw-on"] == "enabled"


def test_threads_per_core_divisor_applied(monkeypatch):
    backend = FakeBackend(
        trait_ids={"DTW": {"CUSTOM_MS_SPLA": 1}, "CVG": {"CUSTOM_MS_SPLA": 1}},
        host_rows={
            "DTW": [{"hostname": "h", "vcpus": 40, "cpu_info": None,
                     "created_at": None, "disabled": 0, "_trait_id": 1}],
            "CVG": [],
        },
    )
    _install(monkeypatch, backend)

    assert spla_hosts.REPORT.run(threads_per_core=1).rows[0]["cores"] == 40
    assert spla_hosts.REPORT.run(threads_per_core=4).rows[0]["cores"] == 10


def test_threads_per_core_must_be_positive(monkeypatch):
    _install(monkeypatch, FakeBackend(trait_ids={}, host_rows={}))
    result = spla_hosts.REPORT.run(threads_per_core=0)
    assert "error" in result.metadata
    assert result.rows == []


def test_empty_trait_name_falls_back_to_default(monkeypatch):
    # Clearing the trait-name field resolves CUSTOM_MS_SPLA, not an error.
    backend = FakeBackend(
        trait_ids={"DTW": {"CUSTOM_MS_SPLA": 9}, "CVG": {"CUSTOM_MS_SPLA": 9}},
        host_rows={"DTW": [], "CVG": []},
    )
    _install(monkeypatch, backend)
    result = spla_hosts.REPORT.run(trait_name="")
    assert "error" not in result.metadata
    assert result.metadata["trait"] == "CUSTOM_MS_SPLA"
    assert backend.queries[0][3] == ("CUSTOM_MS_SPLA",)


def test_trait_not_found_in_a_region_is_reported(monkeypatch):
    # DTW has the trait, CVG does not.
    backend = FakeBackend(
        trait_ids={"DTW": {"CUSTOM_MS_SPLA": 7}},
        host_rows={"DTW": [{"hostname": "dtw-1", "vcpus": 4, "cpu_info": None,
                            "created_at": None, "disabled": 0, "_trait_id": 7}]},
    )
    _install(monkeypatch, backend)

    result = spla_hosts.REPORT.run()

    assert result.metadata["total_hosts"] == 1
    assert "CVG=not found" in result.metadata["trait_ids"]
    assert "CVG" in result.metadata["trait_not_found"]
