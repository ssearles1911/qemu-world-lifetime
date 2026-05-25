"""Locate-instance report — search-term building, region fan-out, actions.

The report's only outside contact is `query` (one MariaDB call per
region/cell plus Keystone directory lookups), `openstack.list_cells`,
and the region config accessors. We stub all of them so these tests
exercise the report's logic without a live database.
"""

from __future__ import annotations

import types
from datetime import datetime

from openstack_bi.config import Region
from openstack_bi.reports import locate_instance


def _region(name: str) -> Region:
    return Region(name=name, host=f"{name.lower()}.db", port=3306,
                  user="ssreports", password="x")


def _ks_domain(did: str, name: str) -> dict:
    return {"id": did, "name": name, "domain_id": "<<root>>", "is_domain": True}


def _ks_project(pid: str, name: str, domain_id: str) -> dict:
    return {"id": pid, "name": name, "domain_id": domain_id, "is_domain": False}


class FakeBackend:
    """Stands in for `openstack_bi.db.query`.

    `instance_rows` maps region -> the raw `instances` rows that region's
    cell returns; the fake applies the report's WHERE clauses itself
    (name / uuid / project_id IN) so we can assert the filters narrow
    results. `keystone_rows` is a flat list of Keystone `project` records
    (domains and projects alike) answering every directory lookup.
    """

    def __init__(self, instance_rows, keystone_rows=None):
        self.instance_rows = instance_rows
        self.keystone_rows = list(keystone_rows or [])
        self.queries = []  # (region_name, database, sql, args)

    def query(self, region, database, sql, args=()):
        region_name = getattr(region, "name", region)
        self.queries.append((region_name, database, sql, tuple(args)))

        if "FROM instances" in sql:
            rows = [dict(r) for r in self.instance_rows.get(region_name, [])]
            ai = 0
            if "n.display_name LIKE" in sql:
                # The name clause binds the pattern twice: display_name OR hostname.
                pat = args[ai]
                rows = [
                    r for r in rows
                    if _like_match(r.get("display_name"), pat)
                    or _like_match(r.get("hostname"), pat)
                ]
                ai += 2
            if "n.uuid LIKE" in sql:
                rows = [r for r in rows if _like_match(r.get("uuid"), args[ai])]
                ai += 1
            elif "n.uuid =" in sql:
                rows = [r for r in rows if r.get("uuid") == args[ai]]
                ai += 1
            if "n.host LIKE" in sql:
                rows = [r for r in rows if _like_match(r.get("host"), args[ai])]
                ai += 1
            if "n.project_id IN" in sql:
                wanted = set(args[ai:])
                rows = [r for r in rows if r.get("project_id") in wanted]
            return rows

        if "FROM project" in sql:
            rows = list(self.keystone_rows)
            if "is_domain = 1" in sql:
                rows = [r for r in rows if r["is_domain"]]
            elif "is_domain = 0" in sql:
                rows = [r for r in rows if not r["is_domain"]]
            if "name LIKE" in sql:
                rows = [r for r in rows if _like_match(r["name"], args[0])]
            elif "domain_id IN" in sql:
                wanted = set(args)
                rows = [r for r in rows if r.get("domain_id") in wanted]
            elif "id IN" in sql:
                wanted = set(args)
                rows = [r for r in rows if r["id"] in wanted]
            if "id, name, domain_id" in sql:
                return [{"id": r["id"], "name": r["name"],
                         "domain_id": r.get("domain_id")} for r in rows]
            if "id, name FROM" in sql:
                return [{"id": r["id"], "name": r["name"]} for r in rows]
            return [{"id": r["id"]} for r in rows]

        raise AssertionError(f"unexpected query: {sql}")


def _like_match(value: str, pattern: str) -> bool:
    """Tiny SQL-LIKE emulator: % -> .*, _ -> ., case-insensitive."""
    import re as _re
    if value is None:
        return False
    parts = []
    for ch in pattern:
        if ch == "%":
            parts.append(".*")
        elif ch == "_":
            parts.append(".")
        else:
            parts.append(_re.escape(ch))
    regex = "^" + "".join(parts) + "$"
    return _re.match(regex, value, _re.IGNORECASE) is not None


def _install(monkeypatch, backend, regions=None):
    if regions is None:
        regions = [_region("DTW"), _region("CVG")]
    monkeypatch.setattr(locate_instance, "query", backend.query)
    monkeypatch.setattr(
        locate_instance, "openstack",
        types.SimpleNamespace(list_cells=lambda region: ["nova_cell1"]),
    )
    monkeypatch.setattr(locate_instance, "parse_regions", lambda: list(regions))
    # The keystone resolvers import these accessors locally from config.
    import openstack_bi.config as cfg
    monkeypatch.setattr(cfg, "keystone_region", lambda: regions[0])
    monkeypatch.setattr(cfg, "keystone_db", lambda: "keystone")


def _row(uuid, name, *, project_id="p1", host="cmp-1", state="active",
         vcpus=4, mem=8192, created=datetime(2024, 1, 1), hostname=None):
    return {
        "uuid": uuid, "display_name": name, "project_id": project_id,
        "host": host, "hostname": hostname if hostname is not None else name,
        "vm_state": state,
        "vcpus": vcpus, "memory_mb": mem, "created_at": created, "id": 1,
    }


# A small two-domain directory reused by several tests.
_DIRECTORY = [
    _ks_domain("d1", "acme-domain"),
    _ks_domain("d2", "other-domain"),
    _ks_project("p1", "web-prod", "d1"),
    _ks_project("p2", "db-staging", "d1"),
    _ks_project("p3", "lab", "d2"),
]


def test_report_is_registered():
    from openstack_bi.reports import by_id
    assert by_id("locate_instance").id == "locate_instance"


def test_report_exposes_domain_and_project_params():
    names = {p.name for p in locate_instance.REPORT.params}
    assert {"name", "uuid", "host", "domain", "project", "regions"} <= names


def test_no_search_term_does_not_query(monkeypatch):
    backend = FakeBackend(instance_rows={})
    _install(monkeypatch, backend)
    result = locate_instance.REPORT.run()
    assert result.rows == []
    assert "search" in result.metadata
    # Crucially, we never touched the database.
    assert backend.queries == []


def test_bare_name_wraps_as_substring(monkeypatch):
    backend = FakeBackend(
        instance_rows={
            "DTW": [_row("u-1", "prod-web-01"), _row("u-2", "staging-db")],
            "CVG": [_row("u-3", "prod-cache")],
        },
        keystone_rows=_DIRECTORY,
    )
    _install(monkeypatch, backend)
    result = locate_instance.REPORT.run(name="prod")
    names = {r["display_name"] for r in result.rows}
    assert names == {"prod-web-01", "prod-cache"}
    # The bare word was auto-wrapped before hitting the DB; the name clause
    # binds the pattern twice (display_name OR hostname).
    inst_q = next(q for q in backend.queries if "FROM instances" in q[2])
    assert inst_q[3] == ("%prod%", "%prod%")
    assert result.metadata["name_search"] == "%prod%"


def test_name_filter_also_matches_hostname(monkeypatch):
    # display_name and hostname differ; a name search hitting only the
    # hostname must still locate the instance.
    backend = FakeBackend(
        instance_rows={
            "DTW": [_row("u-1", "friendly-name", hostname="web-prod-01"),
                    _row("u-2", "other", hostname="db-staging-02")],
            "CVG": [],
        },
    )
    _install(monkeypatch, backend)
    result = locate_instance.REPORT.run(name="web-prod")
    assert {r["uuid"] for r in result.rows} == {"u-1"}
    inst_q = next(q for q in backend.queries if "FROM instances" in q[2])
    assert "n.display_name LIKE %s OR n.hostname LIKE %s" in inst_q[2]


def test_explicit_wildcard_passed_through(monkeypatch):
    backend = FakeBackend(
        instance_rows={"DTW": [_row("u-1", "prod-web-01"), _row("u-2", "prod-db")],
                       "CVG": []},
    )
    _install(monkeypatch, backend)
    result = locate_instance.REPORT.run(name="prod-w%")
    assert {r["display_name"] for r in result.rows} == {"prod-web-01"}
    inst_q = next(q for q in backend.queries if "FROM instances" in q[2])
    assert inst_q[3] == ("prod-w%", "prod-w%")


def test_uuid_exact_match(monkeypatch):
    backend = FakeBackend(
        instance_rows={"DTW": [_row("aaaa-1111", "vm-a"), _row("bbbb-2222", "vm-b")],
                       "CVG": []},
        keystone_rows=_DIRECTORY,
    )
    _install(monkeypatch, backend)
    result = locate_instance.REPORT.run(uuid="aaaa-1111")
    assert len(result.rows) == 1
    assert result.rows[0]["uuid"] == "aaaa-1111"
    inst_q = next(q for q in backend.queries if "FROM instances" in q[2])
    assert "n.uuid = %s" in inst_q[2]
    assert "n.uuid LIKE" not in inst_q[2]


def test_uuid_with_wildcard_uses_like(monkeypatch):
    backend = FakeBackend(
        instance_rows={"DTW": [_row("aaaa-1111", "vm-a"), _row("aaaa-9999", "vm-c"),
                               _row("bbbb-2222", "vm-b")],
                       "CVG": []},
    )
    _install(monkeypatch, backend)
    result = locate_instance.REPORT.run(uuid="aaaa-%")
    assert {r["uuid"] for r in result.rows} == {"aaaa-1111", "aaaa-9999"}
    inst_q = next(q for q in backend.queries if "FROM instances" in q[2])
    assert "n.uuid LIKE %s" in inst_q[2]


def test_host_filter_scopes_to_compute_host(monkeypatch):
    backend = FakeBackend(
        instance_rows={
            "DTW": [_row("u-1", "vm-a", host="dtw-cmp-07"),
                    _row("u-2", "vm-b", host="dtw-cmp-12")],
            "CVG": [_row("u-3", "vm-c", host="cvg-cmp-07")],
        },
    )
    _install(monkeypatch, backend)
    result = locate_instance.REPORT.run(host="dtw-cmp-07")
    assert {r["uuid"] for r in result.rows} == {"u-1"}
    inst_q = next(q for q in backend.queries if "FROM instances" in q[2])
    assert "n.host LIKE %s" in inst_q[2]
    assert "%dtw-cmp-07%" in inst_q[3]
    assert result.metadata["host_search"] == "%dtw-cmp-07%"


def test_host_filter_is_a_sufficient_search_term(monkeypatch):
    # Host alone (no name / uuid) is a valid locate query.
    backend = FakeBackend(
        instance_rows={"DTW": [_row("u-1", "vm-a", host="dtw-cmp-01")], "CVG": []},
    )
    _install(monkeypatch, backend)
    result = locate_instance.REPORT.run(host="cmp-01")
    assert "search" not in result.metadata  # did not short-circuit
    assert {r["uuid"] for r in result.rows} == {"u-1"}


def test_host_filter_combines_with_name(monkeypatch):
    backend = FakeBackend(
        instance_rows={
            "DTW": [_row("u-1", "prod-web", host="dtw-cmp-07"),
                    _row("u-2", "prod-db", host="dtw-cmp-07"),
                    _row("u-3", "prod-web", host="dtw-cmp-12")],
            "CVG": [],
        },
    )
    _install(monkeypatch, backend)
    result = locate_instance.REPORT.run(name="prod-web", host="cmp-07")
    assert {r["uuid"] for r in result.rows} == {"u-1"}


def test_region_selection_limits_fan_out(monkeypatch):
    backend = FakeBackend(
        instance_rows={"DTW": [_row("u-1", "prod-a")], "CVG": [_row("u-2", "prod-b")]},
    )
    _install(monkeypatch, backend)
    result = locate_instance.REPORT.run(name="prod", regions=["CVG"])
    assert {r["region"] for r in result.rows} == {"CVG"}
    queried_regions = {q[0] for q in backend.queries if "FROM instances" in q[2]}
    assert queried_regions == {"CVG"}


def test_rows_carry_console_and_migrate_actions(monkeypatch):
    backend = FakeBackend(
        instance_rows={"DTW": [_row("aaaa-1111", "prod-web")], "CVG": []},
        keystone_rows=_DIRECTORY,
    )
    _install(monkeypatch, backend)
    result = locate_instance.REPORT.run(name="prod")
    row = result.rows[0]
    assert row["_console_url"] == "/instance/DTW/aaaa-1111/console"
    assert row["_migrate_url"] == "/instance/DTW/aaaa-1111/migrate"
    assert row["uuid_link"].startswith("/report/instance_history?")
    # Project + domain resolved from the keystone stub.
    assert row["project_name"] == "web-prod"
    assert row["domain_name"] == "acme-domain"


def test_non_active_instances_are_included(monkeypatch):
    # Unlike the SPLA report, locate finds instances in any state.
    backend = FakeBackend(
        instance_rows={"DTW": [_row("u-1", "prod-err", state="error"),
                               _row("u-2", "prod-off", state="stopped")],
                       "CVG": []},
    )
    _install(monkeypatch, backend)
    result = locate_instance.REPORT.run(name="prod")
    assert {r["vm_state"] for r in result.rows} == {"error", "stopped"}


def test_per_region_rollups_in_metadata(monkeypatch):
    backend = FakeBackend(
        instance_rows={
            "DTW": [_row("u-1", "prod-a", vcpus=4, mem=4096),
                    _row("u-2", "prod-b", vcpus=2, mem=2048)],
            "CVG": [_row("u-3", "prod-c", vcpus=8, mem=8192)],
        },
    )
    _install(monkeypatch, backend)
    result = locate_instance.REPORT.run(name="prod")
    assert result.metadata["total_instances"] == 3
    assert result.metadata["total_vcpus"] == 14
    assert "region_DTW" in result.metadata
    assert "region_CVG" in result.metadata
    assert result.metadata["region_errors"] == "(none)"


# --- domain / project filters ------------------------------------------------

def test_domain_filter_scopes_to_domain_projects(monkeypatch):
    # acme-domain owns p1 + p2; other-domain owns p3.
    backend = FakeBackend(
        instance_rows={
            "DTW": [_row("u-1", "vm-a", project_id="p1"),
                    _row("u-2", "vm-b", project_id="p2"),
                    _row("u-3", "vm-c", project_id="p3")],
            "CVG": [],
        },
        keystone_rows=_DIRECTORY,
    )
    _install(monkeypatch, backend)
    result = locate_instance.REPORT.run(domain="acme")
    assert {r["uuid"] for r in result.rows} == {"u-1", "u-2"}
    inst_q = next(q for q in backend.queries if "FROM instances" in q[2])
    assert "n.project_id IN" in inst_q[2]
    # Two projects resolved under the matched domain.
    assert result.metadata["matched_projects"] == 2
    assert result.metadata["domain_search"] == "%acme%"


def test_project_filter_scopes_to_named_project(monkeypatch):
    backend = FakeBackend(
        instance_rows={
            "DTW": [_row("u-1", "vm-a", project_id="p1"),  # web-prod
                    _row("u-2", "vm-b", project_id="p2")],  # db-staging
            "CVG": [],
        },
        keystone_rows=_DIRECTORY,
    )
    _install(monkeypatch, backend)
    result = locate_instance.REPORT.run(project="staging")
    assert {r["uuid"] for r in result.rows} == {"u-2"}
    assert result.metadata["matched_projects"] == 1
    assert result.metadata["project_search"] == "%staging%"


def test_domain_and_project_intersect(monkeypatch):
    # domain acme -> {p1, p2}; project "db" -> {p2}; intersection -> {p2}.
    backend = FakeBackend(
        instance_rows={
            "DTW": [_row("u-1", "vm-a", project_id="p1"),
                    _row("u-2", "vm-b", project_id="p2")],
            "CVG": [],
        },
        keystone_rows=_DIRECTORY,
    )
    _install(monkeypatch, backend)
    result = locate_instance.REPORT.run(domain="acme", project="db")
    assert {r["uuid"] for r in result.rows} == {"u-2"}
    assert result.metadata["matched_projects"] == 1


def test_domain_filter_is_a_sufficient_search_term(monkeypatch):
    # Domain alone (no name / uuid) is a valid locate query.
    backend = FakeBackend(
        instance_rows={"DTW": [_row("u-1", "vm-a", project_id="p1")], "CVG": []},
        keystone_rows=_DIRECTORY,
    )
    _install(monkeypatch, backend)
    result = locate_instance.REPORT.run(domain="acme")
    assert "search" not in result.metadata  # did not short-circuit
    assert {r["uuid"] for r in result.rows} == {"u-1"}


def test_unmatched_domain_skips_instance_scan(monkeypatch):
    backend = FakeBackend(
        instance_rows={"DTW": [_row("u-1", "vm-a", project_id="p1")], "CVG": []},
        keystone_rows=_DIRECTORY,
    )
    _install(monkeypatch, backend)
    result = locate_instance.REPORT.run(domain="does-not-exist")
    assert result.rows == []
    assert result.metadata["matched_projects"] == 0
    # No domain matched -> we must not have scanned any instances.
    assert not any("FROM instances" in q[2] for q in backend.queries)
