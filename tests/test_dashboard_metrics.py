"""Dashboard metric collector — DB shape, scope dispatch, idempotency."""

from __future__ import annotations

import re

import pytest

from openstack_bi import dashboard_metrics as dm


# --- Static checks on the metric definition set ----------------------------

def test_metric_defs_unique_snake_case_names():
    names = [m.name for m in dm.METRIC_DEFS]
    assert len(names) == len(set(names)), "duplicate metric name"
    for n in names:
        assert re.match(r"^[a-z][a-z0-9_]*$", n), f"non-snake_case: {n!r}"


def test_every_metric_scope_is_known():
    valid = {dm.SCOPE_CELLS, dm.SCOPE_NEUTRON, dm.SCOPE_CINDER, dm.SCOPE_KEYSTONE}
    for m in dm.METRIC_DEFS:
        assert m.scope in valid, f"{m.name} has unknown scope {m.scope!r}"


def test_keystone_metrics_marked_non_combinable():
    # Keystone is global; summing across regions would triple-count.
    for m in dm.METRIC_DEFS:
        if m.scope == dm.SCOPE_KEYSTONE:
            assert m.combinable is False


# --- collect_snapshot integration with mocked DB layer ----------------------

class _Region:
    def __init__(self, name):
        self.name = name


@pytest.fixture
def patched_collector(monkeypatch, tmp_config_db):
    """Monkeypatch the DB-touching boundary so collect_snapshot runs
    without a live MariaDB or Keystone DB. Returns a dict the caller
    can inspect for assertions."""
    from openstack_bi import config_db
    config_db.upsert_region(
        name="dtw", host="h", port=3306, db_user="u", db_password="",
        is_keystone_region=True,
    )
    config_db.upsert_region(
        name="cvg", host="h", port=3306, db_user="u", db_password="",
    )

    queries = []

    def _fake_query(region, database, sql, args=()):
        queries.append({
            "region": region.name, "database": database,
            "sql": sql, "args": tuple(args),
        })
        # Reflect the metric back as a count derived from the SQL text
        # so different metrics return distinguishable values without
        # the test having to enumerate them.
        return [{"n": (hash(sql) & 0xFF) + (1 if region.name == "dtw" else 2)}]

    monkeypatch.setattr(dm, "query", _fake_query)
    monkeypatch.setattr(dm.openstack, "list_cells",
                        lambda r: [f"{r.name}_cell0"])
    monkeypatch.setattr(dm, "parse_regions",
                        lambda: [_Region("dtw"), _Region("cvg")])
    monkeypatch.setattr(dm, "keystone_region",
                        lambda *a, **k: _Region("dtw"))
    return queries


def test_collect_snapshot_emits_per_region_and_combined_rows(patched_collector):
    rows = dm.collect_snapshot(snapshot_date="2026-05-27")
    regions = {r["region"] for r in rows}
    assert regions == {"dtw", "cvg", dm.COMBINED}

    # Every metric appears in _combined exactly once.
    combined_metrics = [r["metric"] for r in rows if r["region"] == dm.COMBINED]
    assert sorted(combined_metrics) == sorted(m.name for m in dm.METRIC_DEFS)


def test_collect_snapshot_keystone_metrics_have_no_per_region_rows(patched_collector):
    rows = dm.collect_snapshot(snapshot_date="2026-05-27")
    keystone_names = {
        m.name for m in dm.METRIC_DEFS if m.scope == dm.SCOPE_KEYSTONE
    }
    for r in rows:
        if r["metric"] in keystone_names:
            assert r["region"] == dm.COMBINED, (
                "keystone metrics must only appear under _combined"
            )


def test_collect_snapshot_combined_value_sums_combinable_metrics(patched_collector):
    rows = dm.collect_snapshot(snapshot_date="2026-05-27")
    by_key = {(r["region"], r["metric"]): r["value"] for r in rows}
    for m in dm.METRIC_DEFS:
        if not m.combinable:
            continue
        expected = by_key[("dtw", m.name)] + by_key[("cvg", m.name)]
        assert by_key[(dm.COMBINED, m.name)] == expected, (
            f"_combined for {m.name} did not equal dtw + cvg"
        )


def test_collect_snapshot_autobackup_metric_binds_date(patched_collector):
    dm.collect_snapshot(snapshot_date="2026-05-27")
    auto = [
        q for q in patched_collector
        if "snapshots" in q["sql"] and "autobackup" in q["sql"]
    ]
    assert auto, "autobackup query never ran"
    for q in auto:
        assert q["args"] == ("%2026-05-27%",)


# --- write/read round trip --------------------------------------------------

def test_history_round_trip_is_idempotent(patched_collector):
    # First run writes today's snapshot.
    rows = dm.collect_snapshot(snapshot_date="2026-05-27")
    dm.write_snapshot(rows)

    history_dtw = dm.history("instances_total", "dtw", days=30)
    assert len(history_dtw) == 1
    assert history_dtw[0][0] == "2026-05-27"

    # Re-running the same date overwrites — does not duplicate.
    dm.write_snapshot(rows)
    history_dtw = dm.history("instances_total", "dtw", days=30)
    assert len(history_dtw) == 1


def test_today_autobackups_csv_empty_when_no_rows(monkeypatch, tmp_config_db):
    monkeypatch.setattr(dm, "query", lambda *a, **k: [])
    monkeypatch.setattr(dm, "cinder_db", lambda: "cinder")
    assert dm.today_autobackups_csv(_Region("dtw"), "2026-05-27") == ""


def test_today_autobackups_csv_writes_header_and_rows(monkeypatch, tmp_config_db):
    monkeypatch.setattr(dm, "query", lambda *a, **k: [
        {"id": "s1", "display_description": "autobackup-acme",
         "created_at": "2026-05-27 03:01:00"},
        {"id": "s2", "display_description": "autobackup-other",
         "created_at": "2026-05-27 03:02:00"},
    ])
    monkeypatch.setattr(dm, "cinder_db", lambda: "cinder")
    out = dm.today_autobackups_csv(_Region("dtw"), "2026-05-27")
    lines = out.strip().splitlines()
    assert lines[0] == "id,display_description,created_at"
    assert "s1" in lines[1]
    assert "s2" in lines[2]
