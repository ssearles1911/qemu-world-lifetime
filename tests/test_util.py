"""util.rebalance_recommendations — the L3/DHCP load-balance suggester."""

from __future__ import annotations

from openstack_bi.util import rebalance_recommendations


def _agent(host, count, alive=True, admin=True):
    return {
        "id": host, "host": host,
        "alive": alive, "admin_state_up": admin,
        "count": count,
    }


def test_balanced_cluster_returns_no_recommendations():
    # All four within the threshold band — nothing interesting to report.
    agents = [
        _agent("a", 100), _agent("b", 105),
        _agent("c", 95),  _agent("d", 100),
    ]
    assert rebalance_recommendations(agents, "count") == []


def test_drains_lopsided_source_into_underloaded_targets():
    agents = [
        _agent("hot", 600), _agent("cold", 50),
        _agent("warm", 100), _agent("ok", 250),
    ]
    recs = rebalance_recommendations(agents, "count")
    assert len(recs) == 1
    rec = recs[0]
    assert rec["source_host"] == "hot"
    assert rec["current"] == 600
    # mean = (600 + 50 + 100 + 250) / 4 = 250; excess = 350.
    assert rec["mean"] == 250
    assert rec["excess"] == 350
    hosts = [t["host"] for t in rec["targets"]]
    assert "cold" in hosts and "warm" in hosts


def test_dead_and_disabled_agents_excluded_from_mean_and_targets():
    agents = [
        _agent("hot", 600),
        _agent("dead", 0, alive=False),          # not a target, not in mean
        _agent("disabled", 0, admin=False),      # not a target, not in mean
        _agent("ok", 200),
        _agent("under", 50),
    ]
    recs = rebalance_recommendations(agents, "count")
    assert recs
    rec = recs[0]
    host_set = {t["host"] for t in rec["targets"]}
    assert "dead" not in host_set
    assert "disabled" not in host_set
    # Mean is computed over the three eligible agents only.
    assert rec["mean"] == (600 + 200 + 50) // 3


def test_cooperative_fill_across_multiple_sources():
    # Two overloaded sources should not double-count one target's deficit.
    agents = [
        _agent("hot1", 500), _agent("hot2", 500),
        _agent("cold", 0),  _agent("med", 250),
    ]
    recs = rebalance_recommendations(agents, "count")
    # mean = 1250 / 4 = 312, deficit on cold = 312, on med = 62.
    total_assigned = sum(t["count"] for r in recs for t in r["targets"])
    # Should not exceed the combined deficit of all underloaded agents.
    assert total_assigned <= 312 + 62


def test_returns_empty_when_only_one_eligible_agent():
    agents = [_agent("a", 100), _agent("b", 0, alive=False)]
    assert rebalance_recommendations(agents, "count") == []


def test_small_jitter_below_threshold_ignored():
    # 5-unit threshold floor catches the trivial case where all counts
    # are tiny and 20% is sub-integer.
    agents = [_agent("a", 10), _agent("b", 11), _agent("c", 9)]
    assert rebalance_recommendations(agents, "count") == []
