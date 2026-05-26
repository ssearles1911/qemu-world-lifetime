"""Neutron L3-agent module — API request shaping + DB row normalization."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from openstack_bi import neutron
from openstack_bi.config import Region

_REGION = Region(name="dtw", host="h", port=3306, user="u", password="p")


def _resp(status_code, body=None):
    r = MagicMock()
    r.status_code = status_code
    r.json.return_value = body
    return r


# --- API actions ------------------------------------------------------------

def test_add_router_to_agent_posts_expected_body():
    sess = MagicMock()
    sess.request.return_value = _resp(201, {"router": {}})

    neutron.add_router_to_agent(sess, "dtw", "agent-1", "router-9")

    path, method = sess.request.call_args[0]
    kwargs = sess.request.call_args[1]
    assert path == "/v2.0/agents/agent-1/l3-routers"
    assert method == "POST"
    assert kwargs["json"] == {"router_id": "router-9"}
    assert kwargs["endpoint_filter"]["service_type"] == "network"
    assert kwargs["endpoint_filter"]["region_name"] == "dtw"


def test_remove_router_from_agent_issues_delete():
    sess = MagicMock()
    sess.request.return_value = _resp(204)

    neutron.remove_router_from_agent(sess, "dtw", "agent-1", "router-9")

    path, method = sess.request.call_args[0]
    assert path == "/v2.0/agents/agent-1/l3-routers/router-9"
    assert method == "DELETE"


def test_move_router_removes_from_source_then_adds_to_target():
    sess = MagicMock()
    sess.request.return_value = _resp(204)

    neutron.move_router(sess, "dtw", "router-9", "src-agent", "dst-agent")

    seq = [(c.args[1], c.args[0]) for c in sess.request.call_args_list]
    assert seq == [
        ("DELETE", "/v2.0/agents/src-agent/l3-routers/router-9"),
        ("POST", "/v2.0/agents/dst-agent/l3-routers"),
    ]


def test_move_router_reports_unscheduled_when_add_fails():
    sess = MagicMock()
    # DELETE from the source succeeds; the POST to the target 409s.
    sess.request.side_effect = [
        _resp(204),
        _resp(409, {"NeutronError": {"message": "agent unavailable"}}),
    ]

    with pytest.raises(neutron.NeutronError) as excinfo:
        neutron.move_router(sess, "dtw", "router-9", "src", "dst")

    msg = str(excinfo.value)
    assert "unscheduled" in msg
    assert "agent unavailable" in msg


def test_neutron_error_surfaces_api_message():
    sess = MagicMock()
    sess.request.return_value = _resp(400, {"NeutronError": {"message": "bad router"}})

    with pytest.raises(neutron.NeutronError) as excinfo:
        neutron.add_router_to_agent(sess, "dtw", "a", "r")
    assert "bad router" in str(excinfo.value)


def test_neutron_error_on_missing_endpoint():
    from keystoneauth1 import exceptions as ksa_exc

    sess = MagicMock()
    sess.request.side_effect = ksa_exc.EndpointNotFound()

    with pytest.raises(neutron.NeutronError):
        neutron.remove_router_from_agent(sess, "nowhere", "a", "r")


# --- DB queries -------------------------------------------------------------

def test_list_l3_agents_marks_alive(monkeypatch):
    monkeypatch.setattr(neutron, "neutron_db", lambda: "neutron")
    rows = [
        {"id": "a1", "host": "net-1", "admin_state_up": 1,
         "availability_zone": "nova", "heartbeat_age": 12, "router_count": 5},
        {"id": "a2", "host": "net-2", "admin_state_up": 0,
         "availability_zone": "nova", "heartbeat_age": 600, "router_count": 0},
        {"id": "a3", "host": "net-3", "admin_state_up": 1,
         "availability_zone": None, "heartbeat_age": None, "router_count": 1},
    ]
    monkeypatch.setattr(neutron, "query", lambda *a, **k: rows)

    agents = {a["id"]: a for a in neutron.list_l3_agents(_REGION)}
    assert agents["a1"]["alive"] is True          # recent heartbeat
    assert agents["a2"]["alive"] is False         # stale heartbeat
    assert agents["a3"]["alive"] is False         # never reported
    assert agents["a2"]["admin_state_up"] is False
    assert agents["a1"]["router_count"] == 5


def test_routers_on_l3_agent_normalizes_flags(monkeypatch):
    monkeypatch.setattr(neutron, "neutron_db", lambda: "neutron")
    rows = [
        {"id": "r1", "name": "ha-rtr", "status": "ACTIVE", "admin_state_up": 1,
         "project_id": "p1", "ha": 1, "distributed": 0,
         "gateway_ips": "203.0.113.5"},
        {"id": "r2", "name": None, "status": "ACTIVE", "admin_state_up": 0,
         "project_id": "p2", "ha": 0, "distributed": 1, "gateway_ips": None},
    ]
    monkeypatch.setattr(neutron, "query", lambda *a, **k: rows)

    routers = {r["id"]: r for r in neutron.routers_on_l3_agent(_REGION, "agent-x")}
    assert routers["r1"]["ha"] is True
    assert routers["r1"]["distributed"] is False
    assert routers["r2"]["distributed"] is True
    assert routers["r2"]["name"] == "(unnamed)"
    assert routers["r2"]["admin_state_up"] is False
    assert routers["r1"]["gateway_ip"] == "203.0.113.5"   # gateway port IP
    assert routers["r2"]["gateway_ip"] == ""              # no gateway port


def test_router_wan_ips_splits_and_handles_missing(monkeypatch):
    monkeypatch.setattr(neutron, "neutron_db", lambda: "neutron")
    monkeypatch.setattr(neutron, "query", lambda *a, **k: [
        {"id": "r1", "name": "edge", "gateway_ips": "203.0.113.5, 203.0.113.6"},
        {"id": "r2", "name": None, "gateway_ips": None},
    ])
    out = neutron.router_wan_ips(_REGION, ["r1", "r2"])
    assert out["r1"]["wan_ips"] == ["203.0.113.5", "203.0.113.6"]
    assert out["r2"]["wan_ips"] == []          # no gateway port
    assert out["r2"]["name"] == "(unnamed)"


def test_router_wan_ips_empty_input_skips_query(monkeypatch):
    called = []
    monkeypatch.setattr(neutron, "neutron_db", lambda: "neutron")
    monkeypatch.setattr(neutron, "query", lambda *a, **k: called.append(a) or [])
    assert neutron.router_wan_ips(_REGION, []) == {}
    assert called == []


# --- VLAN networks ----------------------------------------------------------

def test_create_vlan_network_posts_provider_attributes():
    sess = MagicMock()
    sess.request.return_value = _resp(201, {"network": {"id": "net-1", "name": "acme-vlan"}})

    out = neutron.create_vlan_network(
        sess, "dtw", "acme-vlan", "proj-9", "vlan", 815
    )

    path, method = sess.request.call_args[0]
    body = sess.request.call_args[1]["json"]["network"]
    assert path == "/v2.0/networks"
    assert method == "POST"
    assert body["provider:network_type"] == "vlan"
    assert body["provider:physical_network"] == "vlan"
    assert body["provider:segmentation_id"] == 815
    assert body["project_id"] == "proj-9"
    assert body["name"] == "acme-vlan"
    assert "subnet" not in str(body)  # the tool never creates a subnet
    assert out["id"] == "net-1"


def test_list_vlan_physnets(monkeypatch):
    monkeypatch.setattr(neutron, "neutron_db", lambda: "neutron")
    monkeypatch.setattr(
        neutron, "query",
        lambda *a, **k: [{"physical_network": "vlan"}, {"physical_network": "ext"}],
    )
    assert neutron.list_vlan_physnets(_REGION) == ["vlan", "ext"]


def test_vlan_segment_conflict_found(monkeypatch):
    monkeypatch.setattr(neutron, "neutron_db", lambda: "neutron")
    monkeypatch.setattr(
        neutron, "query",
        lambda *a, **k: [{"id": "net-7", "name": "other-net"}],
    )
    hit = neutron.vlan_segment_conflict(_REGION, "vlan", 815)
    assert hit == {"id": "net-7", "name": "other-net"}


def test_vlan_segment_conflict_free(monkeypatch):
    monkeypatch.setattr(neutron, "neutron_db", lambda: "neutron")
    monkeypatch.setattr(neutron, "query", lambda *a, **k: [])
    assert neutron.vlan_segment_conflict(_REGION, "vlan", 815) is None


def test_list_vlan_networks_normalizes(monkeypatch):
    monkeypatch.setattr(neutron, "neutron_db", lambda: "neutron")
    monkeypatch.setattr(neutron, "query", lambda *a, **k: [
        {"id": "n1", "name": None, "status": "ACTIVE", "admin_state_up": 1,
         "project_id": "p1", "physical_network": "vlan", "segmentation_id": 100},
        {"id": "n2", "name": "acme", "status": "DOWN", "admin_state_up": 0,
         "project_id": "p2", "physical_network": "vlan", "segmentation_id": 815},
    ])
    nets = {n["id"]: n for n in neutron.list_vlan_networks(_REGION)}
    assert nets["n1"]["name"] == "(unnamed)"
    assert nets["n1"]["admin_state_up"] is True
    assert nets["n1"]["segmentation_id"] == 100
    assert nets["n2"]["admin_state_up"] is False
    assert nets["n2"]["physical_network"] == "vlan"


def test_vlan_networks_for_project_normalizes(monkeypatch):
    monkeypatch.setattr(neutron, "neutron_db", lambda: "neutron")
    monkeypatch.setattr(neutron, "query", lambda *a, **k: [
        {"id": "n1", "name": None, "status": "ACTIVE", "admin_state_up": 1,
         "physical_network": "vlan", "segmentation_id": 100},
    ])
    nets = neutron.vlan_networks_for_project(_REGION, "proj-9")
    assert nets[0]["name"] == "(unnamed)"
    assert nets[0]["admin_state_up"] is True
    assert nets[0]["segmentation_id"] == 100


# --- Ports in BUILD ---------------------------------------------------------

def test_list_build_ports_normalizes(monkeypatch):
    monkeypatch.setattr(neutron, "neutron_db", lambda: "neutron")
    monkeypatch.setattr(neutron, "query", lambda *a, **k: [
        {"id": "p1", "name": None, "network_id": "n1",
         "mac_address": "fa:16:3e:00:00:01", "admin_state_up": 1,
         "status": "BUILD", "device_owner": "compute:nova",
         "device_id": "i1", "project_id": "pr1",
         "network_name": "private", "created_at": "2026-05-26 12:00:00"},
        {"id": "p2", "name": "named", "network_id": None, "mac_address": None,
         "admin_state_up": 0, "status": "BUILD",
         "device_owner": "", "device_id": "", "project_id": "pr2",
         "network_name": None, "created_at": None},
    ])
    ports = {p["id"]: p for p in neutron.list_build_ports(_REGION)}
    assert ports["p1"]["status"] == "BUILD"
    assert ports["p1"]["network_name"] == "private"
    assert ports["p1"]["admin_state_up"] is True
    assert ports["p1"]["device_owner"] == "compute:nova"
    assert ports["p1"]["name"] == ""            # null name preserved as empty
    assert ports["p1"]["mac_address"] == "fa:16:3e:00:00:01"
    assert ports["p2"]["name"] == "named"       # non-null name preserved
    assert ports["p2"]["admin_state_up"] is False
    assert ports["p2"]["network_id"] == ""      # null network_id preserved


# --- Network agents ---------------------------------------------------------

def test_list_networks_normalizes(monkeypatch):
    monkeypatch.setattr(neutron, "neutron_db", lambda: "neutron")
    monkeypatch.setattr(neutron, "query", lambda *a, **k: [
        {"id": "n1", "name": "acme", "status": "ACTIVE", "admin_state_up": 1,
         "project_id": "p1", "network_types": "vlan",
         "segment_ids": "815"},
        {"id": "n2", "name": None, "status": "DOWN", "admin_state_up": 0,
         "project_id": "p2", "network_types": None, "segment_ids": None},
    ])
    nets = {n["id"]: n for n in neutron.list_networks(_REGION)}
    assert nets["n1"]["network_types"] == "vlan"
    assert nets["n1"]["segment_ids"] == "815"
    assert nets["n2"]["name"] == ""
    assert nets["n2"]["admin_state_up"] is False
    assert nets["n2"]["network_types"] == ""


def test_dhcp_agents_by_network_groups_and_marks_alive(monkeypatch):
    monkeypatch.setattr(neutron, "neutron_db", lambda: "neutron")
    monkeypatch.setattr(neutron, "query", lambda *a, **k: [
        {"network_id": "n1", "id": "a1", "host": "dhcp01",
         "admin_state_up": 1, "heartbeat_age": 10},   # recent -> alive
        {"network_id": "n1", "id": "a2", "host": "dhcp02",
         "admin_state_up": 0, "heartbeat_age": 600},  # stale -> down
        {"network_id": "n2", "id": "a3", "host": "dhcp03",
         "admin_state_up": 1, "heartbeat_age": None}, # never -> down
    ])
    grouped = neutron.dhcp_agents_by_network(_REGION)
    assert sorted(grouped.keys()) == ["n1", "n2"]
    by_host = {a["host"]: a for a in grouped["n1"]}
    assert by_host["dhcp01"]["alive"] is True
    assert by_host["dhcp02"]["alive"] is False
    assert by_host["dhcp02"]["admin_state_up"] is False
    assert grouped["n2"][0]["alive"] is False        # heartbeat_age None


def test_l3_agents_by_network_groups_per_network(monkeypatch):
    monkeypatch.setattr(neutron, "neutron_db", lambda: "neutron")
    monkeypatch.setattr(neutron, "query", lambda *a, **k: [
        {"network_id": "n1", "agent_id": "a1", "agent_host": "nrtr01",
         "agent_admin_state_up": 1, "heartbeat_age": 8,
         "router_id": "r1", "router_name": "edge-1",
         "interface_role": "network:router_interface"},
        {"network_id": "n1", "agent_id": "a2", "agent_host": "nrtr02",
         "agent_admin_state_up": 1, "heartbeat_age": 200,
         "router_id": "r1", "router_name": "edge-1",
         "interface_role": "network:router_interface"},
        {"network_id": "n2", "agent_id": "a3", "agent_host": "nrtr03",
         "agent_admin_state_up": 1, "heartbeat_age": 4,
         "router_id": "r2", "router_name": "edge-2",
         "interface_role": "network:router_gateway"},
    ])
    grouped = neutron.l3_agents_by_network(_REGION)
    # HA router on net n1 appears once per agent it is bound to.
    assert len(grouped["n1"]) == 2
    assert {p["agent_host"] for p in grouped["n1"]} == {"nrtr01", "nrtr02"}
    assert grouped["n2"][0]["interface_role"] == "network:router_gateway"
    assert grouped["n2"][0]["alive"] is True
