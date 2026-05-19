"""Admin Tools — L3 router management routes."""

from __future__ import annotations

from unittest.mock import MagicMock

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
    local_auth.create_admin("admin", "password123")
    app = create_app()
    app.config["WTF_CSRF_ENABLED"] = False
    return app.test_client()


def _login_keystone(client, with_token=True):
    """Install a Keystone session; `with_token` adds a token-store key."""
    from openstack_bi.auth.session import SESSION_KEY

    with client.session_transaction() as sess:
        sess[SESSION_KEY] = {
            "kind": "keystone", "user_id": "u1", "username": "bob",
            "is_admin": True, "project_ids": ["p1"], "domain_id": "d1",
            "roles": ["admin"],
        }
        if with_token:
            sess["ks_token_key"] = "fake-key"


def test_routers_page_requires_login(client):
    r = client.get("/tools/routers")
    assert r.status_code == 302
    assert "/login" in r.headers["Location"]


def test_routers_page_lists_agents(client, monkeypatch):
    # A session without a token can still browse the DB-backed listing.
    _login_keystone(client, with_token=False)
    from openstack_bi import neutron

    monkeypatch.setattr(neutron, "list_l3_agents", lambda region: [
        {"id": "a1", "host": "net-1", "admin_state_up": True,
         "availability_zone": "nova", "heartbeat_age": 5, "alive": True,
         "router_count": 3},
    ])
    r = client.get("/tools/routers")
    assert r.status_code == 200
    assert b"net-1" in r.data


def test_move_requires_keystone_token(client, monkeypatch):
    # Session has no token-store key -> the move is refused before any
    # API call is made.
    _login_keystone(client, with_token=False)
    from openstack_bi import neutron

    called = []
    monkeypatch.setattr(neutron, "move_router", lambda *a, **k: called.append(a))

    r = client.post("/tools/routers/move", data={
        "region": "dtw", "source_agent": "a1", "target_agent": "a2",
        "router_ids": ["r1"],
    })
    assert r.status_code == 302
    assert called == []


def test_move_rejects_same_source_and_target(client, monkeypatch):
    _login_keystone(client)
    from openstack_bi import neutron
    from openstack_bi.auth import token_store

    monkeypatch.setattr(token_store, "session_for", lambda key: MagicMock())
    called = []
    monkeypatch.setattr(neutron, "move_router", lambda *a, **k: called.append(a))

    r = client.post("/tools/routers/move", data={
        "region": "dtw", "source_agent": "a1", "target_agent": "a1",
        "router_ids": ["r1"],
    })
    assert r.status_code == 302
    assert called == []


def test_move_calls_neutron_for_each_router(client, monkeypatch):
    _login_keystone(client)
    from openstack_bi import neutron
    from openstack_bi.auth import token_store

    monkeypatch.setattr(token_store, "session_for", lambda key: MagicMock())
    calls = []
    monkeypatch.setattr(
        neutron, "move_router",
        lambda sess, region, rid, src, dst: calls.append((region, rid, src, dst)),
    )

    r = client.post("/tools/routers/move", data={
        "region": "dtw", "source_agent": "src", "target_agent": "dst",
        "router_ids": ["r1", "r2"],
    })
    assert r.status_code == 302
    assert calls == [
        ("dtw", "r1", "src", "dst"),
        ("dtw", "r2", "src", "dst"),
    ]


def test_move_unknown_region_is_rejected(client, monkeypatch):
    _login_keystone(client)
    from openstack_bi import neutron
    from openstack_bi.auth import token_store

    monkeypatch.setattr(token_store, "session_for", lambda key: MagicMock())
    called = []
    monkeypatch.setattr(neutron, "move_router", lambda *a, **k: called.append(a))

    r = client.post("/tools/routers/move", data={
        "region": "bogus", "source_agent": "a1", "target_agent": "a2",
        "router_ids": ["r1"],
    })
    assert r.status_code == 302
    assert called == []


# --- VLAN tool --------------------------------------------------------------

def test_vlans_page_requires_login(client):
    r = client.get("/tools/vlans")
    assert r.status_code == 302
    assert "/login" in r.headers["Location"]


def test_vlans_page_renders(client, monkeypatch):
    _login_keystone(client, with_token=False)
    from openstack_bi import neutron
    monkeypatch.setattr(neutron, "list_vlan_physnets", lambda region: ["vlan"])
    r = client.get("/tools/vlans")
    assert r.status_code == 200
    assert b"VLAN" in r.data
    assert b"project-search" in r.data  # searchable project picker


def _vlan_form(**overrides):
    data = {
        "region": "dtw", "project_id": "proj-9", "name": "acme-vlan",
        "physical_network": "vlan", "segmentation_id": "815",
    }
    data.update(overrides)
    return data


def test_vlans_create_requires_keystone_token(client, monkeypatch):
    _login_keystone(client, with_token=False)
    from openstack_bi import neutron

    called = []
    monkeypatch.setattr(neutron, "vlan_segment_conflict", lambda *a, **k: None)
    monkeypatch.setattr(neutron, "create_vlan_network", lambda *a, **k: called.append(a))

    r = client.post("/tools/vlans/create", data=_vlan_form())
    assert r.status_code == 302
    assert called == []


def test_vlans_create_rejects_bad_vlan_id(client, monkeypatch):
    _login_keystone(client)
    from openstack_bi import neutron
    from openstack_bi.auth import token_store

    monkeypatch.setattr(token_store, "session_for", lambda key: MagicMock())
    called = []
    monkeypatch.setattr(neutron, "create_vlan_network", lambda *a, **k: called.append(a))

    r = client.post("/tools/vlans/create", data=_vlan_form(segmentation_id="9999"))
    assert r.status_code == 302
    assert called == []  # out-of-range VLAN rejected before any API call


def test_vlans_create_blocked_by_segment_conflict(client, monkeypatch):
    _login_keystone(client)
    from openstack_bi import neutron
    from openstack_bi.auth import token_store

    monkeypatch.setattr(token_store, "session_for", lambda key: MagicMock())
    monkeypatch.setattr(
        neutron, "vlan_segment_conflict",
        lambda *a, **k: {"id": "net-7", "name": "other-net"},
    )
    called = []
    monkeypatch.setattr(neutron, "create_vlan_network", lambda *a, **k: called.append(a))

    r = client.post("/tools/vlans/create", data=_vlan_form())
    assert r.status_code == 302
    assert called == []  # a conflicting segment blocks the create


def test_vlans_create_calls_neutron(client, monkeypatch):
    _login_keystone(client)
    from openstack_bi import neutron
    from openstack_bi.auth import token_store

    monkeypatch.setattr(token_store, "session_for", lambda key: MagicMock())
    monkeypatch.setattr(neutron, "vlan_segment_conflict", lambda *a, **k: None)
    calls = []
    monkeypatch.setattr(
        neutron, "create_vlan_network",
        lambda sess, region, name, project_id, physnet, vlan: calls.append(
            (region, name, project_id, physnet, vlan)
        ) or {"id": "net-1", "name": name, "project_id": project_id},
    )

    r = client.post("/tools/vlans/create", data=_vlan_form())
    assert r.status_code == 302
    assert calls == [("dtw", "acme-vlan", "proj-9", "vlan", 815)]
