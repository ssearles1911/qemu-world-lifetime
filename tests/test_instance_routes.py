"""Live-migration JSON endpoint behind the SPLA report's modal."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


@pytest.fixture
def client(tmp_config_db):
    from openstack_bi import config_db
    from openstack_bi.auth import local as local_auth
    from openstack_bi.web import create_app

    config_db.upsert_region(
        name="dfw", host="127.0.0.1", port=3306, db_user="r", db_password="",
        is_keystone_region=True,
    )
    local_auth.create_admin("admin", "password123")
    app = create_app()
    app.config["WTF_CSRF_ENABLED"] = False
    return app.test_client()


def _login_keystone(client):
    """Install a Keystone session (with a token-store key) on the client."""
    from openstack_bi.auth.session import SESSION_KEY

    with client.session_transaction() as sess:
        sess[SESSION_KEY] = {
            "kind": "keystone", "user_id": "u1", "username": "bob",
            "is_admin": True, "project_ids": ["p1"], "domain_id": "d1",
            "roles": ["admin"],
        }
        sess["ks_token_key"] = "fake-key"


def test_migrate_get_requires_keystone_token(client):
    # Session is set but nothing is in the token store -> 403 JSON.
    _login_keystone(client)
    r = client.get("/instance/dfw/srv-1/migrate")
    assert r.status_code == 403
    assert r.get_json()["ok"] is False


def test_migrate_get_unknown_region(client):
    _login_keystone(client)
    r = client.get("/instance/bogus/srv-1/migrate")
    assert r.status_code == 400
    assert r.get_json()["ok"] is False


def test_migrate_get_returns_candidates(client, monkeypatch):
    _login_keystone(client)
    from openstack_bi import nova
    from openstack_bi.auth import token_store

    monkeypatch.setattr(token_store, "session_for", lambda key: MagicMock())
    monkeypatch.setattr(
        nova, "get_server",
        lambda s, region, uuid: {"name": "vm1", "status": "ACTIVE", "host": "cmp-1"},
    )
    monkeypatch.setattr(
        nova, "list_compute_hosts",
        lambda s, region: [
            {"host": "cmp-1", "status": "enabled", "state": "up"},
            {"host": "cmp-2", "status": "enabled", "state": "up"},
        ],
    )

    r = client.get("/instance/dfw/srv-1/migrate")
    assert r.status_code == 200
    data = r.get_json()
    assert data["ok"] is True
    assert data["current_host"] == "cmp-1"
    # The instance's current host is excluded from the candidate list.
    assert [h["host"] for h in data["candidates"]] == ["cmp-2"]


def test_migrate_post_starts_migration(client, monkeypatch):
    _login_keystone(client)
    from openstack_bi import nova
    from openstack_bi.auth import token_store

    calls = {}
    monkeypatch.setattr(token_store, "session_for", lambda key: MagicMock())
    monkeypatch.setattr(
        nova, "live_migrate",
        lambda s, region, uuid, host: calls.update(
            region=region, uuid=uuid, host=host),
    )

    r = client.post("/instance/dfw/srv-1/migrate", data={"target_host": "cmp-9"})
    assert r.status_code == 200
    assert r.get_json()["ok"] is True
    assert calls == {"region": "dfw", "uuid": "srv-1", "host": "cmp-9"}


def test_migrate_post_requires_target_host(client, monkeypatch):
    _login_keystone(client)
    from openstack_bi.auth import token_store

    monkeypatch.setattr(token_store, "session_for", lambda key: MagicMock())
    r = client.post("/instance/dfw/srv-1/migrate", data={})
    assert r.status_code == 400
    assert r.get_json()["ok"] is False
