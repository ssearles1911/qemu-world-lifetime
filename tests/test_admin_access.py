"""The Admin / configuration surface is restricted to local administrators.

Keystone sessions are privileged (reports + instance actions) but must
not reach the config pages or see the Admin menu.
"""

from __future__ import annotations

import pytest

ADMIN_PATHS = [
    "/admin", "/admin/regions", "/admin/schemas", "/admin/keystone",
    "/admin/admins", "/admin/audit", "/admin/roles",
]


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


def _login(client, kind):
    from openstack_bi.auth.session import SESSION_KEY

    with client.session_transaction() as sess:
        sess[SESSION_KEY] = {
            "kind": kind, "user_id": "u1", "username": "u1",
            "is_admin": True, "project_ids": [], "domain_id": None, "roles": [],
        }


@pytest.mark.parametrize("path", ADMIN_PATHS)
def test_keystone_session_blocked_from_admin_pages(client, path):
    _login(client, "keystone")
    r = client.get(path)
    # Bounced to the catalog rather than served the config page.
    assert r.status_code in (302, 303)
    assert "/admin" not in r.headers.get("Location", "")


@pytest.mark.parametrize("path", ADMIN_PATHS)
def test_local_admin_reaches_admin_pages(client, path):
    _login(client, "local")
    r = client.get(path)
    assert r.status_code == 200


def test_keystone_session_has_no_admin_menu(client):
    _login(client, "keystone")
    body = client.get("/").get_data(as_text=True)
    assert 'href="/admin"' not in body


def test_local_admin_session_has_admin_menu(client):
    _login(client, "local")
    body = client.get("/").get_data(as_text=True)
    assert 'href="/admin"' in body
