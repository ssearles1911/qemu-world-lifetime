"""Keystone authentication path with mocked HTTP layer.

We don't reach a real Keystone in CI — instead we monkey-patch the
keystoneauth1 v3 Password identity plugin and the session used to fetch
role assignments. Login is gated on the admin role; these tests cover
that gate, project resolution, and the scoped-token / session wiring.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


@pytest.fixture
def configured_keystone(tmp_config_db):
    from openstack_bi import config_db
    config_db.set_web_setting("keystone_auth_url", "http://kc.example/v3")
    config_db.set_web_setting("keystone_default_domain", "Default")
    return config_db


def _stub_password(monkeypatch, user_id="user-123", username="bob",
                   domain_id="dom-1", domain_name="Default"):
    from openstack_bi.auth import keystone as ks_auth

    fake_access = MagicMock()
    fake_access.user = {
        "id": user_id,
        "name": username,
        "domain": {"id": domain_id, "name": domain_name},
    }
    fake_access.user_id = user_id

    fake_password = MagicMock()
    fake_password.get_access.return_value = fake_access
    monkeypatch.setattr(ks_auth.v3_identity, "Password", lambda **kw: fake_password)
    return fake_password


def _assignment(project_id, role_id):
    return {"scope": {"project": {"id": project_id}}, "role": {"id": role_id}}


def _stub_session(monkeypatch, assignments, roles_catalog):
    """Patch keystoneauth1 Session so role_assignments / roles are mocked.

    `roles_catalog` is a {role_id: role_name} mapping returned by /v3/roles.
    """
    from openstack_bi.auth import keystone as ks_auth

    ra_resp = MagicMock()
    ra_resp.status_code = 200
    ra_resp.json.return_value = {"role_assignments": assignments}

    roles_resp = MagicMock()
    roles_resp.status_code = 200
    roles_resp.json.return_value = {
        "roles": [{"id": rid, "name": name} for rid, name in roles_catalog.items()]
    }

    def fake_get(url, *args, **kwargs):
        return ra_resp if "role_assignments" in url else roles_resp

    fake_session = MagicMock()
    fake_session.get_token.return_value = "tk"
    fake_session.get.side_effect = fake_get
    monkeypatch.setattr(ks_auth.ks_session, "Session", lambda **kw: fake_session)
    return fake_session


def test_authenticate_collects_project_ids(configured_keystone, monkeypatch):
    from openstack_bi.auth import keystone as ks_auth

    _stub_password(monkeypatch, user_id="u1", username="bob")
    _stub_session(
        monkeypatch,
        assignments=[
            _assignment("p1", "r-admin"),
            _assignment("p2", "r-admin"),
            _assignment("p3", "r-member"),
        ],
        roles_catalog={"r-admin": "admin", "r-member": "member"},
    )

    identity = ks_auth.authenticate("bob", "pw", domain="Default")
    assert identity.user_id == "u1"
    assert identity.username == "bob"
    assert identity.project_ids == {"p1", "p2", "p3"}
    assert "admin" in identity.role_names
    # A scoped token was obtained for later Nova calls.
    assert identity.scoped_access is not None


def test_authenticate_rejects_non_admin(configured_keystone, monkeypatch):
    from openstack_bi import config_db
    from openstack_bi.auth import keystone as ks_auth

    _stub_password(monkeypatch, user_id="u2", username="carol")
    _stub_session(
        monkeypatch,
        assignments=[_assignment("p1", "r-member")],
        roles_catalog={"r-member": "member"},
    )

    with pytest.raises(ks_auth.KeystoneAuthError):
        ks_auth.authenticate("carol", "pw")

    actions = {row["action"] for row in config_db.recent_audit(20)}
    assert "login_denied_not_admin" in actions


def test_authenticate_honors_configured_admin_role(configured_keystone, monkeypatch):
    from openstack_bi import config_db
    from openstack_bi.auth import keystone as ks_auth

    config_db.set_web_setting("keystone_admin_role", "operator")

    _stub_password(monkeypatch, user_id="u3", username="dave")
    _stub_session(
        monkeypatch,
        assignments=[_assignment("p9", "r-op")],
        roles_catalog={"r-op": "operator"},
    )

    identity = ks_auth.authenticate("dave", "pw")
    assert "operator" in identity.role_names


def test_authenticate_unauthorized(configured_keystone, monkeypatch):
    from keystoneauth1.exceptions import Unauthorized

    from openstack_bi.auth import keystone as ks_auth

    fake_password = MagicMock()
    fake_password.get_access.side_effect = Unauthorized()
    monkeypatch.setattr(ks_auth.v3_identity, "Password", lambda **kw: fake_password)

    fake_session = MagicMock()
    fake_session.get_token.side_effect = Unauthorized()
    monkeypatch.setattr(ks_auth.ks_session, "Session", lambda **kw: fake_session)

    with pytest.raises(ks_auth.KeystoneAuthError):
        ks_auth.authenticate("bob", "pw")


def test_authenticate_requires_auth_url(tmp_config_db):
    from openstack_bi.auth import keystone as ks_auth

    with pytest.raises(ks_auth.KeystoneAuthError):
        ks_auth.authenticate("bob", "pw")


def test_login_keystone_marks_admin_and_stores_token(configured_keystone):
    """A Keystone session is an admin; its scoped token is cached."""
    from flask import Flask, session

    from openstack_bi.auth import token_store
    from openstack_bi.auth.keystone import KeystoneIdentity
    from openstack_bi.auth.session import SESSION_KEY, login_keystone

    app = Flask(__name__)
    app.config["SECRET_KEY"] = "test"
    sentinel = object()
    identity = KeystoneIdentity(
        user_id="u1", username="bob", domain_id="d1", domain_name="Default",
        project_ids={"p1"}, role_names={"admin"}, scoped_access=sentinel,
    )
    with app.test_request_context("/"):
        login_keystone(identity)
        assert session[SESSION_KEY]["is_admin"] is True
        assert session[SESSION_KEY]["kind"] == "keystone"
        key = session["ks_token_key"]
    assert token_store.get(key) is sentinel
