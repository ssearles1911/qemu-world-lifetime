"""Keystone authentication path with mocked HTTP layer.

We don't reach a real Keystone in CI. Role discovery now reads roles
from project-scoped tokens (see openstack_bi.auth.keystone) rather than
`/v3/role_assignments`, so these tests mock two seams: the unscoped
auth, and the `_list_projects` / `_scope_to_project` helpers.

`test_authenticate_succeeds_via_scoped_tokens` is the regression test
for the `/v3/role_assignments` 403 — login no longer makes that call.
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


def _stub_unscoped(monkeypatch, user_id="u1", username="bob"):
    """Mock the unscoped Keystone auth — the credential check + identity."""
    from openstack_bi.auth import keystone as ks_auth

    fake_access = MagicMock()
    fake_access.user = {
        "id": user_id, "name": username,
        "domain": {"id": "d1", "name": "Default"},
    }
    fake_access.user_id = user_id

    fake_password = MagicMock()
    fake_password.get_access.return_value = fake_access

    fake_session = MagicMock()
    fake_session.get_token.return_value = "tk"

    monkeypatch.setattr(ks_auth.v3_identity, "Password", lambda **kw: fake_password)
    monkeypatch.setattr(ks_auth.ks_session, "Session", lambda **kw: fake_session)


def _scoped_access(role_names):
    acc = MagicMock()
    acc.role_names = list(role_names)
    return acc


def _stub_scopes(monkeypatch, project_roles):
    """Mock project enumeration + scoped tokens.

    `project_roles` maps project id -> list of role names on that project.
    """
    from openstack_bi.auth import keystone as ks_auth

    projects = [{"id": pid, "name": pid} for pid in project_roles]
    monkeypatch.setattr(ks_auth, "_list_projects", lambda sess: projects)

    def fake_scope(auth_url, username, password, user_domain, project_id):
        roles = project_roles.get(project_id)
        return _scoped_access(roles) if roles is not None else None

    monkeypatch.setattr(ks_auth, "_scope_to_project", fake_scope)


def test_authenticate_succeeds_via_scoped_tokens(configured_keystone, monkeypatch):
    from openstack_bi.auth import keystone as ks_auth

    _stub_unscoped(monkeypatch, user_id="u1", username="bob")
    _stub_scopes(monkeypatch, {"p1": ["admin"], "p2": ["member"]})

    identity = ks_auth.authenticate("bob", "pw", domain="Default")
    assert identity.user_id == "u1"
    assert identity.username == "bob"
    assert identity.project_ids == {"p1", "p2"}
    assert "admin" in identity.role_names
    # A scoped token was kept for later Nova calls.
    assert identity.scoped_access is not None


def test_authenticate_rejects_non_admin(configured_keystone, monkeypatch):
    from openstack_bi import config_db
    from openstack_bi.auth import keystone as ks_auth

    _stub_unscoped(monkeypatch, user_id="u2", username="carol")
    _stub_scopes(monkeypatch, {"p1": ["member"], "p2": ["reader"]})

    with pytest.raises(ks_auth.KeystoneAuthError):
        ks_auth.authenticate("carol", "pw")

    actions = {row["action"] for row in config_db.recent_audit(20)}
    assert "login_denied_not_admin" in actions


def test_authenticate_honors_configured_admin_role(configured_keystone, monkeypatch):
    from openstack_bi import config_db
    from openstack_bi.auth import keystone as ks_auth

    config_db.set_web_setting("keystone_admin_role", "operator")

    _stub_unscoped(monkeypatch, user_id="u3", username="dave")
    _stub_scopes(monkeypatch, {"p9": ["operator"]})

    identity = ks_auth.authenticate("dave", "pw")
    assert "operator" in identity.role_names


def test_role_truncation_audited(configured_keystone, monkeypatch):
    """More than MAX_SESSION_ROLES roles are truncated, and audited."""
    from openstack_bi import config_db
    from openstack_bi.auth import keystone as ks_auth

    _stub_unscoped(monkeypatch, user_id="u9", username="bob")
    many = ["admin"] + [
        f"role{i:03d}" for i in range(ks_auth.MAX_SESSION_ROLES + 10)
    ]
    _stub_scopes(monkeypatch, {"p1": many})

    identity = ks_auth.authenticate("bob", "pw")
    assert len(identity.role_names) == ks_auth.MAX_SESSION_ROLES
    actions = {row["action"] for row in config_db.recent_audit(30)}
    assert "session_roles_truncated" in actions


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
