"""Keystone authentication path with mocked HTTP layer.

We don't reach a real Keystone in CI — instead we monkey-patch the
keystoneauth1 v3 Password identity plugin and the session.get used to
fetch role_assignments.
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


def _stub_password(monkeypatch, user_id="user-123", username="bob", domain_id="dom-1", domain_name="Default"):
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


def _stub_session(monkeypatch, project_ids):
    from openstack_bi.auth import keystone as ks_auth

    fake_session = MagicMock()
    fake_session.get_token.return_value = "tk"

    fake_resp = MagicMock()
    fake_resp.status_code = 200
    fake_resp.json.return_value = {
        "role_assignments": [
            {"scope": {"project": {"id": pid}}} for pid in project_ids
        ],
    }
    fake_session.get.return_value = fake_resp
    monkeypatch.setattr(ks_auth.ks_session, "Session", lambda **kw: fake_session)
    return fake_session


def test_authenticate_collects_project_ids(configured_keystone, monkeypatch):
    from openstack_bi.auth import keystone as ks_auth

    _stub_password(monkeypatch, user_id="u1", username="bob")
    _stub_session(monkeypatch, ["p1", "p2", "p3"])

    identity = ks_auth.authenticate("bob", "pw", domain="Default")
    assert identity.user_id == "u1"
    assert identity.username == "bob"
    assert identity.project_ids == {"p1", "p2", "p3"}


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
