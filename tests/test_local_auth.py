"""Local administrator authentication."""

from __future__ import annotations

import pytest

from openstack_bi.auth import local as local_auth


def test_create_admin_and_verify(tmp_config_db):
    local_auth.create_admin("alice", "supersecret123")
    user = local_auth.verify("alice", "supersecret123")
    assert user is not None
    assert user["username"] == "alice"


def test_verify_rejects_wrong_password(tmp_config_db):
    local_auth.create_admin("alice", "supersecret123")
    assert local_auth.verify("alice", "wrong") is None


def test_create_admin_rejects_duplicates(tmp_config_db):
    local_auth.create_admin("alice", "supersecret123")
    with pytest.raises(ValueError):
        local_auth.create_admin("alice", "anotherpass1234")


def test_reset_password(tmp_config_db):
    local_auth.create_admin("alice", "supersecret123")
    local_auth.reset_password("alice", "newpassword456")
    assert local_auth.verify("alice", "supersecret123") is None
    assert local_auth.verify("alice", "newpassword456") is not None
