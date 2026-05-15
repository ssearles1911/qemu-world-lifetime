"""Capability registry, role-mapping accessors, and resolution helpers."""

from __future__ import annotations

import pytest

from openstack_bi import config_db
from openstack_bi.auth.capabilities import (
    ALL_CAPABILITIES,
    CAPABILITY_REGISTRY,
    Capability,
    is_known_capability,
)


# --- Registry ---------------------------------------------------------------

def test_registry_matches_enum():
    enum_values = {c.value for c in Capability}
    registry_names = {c.name for c in CAPABILITY_REGISTRY}
    assert enum_values == registry_names
    assert ALL_CAPABILITIES == frozenset(enum_values)


def test_known_capability_lookup():
    assert is_known_capability("view_all_projects")
    assert not is_known_capability("does_not_exist")


# --- config_db role-mapping accessors --------------------------------------

def test_grant_revoke_round_trip(tmp_config_db):
    inserted = config_db.grant_role_capability("Admin", "manage_config")
    assert inserted is True
    # Lowercase normalization on read.
    assert config_db.roles_for_capability("manage_config") == ["admin"]

    # Idempotent.
    inserted_again = config_db.grant_role_capability("admin", "manage_config")
    assert inserted_again is False

    removed = config_db.revoke_role_capability("ADMIN", "manage_config")
    assert removed is True
    assert config_db.roles_for_capability("manage_config") == []
    assert config_db.revoke_role_capability("admin", "manage_config") is False


def test_caps_for_roles_unions(tmp_config_db):
    config_db.grant_role_capability("admin", "manage_config")
    config_db.grant_role_capability("admin", "manage_users")
    config_db.grant_role_capability("auditor", "view_audit_log")

    caps = config_db.caps_for_roles(["admin", "auditor"])
    assert set(caps) == {"manage_config", "manage_users", "view_audit_log"}

    # Unknown roles → empty set.
    assert config_db.caps_for_roles(["nobody"]) == []
    # Empty input short-circuits.
    assert config_db.caps_for_roles([]) == []


def test_count_roles_for_capability(tmp_config_db):
    assert config_db.count_roles_for_capability("manage_config") == 0
    config_db.grant_role_capability("admin", "manage_config")
    config_db.grant_role_capability("operator", "manage_config")
    assert config_db.count_roles_for_capability("manage_config") == 2


# --- current_capabilities + helpers ----------------------------------------

def test_current_capabilities_local_admin_is_unbounded(tmp_config_db):
    from flask import Flask

    from openstack_bi.auth.session import SESSION_KEY, current_capabilities

    app = Flask(__name__)
    app.config["SECRET_KEY"] = "test"

    with app.test_request_context("/"):
        from flask import session
        session[SESSION_KEY] = {
            "kind": "local", "user_id": "1", "username": "alice",
            "is_admin": True, "project_ids": [], "domain_id": None, "roles": [],
        }
        caps = current_capabilities()
        assert caps == ALL_CAPABILITIES


def test_current_capabilities_keystone_user_no_mapping(tmp_config_db):
    from flask import Flask

    from openstack_bi.auth.session import SESSION_KEY, current_capabilities

    app = Flask(__name__)
    app.config["SECRET_KEY"] = "test"

    with app.test_request_context("/"):
        from flask import session
        session[SESSION_KEY] = {
            "kind": "keystone", "user_id": "u1", "username": "bob",
            "is_admin": False, "project_ids": ["p1"], "domain_id": "d1",
            "roles": ["admin"],
        }
        # Empty mapping table -> no capabilities.
        assert current_capabilities() == frozenset()


def test_current_capabilities_keystone_user_mapped(tmp_config_db):
    from flask import Flask

    from openstack_bi.auth.session import SESSION_KEY, current_capabilities

    config_db.grant_role_capability("admin", "manage_config")
    config_db.grant_role_capability("admin", "view_audit_log")

    app = Flask(__name__)
    app.config["SECRET_KEY"] = "test"

    with app.test_request_context("/"):
        from flask import session
        session[SESSION_KEY] = {
            "kind": "keystone", "user_id": "u1", "username": "bob",
            "is_admin": False, "project_ids": [], "domain_id": None,
            "roles": ["admin"],
        }
        assert current_capabilities() == frozenset(
            {"manage_config", "view_audit_log"}
        )


def test_view_all_projects_unscopes_user(tmp_config_db):
    from flask import Flask

    from openstack_bi.auth.session import SESSION_KEY, current_user_project_ids

    config_db.grant_role_capability("admin", "view_all_projects")

    app = Flask(__name__)
    app.config["SECRET_KEY"] = "test"

    with app.test_request_context("/"):
        from flask import session
        session[SESSION_KEY] = {
            "kind": "keystone", "user_id": "u1", "username": "bob",
            "is_admin": False, "project_ids": ["p1", "p2"], "domain_id": None,
            "roles": ["admin"],
        }
        # With view_all_projects, the helper returns None (unscoped).
        assert current_user_project_ids() is None


def test_keystone_user_without_view_all_is_scoped(tmp_config_db):
    from flask import Flask

    from openstack_bi.auth.session import SESSION_KEY, current_user_project_ids

    app = Flask(__name__)
    app.config["SECRET_KEY"] = "test"

    with app.test_request_context("/"):
        from flask import session
        session[SESSION_KEY] = {
            "kind": "keystone", "user_id": "u1", "username": "bob",
            "is_admin": False, "project_ids": ["p1", "p2"], "domain_id": None,
            "roles": [],
        }
        assert current_user_project_ids() == {"p1", "p2"}


# --- Bootstrap-deadlock guard (CLI) ----------------------------------------

def test_cli_revoke_last_manage_config_requires_force(tmp_config_db, capsys):
    from openstack_bi import cli

    # Plant a single mapping for manage_config.
    config_db.grant_role_capability("admin", "manage_config")
    assert config_db.count_roles_for_capability("manage_config") == 1

    # Without --force: refused.
    parser = cli.build_parser()
    args = parser.parse_args(["roles", "revoke", "admin", "manage_config"])
    rc = cli._handle_roles(args)
    assert rc == 2
    err = capsys.readouterr().err
    assert "Refusing" in err
    assert config_db.count_roles_for_capability("manage_config") == 1

    # With --force: allowed.
    args = parser.parse_args(["roles", "revoke", "admin", "manage_config", "--force"])
    rc = cli._handle_roles(args)
    assert rc == 0
    assert config_db.count_roles_for_capability("manage_config") == 0
