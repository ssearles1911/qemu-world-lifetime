"""Migrations + accessors round-trip on the SQLite config DB."""

from __future__ import annotations

from openstack_bi import config_db


def test_migrations_seed_secret_key_and_settings(tmp_config_db):
    assert config_db.web_setting("secret_key")
    assert config_db.web_setting("bind_host") == "127.0.0.1"
    assert config_db.web_setting("bind_port") == "8000"
    schemas = config_db.all_schema_names()
    assert schemas["keystone"] == "keystone"
    assert schemas["nova_api"] == "nova_api"


def test_setup_status_progression(tmp_config_db):
    from openstack_bi.auth import local as local_auth

    assert config_db.setup_status() == config_db.SetupStatus.NO_ADMIN

    local_auth.create_admin("alice", "supersecret123")
    assert config_db.setup_status() == config_db.SetupStatus.NO_REGION

    config_db.upsert_region(
        name="us-east-dtw",
        host="db.example",
        port=3306,
        db_user="reporting",
        db_password="pw",
        is_keystone_region=False,
    )
    # We have a region but none flagged keystone yet.
    assert config_db.setup_status() == config_db.SetupStatus.NO_KEYSTONE_REGION

    config_db.upsert_region(
        name="us-east-dtw",
        host="db.example",
        port=3306,
        db_user="reporting",
        db_password="pw",
        is_keystone_region=True,
    )
    assert config_db.setup_status() == config_db.SetupStatus.NO_KEYSTONE_AUTH_URL

    config_db.set_web_setting("keystone_auth_url", "http://kc.example:5000/v3")
    assert config_db.setup_status() == config_db.SetupStatus.OK


def test_region_upsert_marks_only_one_keystone(tmp_config_db):
    config_db.upsert_region(
        name="a", host="h1", port=3306, db_user="u", db_password="p",
        is_keystone_region=True, display_order=0,
    )
    config_db.upsert_region(
        name="b", host="h2", port=3306, db_user="u", db_password="p",
        is_keystone_region=True, display_order=1,
    )
    keystone = config_db.get_keystone_region_name()
    assert keystone == "b"
    assert {r["name"]: r["is_keystone_region"] for r in config_db.list_all_regions()} == {
        "a": 0,
        "b": 1,
    }


def test_audit_log_round_trip(tmp_config_db):
    config_db.record_audit("local", "alice", "test_event", "detail-here")
    rows = config_db.recent_audit(10)
    assert rows
    assert rows[0]["actor_kind"] == "local"
    assert rows[0]["actor_id"] == "alice"
    assert rows[0]["action"] == "test_event"
    assert rows[0]["detail"] == "detail-here"
