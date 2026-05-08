"""Legacy `.env` parsing — exercised by `opsbi config import-env`."""

from __future__ import annotations

import pytest

from openstack_bi import _env_legacy


@pytest.fixture
def reset_env(monkeypatch):
    for key in list(__import__("os").environ):
        if key.startswith("OS_DB_") or key in {"KEYSTONE_REGION", "KEYSTONE_DB"}:
            monkeypatch.delenv(key, raising=False)


def test_parse_legacy_regions_multi(reset_env, monkeypatch):
    monkeypatch.setenv("OS_DB_REGIONS", "us-east-dtw,us-east-cvg")
    monkeypatch.setenv("OS_DB_HOST__US_EAST_DTW", "h1")
    monkeypatch.setenv("OS_DB_PASSWORD__US_EAST_DTW", "pw1")
    monkeypatch.setenv("OS_DB_HOST__US_EAST_CVG", "h2")
    monkeypatch.setenv("OS_DB_PASSWORD__US_EAST_CVG", "pw2")
    monkeypatch.setenv("OS_DB_USER", "reporting")
    regions = _env_legacy.parse_legacy_regions()
    assert [r["name"] for r in regions] == ["us-east-dtw", "us-east-cvg"]
    by_name = {r["name"]: r for r in regions}
    assert by_name["us-east-dtw"]["host"] == "h1"
    assert by_name["us-east-cvg"]["db_password"] == "pw2"
    assert by_name["us-east-dtw"]["db_user"] == "reporting"


def test_parse_legacy_regions_empty(reset_env):
    assert _env_legacy.parse_legacy_regions() == []
