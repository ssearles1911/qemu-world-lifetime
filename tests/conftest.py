"""Shared pytest fixtures.

Each test gets a fresh tempfile-backed configuration database. We point
the `OPSBI_CONFIG_DB` env var at it before the `config_db` module is
imported so its module-level path resolution sees the override.
"""

from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path

import pytest


@pytest.fixture
def tmp_config_db(tmp_path, monkeypatch):
    """A fresh SQLite config DB at $tmp_path/opsbi.sqlite.

    Returns the resolved path. The test session sees fully-isolated state:
    no carry-over from prior tests, no touching the developer's repo file.
    """
    db_path = tmp_path / "opsbi.sqlite"
    monkeypatch.setenv("OPSBI_CONFIG_DB", str(db_path))

    # Reload modules that capture the path at import time.
    for mod_name in [
        "openstack_bi.config_db",
        "openstack_bi.auth.local",
        "openstack_bi.auth.keystone",
        "openstack_bi.auth.session",
        "openstack_bi.auth",
        "openstack_bi.config",
    ]:
        if mod_name in sys.modules:
            importlib.reload(sys.modules[mod_name])

    from openstack_bi import config_db
    config_db._initialized_paths.clear()
    config_db.init(db_path)
    return db_path
