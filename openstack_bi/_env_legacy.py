"""Legacy `.env`-based config parsing, retained only for `opsbi config import-env`.

This is the pre-SQLite logic from `config.py`, isolated here so the active
codebase stays clean. It reads `os.environ` (optionally hydrated from a
`.env` via `python-dotenv` if installed) and returns plain dicts that the
import command can write into SQLite.
"""

from __future__ import annotations

import os
import re
from typing import Dict, List, Optional, Tuple


def _maybe_load_dotenv(path: Optional[str]) -> None:
    if path is None:
        return
    try:
        from dotenv import load_dotenv  # type: ignore[import-not-found]
    except ImportError:
        return
    load_dotenv(path)


def _suffix(name: str) -> str:
    return re.sub(r"[^A-Z0-9]", "_", name.upper())


def _suffix_candidates(name: str) -> List[str]:
    canonical = _suffix(name)
    raw = name.upper()
    return [canonical] if canonical == raw else [canonical, raw]


def _env(var: str, default: Optional[str] = None) -> Optional[str]:
    val = os.environ.get(var)
    return val if val not in (None, "") else default


def _region_var(region_name: str, base: str, default: Optional[str]) -> Optional[str]:
    for suf in _suffix_candidates(region_name):
        v = _env(f"OS_DB_{base}__{suf}")
        if v is not None:
            return v
    return _env(f"OS_DB_{base}", default)


def parse_legacy_regions(env_path: Optional[str] = None) -> List[Dict[str, object]]:
    _maybe_load_dotenv(env_path)
    raw = _env("OS_DB_REGIONS")
    if raw:
        names = [n.strip() for n in raw.split(",") if n.strip()]
    elif _env("OS_DB_HOST") is not None:
        names = ["default"]
    else:
        return []

    out: List[Dict[str, object]] = []
    for idx, name in enumerate(names):
        host = _region_var(name, "HOST", "127.0.0.1")
        port_s = _region_var(name, "PORT", "3306")
        user = _region_var(name, "USER", "nova")
        password = _region_var(name, "PASSWORD", "") or ""
        try:
            port = int(port_s)
        except ValueError:
            raise RuntimeError(
                f"OS_DB_PORT__{_suffix(name)} is not an integer: {port_s!r}"
            )
        out.append(
            {
                "name": name,
                "host": host,
                "port": port,
                "db_user": user,
                "db_password": password,
                "display_order": idx,
            }
        )
    return out


def parse_legacy_keystone_region() -> Optional[str]:
    return _env("KEYSTONE_REGION")


def parse_legacy_schemas() -> Dict[str, str]:
    return {
        "keystone": _env("KEYSTONE_DB", "keystone") or "keystone",
        "nova_api": _env("NOVA_API_DB", "nova_api") or "nova_api",
        "cinder": _env("CINDER_DB", "cinder") or "cinder",
        "glance": _env("GLANCE_DB", "glance") or "glance",
        "neutron": _env("NEUTRON_DB", "neutron") or "neutron",
    }


def parse_legacy_web() -> Tuple[str, str]:
    return _env("QLR_HOST", "127.0.0.1") or "127.0.0.1", _env("QLR_PORT", "8000") or "8000"
