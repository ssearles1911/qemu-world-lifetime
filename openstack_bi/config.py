"""Region + DB config, backed by the SQLite configuration store.

Configuration used to come from `.env`. It now lives in the SQLite file at
`OPSBI_CONFIG_DB` (defaults to `./opsbi.sqlite`), edited via the admin UI,
the `opsbi config ...` CLI, or the first-run setup wizard. The only env
var still consulted by this module is the SQLite path itself, resolved
inside `config_db`.

Public API (`Region`, `parse_regions`, `resolve_regions`, `keystone_region`,
`keystone_db`, `nova_api_db`, `cinder_db`, `glance_db`, `neutron_db`,
`placement_db`) is unchanged so reports and the CLI dispatcher don't have to.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from . import config_db


@dataclass(frozen=True)
class Region:
    name: str
    host: str
    port: int
    user: str
    password: str


def _row_to_region(row: dict) -> Region:
    return Region(
        name=row["name"],
        host=row["host"],
        port=int(row["port"]),
        user=row["db_user"],
        password=row["db_password"] or "",
    )


def parse_regions() -> List[Region]:
    """All enabled regions in display order. Raises if none are configured."""
    rows = config_db.list_regions()
    if not rows:
        raise RuntimeError(
            "No regions configured. Run `opsbi init` and complete the setup "
            "wizard at http://<host>:<port>/setup, or `opsbi config import-env` "
            "to migrate an existing .env."
        )
    return [_row_to_region(r) for r in rows]


def resolve_regions(selected: Optional[List[str]] = None) -> List[Region]:
    """All configured regions, or the subset named in `selected` (by name).

    `selected=None` or an empty list means "all regions". Unknown names raise
    `ValueError` so typos don't silently produce empty reports.
    """
    all_regions = parse_regions()
    if not selected:
        return all_regions
    by_name = {r.name: r for r in all_regions}
    out: List[Region] = []
    for name in selected:
        if name not in by_name:
            known = ", ".join(r.name for r in all_regions)
            raise ValueError(f"Unknown region: {name!r}. Configured regions: {known}")
        out.append(by_name[name])
    return out


def keystone_region(regions: Optional[List[Region]] = None) -> Region:
    """Which region's DB connection hosts the shared `keystone` schema.

    The region flagged `is_keystone_region` wins; otherwise we fall back to
    the first configured region.
    """
    regions = regions if regions is not None else parse_regions()
    if not regions:
        raise RuntimeError("No regions configured; cannot resolve keystone region.")
    flagged = config_db.get_keystone_region_name()
    if flagged:
        for r in regions:
            if r.name == flagged:
                return r
        known = ", ".join(r.name for r in regions)
        raise RuntimeError(
            f"Keystone region {flagged!r} is not in the active region set. "
            f"Configured regions: {known}"
        )
    return regions[0]


def keystone_db() -> str:
    return config_db.get_schema_name("keystone")


def nova_api_db() -> str:
    return config_db.get_schema_name("nova_api")


def cinder_db() -> str:
    return config_db.get_schema_name("cinder")


def glance_db() -> str:
    return config_db.get_schema_name("glance")


def neutron_db() -> str:
    return config_db.get_schema_name("neutron")


def placement_db() -> str:
    return config_db.get_schema_name("placement")
