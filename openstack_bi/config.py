"""Region + DB config parsed from environment variables.

Config schema
-------------

Multi-region (preferred):

    OS_DB_REGIONS=dfw1,ord1
    OS_DB_HOST__DFW1=replica-dfw1.internal
    OS_DB_PORT__DFW1=3306
    OS_DB_USER__DFW1=reporting
    OS_DB_PASSWORD__DFW1=...
    OS_DB_HOST__ORD1=replica-ord1.internal
    OS_DB_PASSWORD__ORD1=...
    KEYSTONE_REGION=dfw1          # optional; defaults to the first region

The per-region env-var suffix is the region name uppercased with any
non-alphanumeric characters replaced by underscores, so `dfw1` → `DFW1`,
`us-east-2` → `US_EAST_2`.

Single-region fallback (for deployments that haven't migrated yet): if
`OS_DB_REGIONS` is unset but legacy `OS_DB_HOST` / `OS_DB_USER` /
`OS_DB_PASSWORD` are present, a single region named `default` is synthesized.

Keystone is assumed to be **shared across regions** (one Keystone serves the
whole deployment). `KEYSTONE_REGION` names the region whose DB connection can
reach the `keystone` schema — typically the region that physically hosts it.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import List, Optional

# Load .env once, as early as possible, so every os.environ.get() below sees it.
# Real env vars still take precedence.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


@dataclass(frozen=True)
class Region:
    name: str
    host: str
    port: int
    user: str
    password: str

    @property
    def suffix(self) -> str:
        """Env-var suffix form: uppercase, non-alphanumerics → underscore."""
        return _suffix(self.name)


def _suffix(name: str) -> str:
    return re.sub(r"[^A-Z0-9]", "_", name.upper())


def _env(var: str, default: Optional[str] = None) -> Optional[str]:
    val = os.environ.get(var)
    return val if val not in (None, "") else default


def _region_var(region_name: str, base: str, default: Optional[str]) -> Optional[str]:
    """Read OS_DB_<BASE>__<SUFFIX>, falling back to OS_DB_<BASE> (legacy)."""
    suf = _suffix(region_name)
    return _env(f"OS_DB_{base}__{suf}", _env(f"OS_DB_{base}", default))


def parse_regions() -> List[Region]:
    """Parse configured regions from the environment.

    Returns a list of `Region` in the order they appear in `OS_DB_REGIONS`.
    Raises `RuntimeError` if no regions can be resolved at all.
    """
    raw = _env("OS_DB_REGIONS")
    names: List[str]
    if raw:
        names = [n.strip() for n in raw.split(",") if n.strip()]
    elif _env("OS_DB_HOST") is not None:
        # Legacy single-region deployment — synthesize a "default" region from
        # the bare OS_DB_* vars.
        names = ["default"]
    else:
        raise RuntimeError(
            "No regions configured. Set OS_DB_REGIONS (or legacy OS_DB_HOST) in "
            "your environment / .env file."
        )

    regions: List[Region] = []
    for name in names:
        host = _region_var(name, "HOST", "127.0.0.1")
        port_s = _region_var(name, "PORT", "3306")
        user = _region_var(name, "USER", "nova")
        password = _region_var(name, "PASSWORD", "") or ""
        try:
            port = int(port_s)
        except ValueError:
            raise RuntimeError(f"OS_DB_PORT__{_suffix(name)} is not an integer: {port_s!r}")
        regions.append(Region(name=name, host=host, port=port, user=user, password=password))
    return regions


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

    Defaults to the region named by `KEYSTONE_REGION`, or the first configured
    region when that's unset.
    """
    regions = regions if regions is not None else parse_regions()
    if not regions:
        raise RuntimeError("No regions configured; cannot resolve keystone region.")
    target = _env("KEYSTONE_REGION")
    if target:
        for r in regions:
            if r.name == target:
                return r
        known = ", ".join(r.name for r in regions)
        raise RuntimeError(
            f"KEYSTONE_REGION={target!r} does not match any configured region. "
            f"Configured regions: {known}"
        )
    return regions[0]


def keystone_db() -> str:
    return _env("KEYSTONE_DB", "keystone") or "keystone"


def nova_api_db() -> str:
    return _env("NOVA_API_DB", "nova_api") or "nova_api"


def cinder_db() -> str:
    return _env("CINDER_DB", "cinder") or "cinder"


def glance_db() -> str:
    return _env("GLANCE_DB", "glance") or "glance"


def neutron_db() -> str:
    return _env("NEUTRON_DB", "neutron") or "neutron"
