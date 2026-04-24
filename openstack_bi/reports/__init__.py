"""Report plugin registry.

Each report module exports `REPORT` — an instance of a subclass of
`openstack_bi.reports.base.Report`. Adding a new report is a two-step
change: create `reports/<slug>.py` with `REPORT = MyReport()`, then add
a line importing it below.
"""

from __future__ import annotations

from typing import Dict, List

from .base import Report

# --- Report imports ---------------------------------------------------------
# The CLI and web UI enumerate reports in the order they appear in `_ORDER`.
from . import instance_leaderboard  # noqa: F401
from . import project_growth  # noqa: F401
from . import qemu_lifetime  # noqa: F401

_ORDER = [qemu_lifetime, instance_leaderboard, project_growth]


def all_reports() -> List[Report]:
    return [mod.REPORT for mod in _ORDER]


def by_id(report_id: str) -> Report:
    for r in all_reports():
        if r.id == report_id:
            return r
    raise KeyError(f"Unknown report: {report_id!r}")


def registry() -> Dict[str, Report]:
    return {r.id: r for r in all_reports()}
