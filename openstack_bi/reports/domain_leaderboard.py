"""Domain leaderboard: rank Keystone domains by total instance count.

Mirror of `instance_leaderboard` but rolled up one level higher: instead
of ranking individual projects, we sum their instances per domain.
Useful for billing/capacity conversations where the unit of accounting
is the tenant organization, not the individual project.

Each domain row drills down to the project breakdown — the domain name
links into `instance_leaderboard` filtered by that domain.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

from openstack_bi import openstack
from openstack_bi.config import parse_regions
from openstack_bi.db import query
from openstack_bi.util import format_region_errors, safe_for_each_region

from .base import ChartSpec, Param, Report, ReportResult


# Same vm_state buckets the project-level leaderboard surfaces, so the
# two reports remain comparable side-by-side.
SURFACED_STATES: Tuple[str, ...] = (
    "active", "stopped", "shelved", "shelved_offloaded", "error",
)


def _region_choices() -> List[Tuple[str, str]]:
    return [(r.name, r.name) for r in parse_regions()]


def _counts_sql() -> str:
    return """
        SELECT project_id, vm_state, COUNT(*) AS n
        FROM instances
        WHERE deleted = 0
        GROUP BY project_id, vm_state
    """


class DomainLeaderboardReport(Report):
    id = "domain_leaderboard"
    name = "Domain leaderboard"
    description = (
        "Keystone domains ranked by instance count across the selected "
        "regions, broken down by vm_state. Click a domain name to drill "
        "into its project-level breakdown."
    )
    category = "Projects"
    params = [
        Param(name="regions", label="Regions", kind="multiselect",
              choices=_region_choices,
              help="Which regions to span. Empty = all configured regions."),
        Param(name="top", label="Top N (chart)", kind="int", default=20,
              placeholder="20", advanced=True,
              help="How many domains to include in the top-N chart. "
                   "The table always shows everything."),
    ]

    def run(
        self,
        regions: Optional[List[str]] = None,
        top: Optional[int] = 20,
        **_: Any,
    ) -> ReportResult:
        selected_region_names = regions or None
        if selected_region_names is None:
            selected_regions = parse_regions()
        else:
            by_name = {r.name: r for r in parse_regions()}
            selected_regions = [
                by_name[n] for n in selected_region_names if n in by_name
            ]

        # project_id -> (domain_id, project_name, domain_name)
        all_projects = openstack.list_all_projects()
        project_to_domain: Dict[str, str] = {p["id"]: p["domain_id"] for p in all_projects}

        # domain_id -> domain_name (Keystone stores domains as projects with is_domain=1)
        domains = openstack.list_domains()
        domain_name_by_id: Dict[str, str] = {d["id"]: d["name"] for d in domains}
        # `list_domains()` only returns enabled domains. A project that
        # belongs to a disabled (or deleted) domain still has its own
        # `domain_id` — we surface those under a synthetic "(unknown
        # domain)" bucket so the totals line up with the underlying
        # instance counts.
        unknown_domain_label = "(unknown or disabled domain)"

        # Aggregate per (project_id, vm_state) across all cells.
        per_project: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

        sql = _counts_sql()

        def _collect(region):
            rows_out: List[Dict[str, Any]] = []
            for cell in openstack.list_cells(region):
                rows_out.extend(query(region, cell, sql, ()))
            return rows_out

        results, region_errors = safe_for_each_region(selected_regions, _collect)
        for _, region_rows in results:
            for r in region_rows:
                pid = r["project_id"]
                state = r.get("vm_state") or "unknown"
                per_project[pid][state] += int(r["n"] or 0)

        # Roll up project counts into domain counts.
        # domain_key -> {state: count, project_ids: set, domain_name: str}
        per_domain: Dict[str, Dict[str, Any]] = {}
        for pid, by_state in per_project.items():
            domain_id = project_to_domain.get(pid)
            if domain_id is None:
                domain_key = "__unknown__"
                domain_name = unknown_domain_label
            else:
                domain_key = domain_id
                domain_name = domain_name_by_id.get(domain_id, unknown_domain_label)
            bucket = per_domain.setdefault(domain_key, {
                "domain_id": domain_id,
                "domain_name": domain_name,
                "states": defaultdict(int),
                "projects_with_instances": set(),
            })
            bucket["projects_with_instances"].add(pid)
            for state, count in by_state.items():
                bucket["states"][state] += count

        # Build output rows.
        rows_out: List[Dict[str, Any]] = []
        for domain_key, bucket in per_domain.items():
            states = bucket["states"]
            active = states.get("active", 0)
            stopped = states.get("stopped", 0)
            shelved = states.get("shelved", 0) + states.get("shelved_offloaded", 0)
            errored = states.get("error", 0)
            other = sum(
                n for s, n in states.items()
                if s not in ("active", "stopped", "shelved", "shelved_offloaded", "error")
            )
            total = active + stopped + shelved + errored + other
            domain_name = bucket["domain_name"]

            # Drill-down: clicking the domain name jumps into the project
            # leaderboard scoped to this domain. The companion `_link`
            # field is rendered as an anchor by the report template.
            drill_link: Optional[str] = None
            if bucket["domain_id"] is not None:
                qs = urlencode({"domain": domain_name})
                drill_link = f"/report/instance_leaderboard?{qs}"

            row = {
                "domain_name": domain_name,
                "domain_id": bucket["domain_id"] or "",
                "projects_with_instances": len(bucket["projects_with_instances"]),
                "active": active,
                "stopped": stopped,
                "shelved": shelved,
                "error": errored,
                "other": other,
                "total": total,
            }
            if drill_link:
                row["domain_name_link"] = drill_link
            rows_out.append(row)

        rows_out.sort(
            key=lambda r: (-r["active"], -r["total"], r["domain_name"] or "")
        )

        # Top-N chart: domains by active count.
        top_n = max(1, int(top or 20))
        top_rows = rows_out[:top_n]
        chart = ChartSpec(
            kind="bar",
            title=f"Top {len(top_rows)} domains by active instances",
            x_label="Domain",
            y_label="Instances",
            x_categories=[r["domain_name"] for r in top_rows],
            series=[
                {"label": "active",  "data": [r["active"]  for r in top_rows]},
                {"label": "stopped", "data": [r["stopped"] for r in top_rows]},
                {"label": "shelved", "data": [r["shelved"] for r in top_rows]},
                {"label": "error",   "data": [r["error"]   for r in top_rows]},
                {"label": "other",   "data": [r["other"]   for r in top_rows]},
            ],
        )

        metadata = {
            "regions": ", ".join(r.name for r in selected_regions) or "(none)",
            "domains_with_instances": len(rows_out),
            "total_instances": sum(r["total"] for r in rows_out),
            "total_active": sum(r["active"] for r in rows_out),
            "region_errors": format_region_errors(region_errors),
        }

        stem_bits = ["domain-leaderboard"]
        stem_bits.append(
            "-".join(r.name for r in selected_regions)
            if selected_region_names is not None else "all-regions"
        )

        return ReportResult(
            columns=[
                ("domain_name", "Domain"),
                ("domain_id", "Domain ID"),
                ("projects_with_instances", "Projects"),
                ("active", "Active"),
                ("stopped", "Stopped"),
                ("shelved", "Shelved"),
                ("error", "Error"),
                ("other", "Other"),
                ("total", "Total"),
            ],
            rows=rows_out,
            charts=[chart] if top_rows else [],
            metadata=metadata,
            filename_stem="-".join(stem_bits),
        )


REPORT = DomainLeaderboardReport()
