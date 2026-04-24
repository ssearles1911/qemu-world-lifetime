"""Instance leaderboard: rank projects by instance count, broken down by
vm_state. Can be scoped to one domain or run across the whole deployment.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from openstack_bi import openstack
from openstack_bi.config import parse_regions
from openstack_bi.db import query

from .base import ChartSpec, Param, Report, ReportResult


# vm_state buckets surfaced as dedicated columns. Anything else falls into
# the "other" bucket.
SURFACED_STATES: Tuple[str, ...] = ("active", "stopped", "shelved", "shelved_offloaded", "error")


def _domain_choices() -> List[Tuple[str, str]]:
    return [("", "— all domains —")] + [
        (d["name"], f'{d["name"]} ({d["project_count"]} project(s))')
        for d in openstack.list_domains()
    ]


def _region_choices() -> List[Tuple[str, str]]:
    return [(r.name, r.name) for r in parse_regions()]


def _counts_sql(project_ids: Optional[List[str]]) -> Tuple[str, List[Any]]:
    if project_ids:
        ph = ",".join(["%s"] * len(project_ids))
        sql = f"""
            SELECT project_id, vm_state, COUNT(*) AS n
            FROM instances
            WHERE deleted = 0
              AND project_id IN ({ph})
            GROUP BY project_id, vm_state
        """
        return sql, list(project_ids)
    sql = """
        SELECT project_id, vm_state, COUNT(*) AS n
        FROM instances
        WHERE deleted = 0
        GROUP BY project_id, vm_state
    """
    return sql, []


class InstanceLeaderboardReport(Report):
    id = "instance_leaderboard"
    name = "Instance leaderboard"
    description = (
        "Projects ranked by instance count across the selected regions, broken "
        "down by vm_state (active / stopped / shelved / error / other). Scope "
        "to one domain or run across every enabled project in the deployment."
    )
    params = [
        Param(name="domain", label="Domain", kind="select",
              choices=_domain_choices, default="",
              help="Keystone domain to scope the report. Empty = every domain."),
        Param(name="regions", label="Regions", kind="multiselect",
              choices=_region_choices,
              help="Which regions to span. Empty = all configured regions."),
        Param(name="top", label="Top N (chart)", kind="int", default=20,
              placeholder="20",
              help="How many projects to include in the top-N chart. The table always shows everything."),
    ]

    def run(
        self,
        domain: Optional[str] = None,
        regions: Optional[List[str]] = None,
        top: Optional[int] = 20,
        **_: Any,
    ) -> ReportResult:
        selected_region_names = regions or None
        if selected_region_names is None:
            selected_regions = parse_regions()
        else:
            by_name = {r.name: r for r in parse_regions()}
            selected_regions = [by_name[n] for n in selected_region_names if n in by_name]

        domain_obj: Optional[Dict[str, Any]] = None
        project_ids: Optional[List[str]] = None
        name_by_id: Dict[str, str] = {}
        if domain:
            domain_obj = openstack.find_domain(domain)
            if domain_obj is None:
                return ReportResult(
                    columns=[],
                    rows=[],
                    metadata={"error": f"Domain not found: {domain!r}"},
                    filename_stem=f"instance-leaderboard-{domain}",
                )
            projects = openstack.list_projects(domain_obj["id"])
            project_ids = [p["id"] for p in projects]
            name_by_id = {p["id"]: p["name"] for p in projects}

        # per-project counts: {project_id: {vm_state: count}}
        per_project: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        sql, args = _counts_sql(project_ids)
        for region in selected_regions:
            for cell in openstack.list_cells(region):
                for r in query(region, cell, sql, args):
                    pid = r["project_id"]
                    state = r.get("vm_state") or "unknown"
                    per_project[pid][state] += int(r["n"] or 0)

        # Resolve project names. For a domain-scoped run we already have
        # name_by_id; for an unscoped run fetch every project referenced.
        if project_ids is None and per_project:
            pid_list = list(per_project.keys())
            # Use keystone.project directly (cheap for hundreds/thousands).
            from openstack_bi.config import keystone_region, keystone_db
            ph = ",".join(["%s"] * len(pid_list))
            rows = query(
                keystone_region(), keystone_db(),
                f"SELECT id, name, domain_id FROM project WHERE id IN ({ph})",
                pid_list,
            )
            name_by_id = {r["id"]: r["name"] for r in rows}

        # Build output rows.
        rows_out: List[Dict[str, Any]] = []
        for pid, by_state in per_project.items():
            active = by_state.get("active", 0)
            stopped = by_state.get("stopped", 0)
            shelved = by_state.get("shelved", 0) + by_state.get("shelved_offloaded", 0)
            errored = by_state.get("error", 0)
            other = sum(
                n for s, n in by_state.items()
                if s not in ("active", "stopped", "shelved", "shelved_offloaded", "error")
            )
            total = active + stopped + shelved + errored + other
            rows_out.append({
                "project_id": pid,
                "project_name": name_by_id.get(pid, "(unknown)"),
                "active": active,
                "stopped": stopped,
                "shelved": shelved,
                "error": errored,
                "other": other,
                "total": total,
            })

        rows_out.sort(key=lambda r: (-r["active"], -r["total"], r["project_name"] or ""))

        # Build top-N chart on active counts.
        top_n = max(1, int(top or 20))
        top_rows = rows_out[:top_n]
        chart = ChartSpec(
            kind="bar",
            title=f"Top {len(top_rows)} projects by active instances",
            x_label="Project",
            y_label="Instances",
            x_categories=[r["project_name"] for r in top_rows],
            series=[
                {"label": "active", "data": [r["active"] for r in top_rows]},
                {"label": "stopped", "data": [r["stopped"] for r in top_rows]},
                {"label": "shelved", "data": [r["shelved"] for r in top_rows]},
                {"label": "error", "data": [r["error"] for r in top_rows]},
                {"label": "other", "data": [r["other"] for r in top_rows]},
            ],
        )

        metadata = {
            "domain": domain_obj["name"] if domain_obj else "(all domains)",
            "regions": ", ".join(r.name for r in selected_regions) or "(none)",
            "projects_with_instances": len(rows_out),
            "total_instances": sum(r["total"] for r in rows_out),
            "total_active": sum(r["active"] for r in rows_out),
        }

        stem_bits = ["instance-leaderboard"]
        if domain_obj:
            stem_bits.append(domain_obj["name"])
        else:
            stem_bits.append("all-domains")
        stem_bits.append(
            "-".join(r.name for r in selected_regions)
            if selected_region_names is not None else "all-regions"
        )

        return ReportResult(
            columns=[
                ("project_name", "Project"),
                ("project_id", "Project ID"),
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


REPORT = InstanceLeaderboardReport()
