"""Stale Cinder snapshots: one row per snapshot older than N days.

Glance snapshots are excluded here — they're usually retained intentionally
and cost little. Cinder snapshots are the expensive hygiene target.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from openstack_bi import openstack
from openstack_bi.config import (
    cinder_db,
    keystone_db,
    keystone_region,
    parse_regions,
)
from openstack_bi.db import query
from openstack_bi.util import humanize

from .base import Param, Report, ReportResult


def _domain_choices() -> List[Tuple[str, str]]:
    return [("", "— all domains —")] + [
        (d["name"], f'{d["name"]} ({d["project_count"]} project(s))')
        for d in openstack.list_domains()
    ]


def _region_choices() -> List[Tuple[str, str]]:
    return [(r.name, r.name) for r in parse_regions()]


class StaleSnapshotsReport(Report):
    id = "stale_snapshots"
    name = "Stale snapshots"
    description = (
        "Cinder volume snapshots older than N days (default 90). "
        "Optionally scoped to a single project or domain. "
        "Sorted oldest-first; totals per project are in the metadata."
    )
    params = [
        Param(name="days", label="Older than (days)", kind="int",
              default=90, placeholder="90",
              help="Show snapshots older than this many days."),
        Param(name="domain", label="Domain", kind="select",
              choices=_domain_choices, default="",
              help="Scope to this Keystone domain. Empty = all domains."),
        Param(name="project", label="Project name", kind="string",
              help="Optional — filter further to one project by exact name."),
        Param(name="regions", label="Regions", kind="multiselect",
              choices=_region_choices,
              help="Which regions to span. Empty = all configured regions."),
    ]

    def run(
        self,
        days: Optional[int] = 90,
        domain: Optional[str] = None,
        project: Optional[str] = None,
        regions: Optional[List[str]] = None,
        **_: Any,
    ) -> ReportResult:
        days_n = max(0, int(days or 90))
        selected_region_names = regions or None
        all_regions = parse_regions()
        if selected_region_names is None:
            selected_regions = all_regions
        else:
            by_name = {r.name: r for r in all_regions}
            selected_regions = [by_name[n] for n in selected_region_names if n in by_name]

        project_filter: Optional[List[str]] = None
        name_by_id: Dict[str, str] = {}
        domain_obj: Optional[Dict[str, Any]] = None

        if domain:
            domain_obj = openstack.find_domain(domain)
            if domain_obj is None:
                return ReportResult(
                    columns=[], rows=[],
                    metadata={"error": f"Domain not found: {domain!r}"},
                    filename_stem=f"stale-snapshots-{domain}",
                )
            projects = openstack.list_projects(domain_obj["id"])
            if project:
                match = next((p for p in projects if p["name"] == project), None)
                if match is None:
                    return ReportResult(
                        columns=[], rows=[],
                        metadata={"error": f"Project {project!r} not found in domain {domain_obj['name']!r}"},
                        filename_stem="stale-snapshots-none",
                    )
                project_filter = [match["id"]]
                name_by_id = {match["id"]: match["name"]}
            else:
                project_filter = [p["id"] for p in projects]
                name_by_id = {p["id"]: p["name"] for p in projects}
        elif project:
            rows = query(
                keystone_region(), keystone_db(),
                "SELECT id, name FROM project WHERE is_domain = 0 AND name = %s",
                (project,),
            )
            if not rows:
                return ReportResult(
                    columns=[], rows=[],
                    metadata={"error": f"No project named {project!r} found."},
                    filename_stem=f"stale-snapshots-{project}",
                )
            project_filter = [r["id"] for r in rows]
            name_by_id = {r["id"]: r["name"] for r in rows}

        if project_filter is not None:
            ph = ",".join(["%s"] * len(project_filter))
            sql = f"""
                SELECT id, project_id, volume_id, volume_size,
                       display_name, status, created_at
                FROM snapshots
                WHERE deleted = 0
                  AND project_id IN ({ph})
                  AND created_at < (UTC_TIMESTAMP() - INTERVAL %s DAY)
                ORDER BY created_at ASC
            """
            args: List[Any] = list(project_filter) + [days_n]
        else:
            sql = """
                SELECT id, project_id, volume_id, volume_size,
                       display_name, status, created_at
                FROM snapshots
                WHERE deleted = 0
                  AND created_at < (UTC_TIMESTAMP() - INTERVAL %s DAY)
                ORDER BY created_at ASC
            """
            args = [days_n]

        now = datetime.now(timezone.utc).replace(tzinfo=None)
        rows_out: List[Dict[str, Any]] = []
        for region in selected_regions:
            for r in query(region, cinder_db(), sql, args):
                created_at = r.get("created_at")
                age_seconds = (now - created_at).total_seconds() if created_at else None
                rows_out.append({
                    "snapshot_id": r["id"],
                    "region": region.name,
                    "project_id": r["project_id"],
                    "project_name": name_by_id.get(r["project_id"], "(unknown)"),
                    "volume_id": r["volume_id"],
                    "volume_size_gb": r["volume_size"],
                    "name": r.get("display_name") or "",
                    "status": r.get("status"),
                    "created_at": created_at,
                    "age": humanize(age_seconds),
                    "age_days": round(age_seconds / 86400, 1) if age_seconds is not None else None,
                })

        # If we didn't have name_by_id populated, batch-resolve now so rows
        # aren't just "(unknown)".
        unknown_pids = {r["project_id"] for r in rows_out if r["project_name"] == "(unknown)"}
        if unknown_pids:
            pid_list = list(unknown_pids)
            ph = ",".join(["%s"] * len(pid_list))
            rows = query(
                keystone_region(), keystone_db(),
                f"SELECT id, name FROM project WHERE id IN ({ph})",
                pid_list,
            )
            name_by_id.update({r["id"]: r["name"] for r in rows})
            for r in rows_out:
                if r["project_name"] == "(unknown)":
                    r["project_name"] = name_by_id.get(r["project_id"], "(unknown)")

        rows_out.sort(key=lambda r: (r["project_name"] or "", r.get("age_days") or 0))

        totals_by_project: Dict[str, int] = {}
        for r in rows_out:
            totals_by_project[r["project_name"]] = totals_by_project.get(r["project_name"], 0) + 1
        top_offenders = ", ".join(
            f"{name}({n})" for name, n in sorted(totals_by_project.items(), key=lambda kv: -kv[1])[:5]
        )

        metadata = {
            "domain": domain_obj["name"] if domain_obj else "(all domains)",
            "project": project or "(any)",
            "regions": ", ".join(r.name for r in selected_regions) or "(none)",
            "older_than_days": days_n,
            "total_stale_snapshots": len(rows_out),
            "total_stale_gb": sum(int(r["volume_size_gb"] or 0) for r in rows_out),
            "top_offenders": top_offenders or "(none)",
        }

        stem_bits = ["stale-snapshots", f"{days_n}d"]
        if domain_obj:
            stem_bits.append(domain_obj["name"])
        if project:
            stem_bits.append(project)
        stem_bits.append(
            "-".join(r.name for r in selected_regions)
            if selected_region_names is not None else "all-regions"
        )

        return ReportResult(
            columns=[
                ("project_name", "Project"),
                ("region", "Region"),
                ("snapshot_id", "Snapshot ID"),
                ("volume_id", "Volume ID"),
                ("volume_size_gb", "GB"),
                ("name", "Name"),
                ("status", "Status"),
                ("age", "Age"),
                ("age_days", "Age (days)"),
                ("created_at", "Created at"),
            ],
            rows=rows_out,
            groupings=["project_name"],
            metadata=metadata,
            filename_stem="-".join(stem_bits),
        )


REPORT = StaleSnapshotsReport()
