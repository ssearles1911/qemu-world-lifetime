"""Cinder volumes resized (extend_volume) in the last N days.

Data source: `cinder.messages` with `action_id = 'extend_volume'`. The
messages table has short retention by default (Cinder expires rows
daily), so this report only sees resizes inside that horizon. If your
deployment purges messages quickly, expect a short look-back window.
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
from openstack_bi.util import format_region_errors, humanize, safe_for_each_region

from .base import Param, Report, ReportResult


def _domain_choices() -> List[Tuple[str, str]]:
    return [("", "— all domains —")] + [
        (d["name"], f'{d["name"]} ({d["project_count"]} project(s))')
        for d in openstack.list_domains()
    ]


def _region_choices() -> List[Tuple[str, str]]:
    return [(r.name, r.name) for r in parse_regions()]


class VolumeResizesReport(Report):
    id = "volume_resizes"
    name = "Volume resizes"
    description = (
        "Cinder volume extend events in the last N days (from `messages` "
        "where action_id='extend_volume'). Grouped by project. "
        "Retention in `cinder.messages` can be short — older resizes may "
        "not appear."
    )
    params = [
        Param(name="days", label="Last (days)", kind="int",
              default=30, placeholder="30",
              help="Show resize events from the last N days."),
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
        days: Optional[int] = 30,
        domain: Optional[str] = None,
        project: Optional[str] = None,
        regions: Optional[List[str]] = None,
        **_: Any,
    ) -> ReportResult:
        days_n = max(0, int(days or 30))
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
                    filename_stem=f"volume-resizes-{domain}",
                )
            projects = openstack.list_projects(domain_obj["id"])
            if project:
                match = next((p for p in projects if p["name"] == project), None)
                if match is None:
                    return ReportResult(
                        columns=[], rows=[],
                        metadata={"error": f"Project {project!r} not found in domain {domain_obj['name']!r}"},
                        filename_stem="volume-resizes-none",
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
                    filename_stem=f"volume-resizes-{project}",
                )
            project_filter = [r["id"] for r in rows]
            name_by_id = {r["id"]: r["name"] for r in rows}

        base_sql = """
            SELECT project_id, resource_uuid AS volume_id, detail_id,
                   message_level, request_id, created_at
            FROM messages
            WHERE action_id = 'extend_volume'
              AND resource_type = 'VOLUME'
              AND created_at >= (UTC_TIMESTAMP() - INTERVAL %s DAY)
        """
        args: List[Any] = [days_n]
        if project_filter is not None:
            ph = ",".join(["%s"] * len(project_filter))
            base_sql += f" AND project_id IN ({ph})"
            args.extend(project_filter)
        base_sql += " ORDER BY created_at DESC"

        now = datetime.now(timezone.utc).replace(tzinfo=None)
        rows_out: List[Dict[str, Any]] = []
        volume_ids_by_region: Dict[str, List[str]] = {}

        def _collect(region):
            return query(region, cinder_db(), base_sql, args)

        results, region_errors = safe_for_each_region(selected_regions, _collect)
        for region, region_rows in results:
            volume_ids_by_region[region.name] = [r["volume_id"] for r in region_rows if r.get("volume_id")]
            for r in region_rows:
                created_at = r.get("created_at")
                age_seconds = (now - created_at).total_seconds() if created_at else None
                rows_out.append({
                    "region": region.name,
                    "project_id": r["project_id"],
                    "project_name": name_by_id.get(r["project_id"], "(unknown)"),
                    "volume_id": r.get("volume_id"),
                    "detail": r.get("detail_id") or "",
                    "message_level": r.get("message_level"),
                    "request_id": r.get("request_id"),
                    "created_at": created_at,
                    "age": humanize(age_seconds) if age_seconds is not None else "-",
                })

        # Fill current size from cinder.volumes for displayed volumes.
        for region in selected_regions:
            vids = volume_ids_by_region.get(region.name, [])
            if not vids:
                continue
            unique = list(set(vids))
            ph = ",".join(["%s"] * len(unique))
            try:
                size_rows = query(
                    region, cinder_db(),
                    f"SELECT id, size, display_name FROM volumes WHERE id IN ({ph})",
                    unique,
                )
            except Exception:  # noqa: BLE001
                continue
            size_map = {r["id"]: r for r in size_rows}
            for r in rows_out:
                if r["region"] == region.name and r["volume_id"] in size_map:
                    info = size_map[r["volume_id"]]
                    r["current_size_gb"] = info.get("size")
                    r["volume_name"] = info.get("display_name") or ""

        unknown_pids = {r["project_id"] for r in rows_out if r["project_name"] == "(unknown)"}
        if unknown_pids:
            pid_list = list(unknown_pids)
            ph = ",".join(["%s"] * len(pid_list))
            lookup = query(
                keystone_region(), keystone_db(),
                f"SELECT id, name FROM project WHERE id IN ({ph})",
                pid_list,
            )
            name_by_id.update({r["id"]: r["name"] for r in lookup})
            for r in rows_out:
                if r["project_name"] == "(unknown)":
                    r["project_name"] = name_by_id.get(r["project_id"], "(unknown)")

        rows_out.sort(key=lambda r: (r["project_name"] or "", r.get("created_at") or datetime.min))

        metadata = {
            "domain": domain_obj["name"] if domain_obj else "(all domains)",
            "project": project or "(any)",
            "regions": ", ".join(r.name for r in selected_regions) or "(none)",
            "last_days": days_n,
            "total_resize_events": len(rows_out),
            "note": "Source: cinder.messages action_id='extend_volume'. "
                    "Retention in that table can be short — older resizes may not appear.",
            "region_errors": format_region_errors(region_errors),
        }

        stem_bits = ["volume-resizes", f"{days_n}d"]
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
                ("volume_id", "Volume ID"),
                ("volume_name", "Volume"),
                ("current_size_gb", "Size (GB)"),
                ("created_at", "Event (UTC)"),
                ("age", "Age"),
                ("detail", "Detail"),
                ("message_level", "Level"),
                ("request_id", "Request ID"),
            ],
            rows=rows_out,
            groupings=["project_name"],
            metadata=metadata,
            filename_stem="-".join(stem_bits),
        )


REPORT = VolumeResizesReport()
