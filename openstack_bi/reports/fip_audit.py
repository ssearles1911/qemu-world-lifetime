"""Floating IP audit: unbound FIPs by project, per region.

Unbound = `floatingips.fixed_port_id IS NULL`. These are allocated to a
project but not associated with an instance — typically wasted if they've
sat unbound for a while.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from openstack_bi import openstack
from openstack_bi.config import (
    keystone_db,
    keystone_region,
    neutron_db,
    parse_regions,
)
from openstack_bi.db import query
from openstack_bi.util import humanize

from .base import ChartSpec, Param, Report, ReportResult


def _domain_choices() -> List[Tuple[str, str]]:
    return [("", "— all domains —")] + [
        (d["name"], f'{d["name"]} ({d["project_count"]} project(s))')
        for d in openstack.list_domains()
    ]


def _region_choices() -> List[Tuple[str, str]]:
    return [(r.name, r.name) for r in parse_regions()]


class FipAuditReport(Report):
    id = "fip_audit"
    name = "Floating IP audit"
    description = (
        "Unbound floating IPs per project, per region. Sorted oldest-first "
        "so long-idle allocations surface at the top. Top-N chart of projects "
        "holding the most unbound FIPs."
    )
    params = [
        Param(name="older_than", label="Older than (days)", kind="int",
              default=0, placeholder="0",
              help="Only show FIPs allocated more than this many days ago. 0 = all unbound."),
        Param(name="domain", label="Domain", kind="select",
              choices=_domain_choices, default="",
              help="Scope to this Keystone domain. Empty = all domains."),
        Param(name="regions", label="Regions", kind="multiselect",
              choices=_region_choices,
              help="Which regions to span. Empty = all configured regions."),
        Param(name="top", label="Top N (chart)", kind="int", default=20,
              placeholder="20",
              help="Projects in the top-N chart. Table always shows everything."),
    ]

    def run(
        self,
        older_than: Optional[int] = 0,
        domain: Optional[str] = None,
        regions: Optional[List[str]] = None,
        top: Optional[int] = 20,
        **_: Any,
    ) -> ReportResult:
        days_n = max(0, int(older_than or 0))
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
                    filename_stem=f"fip-audit-{domain}",
                )
            projects = openstack.list_projects(domain_obj["id"])
            project_filter = [p["id"] for p in projects]
            name_by_id = {p["id"]: p["name"] for p in projects}

        where_clauses = ["fip.fixed_port_id IS NULL"]
        args: List[Any] = []
        if project_filter is not None:
            ph = ",".join(["%s"] * len(project_filter))
            where_clauses.append(f"fip.project_id IN ({ph})")
            args.extend(project_filter)
        if days_n > 0:
            where_clauses.append(
                "fip.standard_attr_id IN ("
                "  SELECT id FROM standardattributes "
                "  WHERE created_at < (UTC_TIMESTAMP() - INTERVAL %s DAY))"
            )
            args.append(days_n)
        where_sql = " AND ".join(where_clauses)

        # Neutron's floatingips has no direct created_at; age comes from
        # standardattributes via standard_attr_id. That mapping exists in
        # all Neutron releases newer than Ocata. We try to join it and fall
        # back gracefully if the column layout differs.
        sql_with_timestamps = f"""
            SELECT fip.id,
                   fip.floating_ip_address,
                   fip.floating_network_id,
                   fip.router_id,
                   fip.project_id,
                   fip.status,
                   sa.created_at AS created_at,
                   sa.updated_at AS updated_at,
                   n.name AS network_name
            FROM floatingips fip
            LEFT JOIN standardattributes sa ON sa.id = fip.standard_attr_id
            LEFT JOIN networks n ON n.id = fip.floating_network_id
            WHERE {where_sql}
            ORDER BY sa.created_at ASC
        """

        sql_no_timestamps = f"""
            SELECT fip.id,
                   fip.floating_ip_address,
                   fip.floating_network_id,
                   fip.router_id,
                   fip.project_id,
                   fip.status,
                   NULL AS created_at,
                   NULL AS updated_at,
                   n.name AS network_name
            FROM floatingips fip
            LEFT JOIN networks n ON n.id = fip.floating_network_id
            WHERE {where_sql.replace('fip.standard_attr_id IN (  SELECT id FROM standardattributes   WHERE created_at < (UTC_TIMESTAMP() - INTERVAL %s DAY))', '1=1')}
        """

        now = datetime.now(timezone.utc).replace(tzinfo=None)
        rows_out: List[Dict[str, Any]] = []

        for region in selected_regions:
            try:
                rows = query(region, neutron_db(), sql_with_timestamps, args)
            except Exception:  # noqa: BLE001
                # Some Neutron releases don't have standardattributes; drop
                # the timestamp join and age filter.
                rows = query(region, neutron_db(), sql_no_timestamps, args if days_n == 0 else args[:-1])
            for r in rows:
                created_at = r.get("created_at")
                age_seconds = (now - created_at).total_seconds() if created_at else None
                rows_out.append({
                    "region": region.name,
                    "project_id": r["project_id"],
                    "project_name": name_by_id.get(r["project_id"], "(unknown)"),
                    "fip_address": r["floating_ip_address"],
                    "floating_network_id": r["floating_network_id"],
                    "network_name": r.get("network_name"),
                    "router_id": r.get("router_id"),
                    "status": r.get("status"),
                    "created_at": created_at,
                    "age": humanize(age_seconds) if age_seconds is not None else "-",
                    "age_days": round(age_seconds / 86400, 1) if age_seconds is not None else None,
                })

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

        rows_out.sort(key=lambda r: (-(r.get("age_days") or 0), r["project_name"] or ""))

        # Top-N chart by unbound count per project.
        by_project: Dict[str, int] = {}
        for r in rows_out:
            by_project[r["project_name"]] = by_project.get(r["project_name"], 0) + 1
        ranked = sorted(by_project.items(), key=lambda kv: -kv[1])
        top_n = max(1, int(top or 20))
        top_rows = ranked[:top_n]
        chart = ChartSpec(
            kind="bar",
            title=f"Top {len(top_rows)} projects by unbound FIPs",
            x_label="Project",
            y_label="Unbound FIPs",
            x_categories=[name for name, _ in top_rows],
            series=[{"label": "unbound", "data": [n for _, n in top_rows]}],
        )

        metadata = {
            "domain": domain_obj["name"] if domain_obj else "(all domains)",
            "older_than_days": days_n,
            "regions": ", ".join(r.name for r in selected_regions) or "(none)",
            "total_unbound_fips": len(rows_out),
            "projects_holding_unbound": len(by_project),
        }

        stem_bits = ["fip-audit"]
        if domain_obj:
            stem_bits.append(domain_obj["name"])
        if days_n > 0:
            stem_bits.append(f"{days_n}d")
        stem_bits.append(
            "-".join(r.name for r in selected_regions)
            if selected_region_names is not None else "all-regions"
        )

        return ReportResult(
            columns=[
                ("project_name", "Project"),
                ("region", "Region"),
                ("fip_address", "FIP"),
                ("network_name", "Network"),
                ("router_id", "Router"),
                ("status", "Status"),
                ("age", "Age"),
                ("age_days", "Age (days)"),
                ("created_at", "Allocated at"),
            ],
            rows=rows_out,
            groupings=["project_name"],
            charts=[chart] if top_rows else [],
            metadata=metadata,
            filename_stem="-".join(stem_bits),
        )


REPORT = FipAuditReport()
