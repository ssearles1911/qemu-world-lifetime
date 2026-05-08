"""SPLA-licensed instances.

Identifies active VMs whose boot volume's Glance metadata `image_name`
matches a configurable LIKE pattern (default `%SPLA%`). The original
workflow this replaces is a hand-run cross-DB join against Nova,
Cinder, and an auxiliary MAAS-style schema; this report makes the
filters parametric and surfaces project + domain context, host
information, and per-region totals so it's actually scannable.

Filters surfaced in the form:
  • image-name LIKE pattern (the SPLA matcher itself)
  • host name include / exclude LIKE patterns
  • exclude host aggregates (multiselect of every aggregate in any region)
  • regions

Optional, configured by an admin under /admin/schemas:
  • `spla_managed_schema` — when set, the report adds
    `NOT EXISTS (SELECT 1 FROM <schema>.project WHERE project_id = ...)`
    to filter out projects that are managed elsewhere (the original
    `maas.project` exclusion).
"""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlencode

from openstack_bi import config_db, openstack
from openstack_bi.config import (
    Region,
    cinder_db,
    keystone_db,
    keystone_region,
    nova_api_db,
    parse_regions,
)
from openstack_bi.db import query
from openstack_bi.util import format_region_errors, safe_for_each_region

from .base import Param, Report, ReportResult


# Identifier validation — schema names can't be parameterized in
# MariaDB, so before we splice `spla_managed_schema` into a query we
# require it to look like a SQL identifier. Admin-controlled value, but
# defense in depth.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _region_choices() -> List[Tuple[str, str]]:
    return [(r.name, r.name) for r in parse_regions()]


def _aggregate_choices() -> List[Tuple[str, str]]:
    """Union of aggregate names across every configured region.

    The form de-dupes by name; the same name appearing in multiple
    regions resolves to the union of host members at run time.
    """
    seen: Dict[str, set] = defaultdict(set)
    for row in openstack.list_aggregates():
        seen[row["name"]].add(row["region"])
    out: List[Tuple[str, str]] = []
    for name in sorted(seen):
        regions = sorted(seen[name])
        if len(regions) == len(parse_regions()):
            label = name
        else:
            label = f"{name} ({', '.join(regions)})"
        out.append((name, label))
    return out


def _resolve_project_directory(project_ids: Sequence[str]) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Return `(project_name_by_id, domain_name_by_id)` for the given ids.

    Two passes: project rows first (to learn each project's domain_id),
    then a single follow-up to resolve those domains to names.
    """
    if not project_ids:
        return {}, {}
    pid_list = list({pid for pid in project_ids if pid})
    proj_ph = ",".join(["%s"] * len(pid_list))
    proj_rows = query(
        keystone_region(), keystone_db(),
        f"SELECT id, name, domain_id FROM project WHERE id IN ({proj_ph})",
        pid_list,
    )
    project_name_by_id = {r["id"]: r["name"] for r in proj_rows}
    project_to_domain_id = {r["id"]: r["domain_id"] for r in proj_rows}

    domain_ids = list({d for d in project_to_domain_id.values() if d})
    domain_name_by_pid: Dict[str, str] = {}
    if domain_ids:
        dom_ph = ",".join(["%s"] * len(domain_ids))
        dom_rows = query(
            keystone_region(), keystone_db(),
            f"SELECT id, name FROM project WHERE id IN ({dom_ph}) AND is_domain = 1",
            domain_ids,
        )
        domain_name_by_id = {r["id"]: r["name"] for r in dom_rows}
        for pid, did in project_to_domain_id.items():
            if did and did in domain_name_by_id:
                domain_name_by_pid[pid] = domain_name_by_id[did]
    return project_name_by_id, domain_name_by_pid


def _build_clauses(
    cinder_schema: str,
    image_pattern: str,
    host_include: Optional[str],
    host_exclude: Optional[str],
    excluded_hosts: Sequence[str],
    maas_schema: Optional[str],
) -> Tuple[str, List[Any]]:
    """Compose the per-cell SQL + params for one cell scan.

    Schema names are spliced (validated identifiers); everything else
    rides through pymysql parameter binding.
    """
    args: List[Any] = []
    sql_parts: List[str] = [
        f"""
        WITH spla_volumes AS (
            SELECT vgm.volume_id, MIN(vgm.value) AS image_name
            FROM {cinder_schema}.volume_glance_metadata vgm
            WHERE vgm.deleted = 0
              AND vgm.`key` = 'image_name'
              AND vgm.value LIKE %s
            GROUP BY vgm.volume_id
        )
        SELECT
            n.created_at, n.id, n.hostname, n.display_name,
            n.vcpus, n.memory_mb, n.uuid, n.project_id,
            n.vm_state, n.host,
            sv.image_name
        FROM instances n
        JOIN {cinder_schema}.volume_attachment va ON va.instance_uuid = n.uuid
        JOIN {cinder_schema}.volumes v
          ON v.id = va.volume_id
         AND v.attach_status = 'attached'
         AND v.bootable = 1
        JOIN spla_volumes sv ON sv.volume_id = v.id
        WHERE n.deleted = 0
          AND n.vm_state = 'active'
        """
    ]
    args.append(image_pattern)

    if host_include:
        sql_parts.append("AND n.host LIKE %s")
        args.append(host_include)
    if host_exclude:
        sql_parts.append("AND n.host NOT LIKE %s")
        args.append(host_exclude)
    if excluded_hosts:
        ph = ",".join(["%s"] * len(excluded_hosts))
        sql_parts.append(f"AND n.host NOT IN ({ph})")
        args.extend(excluded_hosts)
    if maas_schema:
        sql_parts.append(
            f"AND NOT EXISTS (SELECT 1 FROM {maas_schema}.project mp "
            f"WHERE mp.project_id = n.project_id)"
        )

    sql_parts.append("ORDER BY n.created_at DESC")
    return "\n".join(sql_parts), args


class SplaInstancesReport(Report):
    id = "spla_instances"
    name = "SPLA-licensed instances"
    description = (
        "Active VMs whose boot volume's Glance image_name matches a "
        "configurable LIKE pattern (default '%SPLA%'). Host name include / "
        "exclude patterns and host-aggregate exclusion narrow further. "
        "Per-region vCPU and memory rollups appear as metadata cards; "
        "click an instance UUID to drill into its action history."
    )
    category = "Licensing"
    scope_to_projects = False  # cross-tenant compliance report; admin-only
    params = [
        Param(
            name="image_pattern", label="Image name LIKE", kind="string",
            default="%SPLA%",
            placeholder="%SPLA%",
            help="LIKE pattern matched against the boot volume's "
                 "image_name in volume_glance_metadata. Required.",
        ),
        Param(
            name="regions", label="Regions", kind="multiselect",
            choices=_region_choices,
            help="Which regions to span. Empty = all configured regions.",
        ),
        Param(
            name="host_include_pattern", label="Host name LIKE", kind="string",
            placeholder="(empty = no host include filter)",
            advanced=True,
            help="Restrict to compute hosts whose name matches this LIKE pattern.",
        ),
        Param(
            name="host_exclude_pattern", label="Host name NOT LIKE", kind="string",
            default="%mkvm%",
            placeholder="%mkvm%",
            advanced=True,
            help="Exclude compute hosts whose name matches this LIKE pattern.",
        ),
        Param(
            name="exclude_aggregates", label="Exclude host aggregates",
            kind="multiselect",
            choices=_aggregate_choices,
            advanced=True,
            help="Hosts that belong to any selected aggregate are excluded.",
        ),
    ]

    def run(
        self,
        image_pattern: Optional[str] = None,
        regions: Optional[List[str]] = None,
        host_include_pattern: Optional[str] = None,
        host_exclude_pattern: Optional[str] = None,
        exclude_aggregates: Optional[List[str]] = None,
        **_: Any,
    ) -> ReportResult:
        pattern = (image_pattern or "").strip() or "%SPLA%"
        host_include = (host_include_pattern or "").strip() or None
        host_exclude = (host_exclude_pattern or "").strip() or None
        excluded_aggregates = list(exclude_aggregates or [])

        selected_region_names = regions or None
        if selected_region_names is None:
            selected_regions = parse_regions()
        else:
            by_name = {r.name: r for r in parse_regions()}
            selected_regions = [
                by_name[n] for n in selected_region_names if n in by_name
            ]

        # Optional MAAS-style "managed projects" exclusion. The schema
        # name is admin-supplied, so validate before splicing.
        maas_schema = (config_db.web_setting("spla_managed_schema", "") or "").strip()
        if maas_schema and not _IDENTIFIER_RE.match(maas_schema):
            return ReportResult(
                columns=[], rows=[],
                metadata={
                    "error": (
                        f"Configured spla_managed_schema {maas_schema!r} is "
                        "not a valid SQL identifier. Fix it under "
                        "Admin → Schemas, or clear the field."
                    )
                },
                filename_stem="spla-instances-bad-config",
            )

        cinder_schema = cinder_db()
        if not _IDENTIFIER_RE.match(cinder_schema):
            return ReportResult(
                columns=[], rows=[],
                metadata={"error": f"Cinder schema name {cinder_schema!r} is not a valid identifier."},
                filename_stem="spla-instances-bad-config",
            )

        # Per region: resolve the union of hosts that should be excluded
        # because they belong to any selected aggregate.
        def _collect(region: Region) -> List[Dict[str, Any]]:
            try:
                excluded_hosts = openstack.aggregate_hosts(region, excluded_aggregates)
            except Exception:  # noqa: BLE001 — surfaced via region_errors
                excluded_hosts = []
                # Re-raise so safe_for_each_region records the error;
                # otherwise we'd silently miss aggregate config issues.
                raise
            sql, args = _build_clauses(
                cinder_schema=cinder_schema,
                image_pattern=pattern,
                host_include=host_include,
                host_exclude=host_exclude,
                excluded_hosts=excluded_hosts,
                maas_schema=maas_schema or None,
            )
            cell_rows: List[Dict[str, Any]] = []
            for cell in openstack.list_cells(region):
                rows = query(region, cell, sql, args)
                for r in rows:
                    r["region"] = region.name
                    r["cell_db"] = cell
                cell_rows.extend(rows)
            return cell_rows

        results, region_errors = safe_for_each_region(selected_regions, _collect)
        instance_rows: List[Dict[str, Any]] = [
            r for _region, region_rows in results for r in region_rows
        ]

        # Resolve project + domain names from keystone in one pass.
        project_name_by_id, domain_name_by_pid = _resolve_project_directory(
            [r.get("project_id") for r in instance_rows]
        )

        # Build output rows + drill-down link.
        rows_out: List[Dict[str, Any]] = []
        for r in instance_rows:
            pid = r.get("project_id")
            uuid = r.get("uuid") or ""
            row: Dict[str, Any] = {
                "created_at": r.get("created_at"),
                "region": r.get("region"),
                "domain_name": domain_name_by_pid.get(pid, ""),
                "project_name": project_name_by_id.get(pid, ""),
                "project_id": pid,
                "display_name": r.get("display_name"),
                "hostname": r.get("hostname"),
                "host": r.get("host"),
                "vm_state": r.get("vm_state"),
                "vcpus": int(r.get("vcpus") or 0),
                "memory_mb": int(r.get("memory_mb") or 0),
                "image_name": r.get("image_name"),
                "uuid": uuid,
            }
            if uuid:
                qs = urlencode({"instance_uuid": uuid})
                row["uuid_link"] = f"/report/instance_history?{qs}"
            rows_out.append(row)

        # Per-region totals: count, sum vCPU, sum memory (GB).
        region_totals: Dict[str, Dict[str, int]] = defaultdict(
            lambda: {"count": 0, "vcpus": 0, "memory_mb": 0}
        )
        for r in rows_out:
            region_name = r.get("region") or "(unknown)"
            t = region_totals[region_name]
            t["count"] += 1
            t["vcpus"] += int(r.get("vcpus") or 0)
            t["memory_mb"] += int(r.get("memory_mb") or 0)

        metadata: Dict[str, Any] = {
            "image_pattern": pattern,
            "host_include": host_include or "(none)",
            "host_exclude": host_exclude or "(none)",
            "excluded_aggregates": ", ".join(excluded_aggregates) or "(none)",
            "managed_projects_schema": maas_schema or "(unset; not excluded)",
            "regions": ", ".join(r.name for r in selected_regions) or "(none)",
            "total_instances": len(rows_out),
            "total_vcpus": sum(r["vcpus"] for r in rows_out),
            "total_memory_gb": sum(r["memory_mb"] for r in rows_out) // 1024,
            "region_errors": format_region_errors(region_errors),
        }
        # One card per region with its own roll-up.
        for region_name, totals in sorted(region_totals.items()):
            metadata[f"region_{region_name}"] = (
                f"{totals['count']} VMs · "
                f"{totals['vcpus']} vCPU · "
                f"{totals['memory_mb'] // 1024} GB"
            )

        stem_bits = ["spla-instances"]
        clean = re.sub(r"[^A-Za-z0-9]+", "-", pattern).strip("-").lower() or "all"
        stem_bits.append(clean)
        if selected_region_names is not None:
            stem_bits.append("-".join(r.name for r in selected_regions))
        else:
            stem_bits.append("all-regions")

        return ReportResult(
            columns=[
                ("created_at", "Created (UTC)"),
                ("region", "Region"),
                ("domain_name", "Domain"),
                ("project_name", "Project"),
                ("display_name", "Instance"),
                ("hostname", "Hostname"),
                ("host", "Compute host"),
                ("vm_state", "State"),
                ("vcpus", "vCPU"),
                ("memory_mb", "Memory (MB)"),
                ("image_name", "Matched image"),
                ("uuid", "Instance UUID"),
            ],
            rows=rows_out,
            metadata=metadata,
            filename_stem="-".join(stem_bits),
        )


REPORT = SplaInstancesReport()
