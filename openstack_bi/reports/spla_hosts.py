"""SPLA-licensed compute hosts.

Lists the physical compute nodes carrying the SPLA placement trait —
the hosts an SPLA core licence is billed against — with their CPU
topology (vCPUs and derived physical cores), CPU model, in-service
date, and per-region rollups. This is the host-side companion to the
`spla_instances` report: that one counts the VMs, this one counts the
tin they run on.

Identifying SPLA hosts
----------------------
A host is SPLA-licensed when its Nova `compute_nodes` row shares a UUID
with a Placement `resource_providers` row that carries a specific
trait. The trait is named (default ``CUSTOM_MS_SPLA``) and resolved to its
numeric id *per region* via `placement.traits`, because Placement
assigns trait ids per deployment — the same name need not be the same
id in DTW and CVG. An optional ``trait_id`` override skips name
resolution and applies one id to every region; that reproduces the
hand-run script this replaces, which hardcoded ``trait_id = 289``.

Cores
-----
SPLA is licensed per physical core, but `compute_nodes.vcpus` counts
hardware threads. Cores are derived as ``vcpus / threads_per_core``
(default 2 for SMT / hyper-threaded Intel/AMD hosts; set it to 1 for
hosts with SMT disabled).

Options surfaced in the form:
  • SPLA trait name (the matcher itself)
  • regions
  • threads per core (advanced)
  • trait id override (advanced)
  • include hosts whose nova-compute service is disabled (advanced)

`placement` is registered as a service schema (migration 0003); its
name is overridable under Admin -> Schemas.
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from openstack_bi import openstack
from openstack_bi.config import Region, parse_regions, placement_db
from openstack_bi.db import query
from openstack_bi.util import format_region_errors, safe_for_each_region

from .base import Param, Report, ReportResult


# Schema names can't be parameterized in MariaDB, so we require the
# Placement schema name to look like a SQL identifier before splicing
# it into a query. Admin-controlled value, but defense in depth — same
# guard the spla_instances report applies to the Cinder schema.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

DEFAULT_TRAIT_NAME = "CUSTOM_MS_SPLA"
DEFAULT_THREADS_PER_CORE = 2


def _region_choices() -> List[Tuple[str, str]]:
    return [(r.name, r.name) for r in parse_regions()]


def _cpu_model(cpu_info: Any) -> str:
    """Extract a human-readable CPU model from a `compute_nodes.cpu_info`
    JSON blob, falling back to the vendor, then to a marker."""
    if not cpu_info:
        return ""
    try:
        info = json.loads(cpu_info)
    except (json.JSONDecodeError, TypeError):
        return "unknown"
    if not isinstance(info, dict):
        return "unknown"
    return str(info.get("model") or info.get("vendor") or "")


def _in_service(created_at: Any) -> str:
    """Format `compute_nodes.created_at` as a plain YYYY-MM-DD date."""
    if isinstance(created_at, datetime):
        return created_at.strftime("%Y-%m-%d")
    return str(created_at) if created_at else ""


def _host_sql(placement_schema: str, include_disabled: bool) -> str:
    """Per-cell query: SPLA-traited compute nodes joined to their
    nova-compute service row. The single ``%s`` placeholder is the
    resolved placement trait id.

    `placement_schema` is spliced (validated identifier); the trait id
    rides through pymysql parameter binding.
    """
    # Filtering the service join on binary + deleted keeps `s.disabled`
    # unambiguous (it's the *compute* service we care about) and stops a
    # stale deleted service record from double-counting a host. The
    # original hand-run script omitted these, at the cost of both.
    disabled_clause = "" if include_disabled else "AND s.disabled = 0"
    return f"""
        SELECT
            cn.hypervisor_hostname AS hostname,
            cn.vcpus               AS vcpus,
            cn.cpu_info            AS cpu_info,
            cn.created_at          AS created_at,
            s.disabled             AS disabled
        FROM compute_nodes cn
        JOIN services s
          ON s.host = cn.host
         AND s.binary = 'nova-compute'
         AND s.deleted = 0
        WHERE cn.deleted = 0
          AND cn.uuid IN (
              SELECT rp.uuid
              FROM {placement_schema}.resource_providers rp
              JOIN {placement_schema}.resource_provider_traits rpt
                ON rpt.resource_provider_id = rp.id
              WHERE rpt.trait_id = %s
          )
          {disabled_clause}
        ORDER BY cn.hypervisor_hostname
    """


class SplaHostsReport(Report):
    id = "spla_hosts"
    name = "SPLA-licensed hosts"
    description = (
        "Physical compute nodes carrying the SPLA placement trait "
        "(default 'CUSTOM_MS_SPLA'), with vCPU count, derived physical "
        "cores, CPU model, and in-service date. Trait name is resolved "
        "to its id per region; a trait-id override is available. "
        "Per-region host / vCPU / core rollups appear as metadata cards."
    )
    category = "Licensing"
    scope_to_projects = False  # infrastructure inventory; admin-only
    params = [
        Param(
            name="trait_name", label="SPLA trait name", kind="string",
            default=DEFAULT_TRAIT_NAME,
            placeholder=DEFAULT_TRAIT_NAME,
            help="Placement trait that marks a host as SPLA-licensed. "
                 "Resolved to its numeric id per region via "
                 "`placement.traits`. Ignored when a trait-id override "
                 "is supplied.",
        ),
        Param(
            name="regions", label="Regions", kind="multiselect",
            choices=_region_choices,
            help="Which regions to span. Empty = all configured regions.",
        ),
        Param(
            name="threads_per_core", label="Threads per core", kind="int",
            default=DEFAULT_THREADS_PER_CORE,
            advanced=True,
            help="Divisor applied to vCPUs to derive physical cores. 2 for "
                 "SMT / hyper-threaded hosts (the usual case); 1 if SMT is "
                 "disabled. Must be >= 1.",
        ),
        Param(
            name="trait_id", label="Trait id override", kind="int",
            placeholder="(resolve from trait name)",
            advanced=True,
            help="Skip name resolution and filter on this raw placement "
                 "trait id in every region. Reproduces the legacy "
                 "hardcoded trait_id behaviour.",
        ),
        Param(
            name="include_disabled", label="Include disabled compute services",
            kind="bool", default=False,
            advanced=True,
            help="Off by default: only hosts whose nova-compute service is "
                 "enabled are counted. Check to also include hosts whose "
                 "service is disabled.",
        ),
    ]

    def run(
        self,
        trait_name: Optional[str] = None,
        regions: Optional[List[str]] = None,
        threads_per_core: Optional[int] = DEFAULT_THREADS_PER_CORE,
        trait_id: Optional[int] = None,
        include_disabled: bool = False,
        **_: Any,
    ) -> ReportResult:
        # Re-apply the Param defaults here too: the CLI and web layers
        # inject them, but a direct run() call (tests, scheduled jobs)
        # may not. An empty trait name falls back to CUSTOM_MS_SPLA; when a
        # trait-id override is given the name is unused anyway.
        name = (trait_name or "").strip() or DEFAULT_TRAIT_NAME
        override = trait_id  # None unless the operator set it explicitly
        include_disabled = bool(include_disabled)

        threads = threads_per_core if threads_per_core is not None else DEFAULT_THREADS_PER_CORE
        try:
            threads = int(threads)
        except (TypeError, ValueError):
            threads = 0
        if threads < 1:
            return ReportResult(
                columns=[], rows=[],
                metadata={"error": "Threads per core must be an integer >= 1."},
                filename_stem="spla-hosts-bad-config",
            )

        placement_schema = placement_db()
        if not _IDENTIFIER_RE.match(placement_schema):
            return ReportResult(
                columns=[], rows=[],
                metadata={
                    "error": (
                        f"Placement schema name {placement_schema!r} is not a "
                        "valid SQL identifier. Fix it under Admin -> Schemas."
                    )
                },
                filename_stem="spla-hosts-bad-config",
            )

        selected_region_names = regions or None
        if selected_region_names is None:
            selected_regions = parse_regions()
        else:
            by_name = {r.name: r for r in parse_regions()}
            selected_regions = [
                by_name[n] for n in selected_region_names if n in by_name
            ]

        host_sql = _host_sql(placement_schema, include_disabled)

        # Per-region trait resolution: region name -> resolved trait id,
        # or None when the named trait does not exist in that region's
        # Placement DB. Regions that failed entirely never land here —
        # they show up in `region_errors` instead.
        trait_resolution: Dict[str, Optional[int]] = {}

        def _collect(region: Region) -> List[Dict[str, Any]]:
            if override is not None:
                tid: int = int(override)
            else:
                rows = query(
                    region, placement_schema,
                    "SELECT id FROM traits WHERE name = %s",
                    (name,),
                )
                if not rows:
                    trait_resolution[region.name] = None
                    return []
                tid = int(rows[0]["id"])
            trait_resolution[region.name] = tid

            host_rows: List[Dict[str, Any]] = []
            for cell in openstack.list_cells(region):
                for row in query(region, cell, host_sql, (tid,)):
                    row["region"] = region.name
                    host_rows.append(row)
            return host_rows

        results, region_errors = safe_for_each_region(selected_regions, _collect)
        raw_rows: List[Dict[str, Any]] = [
            r for _region, region_rows in results for r in region_rows
        ]

        rows_out: List[Dict[str, Any]] = []
        for r in raw_rows:
            vcpus = int(r.get("vcpus") or 0)
            rows_out.append({
                "hostname": r.get("hostname") or "",
                "region": r.get("region") or "",
                "vcpus": vcpus,
                "cores": vcpus // threads,
                "cpu_model": _cpu_model(r.get("cpu_info")),
                "in_service": _in_service(r.get("created_at")),
                "service_state": "disabled" if r.get("disabled") else "enabled",
            })
        rows_out.sort(key=lambda r: (r["region"], r["hostname"]))

        # Per-region rollups: host count, vCPU sum, core sum.
        region_totals: Dict[str, Dict[str, int]] = defaultdict(
            lambda: {"hosts": 0, "vcpus": 0, "cores": 0}
        )
        for r in rows_out:
            t = region_totals[r["region"] or "(unknown)"]
            t["hosts"] += 1
            t["vcpus"] += r["vcpus"]
            t["cores"] += r["cores"]

        # How the trait resolved, for the metadata block.
        if override is not None:
            trait_label = f"(trait id {override})"
            trait_ids_label = f"{override} (override, all regions)"
        else:
            trait_label = name
            parts: List[str] = []
            not_found: List[str] = []
            for region in selected_regions:
                if region.name not in trait_resolution:
                    continue  # region failed; see region_errors
                resolved = trait_resolution[region.name]
                if resolved is None:
                    parts.append(f"{region.name}=not found")
                    not_found.append(region.name)
                else:
                    parts.append(f"{region.name}={resolved}")
            trait_ids_label = ", ".join(parts) or "(none resolved)"

        metadata: Dict[str, Any] = {
            "trait": trait_label,
            "trait_ids": trait_ids_label,
            "threads_per_core": threads,
            "disabled_services": "included" if include_disabled else "excluded",
            "regions": ", ".join(r.name for r in selected_regions) or "(none)",
            "total_hosts": len(rows_out),
            "total_vcpus": sum(r["vcpus"] for r in rows_out),
            "total_cores": sum(r["cores"] for r in rows_out),
            "region_errors": format_region_errors(region_errors),
        }
        if override is None and not_found:
            metadata["trait_not_found"] = (
                f"{', '.join(not_found)} — no trait named {name!r} in "
                "that region's Placement DB"
            )
        for region_name, totals in sorted(region_totals.items()):
            metadata[f"region_{region_name}"] = (
                f"{totals['hosts']} hosts · "
                f"{totals['vcpus']} vCPU · "
                f"{totals['cores']} cores"
            )

        stem_bits = ["spla-hosts"]
        if override is not None:
            stem_bits.append(f"trait-{override}")
        else:
            clean = re.sub(r"[^A-Za-z0-9]+", "-", name).strip("-").lower() or "trait"
            stem_bits.append(clean)
        if selected_region_names is not None:
            stem_bits.append("-".join(r.name for r in selected_regions) or "no-regions")
        else:
            stem_bits.append("all-regions")

        return ReportResult(
            columns=[
                ("hostname", "Hostname"),
                ("region", "Region"),
                ("vcpus", "vCPUs"),
                ("cores", "Cores"),
                ("cpu_model", "CPU Model"),
                ("in_service", "In Service"),
                ("service_state", "Service"),
            ],
            rows=rows_out,
            groupings=["region"],
            metadata=metadata,
            filename_stem="-".join(stem_bits),
        )


REPORT = SplaHostsReport()
