"""Per-subnet drill-down of floating-IP-bearing external networks.

Where `fip_pools` summarizes one row per (region, network), this report
expands each external network into its constituent subnets and IP
allocation pool ranges. One row per (region, network, subnet, pool range).

For each row:

    cidr           — subnet CIDR
    gateway_ip     — subnet gateway, if any
    ip_version     — 4 or 6 (FIPs are 4 in practice; 6 is included for completeness)
    pool_first_ip  — first IP of this allocation pool range (or '' if no pool)
    pool_last_ip   — last IP of this allocation pool range
    pool_size      — addresses inclusive in the range
    used           — rows in `ipallocations` whose ip falls inside this range
                     (matches `openstack ip availability` per-subnet `used_ips`,
                     summed back across pools)
    free           — pool_size - used (clamped >= 0)
    pct_used       — used / pool_size, %

`ipallocations` rows belong to *all* ports on the network, not only
floating-IP ports — that's intentional: the allocation pool is shared
across router gateways, FIP ports, and any other ports plugged into the
external network, so "what's actually consumed" is the right number.

Subnets with no declared `ipallocationpools` rows still produce a row
(pool_size=0, status="no declared pool") so they're visible.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from openstack_bi.config import neutron_db, parse_regions
from openstack_bi.db import query
from openstack_bi.util import format_region_errors, safe_for_each_region

from .base import ChartSpec, Param, Report, ReportResult


def _region_choices() -> List[Tuple[str, str]]:
    return [(r.name, r.name) for r in parse_regions()]


def _collect_region(region) -> List[Dict[str, Any]]:
    network_ids_rows = query(
        region, neutron_db(),
        "SELECT DISTINCT floating_network_id AS id FROM floatingips",
    )
    network_ids = [r["id"] for r in network_ids_rows if r["id"]]
    if not network_ids:
        return []

    ph = ",".join(["%s"] * len(network_ids))

    network_names = {
        r["id"]: r["name"]
        for r in query(
            region, neutron_db(),
            f"SELECT id, name FROM networks WHERE id IN ({ph})",
            list(network_ids),
        )
    }

    subnets = query(
        region, neutron_db(),
        f"""
        SELECT id, network_id, name, cidr, gateway_ip, ip_version
        FROM subnets
        WHERE network_id IN ({ph})
        ORDER BY network_id, ip_version, cidr
        """,
        list(network_ids),
    )
    if not subnets:
        return []
    subnet_ids = [s["id"] for s in subnets]
    sph = ",".join(["%s"] * len(subnet_ids))

    pools = query(
        region, neutron_db(),
        f"""
        SELECT subnet_id, first_ip, last_ip,
               INET_ATON(first_ip) AS first_num,
               INET_ATON(last_ip)  AS last_num,
               INET_ATON(last_ip) - INET_ATON(first_ip) + 1 AS pool_size
        FROM ipallocationpools
        WHERE subnet_id IN ({sph})
        ORDER BY subnet_id, INET_ATON(first_ip)
        """,
        list(subnet_ids),
    )

    # Allocations bucketed by subnet — we'll do the in-range filtering in Python
    # so a subnet with multiple pools doesn't require N queries.
    allocs = query(
        region, neutron_db(),
        f"""
        SELECT subnet_id, ip_address,
               INET_ATON(ip_address) AS ip_num
        FROM ipallocations
        WHERE subnet_id IN ({sph})
        """,
        list(subnet_ids),
    )
    allocs_by_subnet: Dict[str, List[int]] = {}
    for a in allocs:
        allocs_by_subnet.setdefault(a["subnet_id"], []).append(int(a["ip_num"]))

    pools_by_subnet: Dict[str, List[Dict[str, Any]]] = {}
    for p in pools:
        pools_by_subnet.setdefault(p["subnet_id"], []).append(p)

    rows: List[Dict[str, Any]] = []
    for s in subnets:
        subnet_pools = pools_by_subnet.get(s["id"], [])
        subnet_allocs = allocs_by_subnet.get(s["id"], [])
        if not subnet_pools:
            rows.append({
                "region": region.name,
                "network_id": s["network_id"],
                "network_name": network_names.get(s["network_id"]) or "",
                "subnet_id": s["id"],
                "subnet_name": s.get("name") or "",
                "cidr": s["cidr"],
                "gateway_ip": s.get("gateway_ip") or "",
                "ip_version": s["ip_version"],
                "pool_first_ip": "",
                "pool_last_ip": "",
                "pool_size": 0,
                "used": len(subnet_allocs),
                "free": 0,
                "pct_used": "-",
                "status": "no declared pool",
            })
            continue

        for p in subnet_pools:
            # INET_ATON returns NULL for IPv6 — pool_size/first_num/last_num
            # all come back as None in that case.
            pool_size = int(p["pool_size"]) if p.get("pool_size") is not None else 0
            first_num = p.get("first_num")
            last_num = p.get("last_num")

            if first_num is not None and last_num is not None:
                used = sum(
                    1 for n in subnet_allocs if first_num <= n <= last_num
                )
            else:
                # IPv6 (or any case INET_ATON couldn't resolve): fall back to
                # "all allocs in subnet" attribution if there's only one pool;
                # otherwise we can't disambiguate, so report 0.
                used = len(subnet_allocs) if len(subnet_pools) == 1 else 0

            free = max(pool_size - used, 0) if pool_size else 0
            pct = round((used / pool_size) * 100, 1) if pool_size else None

            if pool_size == 0:
                status = "no declared pool"
            elif pct is not None and pct >= 95:
                status = "near-full"
            elif pct is not None and pct >= 80:
                status = "warn"
            else:
                status = "ok"

            rows.append({
                "region": region.name,
                "network_id": s["network_id"],
                "network_name": network_names.get(s["network_id"]) or "",
                "subnet_id": s["id"],
                "subnet_name": s.get("name") or "",
                "cidr": s["cidr"],
                "gateway_ip": s.get("gateway_ip") or "",
                "ip_version": s["ip_version"],
                "pool_first_ip": p["first_ip"],
                "pool_last_ip": p["last_ip"],
                "pool_size": pool_size,
                "used": used,
                "free": free,
                "pct_used": pct if pct is not None else "-",
                "status": status,
            })

    return rows


class FipSubnetsReport(Report):
    id = "fip_subnets"
    name = "Floating IP subnets"
    description = (
        "Per-subnet, per-allocation-pool drill-down of the external networks "
        "referenced by floating IPs. One row per (region, network, subnet, "
        "pool range) with CIDR, gateway, range bounds, used vs. free. "
        "Per-region TOTAL row at the bottom of each region's section."
    )
    params = [
        Param(name="regions", label="Regions", kind="multiselect",
              choices=_region_choices,
              help="Which regions to span. Empty = all configured regions."),
    ]

    def run(
        self,
        regions: List[str] = None,  # type: ignore[assignment]
        **_: Any,
    ) -> ReportResult:
        selected_region_names = regions or None
        all_regions = parse_regions()
        if selected_region_names is None:
            selected_regions = all_regions
        else:
            by_name = {r.name: r for r in all_regions}
            selected_regions = [by_name[n] for n in selected_region_names if n in by_name]

        results, region_errors = safe_for_each_region(selected_regions, _collect_region)
        rows_out: List[Dict[str, Any]] = []
        for _, region_rows in results:
            rows_out.extend(region_rows)

        rows_out.sort(key=lambda r: (
            r["region"],
            r["network_name"] or r["network_id"],
            r["cidr"],
            r["pool_first_ip"],
        ))

        chart_rows = [r for r in rows_out if r["pool_size"]]
        # Snapshot real-row stats *before* totals get inlined into rows_out
        # so the metadata block doesn't double-count the synthesized rows.
        real_rows = list(rows_out)

        # Per-region totals: one synthesized row at the end of each region's
        # block. Numeric columns sum; pct_used is recomputed from the totals;
        # non-summable string columns are blanked. Keeping the grouping at
        # `["region"]` keeps the total row visually attached to its region in
        # both the web and CLI renderers.
        rows_with_totals: List[Dict[str, Any]] = []
        current_region: str = ""
        region_buf: List[Dict[str, Any]] = []

        def _flush_region(buf: List[Dict[str, Any]]) -> None:
            if not buf:
                return
            rows_with_totals.extend(buf)
            pool_size_sum = sum(r["pool_size"] for r in buf)
            used_sum = sum(r["used"] for r in buf)
            free_sum = sum(r["free"] for r in buf)
            pct = (
                round((used_sum / pool_size_sum) * 100, 1)
                if pool_size_sum else "-"
            )
            rows_with_totals.append({
                "region": buf[0]["region"],
                "network_id": "",
                "network_name": "TOTAL",
                "subnet_id": "",
                "subnet_name": "",
                "cidr": "",
                "gateway_ip": "",
                "ip_version": "",
                "pool_first_ip": "",
                "pool_last_ip": "",
                "pool_size": pool_size_sum,
                "used": used_sum,
                "free": free_sum,
                "pct_used": pct,
                "status": "",
            })

        for r in rows_out:
            if r["region"] != current_region:
                _flush_region(region_buf)
                region_buf = []
                current_region = r["region"]
            region_buf.append(r)
        _flush_region(region_buf)
        rows_out = rows_with_totals
        chart_labels = [
            f"{r['region']} / {r['network_name'] or r['network_id'][:8]} / "
            f"{r['cidr']} [{r['pool_first_ip']}-{r['pool_last_ip']}]"
            for r in chart_rows
        ]
        chart = ChartSpec(
            kind="stacked_bar",
            title="FIP subnet pool utilization",
            x_label="Region / network / subnet [pool]",
            y_label="IPs",
            x_categories=chart_labels,
            series=[
                {"label": "used", "data": [r["used"] for r in chart_rows]},
                {"label": "free", "data": [r["free"] for r in chart_rows]},
            ],
        )

        metadata = {
            "regions": ", ".join(r.name for r in selected_regions) or "(none)",
            "subnet_pool_rows": len(real_rows),
            "distinct_networks": len({(r["region"], r["network_id"]) for r in real_rows}),
            "distinct_subnets": len({(r["region"], r["subnet_id"]) for r in real_rows}),
            "total_pool_size": sum(r["pool_size"] for r in real_rows),
            "total_used": sum(
                r["used"] for r in real_rows if r["pool_size"]
            ),
            "region_errors": format_region_errors(region_errors),
        }

        stem_bits = ["fip-subnets"]
        stem_bits.append(
            "-".join(r.name for r in selected_regions)
            if selected_region_names is not None else "all-regions"
        )

        return ReportResult(
            columns=[
                ("region", "Region"),
                ("network_name", "Network"),
                ("subnet_name", "Subnet"),
                ("cidr", "CIDR"),
                ("gateway_ip", "Gateway"),
                ("ip_version", "IPv"),
                ("pool_first_ip", "Pool first"),
                ("pool_last_ip", "Pool last"),
                ("pool_size", "Pool size"),
                ("used", "Used"),
                ("free", "Free"),
                ("pct_used", "% used"),
                ("status", "Status"),
                ("network_id", "Network ID"),
                ("subnet_id", "Subnet ID"),
            ],
            rows=rows_out,
            groupings=["region"],
            charts=[chart] if chart_rows else [],
            metadata=metadata,
            filename_stem="-".join(stem_bits),
        )


REPORT = FipSubnetsReport()
