"""Neutron (networking) service module — L3-agent maintenance support.

Two halves, deliberately in one module because the router-rescheduling
tool needs both:

* **DB queries (read).** L3 agents and their router bindings, read
  straight from the `neutron` schema on the region's MariaDB replica.
  Fast and cheap — this is the default path for listing.
* **API actions (state-changing).** Rescheduling a router between L3
  agents goes through the Neutron API on behalf of the logged-in
  Keystone user, the same token-scoped pattern as `nova.py`.

`nova.py` is API-only because Nova reads already come from the per-cell
DBs queried elsewhere; Neutron has no such existing read path, so its
DB queries live here alongside the actions.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from keystoneauth1 import exceptions as ksa_exc
from keystoneauth1.session import Session

from .config import Region, neutron_db
from .db import query

# Neutron's default `agent_down_time`: an L3 agent whose last heartbeat
# is older than this is treated as down. Operators can raise the real
# value in neutron.conf, but 75s is the upstream default and a safe
# display threshold.
AGENT_DOWN_SECONDS = 75

# `agents.agent_type` value for the L3 agent. DHCP agents are
# 'DHCP agent', Open vSwitch agents 'Open vSwitch agent', etc.
L3_AGENT_TYPE = "L3 agent"
DHCP_AGENT_TYPE = "DHCP agent"


# --- DB queries (read; fast path) -------------------------------------------

def list_l3_agents(region: Region) -> List[Dict[str, Any]]:
    """Every L3 agent in `region`, with a derived alive flag and the
    count of routers currently scheduled to it.

    Liveness is computed the way Neutron does it: an agent is alive when
    its last heartbeat is newer than `AGENT_DOWN_SECONDS`.
    """
    rows = query(
        region, neutron_db(),
        """
        SELECT
            a.id,
            a.host,
            a.admin_state_up,
            a.availability_zone,
            TIMESTAMPDIFF(SECOND, a.heartbeat_timestamp, UTC_TIMESTAMP())
                AS heartbeat_age,
            (SELECT COUNT(*) FROM routerl3agentbindings rb
             WHERE rb.l3_agent_id = a.id) AS router_count
        FROM agents a
        WHERE a.agent_type = %s
        ORDER BY a.host
        """,
        (L3_AGENT_TYPE,),
    )
    agents: List[Dict[str, Any]] = []
    for r in rows:
        age = r.get("heartbeat_age")
        age = int(age) if age is not None else None
        agents.append({
            "id": r["id"],
            "host": r.get("host") or "",
            "admin_state_up": bool(r.get("admin_state_up")),
            "availability_zone": r.get("availability_zone") or "",
            "heartbeat_age": age,
            "alive": age is not None and age < AGENT_DOWN_SECONDS,
            "router_count": int(r.get("router_count") or 0),
        })
    return agents


def routers_on_l3_agent(region: Region, agent_id: str) -> List[Dict[str, Any]]:
    """Routers currently scheduled to one L3 agent, with their HA /
    distributed (DVR) flags and the gateway-port IP(s) — the router's
    external/WAN address — so the operator has context before moving it.

    A router's WAN interface is its gateway port (`routers.gw_port_id`)
    on the external network; the correlated subquery pulls that port's
    fixed IP(s) from `ipallocations`. An internal-only router has no
    gateway port, so `gateway_ips` is NULL there.
    """
    rows = query(
        region, neutron_db(),
        """
        SELECT
            r.id,
            r.name,
            r.status,
            r.admin_state_up,
            r.project_id,
            COALESCE(rea.ha, 0)          AS ha,
            COALESCE(rea.distributed, 0) AS distributed,
            (SELECT GROUP_CONCAT(ia.ip_address ORDER BY ia.ip_address
                                 SEPARATOR ', ')
             FROM ipallocations ia
             WHERE ia.port_id = r.gw_port_id) AS gateway_ips
        FROM routerl3agentbindings rb
        JOIN routers r ON r.id = rb.router_id
        LEFT JOIN router_extra_attributes rea ON rea.router_id = r.id
        WHERE rb.l3_agent_id = %s
        ORDER BY r.name, r.id
        """,
        (agent_id,),
    )
    routers: List[Dict[str, Any]] = []
    for r in rows:
        routers.append({
            "id": r["id"],
            "name": r.get("name") or "(unnamed)",
            "status": r.get("status") or "",
            "admin_state_up": bool(r.get("admin_state_up")),
            "project_id": r.get("project_id") or "",
            "ha": bool(r.get("ha")),
            "distributed": bool(r.get("distributed")),
            "gateway_ip": r.get("gateway_ips") or "",
        })
    return routers


def list_dhcp_agents(region: Region) -> List[Dict[str, Any]]:
    """Every DHCP agent in `region` with a derived alive flag and the
    count of networks currently scheduled to it.

    Companion to `list_l3_agents` — same shape, different `agent_type`
    filter and binding table (`networkdhcpagentbindings` instead of
    `routerl3agentbindings`).
    """
    rows = query(
        region, neutron_db(),
        """
        SELECT
            a.id,
            a.host,
            a.admin_state_up,
            a.availability_zone,
            TIMESTAMPDIFF(SECOND, a.heartbeat_timestamp, UTC_TIMESTAMP())
                AS heartbeat_age,
            (SELECT COUNT(*) FROM networkdhcpagentbindings nb
             WHERE nb.dhcp_agent_id = a.id) AS network_count
        FROM agents a
        WHERE a.agent_type = %s
        ORDER BY a.host
        """,
        (DHCP_AGENT_TYPE,),
    )
    agents: List[Dict[str, Any]] = []
    for r in rows:
        age = r.get("heartbeat_age")
        age = int(age) if age is not None else None
        agents.append({
            "id": r["id"],
            "host": r.get("host") or "",
            "admin_state_up": bool(r.get("admin_state_up")),
            "availability_zone": r.get("availability_zone") or "",
            "heartbeat_age": age,
            "alive": age is not None and age < AGENT_DOWN_SECONDS,
            "network_count": int(r.get("network_count") or 0),
        })
    return agents


def networks_on_dhcp_agent(
    region: Region, agent_id: str
) -> List[Dict[str, Any]]:
    """Networks currently scheduled to one DHCP agent.

    Used by the DHCP maintenance tool: the operator picks an agent,
    sees what it is serving, and reschedules the networks to a healthy
    target agent before taking the network node down. Segment metadata
    is included so the operator can recognise familiar networks at a
    glance.
    """
    rows = query(
        region, neutron_db(),
        """
        SELECT
            n.id, n.name, n.status, n.admin_state_up, n.project_id,
            GROUP_CONCAT(DISTINCT ns.network_type
                         ORDER BY ns.network_type SEPARATOR ', ')
                         AS network_types,
            GROUP_CONCAT(DISTINCT ns.segmentation_id
                         ORDER BY ns.segmentation_id SEPARATOR ', ')
                         AS segment_ids
        FROM networkdhcpagentbindings nb
        JOIN networks n ON n.id = nb.network_id
        LEFT JOIN networksegments ns ON ns.network_id = n.id
        WHERE nb.dhcp_agent_id = %s
        GROUP BY n.id, n.name, n.status, n.admin_state_up, n.project_id
        ORDER BY n.name, n.id
        """,
        (agent_id,),
    )
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append({
            "id": r["id"],
            "name": r.get("name") or "(unnamed)",
            "status": r.get("status") or "",
            "admin_state_up": bool(r.get("admin_state_up")),
            "project_id": r.get("project_id") or "",
            "network_types": r.get("network_types") or "",
            "segment_ids": r.get("segment_ids") or "",
        })
    return out


def dhcp_bindings_index(
    region: Region,
) -> Dict[str, List[Dict[str, str]]]:
    """Every DHCP binding in `region`, grouped by `network_id`.

    One bulk query; callers iterate per-network. Used both by the
    move-page enrichment (which needs to know where a network's
    *other* bindings live) and by the redundancy report (which
    classifies networks by how their bindings spread across hosts).
    """
    rows = query(
        region, neutron_db(),
        """
        SELECT nb.network_id, a.id AS agent_id, a.host
        FROM networkdhcpagentbindings nb
        JOIN agents a ON a.id = nb.dhcp_agent_id
        WHERE a.agent_type = %s
        ORDER BY a.host
        """,
        (DHCP_AGENT_TYPE,),
    )
    out: Dict[str, List[Dict[str, str]]] = {}
    for r in rows:
        out.setdefault(r["network_id"], []).append({
            "agent_id": r["agent_id"],
            "host": r.get("host") or "",
        })
    return out


def dhcp_redundancy(region: Region) -> List[Dict[str, Any]]:
    """Per-network DHCP-binding classification in `region`.

    Returns rows shaped:

        {id, name, project_id, hosts: [host,...], status}

    where `status` is one of:

        * `colocated` — 2+ bindings, all on the same physical host;
          the network *thinks* it has DHCP redundancy but in reality
          loses DHCP entirely if that one host goes down.
        * `single`    — exactly one binding; no redundancy by design.
        * `redundant` — 2+ bindings spread across distinct hosts.

    Sorted with colocated first (the operationally interesting case),
    then single, then redundant — so the audit page surfaces the bug
    states up top without forcing the operator to scroll.
    """
    rows = query(
        region, neutron_db(),
        """
        SELECT n.id, n.name, n.project_id, a.host
        FROM networkdhcpagentbindings nb
        JOIN agents a ON a.id = nb.dhcp_agent_id
        JOIN networks n ON n.id = nb.network_id
        WHERE a.agent_type = %s
        ORDER BY n.name, a.host
        """,
        (DHCP_AGENT_TYPE,),
    )
    by_net: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        info = by_net.setdefault(r["id"], {
            "id": r["id"],
            "name": r.get("name") or "(unnamed)",
            "project_id": r.get("project_id") or "",
            "hosts": [],
        })
        info["hosts"].append(r.get("host") or "")

    def _status(hosts: List[str]) -> str:
        if len(hosts) >= 2 and len(set(hosts)) == 1:
            return "colocated"
        if len(hosts) == 1:
            return "single"
        return "redundant"

    out: List[Dict[str, Any]] = []
    for info in by_net.values():
        info["status"] = _status(info["hosts"])
        out.append(info)

    severity = {"colocated": 0, "single": 1, "redundant": 2}
    out.sort(key=lambda x: (severity[x["status"]], x["name"]))
    return out


def router_wan_ips(
    region: Region, router_ids: List[str]
) -> Dict[str, Dict[str, Any]]:
    """Resolve routers (by id) to their gateway-port WAN IP(s).

    Unlike `routers_on_l3_agent`, this looks routers up by id regardless
    of which L3 agent currently hosts them — so it stays correct for a
    router that was just moved, and a move (which only rewrites
    `routerl3agentbindings`) cannot make a replica's answer wrong.

    Returns `{router_id: {id, name, wan_ips: [...], gateway_ip: str}}`;
    a router with no gateway port has `wan_ips == []`. An empty
    `router_ids` short-circuits without a query.
    """
    ids = [rid for rid in dict.fromkeys(router_ids) if rid]
    if not ids:
        return {}
    placeholders = ",".join(["%s"] * len(ids))
    rows = query(
        region, neutron_db(),
        f"""
        SELECT
            r.id,
            r.name,
            (SELECT GROUP_CONCAT(ia.ip_address ORDER BY ia.ip_address
                                 SEPARATOR ', ')
             FROM ipallocations ia
             WHERE ia.port_id = r.gw_port_id) AS gateway_ips
        FROM routers r
        WHERE r.id IN ({placeholders})
        """,
        ids,
    )
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        joined = r.get("gateway_ips") or ""
        wan_ips = [ip.strip() for ip in joined.split(",") if ip.strip()]
        out[r["id"]] = {
            "id": r["id"],
            "name": r.get("name") or "(unnamed)",
            "wan_ips": wan_ips,
            "gateway_ip": joined,
        }
    return out


def list_vlan_physnets(region: Region) -> List[str]:
    """Physical-network labels that can carry VLAN networks, discovered
    from the ML2 VLAN allocation table."""
    rows = query(
        region, neutron_db(),
        "SELECT DISTINCT physical_network FROM ml2_vlan_allocations "
        "WHERE physical_network IS NOT NULL AND physical_network <> '' "
        "ORDER BY physical_network",
    )
    return [r["physical_network"] for r in rows if r.get("physical_network")]


def vlan_networks_for_project(
    region: Region, project_id: str
) -> List[Dict[str, Any]]:
    """VLAN networks owned by `project_id` in `region` — context shown
    next to the create form so the admin can see what already exists."""
    rows = query(
        region, neutron_db(),
        """
        SELECT n.id, n.name, n.status, n.admin_state_up,
               ns.physical_network, ns.segmentation_id
        FROM networksegments ns
        JOIN networks n ON n.id = ns.network_id
        WHERE ns.network_type = 'vlan'
          AND n.project_id = %s
        ORDER BY ns.segmentation_id
        """,
        (project_id,),
    )
    networks: List[Dict[str, Any]] = []
    for r in rows:
        networks.append({
            "id": r["id"],
            "name": r.get("name") or "(unnamed)",
            "status": r.get("status") or "",
            "admin_state_up": bool(r.get("admin_state_up")),
            "physical_network": r.get("physical_network") or "",
            "segmentation_id": r.get("segmentation_id"),
        })
    return networks


def list_vlan_networks(region: Region) -> List[Dict[str, Any]]:
    """Every VLAN network in `region` with its name, owning project,
    physnet, segmentation id, and state. Sorted by VLAN id.

    Used by the region-wide VLAN list tool. The owning project's name
    and domain are resolved by the caller — the `networks` table only
    has `project_id`.
    """
    rows = query(
        region, neutron_db(),
        """
        SELECT n.id, n.name, n.status, n.admin_state_up, n.project_id,
               ns.physical_network, ns.segmentation_id
        FROM networksegments ns
        JOIN networks n ON n.id = ns.network_id
        WHERE ns.network_type = 'vlan'
        ORDER BY ns.segmentation_id, n.name
        """,
    )
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append({
            "id": r["id"],
            "name": r.get("name") or "(unnamed)",
            "status": r.get("status") or "",
            "admin_state_up": bool(r.get("admin_state_up")),
            "project_id": r.get("project_id") or "",
            "physical_network": r.get("physical_network") or "",
            "segmentation_id": r.get("segmentation_id"),
        })
    return out


def vlan_segment_conflict(
    region: Region, physical_network: str, segmentation_id: int
) -> Optional[Dict[str, str]]:
    """Return the network already bound to this `(physnet, VLAN)` segment,
    or None when the VLAN is free.

    Best-effort pre-check only: it runs against a possibly-lagging
    replica, so Neutron remains the authority — the create call will
    still reject a genuine collision.
    """
    rows = query(
        region, neutron_db(),
        """
        SELECT n.id, n.name
        FROM networksegments ns
        JOIN networks n ON n.id = ns.network_id
        WHERE ns.network_type = 'vlan'
          AND ns.physical_network = %s
          AND ns.segmentation_id = %s
        LIMIT 1
        """,
        (physical_network, segmentation_id),
    )
    if not rows:
        return None
    return {"id": rows[0]["id"], "name": rows[0].get("name") or "(unnamed)"}


def list_build_ports(region: Region) -> List[Dict[str, Any]]:
    """Every Neutron port currently in the BUILD state in `region`.

    A port stuck in BUILD is a common operational signal — broken
    binding, wedged neutron-server task, DHCP/L2 agent failing to wire
    the port up — so the tool lists them oldest-first so the most-stuck
    rows surface at the top.

    The owning project's name is resolved by the caller via Keystone;
    the `ports` table only has `project_id`.
    """
    rows = query(
        region, neutron_db(),
        """
        SELECT
            p.id, p.name, p.network_id, p.mac_address,
            p.admin_state_up, p.status,
            p.device_owner, p.device_id, p.project_id,
            n.name              AS network_name,
            sa.created_at       AS created_at
        FROM ports p
        LEFT JOIN networks n            ON n.id = p.network_id
        LEFT JOIN standardattributes sa ON sa.id = p.standard_attr_id
        WHERE p.status LIKE %s
        ORDER BY sa.created_at, p.id
        """,
        ("%build%",),
    )
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append({
            "id": r["id"],
            "name": r.get("name") or "",
            "network_id": r.get("network_id") or "",
            "network_name": r.get("network_name") or "",
            "mac_address": r.get("mac_address") or "",
            "admin_state_up": bool(r.get("admin_state_up")),
            "status": r.get("status") or "",
            "device_owner": r.get("device_owner") or "",
            "device_id": r.get("device_id") or "",
            "project_id": r.get("project_id") or "",
            "created_at": r.get("created_at"),
        })
    return out


def list_networks(region: Region) -> List[Dict[str, Any]]:
    """Every Neutron network in `region`.

    Used by the network-agents tool — the operator searches/picks a
    network and then expands it to see its DHCP + L3 agents. Includes
    a comma-joined list of segment types and ids so multi-segment
    networks show all their segments at a glance.

    Owning project's name is resolved by the caller via Keystone.
    """
    rows = query(
        region, neutron_db(),
        """
        SELECT n.id, n.name, n.status, n.admin_state_up, n.project_id,
               GROUP_CONCAT(DISTINCT ns.network_type
                            ORDER BY ns.network_type SEPARATOR ', ')
                            AS network_types,
               GROUP_CONCAT(DISTINCT ns.segmentation_id
                            ORDER BY ns.segmentation_id SEPARATOR ', ')
                            AS segment_ids
        FROM networks n
        LEFT JOIN networksegments ns ON ns.network_id = n.id
        GROUP BY n.id, n.name, n.status, n.admin_state_up, n.project_id
        ORDER BY n.name, n.id
        """,
    )
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append({
            "id": r["id"],
            "name": r.get("name") or "",
            "status": r.get("status") or "",
            "admin_state_up": bool(r.get("admin_state_up")),
            "project_id": r.get("project_id") or "",
            "network_types": r.get("network_types") or "",
            "segment_ids": r.get("segment_ids") or "",
        })
    return out


def dhcp_agents_by_network(
    region: Region,
) -> Dict[str, List[Dict[str, Any]]]:
    """All DHCP-agent bindings in `region`, grouped by `network_id`.

    One bulk query so the network-list page can pre-render every
    expand-row server-side, dropping the per-click HTTP round trip the
    JSON endpoint used to need. Returns `{}` for networks with no
    bindings (callers index with `.get(net_id, [])`).
    """
    rows = query(
        region, neutron_db(),
        """
        SELECT nb.network_id, a.id, a.host, a.admin_state_up,
               TIMESTAMPDIFF(SECOND, a.heartbeat_timestamp, UTC_TIMESTAMP())
                   AS heartbeat_age
        FROM networkdhcpagentbindings nb
        JOIN agents a ON a.id = nb.dhcp_agent_id
        ORDER BY a.host
        """,
    )
    out: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        age = r.get("heartbeat_age")
        age = int(age) if age is not None else None
        out.setdefault(r["network_id"], []).append({
            "id": r["id"],
            "host": r.get("host") or "",
            "admin_state_up": bool(r.get("admin_state_up")),
            "heartbeat_age": age,
            "alive": age is not None and age < AGENT_DOWN_SECONDS,
        })
    return out


def l3_agents_by_network(
    region: Region,
) -> Dict[str, List[Dict[str, Any]]]:
    """All L3-router-interface ports in `region` resolved to their
    (router, agent) bindings, grouped by the port's `network_id`.

    Companion to `dhcp_agents_by_network`: together they let the
    network-list page pre-render every expand-row with one query each,
    instead of the previous JSON endpoint per click. One result row per
    (network, router, agent) — an HA router naturally shows up once per
    agent it is scheduled to.
    """
    rows = query(
        region, neutron_db(),
        """
        SELECT p.network_id        AS network_id,
               a.id                AS agent_id,
               a.host              AS agent_host,
               a.admin_state_up    AS agent_admin_state_up,
               TIMESTAMPDIFF(SECOND, a.heartbeat_timestamp, UTC_TIMESTAMP())
                   AS heartbeat_age,
               r.id                AS router_id,
               r.name              AS router_name,
               p.device_owner      AS interface_role
        FROM ports p
        JOIN routerl3agentbindings rb ON rb.router_id = p.device_id
        JOIN agents a ON a.id = rb.l3_agent_id
        LEFT JOIN routers r ON r.id = p.device_id
        WHERE p.device_owner IN (
            'network:router_interface',
            'network:router_interface_distributed',
            'network:ha_router_replicated_interface',
            'network:router_gateway'
        )
        ORDER BY r.name, a.host
        """,
    )
    out: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        age = r.get("heartbeat_age")
        age = int(age) if age is not None else None
        out.setdefault(r["network_id"], []).append({
            "agent_id": r["agent_id"],
            "agent_host": r.get("agent_host") or "",
            "admin_state_up": bool(r.get("agent_admin_state_up")),
            "heartbeat_age": age,
            "alive": age is not None and age < AGENT_DOWN_SECONDS,
            "router_id": r.get("router_id") or "",
            "router_name": r.get("router_name") or "",
            "interface_role": r.get("interface_role") or "",
        })
    return out


# --- API actions (Keystone-token; state-changing) ---------------------------

class NeutronError(Exception):
    """A Neutron API call failed; the message is safe to show the user."""


def _endpoint_filter(region: str) -> Dict[str, str]:
    return {
        "service_type": "network",
        "interface": "public",
        "region_name": region,
    }


def _error_message(resp) -> str:
    """Pull a human-readable message out of a Neutron error response.

    Neutron errors are `{"NeutronError": {"type", "message", "detail"}}`
    on modern releases; some return a bare string under the same key.
    """
    try:
        body = resp.json()
    except Exception:  # noqa: BLE001
        body = None
    if isinstance(body, dict):
        err = body.get("NeutronError")
        if isinstance(err, dict) and err.get("message"):
            return f"Neutron API: {err['message']}"
        if isinstance(err, str) and err:
            return f"Neutron API: {err}"
    return f"Neutron API returned HTTP {resp.status_code}."


def _request(session: Session, region: str, method: str, path: str, **kwargs):
    """Issue one Neutron request, raising NeutronError on any failure."""
    try:
        resp = session.request(
            path, method,
            endpoint_filter=_endpoint_filter(region),
            raise_exc=False,
            **kwargs,
        )
    except ksa_exc.EndpointNotFound:
        raise NeutronError(
            f"No network endpoint for region {region!r} in the token's "
            "service catalog."
        )
    except ksa_exc.ClientException as exc:  # connection / SSL / etc.
        raise NeutronError(f"Could not reach the network API: {exc}")
    if resp.status_code >= 400:
        raise NeutronError(_error_message(resp))
    return resp


def add_router_to_agent(
    session: Session, region: str, agent_id: str, router_id: str
) -> None:
    """Schedule `router_id` onto the L3 agent `agent_id`."""
    _request(
        session, region, "POST",
        f"/v2.0/agents/{agent_id}/l3-routers",
        json={"router_id": router_id},
    )


def remove_router_from_agent(
    session: Session, region: str, agent_id: str, router_id: str
) -> None:
    """Unschedule `router_id` from the L3 agent `agent_id`."""
    _request(
        session, region, "DELETE",
        f"/v2.0/agents/{agent_id}/l3-routers/{router_id}",
    )


def get_router(session: Session, region: str, router_id: str) -> Dict[str, Any]:
    """Return basic detail for one router: id, name, status."""
    resp = _request(session, region, "GET", f"/v2.0/routers/{router_id}")
    router = (resp.json() or {}).get("router", {}) or {}
    return {
        "id": router.get("id") or router_id,
        "name": router.get("name") or "",
        "status": router.get("status") or "",
    }


def move_router(
    session: Session,
    region: str,
    router_id: str,
    source_agent_id: str,
    target_agent_id: str,
) -> None:
    """Reschedule a router from one L3 agent to another.

    Remove-from-source, then add-to-target: safe for legacy, HA, and DVR
    routers, and matches the documented `openstack network agent
    remove/add router` procedure. A legacy router has a brief reschedule
    gap, inherent to moving a single-homed router.

    If the add fails after the remove succeeded, the router is left
    unscheduled — the raised error says so explicitly so the operator
    knows to reschedule it by hand.
    """
    remove_router_from_agent(session, region, source_agent_id, router_id)
    try:
        add_router_to_agent(session, region, target_agent_id, router_id)
    except NeutronError as exc:
        raise NeutronError(
            f"removed from the source agent but could NOT be added to the "
            f"target ({exc}). The router is currently unscheduled — "
            f"reschedule it manually."
        )


def add_network_to_dhcp_agent(
    session: Session, region: str, agent_id: str, network_id: str
) -> None:
    """Schedule `network_id` onto the DHCP agent `agent_id`."""
    _request(
        session, region, "POST",
        f"/v2.0/agents/{agent_id}/dhcp-networks",
        json={"network_id": network_id},
    )


def remove_network_from_dhcp_agent(
    session: Session, region: str, agent_id: str, network_id: str
) -> None:
    """Unschedule `network_id` from the DHCP agent `agent_id`."""
    _request(
        session, region, "DELETE",
        f"/v2.0/agents/{agent_id}/dhcp-networks/{network_id}",
    )


def move_network(
    session: Session,
    region: str,
    network_id: str,
    source_agent_id: str,
    target_agent_id: str,
) -> None:
    """Reschedule a network from one DHCP agent to another.

    Remove-from-source then add-to-target — same model as `move_router`,
    and the explicit drain pattern operators use during maintenance
    (preferable to disabling the source agent, which Neutron does *not*
    auto-reschedule from — only dead agents trigger automatic DHCP
    failover). A brief DHCP outage during the gap is normally harmless
    because existing leases keep working, but short lease times or
    mid-boot PXE hosts can feel it.

    If the add fails after the remove succeeded, the network is left
    unscheduled — the raised error says so explicitly so the operator
    knows to reschedule it by hand.
    """
    remove_network_from_dhcp_agent(session, region, source_agent_id, network_id)
    try:
        add_network_to_dhcp_agent(session, region, target_agent_id, network_id)
    except NeutronError as exc:
        raise NeutronError(
            f"removed from the source agent but could NOT be added to the "
            f"target ({exc}). The network is currently unscheduled — "
            f"reschedule it manually."
        )


def create_vlan_network(
    session: Session,
    region: str,
    name: str,
    project_id: str,
    physical_network: str,
    segmentation_id: int,
) -> Dict[str, Any]:
    """Create a provider VLAN network owned by `project_id`.

    The `provider:*` attributes require an admin token — which the
    logged-in Keystone user holds. No subnet is created; the owning
    project's own users add that afterwards.
    """
    resp = _request(
        session, region, "POST", "/v2.0/networks",
        json={"network": {
            "name": name,
            "project_id": project_id,
            "admin_state_up": True,
            "provider:network_type": "vlan",
            "provider:physical_network": physical_network,
            "provider:segmentation_id": int(segmentation_id),
        }},
    )
    net = (resp.json() or {}).get("network", {}) or {}
    return {
        "id": net.get("id") or "",
        "name": net.get("name") or name,
        "project_id": net.get("project_id") or project_id,
    }
