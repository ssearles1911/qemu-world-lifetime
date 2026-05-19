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
