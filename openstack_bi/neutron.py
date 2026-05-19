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

from typing import Any, Dict, List

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
    distributed (DVR) flags so the operator can see the router type
    before moving it."""
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
            COALESCE(rea.distributed, 0) AS distributed
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
        })
    return routers


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
