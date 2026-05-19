"""Admin Tools — operational tasks run as the logged-in Keystone user.

Unlike the `/admin` configuration area (local-admin only), the Tools
section is open to any signed-in user. Listings are DB-backed and work
for anyone; the state-changing actions need a Keystone-scoped token,
exactly like the per-instance Nova actions in `instance_routes`. A
session without a usable token can browse but not move.

First tool: L3 router management — reschedule Neutron virtual routers
between L3 agents, e.g. to drain a network node before maintenance.
"""

from __future__ import annotations

from typing import Dict, List, Optional

from flask import (
    Flask, flash, redirect, render_template, request, session, url_for,
)

from .. import config_db, neutron
from ..auth import token_store
from ..auth.session import current_user, login_required
from ..config import Region, keystone_db, keystone_region, parse_regions
from ..db import query


def register(app: Flask) -> None:
    app.add_url_rule("/tools", view_func=index, endpoint="tools_index")
    app.add_url_rule(
        "/tools/routers", view_func=routers, endpoint="tools_routers",
    )
    app.add_url_rule(
        "/tools/routers/move", view_func=routers_move,
        endpoint="tools_routers_move", methods=("POST",),
    )


def _regions() -> List[Region]:
    try:
        return parse_regions()
    except Exception:  # noqa: BLE001 — no regions configured yet
        return []


def _resolve_region(name: str) -> Optional[Region]:
    for r in _regions():
        if r.name == name:
            return r
    return None


def _keystone_session():
    """The logged-in user's network-capable Session, or None."""
    return token_store.session_for(session.get("ks_token_key"))


def _audit(action: str, detail: str) -> None:
    info = current_user() or {}
    config_db.record_audit(
        info.get("kind") or "?",
        str(info.get("user_id") or info.get("username") or ""),
        action, detail,
    )


def _project_names(project_ids: List[str]) -> Dict[str, str]:
    """Resolve a set of project ids to names via the shared Keystone DB.

    Best-effort: a failure here just leaves the UI showing raw ids.
    """
    ids = sorted({pid for pid in project_ids if pid})
    if not ids:
        return {}
    placeholders = ",".join(["%s"] * len(ids))
    try:
        rows = query(
            keystone_region(), keystone_db(),
            f"SELECT id, name FROM project WHERE id IN ({placeholders})",
            ids,
        )
    except Exception:  # noqa: BLE001
        return {}
    return {r["id"]: r["name"] for r in rows}


@login_required
def index():
    return render_template("tools/index.html")


@login_required
def routers():
    """L3 router management page.

    `?region=` selects the region; `?agent=` drills into one L3 agent's
    routers and reveals the move form.
    """
    regions = _regions()
    if not regions:
        flash("No regions are configured.", "error")
        return render_template(
            "tools/routers.html",
            regions=[], region=None, agents=[], selected_agent=None,
            routers=[], target_agents=[], has_token=False, error=None,
        )

    region_name = request.args.get("region") or regions[0].name
    region = _resolve_region(region_name)
    if region is None:
        flash(f"Unknown region {region_name!r}.", "error")
        region = regions[0]

    error: Optional[str] = None
    agents: List[Dict] = []
    try:
        agents = neutron.list_l3_agents(region)
    except Exception as exc:  # noqa: BLE001 — surface DB errors in the page
        error = f"Could not list L3 agents for {region.name}: {exc}"

    selected_agent_id = request.args.get("agent") or None
    selected_agent = next(
        (a for a in agents if a["id"] == selected_agent_id), None
    )

    agent_routers: List[Dict] = []
    if selected_agent is not None and error is None:
        try:
            agent_routers = neutron.routers_on_l3_agent(region, selected_agent_id)
        except Exception as exc:  # noqa: BLE001
            error = f"Could not list routers on the selected agent: {exc}"
        else:
            names = _project_names([r["project_id"] for r in agent_routers])
            for r in agent_routers:
                r["project_name"] = names.get(r["project_id"], "")

    # Every other L3 agent is a candidate target.
    target_agents = [
        a for a in agents
        if selected_agent is None or a["id"] != selected_agent["id"]
    ]

    return render_template(
        "tools/routers.html",
        regions=regions, region=region,
        agents=agents, selected_agent=selected_agent,
        routers=agent_routers, target_agents=target_agents,
        has_token=_keystone_session() is not None,
        error=error,
    )


@login_required
def routers_move():
    """Move the selected routers from the source L3 agent to the target."""
    region_name = (request.form.get("region") or "").strip()
    source = (request.form.get("source_agent") or "").strip()
    target = (request.form.get("target_agent") or "").strip()
    router_ids = [r for r in request.form.getlist("router_ids") if r]

    region = _resolve_region(region_name)
    back = url_for("tools_routers", region=region_name, agent=source)
    if region is None:
        flash(f"Unknown region {region_name!r}.", "error")
        return redirect(url_for("tools_routers"))
    if not source or not target:
        flash("Choose both a source and a target L3 agent.", "error")
        return redirect(back)
    if source == target:
        flash("The source and target L3 agents must be different.", "error")
        return redirect(back)
    if not router_ids:
        flash("Select at least one router to move.", "error")
        return redirect(back)

    ks = _keystone_session()
    if ks is None:
        flash(
            "Sign in with Keystone to move routers — a scoped token is "
            "required to call the network API.",
            "error",
        )
        return redirect(back)

    moved: List[str] = []
    failed: List[str] = []
    for rid in router_ids:
        try:
            neutron.move_router(ks, region.name, rid, source, target)
        except neutron.NeutronError as exc:
            failed.append(rid)
            _audit(
                "l3_router_move_failed",
                f"{rid}: {source} -> {target} ({region.name}): {exc}",
            )
            flash(f"Router {rid}: {exc}", "error")
        else:
            moved.append(rid)
            _audit(
                "l3_router_move",
                f"{rid}: {source} -> {target} ({region.name})",
            )

    if moved:
        flash(
            f"Moved {len(moved)} router(s) to the target L3 agent. "
            "Re-open the source agent to confirm it is drained.",
            "success",
        )
    if not moved and not failed:
        flash("No routers were moved.", "info")
    return redirect(back)
