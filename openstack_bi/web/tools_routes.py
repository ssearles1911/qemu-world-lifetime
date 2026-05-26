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
    Flask, flash, jsonify, redirect, render_template, request, session,
    url_for,
)

from .. import config_db, netcheck, neutron, openstack
from ..auth import token_store
from ..auth.session import current_user, login_required
from ..config import Region, keystone_db, keystone_region, parse_regions
from ..db import query
from ..util import safe_for_each_region


def register(app: Flask) -> None:
    app.add_url_rule("/tools", view_func=index, endpoint="tools_index")
    app.add_url_rule(
        "/tools/routers", view_func=routers, endpoint="tools_routers",
    )
    app.add_url_rule(
        "/tools/routers/move", view_func=routers_move,
        endpoint="tools_routers_move", methods=("POST",),
    )
    app.add_url_rule(
        "/tools/routers/verify", view_func=routers_verify,
        endpoint="tools_routers_verify", methods=("POST",),
    )
    app.add_url_rule("/tools/vlans", view_func=vlans, endpoint="tools_vlans")
    app.add_url_rule(
        "/tools/vlans/create", view_func=vlans_create,
        endpoint="tools_vlans_create", methods=("POST",),
    )
    app.add_url_rule(
        "/tools/vlans/list", view_func=vlan_list, endpoint="tools_vlan_list",
    )
    app.add_url_rule(
        "/tools/ports/build", view_func=ports_build,
        endpoint="tools_ports_build",
    )
    app.add_url_rule(
        "/tools/networks", view_func=networks_page,
        endpoint="tools_networks",
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
            f"Moved {len(moved)} router(s) to the target L3 agent — "
            "verifying their reachability below.",
            "success",
        )
        # Land on the target agent (where the routers now are) and let
        # the page auto-run reachability verification.
        return redirect(url_for(
            "tools_routers", region=region_name, agent=target, verify=1,
        ))
    if not failed:
        flash("No routers were moved.", "info")
    return redirect(back)


# Hard cap on a single verification batch — bounds how long a worker
# thread can be held pinging.
_MAX_VERIFY = 250


@login_required
def routers_verify():
    """Ping the WAN IPs of the given routers and report reachability.

    Read-only and server-side: it shells out to `ping`, never touches
    the OpenStack API, and so needs no Keystone token (unlike the move).
    That also lets the post-move auto-verify run for token-less sessions.
    """
    region_name = (request.form.get("region") or "").strip()
    router_ids = [r for r in request.form.getlist("router_ids") if r]

    region = _resolve_region(region_name)
    if region is None:
        return jsonify(ok=False, error=f"Unknown region {region_name!r}."), 400
    if not router_ids:
        return jsonify(ok=False, error="No routers to verify."), 400
    if len(router_ids) > _MAX_VERIFY:
        return jsonify(
            ok=False,
            error=f"Too many routers to verify at once (max {_MAX_VERIFY}).",
        ), 400

    try:
        wan = neutron.router_wan_ips(region, router_ids)
    except Exception as exc:  # noqa: BLE001
        return jsonify(
            ok=False, error=f"Could not resolve router WAN IPs: {exc}",
        ), 502

    # One flat list of IPs to ping (a router may have >1 gateway IP).
    all_ips: List[str] = []
    for info in wan.values():
        all_ips.extend(info["wan_ips"])
    ping = netcheck.ping_hosts(all_ips)
    pinged = ping["results"]
    available = ping["ping_available"]

    results: List[Dict] = []
    reachable = unreachable = unknown = 0
    for rid in router_ids:
        info = wan.get(rid)
        if info is None:
            results.append({
                "id": rid, "name": rid, "wan_ip": "",
                "reachable": None, "latency_ms": None,
                "note": "router not found",
            })
            unknown += 1
            continue
        ips = info["wan_ips"]
        if not ips:
            results.append({
                "id": rid, "name": info["name"], "wan_ip": "",
                "reachable": None, "latency_ms": None,
                "note": "no gateway port",
            })
            unknown += 1
            continue
        # A router counts as reachable if any of its WAN IPs answers.
        hit = next(
            (pinged[i] for i in ips if pinged.get(i, {}).get("reachable")),
            None,
        )
        res = hit or pinged.get(ips[0], {})
        if not available:
            verdict, note = None, ""
            unknown += 1
        elif res.get("reachable"):
            verdict, note = True, res.get("note") or ""
            reachable += 1
        else:
            verdict, note = False, res.get("note") or ""
            unreachable += 1
        results.append({
            "id": rid, "name": info["name"],
            "wan_ip": ", ".join(ips),
            "reachable": verdict,
            "latency_ms": res.get("latency_ms"),
            "note": note,
        })

    summary = {
        "total": len(router_ids),
        "reachable": reachable,
        "unreachable": unreachable,
        "unknown": unknown,
    }
    _audit(
        "l3_router_verify",
        f"{region.name}: {reachable}/{len(router_ids)} reachable",
    )
    return jsonify(
        ok=True, results=results, summary=summary, warning=ping["error"],
    )


# --- VLAN networks ----------------------------------------------------------

def _project_choices() -> List[Dict[str, str]]:
    """Non-domain Keystone projects as `{id, name, label}` for the picker.

    Label is `project (domain)` so duplicate names across domains stay
    distinguishable. Best-effort — returns [] if Keystone is unreachable.
    """
    try:
        projects = openstack.list_all_projects()
        domains = {d["id"]: d["name"] for d in openstack.list_domains()}
    except Exception:  # noqa: BLE001
        return []
    out: List[Dict[str, str]] = []
    for p in projects:
        domain = domains.get(p.get("domain_id"), "")
        label = f"{p['name']} ({domain})" if domain else p["name"]
        out.append({"id": p["id"], "name": p["name"], "label": label})
    out.sort(key=lambda c: c["label"].lower())
    return out


@login_required
def vlans():
    """Create a provider VLAN network for a target project.

    `?region=` selects the region; `?project=` selects the target
    project and reveals that project's existing VLAN networks plus the
    create form.
    """
    regions = _regions()
    if not regions:
        flash("No regions are configured.", "error")
        return render_template(
            "tools/vlans.html",
            regions=[], region=None, projects=[], physnets=[],
            selected_project=None, project_networks=[],
            has_token=False, error=None,
        )

    region_name = request.args.get("region") or regions[0].name
    region = _resolve_region(region_name)
    if region is None:
        flash(f"Unknown region {region_name!r}.", "error")
        region = regions[0]

    error: Optional[str] = None
    physnets: List[str] = []
    try:
        physnets = neutron.list_vlan_physnets(region)
    except Exception as exc:  # noqa: BLE001
        error = f"Could not read VLAN physical networks for {region.name}: {exc}"

    projects = _project_choices()
    selected_project_id = request.args.get("project") or None
    selected_project = next(
        (p for p in projects if p["id"] == selected_project_id), None
    )

    project_networks: List[Dict] = []
    if selected_project is not None and error is None:
        try:
            project_networks = neutron.vlan_networks_for_project(
                region, selected_project_id
            )
        except Exception as exc:  # noqa: BLE001
            error = f"Could not list the project's VLAN networks: {exc}"

    return render_template(
        "tools/vlans.html",
        regions=regions, region=region, projects=projects, physnets=physnets,
        selected_project=selected_project, project_networks=project_networks,
        has_token=_keystone_session() is not None, error=error,
    )


@login_required
def vlans_create():
    """Create the provider VLAN network. No subnet is created — the
    owning project's users add that themselves."""
    region_name = (request.form.get("region") or "").strip()
    project_id = (request.form.get("project_id") or "").strip()
    name = (request.form.get("name") or "").strip()
    physnet = (request.form.get("physical_network") or "").strip()
    vlan_raw = (request.form.get("segmentation_id") or "").strip()

    region = _resolve_region(region_name)
    back = url_for("tools_vlans", region=region_name, project=project_id)
    if region is None:
        flash(f"Unknown region {region_name!r}.", "error")
        return redirect(url_for("tools_vlans"))
    if not project_id:
        flash("Choose a target project.", "error")
        return redirect(back)
    if not name:
        flash("Enter a network name.", "error")
        return redirect(back)
    if not physnet:
        flash("Choose a physical network.", "error")
        return redirect(back)
    try:
        vlan_id = int(vlan_raw)
    except ValueError:
        flash("VLAN ID must be a whole number between 1 and 4094.", "error")
        return redirect(back)
    if not 1 <= vlan_id <= 4094:
        flash("VLAN ID must be between 1 and 4094.", "error")
        return redirect(back)

    ks = _keystone_session()
    if ks is None:
        flash(
            "Sign in with Keystone to create networks — a scoped admin "
            "token is required to call the network API.",
            "error",
        )
        return redirect(back)

    # Best-effort pre-check against the (replica) DB. Neutron rejects a
    # genuine collision regardless; this just gives a friendlier message.
    try:
        conflict = neutron.vlan_segment_conflict(region, physnet, vlan_id)
    except Exception:  # noqa: BLE001
        conflict = None
    if conflict is not None:
        flash(
            f"VLAN {vlan_id} on physnet {physnet!r} is already used by "
            f"network '{conflict['name']}' ({conflict['id']}).",
            "error",
        )
        return redirect(back)

    try:
        net = neutron.create_vlan_network(
            ks, region.name, name, project_id, physnet, vlan_id
        )
    except neutron.NeutronError as exc:
        _audit(
            "vlan_network_create_failed",
            f"{name!r} vlan={vlan_id} physnet={physnet} "
            f"project={project_id} ({region.name}): {exc}",
        )
        flash(f"Could not create the network: {exc}", "error")
        return redirect(back)

    _audit(
        "vlan_network_create",
        f"{net['id']} {name!r} vlan={vlan_id} physnet={physnet} "
        f"project={project_id} ({region.name})",
    )
    flash(
        f"Created VLAN network {name!r} ({net['id']}) on VLAN {vlan_id}. "
        "The project's users can now create a subnet on it.",
        "success",
    )
    return redirect(back)


def _project_directory(project_ids) -> Dict[str, Dict[str, str]]:
    """Map `project_id` -> `{"name", "domain"}` via the Keystone DB.

    Two-pass lookup: projects first (gives us each project's
    `domain_id`), then those domain ids resolved to names. Best-effort
    — a Keystone read failure returns `{}` rather than blowing up the
    listing page.
    """
    pids = sorted({pid for pid in project_ids if pid})
    if not pids:
        return {}
    placeholders = ",".join(["%s"] * len(pids))
    try:
        proj_rows = query(
            keystone_region(), keystone_db(),
            f"SELECT id, name, domain_id FROM project WHERE id IN ({placeholders})",
            pids,
        )
    except Exception:  # noqa: BLE001
        return {}
    name_by_pid = {r["id"]: r.get("name") or "" for r in proj_rows}
    dom_id_by_pid = {r["id"]: r.get("domain_id") for r in proj_rows}
    dom_ids = sorted({d for d in dom_id_by_pid.values() if d})
    domain_name_by_id: Dict[str, str] = {}
    if dom_ids:
        ph2 = ",".join(["%s"] * len(dom_ids))
        try:
            drows = query(
                keystone_region(), keystone_db(),
                f"SELECT id, name FROM project WHERE id IN ({ph2}) "
                "AND is_domain = 1",
                dom_ids,
            )
            domain_name_by_id = {r["id"]: r.get("name") or "" for r in drows}
        except Exception:  # noqa: BLE001
            pass
    out: Dict[str, Dict[str, str]] = {}
    for pid in pids:
        out[pid] = {
            "name": name_by_pid.get(pid, ""),
            "domain": domain_name_by_id.get(dom_id_by_pid.get(pid), ""),
        }
    return out


@login_required
def vlan_list():
    """Read-only list of every VLAN network in a region.

    Sister page to the per-project VLAN panel — same data shape, just
    not filtered by project. Showing project + domain inline so an
    operator can scan tenancy at a glance.
    """
    regions = _regions()
    if not regions:
        flash("No regions are configured.", "error")
        return render_template(
            "tools/vlan_list.html",
            regions=[], region=None, networks=[], error=None,
        )

    region_name = request.args.get("region") or regions[0].name
    region = _resolve_region(region_name)
    if region is None:
        flash(f"Unknown region {region_name!r}.", "error")
        region = regions[0]

    error: Optional[str] = None
    networks: List[Dict] = []
    try:
        networks = neutron.list_vlan_networks(region)
    except Exception as exc:  # noqa: BLE001
        error = f"Could not list VLAN networks for {region.name}: {exc}"

    directory = _project_directory([n["project_id"] for n in networks])
    for n in networks:
        info = directory.get(n["project_id"], {})
        n["project_name"] = info.get("name", "")
        n["domain_name"] = info.get("domain", "")

    return render_template(
        "tools/vlan_list.html",
        regions=regions, region=region, networks=networks, error=error,
    )


@login_required
def ports_build():
    """All Neutron ports currently in BUILD across the selected regions.

    The region selector is a multi-select fieldset (same pattern as the
    reports' multiselect Param) — no `region` query params means "all
    configured regions". Multi-region fan-out tolerates per-region
    failure: a dead replica in one region still lets the others render.
    """
    all_regions = _regions()
    if not all_regions:
        flash("No regions are configured.", "error")
        return render_template(
            "tools/ports_build.html",
            all_regions=[], selected_region_names=[],
            rows=[], region_errors=[],
        )

    by_name = {r.name: r for r in all_regions}
    requested = [n for n in request.args.getlist("region") if n in by_name]
    selected_regions = [by_name[n] for n in requested] if requested else all_regions
    selected_region_names = [r.name for r in selected_regions]

    def _collect(region: Region):
        return neutron.list_build_ports(region)

    results, region_errors = safe_for_each_region(selected_regions, _collect)
    rows: List[Dict] = []
    for region, region_rows in results:
        for r in region_rows:
            r["region"] = region.name
            rows.append(r)

    directory = _project_directory([r["project_id"] for r in rows])
    for r in rows:
        info = directory.get(r["project_id"], {})
        r["project_name"] = info.get("name", "")

    return render_template(
        "tools/ports_build.html",
        all_regions=all_regions,
        selected_region_names=selected_region_names,
        rows=rows, region_errors=region_errors,
    )


@login_required
def networks_page():
    """Per-region network list. Each row's caret expands to show the
    DHCP + L3 agents hosting that network — equivalent of
    `openstack network agent list --network <id>`, but with all the
    agent data pre-fetched in two bulk DB queries on initial page
    load. Expanding is then a pure DOM toggle — zero round trips.
    """
    regions = _regions()
    if not regions:
        flash("No regions are configured.", "error")
        return render_template(
            "tools/networks.html",
            regions=[], region=None, networks=[], error=None,
        )

    region_name = request.args.get("region") or regions[0].name
    region = _resolve_region(region_name)
    if region is None:
        flash(f"Unknown region {region_name!r}.", "error")
        region = regions[0]

    error: Optional[str] = None
    nets: List[Dict] = []
    dhcp_by_net: Dict[str, List[Dict]] = {}
    l3_by_net: Dict[str, List[Dict]] = {}
    try:
        nets = neutron.list_networks(region)
        dhcp_by_net = neutron.dhcp_agents_by_network(region)
        l3_by_net = neutron.l3_agents_by_network(region)
    except Exception as exc:  # noqa: BLE001
        error = f"Could not list networks for {region.name}: {exc}"

    directory = _project_directory([n["project_id"] for n in nets])
    for n in nets:
        info = directory.get(n["project_id"], {})
        n["project_name"] = info.get("name", "")

    # Embed agent bindings as a single JSON blob keyed by network id —
    # JS builds the expand-row HTML on first click rather than at page
    # render. Keeps the initial DOM small enough that toggling one row
    # doesn't trigger a full-table relayout (the Chrome mouse stall
    # users hit when ~700 networks' worth of expand content was
    # pre-rendered).
    agents_by_id = {
        n["id"]: {
            "dhcp": dhcp_by_net.get(n["id"], []),
            "l3": l3_by_net.get(n["id"], []),
        }
        for n in nets
    }

    return render_template(
        "tools/networks.html",
        regions=regions, region=region, networks=nets,
        agents_by_id=agents_by_id, error=error,
    )
