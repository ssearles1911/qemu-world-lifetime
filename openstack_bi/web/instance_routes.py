"""Per-instance Nova actions: live migration and console access.

These are the only state-changing OpenStack operations in the app. They
run as the logged-in Keystone user via the scoped token kept in
openstack_bi.auth.token_store. The live-migrate endpoint speaks JSON —
it backs the modal on the SPLA report — while the console endpoint
redirects the browser to the console URL. A local-admin session (or an
expired token) has no usable token: live-migrate answers with a JSON
error, console redirects back with a flash.
"""

from __future__ import annotations

from flask import (
    Flask, flash, jsonify, redirect, request, session, url_for,
)

from .. import config_db, nova
from ..auth import token_store
from ..auth.session import current_user, login_required
from ..config import parse_regions


def register(app: Flask) -> None:
    app.add_url_rule(
        "/instance/<region>/<uuid>/migrate",
        view_func=migrate, endpoint="instance_migrate",
        methods=("GET", "POST"),
    )
    app.add_url_rule(
        "/instance/<region>/<uuid>/console",
        view_func=console, endpoint="instance_console",
    )


def _spla_url() -> str:
    return url_for("run_report", report_id="spla_instances")


def _known_region(region: str) -> bool:
    try:
        return any(r.name == region for r in parse_regions())
    except Exception:  # noqa: BLE001
        return False


def _keystone_session():
    """The logged-in user's Nova-capable Session, or None."""
    return token_store.session_for(session.get("ks_token_key"))


def _audit(action: str, detail: str) -> None:
    info = current_user() or {}
    config_db.record_audit(
        info.get("kind") or "?",
        str(info.get("user_id") or info.get("username") or ""),
        action, detail,
    )


@login_required
def migrate(region: str, uuid: str):
    """Live-migration JSON endpoint behind the SPLA report's modal.

    GET  -> {ok: true, server, current_host, candidates} | {ok: false, error}
    POST -> {ok: true, message}                          | {ok: false, error}
    """
    if not _known_region(region):
        return jsonify(ok=False, error=f"Unknown region {region!r}."), 400
    ks = _keystone_session()
    if ks is None:
        return jsonify(
            ok=False,
            error="Sign in with Keystone to perform live migration.",
        ), 403

    if request.method == "POST":
        target = (request.form.get("target_host") or "").strip()
        if not target:
            return jsonify(ok=False, error="Choose a target host."), 400
        try:
            nova.live_migrate(ks, region, uuid, target)
        except nova.NovaError as exc:
            return jsonify(ok=False, error=str(exc)), 502
        _audit("live_migration", f"{uuid} -> {target} ({region})")
        return jsonify(
            ok=True,
            message=(
                f"Live migration to {target} started. Re-run the report "
                "shortly to confirm the new host."
            ),
        )

    # GET — target-host picker data.
    try:
        server = nova.get_server(ks, region, uuid)
        hosts = nova.list_compute_hosts(ks, region)
    except nova.NovaError as exc:
        return jsonify(ok=False, error=str(exc)), 502
    current_host = server.get("host") or ""
    candidates = [h for h in hosts if h["host"] != current_host]
    return jsonify(
        ok=True,
        server=server,
        current_host=current_host,
        candidates=candidates,
    )


@login_required
def console(region: str, uuid: str):
    if not _known_region(region):
        flash(f"Unknown region {region!r}.", "error")
        return redirect(_spla_url())
    ks = _keystone_session()
    if ks is None:
        flash("Sign in with Keystone to open a console.", "error")
        return redirect(_spla_url())
    try:
        url = nova.remote_console(ks, region, uuid)
    except nova.NovaError as exc:
        flash(str(exc), "error")
        return redirect(_spla_url())
    _audit("console_open", f"{uuid} ({region})")
    return redirect(url)
