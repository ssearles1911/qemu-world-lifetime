"""Per-instance Nova actions: live migration and console access.

These are the only state-changing OpenStack operations in the app. They
run as the logged-in Keystone user via the scoped token kept in
openstack_bi.auth.token_store; a local-admin session (or an expired
token) has no usable token and is sent back with a re-login prompt.
"""

from __future__ import annotations

from flask import (
    Flask, flash, redirect, render_template, request, session, url_for,
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
    if not _known_region(region):
        flash(f"Unknown region {region!r}.", "error")
        return redirect(_spla_url())
    ks = _keystone_session()
    if ks is None:
        flash("Sign in with Keystone to perform that action.", "error")
        return redirect(_spla_url())

    if request.method == "POST":
        target = (request.form.get("target_host") or "").strip()
        if not target:
            flash("Choose a target host.", "error")
            return redirect(url_for("instance_migrate", region=region, uuid=uuid))
        try:
            nova.live_migrate(ks, region, uuid, target)
        except nova.NovaError as exc:
            flash(str(exc), "error")
            return redirect(url_for("instance_migrate", region=region, uuid=uuid))
        _audit("live_migration", f"{uuid} -> {target} ({region})")
        flash(
            f"Live migration of {uuid} to {target} started. Re-run the "
            "report shortly to see the new host.",
            "success",
        )
        return redirect(_spla_url())

    # GET — render the target-host picker.
    try:
        server = nova.get_server(ks, region, uuid)
        hosts = nova.list_compute_hosts(ks, region)
    except nova.NovaError as exc:
        flash(str(exc), "error")
        return redirect(_spla_url())
    current_host = server.get("host") or ""
    candidates = [h for h in hosts if h["host"] != current_host]
    return render_template(
        "instance/migrate.html",
        region=region, uuid=uuid, server=server,
        current_host=current_host, candidates=candidates,
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
