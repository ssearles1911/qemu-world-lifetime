"""First-run setup wizard.

Active while `setup_status() == NO_ADMIN`. Once an admin exists, the
wizard's open routes lock down and the same step UI is reused — admin-
only — at /admin/setup-resume for partial states (a missing region, an
unconfigured Keystone URL after a fresh migration, etc.).

Steps:
  1. admin    — create the first local administrator
  2. region   — add at least one region (and which is the keystone region)
  3. schema   — confirm/edit the per-service schema names
  4. keystone — set the Keystone v3 auth URL + default domain
"""

from __future__ import annotations

from flask import Flask, abort, flash, redirect, render_template, request, url_for

from .. import config_db
from ..auth import local as local_auth
from ..auth.session import admin_required, login_local


_STEP_ORDER = ["admin", "region", "schema", "keystone"]


def register(app: Flask) -> None:
    app.add_url_rule("/setup", view_func=setup, endpoint="setup")
    app.add_url_rule(
        "/setup/<step>", view_func=setup_step, endpoint="setup_step",
        methods=("GET", "POST"),
    )
    app.add_url_rule(
        "/admin/setup-resume", view_func=admin_resume, endpoint="admin_setup_resume",
    )


def _next_step_for_status(status: str) -> str:
    return {
        config_db.SetupStatus.NO_ADMIN: "admin",
        config_db.SetupStatus.NO_REGION: "region",
        config_db.SetupStatus.NO_KEYSTONE_REGION: "region",
        config_db.SetupStatus.NO_KEYSTONE_AUTH_URL: "keystone",
        config_db.SetupStatus.OK: "done",
    }.get(status, "admin")


def setup():
    status = config_db.setup_status()
    if status == config_db.SetupStatus.OK:
        return redirect(url_for("catalog"))
    return redirect(url_for("setup_step", step=_next_step_for_status(status)))


def setup_step(step: str):
    if step not in _STEP_ORDER and step != "done":
        abort(404)

    status = config_db.setup_status()

    # The first-run path stays open only as long as no admin exists.
    if status != config_db.SetupStatus.NO_ADMIN:
        # Past the admin step: every other step is admin-only.
        from ..auth.session import current_user
        info = current_user()
        if info is None or not info.get("is_admin"):
            return redirect(url_for("login", next=request.path))

    handler = {
        "admin": _step_admin,
        "region": _step_region,
        "schema": _step_schema,
        "keystone": _step_keystone,
        "done": _step_done,
    }[step]
    return handler()


@admin_required
def admin_resume():
    status = config_db.setup_status()
    return redirect(url_for("setup_step", step=_next_step_for_status(status)))


# --- Step handlers -----------------------------------------------------------


def _step_admin():
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        confirm = request.form.get("confirm") or ""
        if password != confirm:
            flash("Passwords did not match.", "error")
        elif len(password) < 8:
            flash("Password must be at least 8 characters.", "error")
        else:
            try:
                local_auth.create_admin(username, password)
            except ValueError as exc:
                flash(str(exc), "error")
            else:
                # Auto-log them in so the rest of the wizard runs as the
                # newly-minted admin.
                user = config_db.get_local_user(username)
                if user:
                    login_local(user)
                flash("Administrator created. Continue with regions.", "success")
                return redirect(url_for("setup_step", step="region"))
    return render_template(
        "setup/admin.html",
        step="admin",
        steps=_STEP_ORDER,
        admin_count=config_db.count_local_admins(),
    )


def _step_region():
    regions = config_db.list_all_regions()
    if request.method == "POST":
        action = request.form.get("action") or "add"
        if action == "add":
            name = (request.form.get("name") or "").strip()
            host = (request.form.get("host") or "").strip()
            port_s = (request.form.get("port") or "3306").strip()
            db_user = (request.form.get("db_user") or "").strip()
            db_password = request.form.get("db_password") or ""
            is_keystone = bool(request.form.get("is_keystone_region"))
            if not (name and host and db_user):
                flash("Name, host, and DB user are required.", "error")
            else:
                try:
                    port = int(port_s)
                except ValueError:
                    flash(f"Port {port_s!r} is not an integer.", "error")
                else:
                    config_db.upsert_region(
                        name=name, host=host, port=port,
                        db_user=db_user, db_password=db_password,
                        is_keystone_region=is_keystone,
                        display_order=len(regions),
                    )
                    config_db.record_audit(
                        "system", None, "region_upserted", name,
                    )
                    flash(f"Region {name!r} saved.", "success")
                    return redirect(url_for("setup_step", step="region"))
        elif action == "delete":
            name = request.form.get("name") or ""
            if name:
                config_db.delete_region(name)
                config_db.record_audit("system", None, "region_deleted", name)
                flash(f"Region {name!r} removed.", "success")
                return redirect(url_for("setup_step", step="region"))
        elif action == "advance":
            status = config_db.setup_status()
            if status in (
                config_db.SetupStatus.NO_REGION,
                config_db.SetupStatus.NO_KEYSTONE_REGION,
            ):
                flash(
                    "Add at least one region and mark which one hosts the "
                    "shared Keystone schema before continuing.",
                    "error",
                )
            else:
                return redirect(url_for("setup_step", step="schema"))
    return render_template(
        "setup/region.html",
        step="region",
        steps=_STEP_ORDER,
        regions=config_db.list_all_regions(),
    )


def _step_schema():
    if request.method == "POST":
        for service in ("keystone", "nova_api", "cinder", "glance", "neutron"):
            value = (request.form.get(f"schema_{service}") or "").strip()
            if value:
                config_db.set_schema_name(service, value)
        config_db.record_audit("system", None, "schemas_updated", "")
        flash("Schema names saved.", "success")
        return redirect(url_for("setup_step", step="keystone"))
    return render_template(
        "setup/schema.html",
        step="schema",
        steps=_STEP_ORDER,
        schemas=config_db.all_schema_names(),
    )


def _step_keystone():
    if request.method == "POST":
        url = (request.form.get("keystone_auth_url") or "").strip()
        domain = (request.form.get("keystone_default_domain") or "Default").strip()
        if not url:
            flash("Keystone auth URL is required.", "error")
        else:
            config_db.set_web_setting("keystone_auth_url", url)
            config_db.set_web_setting("keystone_default_domain", domain or "Default")
            config_db.record_audit("system", None, "keystone_settings_updated", url)
            flash("Keystone settings saved.", "success")
            return redirect(url_for("setup_step", step="done"))
    return render_template(
        "setup/keystone.html",
        step="keystone",
        steps=_STEP_ORDER,
        keystone_auth_url=config_db.web_setting("keystone_auth_url", "") or "",
        keystone_default_domain=config_db.web_setting("keystone_default_domain", "Default") or "Default",
    )


def _step_done():
    return render_template(
        "setup/done.html",
        step="done",
        steps=_STEP_ORDER,
    )
