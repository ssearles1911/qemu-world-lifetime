"""Administrator pages: regions, schemas, Keystone settings, admins, audit log."""

from __future__ import annotations

from flask import Flask, flash, redirect, render_template, request, url_for

from .. import config_db
from ..auth import local as local_auth
from ..auth.session import admin_required


def register(app: Flask) -> None:
    app.add_url_rule("/admin", view_func=index, endpoint="admin_index")
    app.add_url_rule(
        "/admin/regions", view_func=regions, endpoint="admin_regions",
        methods=("GET", "POST"),
    )
    app.add_url_rule(
        "/admin/schemas", view_func=schemas, endpoint="admin_schemas",
        methods=("GET", "POST"),
    )
    app.add_url_rule(
        "/admin/keystone", view_func=keystone, endpoint="admin_keystone",
        methods=("GET", "POST"),
    )
    app.add_url_rule(
        "/admin/admins", view_func=admins, endpoint="admin_admins",
        methods=("GET", "POST"),
    )
    app.add_url_rule("/admin/audit", view_func=audit, endpoint="admin_audit")


@admin_required
def index():
    return render_template(
        "admin/index.html",
        setup_status=config_db.setup_status(),
    )


@admin_required
def regions():
    if request.method == "POST":
        action = request.form.get("action") or "save"
        name = (request.form.get("name") or "").strip()
        if action == "delete":
            if name:
                config_db.delete_region(name)
                config_db.record_audit("system", None, "region_deleted", name)
                flash(f"Region {name!r} removed.", "success")
        else:
            host = (request.form.get("host") or "").strip()
            port_s = (request.form.get("port") or "3306").strip()
            db_user = (request.form.get("db_user") or "").strip()
            db_password = request.form.get("db_password") or ""
            is_keystone = bool(request.form.get("is_keystone_region"))
            display_order_s = (request.form.get("display_order") or "0").strip()
            enabled = bool(request.form.get("enabled"))
            if not (name and host and db_user):
                flash("Name, host, and DB user are required.", "error")
            else:
                try:
                    port = int(port_s)
                    display_order = int(display_order_s)
                except ValueError:
                    flash("Port and display order must be integers.", "error")
                else:
                    config_db.upsert_region(
                        name=name, host=host, port=port,
                        db_user=db_user, db_password=db_password,
                        is_keystone_region=is_keystone,
                        display_order=display_order,
                        enabled=enabled,
                    )
                    config_db.record_audit("system", None, "region_upserted", name)
                    flash(f"Region {name!r} saved.", "success")
        return redirect(url_for("admin_regions"))
    return render_template(
        "admin/regions.html",
        regions=config_db.list_all_regions(),
    )


@admin_required
def schemas():
    if request.method == "POST":
        for service in ("keystone", "nova_api", "cinder", "glance", "neutron"):
            value = (request.form.get(f"schema_{service}") or "").strip()
            if value:
                config_db.set_schema_name(service, value)
        config_db.record_audit("system", None, "schemas_updated", "")
        flash("Schema names saved.", "success")
        return redirect(url_for("admin_schemas"))
    return render_template(
        "admin/schemas.html",
        schemas=config_db.all_schema_names(),
    )


@admin_required
def keystone():
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
        return redirect(url_for("admin_keystone"))
    return render_template(
        "admin/keystone.html",
        keystone_auth_url=config_db.web_setting("keystone_auth_url", "") or "",
        keystone_default_domain=(
            config_db.web_setting("keystone_default_domain", "Default") or "Default"
        ),
    )


@admin_required
def admins():
    if request.method == "POST":
        action = request.form.get("action") or "create"
        username = (request.form.get("username") or "").strip()
        if action == "create":
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
                    flash(f"Administrator {username!r} created.", "success")
        elif action == "reset":
            password = request.form.get("password") or ""
            confirm = request.form.get("confirm") or ""
            if password != confirm:
                flash("Passwords did not match.", "error")
            elif len(password) < 8:
                flash("Password must be at least 8 characters.", "error")
            else:
                try:
                    local_auth.reset_password(username, password)
                except ValueError as exc:
                    flash(str(exc), "error")
                else:
                    flash(f"Password reset for {username!r}.", "success")
        elif action == "delete":
            if config_db.count_local_admins() <= 1:
                flash("Refusing to remove the last administrator.", "error")
            else:
                local_auth.delete_user(username)
                flash(f"Administrator {username!r} removed.", "success")
        return redirect(url_for("admin_admins"))
    return render_template(
        "admin/admins.html",
        admins=local_auth.list_admins(),
    )


@admin_required
def audit():
    return render_template(
        "admin/audit.html",
        rows=config_db.recent_audit(500),
    )
