"""Administrator pages: regions, schemas, Keystone settings, admins, audit log, role mapping."""

from __future__ import annotations

import re

from flask import Flask, flash, redirect, render_template, request, url_for

from .. import config_db
from ..auth import local as local_auth
from ..auth.capabilities import (
    CAPABILITY_REGISTRY,
    Capability,
    is_known_capability,
)
from ..auth.session import current_user, requires_capability

# SQL identifier shape — schema names can't be parameterized in MariaDB,
# so admin-supplied values that get spliced into a query must match this.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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
        "/admin/roles", view_func=roles, endpoint="admin_roles",
        methods=("GET", "POST"),
    )
    app.add_url_rule(
        "/admin/admins", view_func=admins, endpoint="admin_admins",
        methods=("GET", "POST"),
    )
    app.add_url_rule("/admin/audit", view_func=audit, endpoint="admin_audit")


def _admin_index_caps_required():
    return (
        Capability.MANAGE_CONFIG.value,
        Capability.MANAGE_USERS.value,
        Capability.VIEW_AUDIT_LOG.value,
    )


@requires_capability(*_admin_index_caps_required())
def index():
    return render_template(
        "admin/index.html",
        setup_status=config_db.setup_status(),
    )


@requires_capability(Capability.MANAGE_CONFIG.value)
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


@requires_capability(Capability.MANAGE_CONFIG.value)
def schemas():
    if request.method == "POST":
        for service in ("keystone", "nova_api", "cinder", "glance", "neutron"):
            value = (request.form.get(f"schema_{service}") or "").strip()
            if value:
                config_db.set_schema_name(service, value)
        # Report-specific schemas live in web_settings (not schema_names)
        # because they aren't OpenStack service schemas — they're optional
        # auxiliary databases that some reports cross-join against.
        spla_schema = (request.form.get("spla_managed_schema") or "").strip()
        # Validate identifier shape — schema names can't be parameterized
        # in MariaDB, so we splice the string. Admin-controlled, but still
        # reject anything that wouldn't be a valid identifier.
        if spla_schema and not _IDENTIFIER_RE.match(spla_schema):
            flash(
                f"Invalid schema name {spla_schema!r}: must match "
                f"[A-Za-z_][A-Za-z0-9_]*",
                "error",
            )
        else:
            config_db.set_web_setting("spla_managed_schema", spla_schema)
            config_db.record_audit("system", None, "schemas_updated", "")
            flash("Schema names saved.", "success")
        return redirect(url_for("admin_schemas"))
    return render_template(
        "admin/schemas.html",
        schemas=config_db.all_schema_names(),
        spla_managed_schema=config_db.web_setting("spla_managed_schema", "") or "",
    )


@requires_capability(Capability.MANAGE_CONFIG.value)
def keystone():
    if request.method == "POST":
        url = (request.form.get("keystone_auth_url") or "").strip()
        domain = (request.form.get("keystone_default_domain") or "Default").strip()
        admin_role = (request.form.get("keystone_admin_role") or "admin").strip()
        if not url:
            flash("Keystone auth URL is required.", "error")
        else:
            config_db.set_web_setting("keystone_auth_url", url)
            config_db.set_web_setting("keystone_default_domain", domain or "Default")
            config_db.set_web_setting("keystone_admin_role", admin_role or "admin")
            config_db.record_audit("system", None, "keystone_settings_updated", url)
            flash("Keystone settings saved.", "success")
        return redirect(url_for("admin_keystone"))
    return render_template(
        "admin/keystone.html",
        keystone_auth_url=config_db.web_setting("keystone_auth_url", "") or "",
        keystone_default_domain=(
            config_db.web_setting("keystone_default_domain", "Default") or "Default"
        ),
        keystone_admin_role=(
            config_db.web_setting("keystone_admin_role", "admin") or "admin"
        ),
    )


@requires_capability(Capability.MANAGE_USERS.value)
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


@requires_capability(Capability.VIEW_AUDIT_LOG.value)
def audit():
    return render_template(
        "admin/audit.html",
        rows=config_db.recent_audit(500),
    )


@requires_capability(Capability.MANAGE_CONFIG.value)
def roles():
    """Edit which Keystone role names grant which capabilities.

    Bootstrap-deadlock guard: a non-local-admin actor cannot revoke the
    last role mapped to `manage_config` — doing so would leave nobody
    able to edit this very page. Local admins are exempt because their
    `is_admin` flag bypasses capability checks regardless.
    """
    info = current_user() or {}
    actor_kind = info.get("kind") or "?"
    actor_id = str(info.get("user_id") or info.get("username") or "")
    is_local_admin = bool(info.get("is_admin"))

    if request.method == "POST":
        action = request.form.get("action") or ""
        capability = (request.form.get("capability") or "").strip()
        role_name = (request.form.get("role_name") or "").strip().lower()

        if not is_known_capability(capability):
            flash(f"Unknown capability: {capability!r}.", "error")
        elif not role_name:
            flash("Role name is required.", "error")
        elif action == "grant":
            inserted = config_db.grant_role_capability(role_name, capability)
            if inserted:
                config_db.record_audit(
                    actor_kind, actor_id, "capability_grant",
                    f"{role_name}:{capability}",
                )
                flash(
                    f"Role {role_name!r} now grants {capability}.", "success",
                )
            else:
                flash(
                    f"Role {role_name!r} already grants {capability}.", "info",
                )
        elif action == "revoke":
            if (
                capability == Capability.MANAGE_CONFIG.value
                and not is_local_admin
                and config_db.count_roles_for_capability(capability) <= 1
            ):
                flash(
                    "Refusing to remove the last role mapped to "
                    "`manage_config` — only a local administrator can do "
                    "that, to avoid locking everyone out.",
                    "error",
                )
            else:
                removed = config_db.revoke_role_capability(role_name, capability)
                if removed:
                    config_db.record_audit(
                        actor_kind, actor_id, "capability_revoke",
                        f"{role_name}:{capability}",
                    )
                    flash(
                        f"Role {role_name!r} no longer grants {capability}.",
                        "success",
                    )
                else:
                    flash(
                        f"Role {role_name!r} was not mapped to {capability}.",
                        "info",
                    )
        else:
            flash("Unknown action.", "error")
        return redirect(url_for("admin_roles"))

    mapping = {
        cap.name: config_db.roles_for_capability(cap.name)
        for cap in CAPABILITY_REGISTRY
    }
    return render_template(
        "admin/roles.html",
        registry=CAPABILITY_REGISTRY,
        mapping=mapping,
    )
