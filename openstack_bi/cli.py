"""`opsbi` — CLI that dispatches to registered report plugins.

Each report contributes its own argparse subparser. `opsbi <report> --help`
shows the report's parameters; `opsbi <report> [args]` runs it and prints a
grouped text table (or flat, if the report doesn't declare groupings).

Top-level helpers:
    opsbi init                    Apply config-DB migrations + seed defaults
    opsbi list-regions            List configured regions
    opsbi list-domains            List enabled Keystone domains
    opsbi list-cells              List Nova cell DBs per region
    opsbi admin {create,list,reset-password,delete}
    opsbi config {show,set,import-env}
    opsbi roles {list,grant,revoke,capabilities}
"""

from __future__ import annotations

import argparse
import getpass
import sys
from collections import defaultdict
from typing import Any, Dict, List, Optional

import pymysql

from openstack_bi import _env_legacy, config_db
from openstack_bi.auth import local as local_auth
from openstack_bi.auth.capabilities import (
    CAPABILITY_REGISTRY,
    Capability,
    is_known_capability,
)
from openstack_bi.config import parse_regions
from openstack_bi.reports import all_reports
from openstack_bi.reports.base import Param, Report, ReportResult


def _cli_actor() -> str:
    """Identifier recorded in the audit log for CLI invocations."""
    try:
        return f"cli:{getpass.getuser()}"
    except Exception:  # noqa: BLE001
        return "cli:?"


def _add_param(sp: argparse.ArgumentParser, param: Param) -> None:
    flag = "--" + param.name.replace("_", "-")
    kwargs: Dict[str, Any] = {"help": param.help or param.label}
    if param.kind == "int":
        kwargs["type"] = int
    elif param.kind == "bool":
        kwargs["action"] = "store_true"
        sp.add_argument(flag, **kwargs)
        return
    elif param.kind == "multiselect":
        kwargs["action"] = "append"
        kwargs["metavar"] = param.label.upper()
    if param.default is not None and param.kind != "bool":
        kwargs["default"] = param.default
    sp.add_argument(flag, **kwargs)


def _coerce_param(param: Param, raw: Any) -> Any:
    """Normalise argparse's multiselect=None into [] so report code can
    treat the two cases identically ('no filter' vs. 'no override')."""
    if param.kind == "multiselect" and raw is None:
        return []
    return raw


def _print_text(result: ReportResult, out=sys.stdout) -> None:
    if not result.rows:
        if "error" in result.metadata:
            print(result.metadata["error"], file=out)
            return
        _print_metadata(result, out)
        print("(no rows)", file=out)
        return

    _print_metadata(result, out)
    out.write("\n")

    visible_cols = [(k, label) for k, label in result.columns if not label.startswith("_")]
    # Columns whose key starts with an underscore are hidden from CLI output.

    if not result.groupings:
        _print_flat(visible_cols, result.rows, out)
        return

    groups: Dict[tuple, List[Dict[str, Any]]] = defaultdict(list)
    for row in result.rows:
        key = tuple(row.get(g) for g in result.groupings)
        groups[key].append(row)

    # Stable group order: first time we saw each key.
    seen: List[tuple] = []
    for row in result.rows:
        key = tuple(row.get(g) for g in result.groupings)
        if key not in seen:
            seen.append(key)

    total = 0
    for key in seen:
        rows = groups[key]
        heading = " / ".join(str(v) if v is not None else "(none)" for v in key)
        print(f"  {heading}  ({len(rows)} row{'s' if len(rows) != 1 else ''})", file=out)
        _print_flat(visible_cols, rows, out, indent="    ")
        out.write("\n")
        total += len(rows)

    print(f"Total: {total} row(s).", file=out)


def _print_metadata(result: ReportResult, out) -> None:
    for key, value in result.metadata.items():
        print(f"{key}: {value}", file=out)


def _print_flat(cols, rows, out, indent: str = "") -> None:
    widths = {
        key: max(
            len(label),
            min(60, max((len(str(r.get(key) or "")) for r in rows), default=0)),
        )
        for key, label in cols
    }
    header = indent + "  ".join(label.ljust(widths[key]) for key, label in cols)
    sep = indent + "  ".join("-" * widths[key] for key, _ in cols)
    print(header, file=out)
    print(sep, file=out)
    for r in rows:
        print(
            indent + "  ".join(str(r.get(key) or "").ljust(widths[key]) for key, _ in cols),
            file=out,
        )


def _handle_top_level(name: str) -> int:
    from openstack_bi import openstack

    if name == "init":
        config_db.init()
        print(f"Initialized config DB at {config_db.db_path()}")
        for warning in config_db.check_file_perms():
            print(warning, file=sys.stderr)
        return 0
    if name == "list-regions":
        for r in parse_regions():
            print(f"{r.name:<16}  {r.host}:{r.port}  user={r.user}")
        return 0
    if name == "list-domains":
        for d in openstack.list_domains():
            print(f"{d['id']}  {d['name']:<30}  ({d['project_count']} project(s))")
        return 0
    if name == "list-cells":
        for region in parse_regions():
            for cell in openstack.list_cells(region):
                print(f"{region.name}\t{cell}")
        return 0
    raise AssertionError(f"unknown top-level command: {name}")


def _handle_admin(args: argparse.Namespace) -> int:
    sub = args.admin_command
    if sub == "create":
        username = args.username or input("Username: ").strip()
        password = args.password or getpass.getpass("Password: ")
        confirm = password if args.password else getpass.getpass("Confirm: ")
        if password != confirm:
            print("Passwords did not match.", file=sys.stderr)
            return 2
        try:
            local_auth.create_admin(username, password)
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        print(f"Created administrator {username!r}.")
        return 0
    if sub == "list":
        for u in local_auth.list_admins():
            last = u.get("last_login_at") or "(never)"
            print(f"{u['username']:<24}  created={u['created_at']}  last_login={last}")
        return 0
    if sub == "reset-password":
        username = args.username
        password = args.password or getpass.getpass("New password: ")
        confirm = password if args.password else getpass.getpass("Confirm: ")
        if password != confirm:
            print("Passwords did not match.", file=sys.stderr)
            return 2
        try:
            local_auth.reset_password(username, password)
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        print(f"Reset password for {username!r}.")
        return 0
    if sub == "delete":
        if config_db.count_local_admins() <= 1:
            print("Refusing to remove the last administrator.", file=sys.stderr)
            return 2
        local_auth.delete_user(args.username)
        print(f"Deleted administrator {args.username!r}.")
        return 0
    raise AssertionError(f"unknown admin command: {sub}")


def _handle_config(args: argparse.Namespace) -> int:
    sub = args.config_command
    if sub == "show":
        print(f"# config DB: {config_db.db_path()}")
        print(f"# setup status: {config_db.setup_status()}")
        print()
        print("[regions]")
        for r in config_db.list_all_regions():
            ks = " keystone" if r["is_keystone_region"] else ""
            en = "" if r["enabled"] else " disabled"
            print(f"  {r['name']:<16} {r['host']}:{r['port']}  user={r['db_user']}{ks}{en}")
        print()
        print("[schema_names]")
        for service, name in config_db.all_schema_names().items():
            print(f"  {service:<12} {name}")
        print()
        print("[web_settings]")
        for key, value in config_db.all_web_settings().items():
            shown = value if key != "secret_key" else "(set; hidden)"
            print(f"  {key:<24} {shown}")
        print()
        admins = local_auth.list_admins()
        print(f"[local_admins]  ({len(admins)})")
        for u in admins:
            print(f"  {u['username']}")
        return 0
    if sub == "set":
        config_db.set_web_setting(args.key, args.value)
        print(f"Set web_setting {args.key} = {args.value!r}")
        return 0
    if sub == "import-env":
        env_path = args.env_file
        regions = _env_legacy.parse_legacy_regions(env_path)
        if not regions:
            print(
                "No legacy region configuration found in environment.",
                file=sys.stderr,
            )
            return 2
        keystone_target = _env_legacy.parse_legacy_keystone_region()
        for r in regions:
            config_db.upsert_region(
                name=r["name"],
                host=str(r["host"]),
                port=int(r["port"]),
                db_user=str(r["db_user"]),
                db_password=str(r["db_password"]),
                is_keystone_region=(r["name"] == keystone_target) if keystone_target else False,
                display_order=int(r["display_order"]),
            )
        if not keystone_target and regions:
            # Legacy default: the first listed region.
            config_db.upsert_region(
                name=regions[0]["name"],
                host=str(regions[0]["host"]),
                port=int(regions[0]["port"]),
                db_user=str(regions[0]["db_user"]),
                db_password=str(regions[0]["db_password"]),
                is_keystone_region=True,
                display_order=int(regions[0]["display_order"]),
            )
        for service, name in _env_legacy.parse_legacy_schemas().items():
            config_db.set_schema_name(service, name)
        bind_host, bind_port = _env_legacy.parse_legacy_web()
        config_db.set_web_setting("bind_host", bind_host)
        config_db.set_web_setting("bind_port", bind_port)
        config_db.record_audit("system", None, "import_env", env_path or "")
        print(f"Imported {len(regions)} region(s) from environment.")
        return 0
    raise AssertionError(f"unknown config command: {sub}")


def _handle_roles(args: argparse.Namespace) -> int:
    sub = args.roles_command
    actor = _cli_actor()
    if sub == "capabilities":
        for cap in CAPABILITY_REGISTRY:
            print(f"{cap.name:<24}  {cap.label}")
            print(f"{'':<24}  {cap.description}")
        return 0
    if sub == "list":
        rows = config_db.list_role_caps()
        if not rows:
            print("(no role mappings)")
            return 0
        per_cap: Dict[str, List[str]] = defaultdict(list)
        for row in rows:
            per_cap[row["capability"]].append(row["role_name"])
        for cap in sorted(per_cap):
            print(f"{cap}")
            for role in sorted(per_cap[cap]):
                print(f"  {role}")
        return 0
    if sub == "grant":
        capability = args.capability
        role_name = (args.role or "").strip().lower()
        if not is_known_capability(capability):
            print(f"error: unknown capability {capability!r}", file=sys.stderr)
            return 2
        if not role_name:
            print("error: role name is required", file=sys.stderr)
            return 2
        if config_db.grant_role_capability(role_name, capability):
            config_db.record_audit(
                "cli", actor, "capability_grant",
                f"{role_name}:{capability}",
            )
            print(f"Granted {role_name!r} -> {capability}")
        else:
            print(f"Already granted: {role_name!r} -> {capability}")
        return 0
    if sub == "revoke":
        capability = args.capability
        role_name = (args.role or "").strip().lower()
        if not is_known_capability(capability):
            print(f"error: unknown capability {capability!r}", file=sys.stderr)
            return 2
        # CLI shares the bootstrap-deadlock guard the web UI enforces, but
        # the CLI runs as whoever has shell access — which we already
        # treat as root-equivalent — so the guard is informational here.
        if (
            capability == Capability.MANAGE_CONFIG.value
            and config_db.count_roles_for_capability(capability) <= 1
            and not args.force
        ):
            print(
                "Refusing to remove the last role mapped to "
                "`manage_config` without --force.",
                file=sys.stderr,
            )
            return 2
        if config_db.revoke_role_capability(role_name, capability):
            config_db.record_audit(
                "cli", actor, "capability_revoke",
                f"{role_name}:{capability}",
            )
            print(f"Revoked {role_name!r} -> {capability}")
        else:
            print(f"No mapping found for {role_name!r} -> {capability}")
        return 0
    raise AssertionError(f"unknown roles command: {sub}")


def _dispatch_report(report: Report, ns: argparse.Namespace) -> int:
    kwargs: Dict[str, Any] = {}
    for param in report.params:
        kwargs[param.name] = _coerce_param(param, getattr(ns, param.name, None))
    result = report.run(**kwargs)
    _print_text(result)
    return 0


def build_parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        prog="opsbi",
        description="OpenStack operations/BI reporting suite.",
    )
    sub = root.add_subparsers(dest="command", required=True, metavar="<command>")

    for name, desc in [
        ("init", "Initialize the configuration SQLite DB."),
        ("list-regions", "List configured regions."),
        ("list-domains", "List enabled Keystone domains."),
        ("list-cells", "List discovered Nova cell DBs per region."),
    ]:
        sub.add_parser(name, help=desc, description=desc)

    admin = sub.add_parser("admin", help="Manage local administrators.")
    admin_sub = admin.add_subparsers(dest="admin_command", required=True, metavar="<subcommand>")

    sp_create = admin_sub.add_parser("create", help="Create a new local administrator.")
    sp_create.add_argument("username", nargs="?")
    sp_create.add_argument(
        "--password", help="Password (omit for an interactive prompt — recommended).",
    )

    admin_sub.add_parser("list", help="List local administrators.")

    sp_reset = admin_sub.add_parser("reset-password", help="Reset an administrator's password.")
    sp_reset.add_argument("username")
    sp_reset.add_argument("--password", help="New password (omit for prompt).")

    sp_del = admin_sub.add_parser("delete", help="Remove a local administrator.")
    sp_del.add_argument("username")

    config_p = sub.add_parser("config", help="Inspect or update configuration entries.")
    config_sub = config_p.add_subparsers(dest="config_command", required=True, metavar="<subcommand>")
    config_sub.add_parser("show", help="Print the active configuration.")
    sp_set = config_sub.add_parser("set", help="Set a web_settings key.")
    sp_set.add_argument("key")
    sp_set.add_argument("value")
    sp_imp = config_sub.add_parser(
        "import-env", help="Migrate a legacy .env into the SQLite store.",
    )
    sp_imp.add_argument(
        "--env-file",
        help="Path to a .env file (otherwise reads only the current environment).",
    )

    roles_p = sub.add_parser(
        "roles",
        help="Manage the Keystone role -> application capability mapping.",
    )
    roles_sub = roles_p.add_subparsers(
        dest="roles_command", required=True, metavar="<subcommand>",
    )
    roles_sub.add_parser(
        "capabilities", help="List the application capability registry.",
    )
    roles_sub.add_parser("list", help="List existing role -> capability mappings.")
    sp_grant = roles_sub.add_parser("grant", help="Grant a capability to a role.")
    sp_grant.add_argument("role", help="Keystone role name.")
    sp_grant.add_argument("capability", help="Capability identifier (see `roles capabilities`).")
    sp_revoke = roles_sub.add_parser("revoke", help="Revoke a capability from a role.")
    sp_revoke.add_argument("role")
    sp_revoke.add_argument("capability")
    sp_revoke.add_argument(
        "--force", action="store_true",
        help="Allow removing the last role mapped to `manage_config`.",
    )

    for report in all_reports():
        sp = sub.add_parser(
            report.id.replace("_", "-"),
            help=report.name,
            description=f"{report.name}. {report.description}",
        )
        sp.set_defaults(_report=report)
        for param in report.params:
            _add_param(sp, param)

    return root


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command in ("init", "list-regions", "list-domains", "list-cells"):
            return _handle_top_level(args.command)
        if args.command == "admin":
            return _handle_admin(args)
        if args.command == "config":
            return _handle_config(args)
        if args.command == "roles":
            return _handle_roles(args)
        report: Report = args._report
        return _dispatch_report(report, args)
    except pymysql.MySQLError as exc:
        print(f"DB error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
