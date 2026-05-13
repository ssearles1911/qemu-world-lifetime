"""SQLite-backed configuration store.

Holds everything that used to live in `.env`: regions, per-service schema
names, the web bind, the Keystone auth URL, the Flask SECRET_KEY, local
admin accounts, and an audit log. The path to the SQLite file is the only
thing read from the environment (`OPSBI_CONFIG_DB`); everything else is
managed through `opsbi config ...`, `opsbi admin ...`, the first-run web
setup wizard, and the admin pages.

Migrations are applied on first connect. Files in `migrations/NNNN_*.sql`
are executed in numeric order; PRAGMA `user_version` tracks the highest
applied version.
"""

from __future__ import annotations

import os
import re
import secrets
import sqlite3
import stat
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

DEFAULT_DB_PATH = Path("opsbi.sqlite").resolve()
ENV_VAR = "OPSBI_CONFIG_DB"

_MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"

_lock = threading.Lock()
_initialized_paths: set = set()


def db_path() -> Path:
    """Resolved path to the configuration SQLite file."""
    raw = os.environ.get(ENV_VAR, "").strip()
    return Path(raw).resolve() if raw else DEFAULT_DB_PATH


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _connect(path: Optional[Path] = None) -> sqlite3.Connection:
    p = path or db_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def _migration_files() -> List[Tuple[int, Path]]:
    out: List[Tuple[int, Path]] = []
    if not _MIGRATIONS_DIR.is_dir():
        return out
    for child in sorted(_MIGRATIONS_DIR.iterdir()):
        m = re.match(r"^(\d+)_.*\.sql$", child.name)
        if m:
            out.append((int(m.group(1)), child))
    return out


def _apply_migrations(conn: sqlite3.Connection) -> int:
    cur = conn.execute("PRAGMA user_version")
    current = int(cur.fetchone()[0])
    applied = 0
    for version, path in _migration_files():
        if version <= current:
            continue
        sql = path.read_text(encoding="utf-8")
        with conn:
            conn.executescript(sql)
            conn.execute(f"PRAGMA user_version = {version}")
        applied += 1
    return applied


def _ensure_secret_key(conn: sqlite3.Connection) -> None:
    """Generate a SECRET_KEY on first init. Stored in `web_settings`.

    Sessions are signed from request #1 — important because the setup
    wizard runs unauthenticated and CSRF tokens depend on it.
    """
    row = conn.execute(
        "SELECT value FROM web_settings WHERE key = 'secret_key'"
    ).fetchone()
    if row is None or not row[0]:
        with conn:
            conn.execute(
                "INSERT OR REPLACE INTO web_settings (key, value) VALUES ('secret_key', ?)",
                (secrets.token_urlsafe(48),),
            )


def init(path: Optional[Path] = None) -> sqlite3.Connection:
    """Open the config DB, apply migrations, and seed defaults.

    Idempotent — safe to call repeatedly.
    """
    conn = _connect(path)
    _apply_migrations(conn)
    _ensure_secret_key(conn)
    return conn


def check_file_perms(path: Optional[Path] = None) -> List[str]:
    """Return a list of warnings/errors about the SQLite file's permissions.

    A non-empty list with any entry starting with 'ERROR:' means the app
    should refuse to start. Entries starting with 'WARN:' are advisory.
    """
    p = path or db_path()
    if not p.exists():
        return []  # nothing to inspect yet; init() will create it
    out: List[str] = []
    try:
        st = os.stat(p)
    except OSError as exc:
        return [f"WARN: cannot stat {p}: {exc}"]
    mode = st.st_mode
    if mode & stat.S_IROTH:
        out.append(f"WARN: {p} is world-readable; restrict with `chmod 600 {p}`")
    if mode & stat.S_IWOTH:
        out.append(f"ERROR: {p} is world-writable; refuse to start until fixed")
    if mode & stat.S_IWGRP:
        try:
            current_uid = os.geteuid()
        except AttributeError:  # non-POSIX
            current_uid = -1
        if current_uid != -1 and current_uid != st.st_uid:
            out.append(
                f"ERROR: {p} is group-writable and owned by another user; "
                "refuse to start until fixed"
            )
        else:
            out.append(f"WARN: {p} is group-writable; consider `chmod 600 {p}`")
    return out


@contextmanager
def cursor() -> Iterator[sqlite3.Cursor]:
    """Open a short-lived cursor against the config DB.

    Apps can call this without worrying about init — first use of any
    accessor below ensures migrations have run.
    """
    with _lock:
        path = db_path()
        if str(path) not in _initialized_paths:
            init(path)
            _initialized_paths.add(str(path))
    conn = _connect()
    try:
        yield conn.cursor()
        conn.commit()
    finally:
        conn.close()


# --- Region accessors --------------------------------------------------------

def list_regions() -> List[Dict[str, Any]]:
    with cursor() as cur:
        cur.execute(
            "SELECT name, host, port, db_user, db_password, is_keystone_region, "
            "display_order, enabled FROM regions WHERE enabled = 1 "
            "ORDER BY display_order, name"
        )
        return [dict(row) for row in cur.fetchall()]


def list_all_regions() -> List[Dict[str, Any]]:
    """Includes disabled regions — for admin UI."""
    with cursor() as cur:
        cur.execute(
            "SELECT name, host, port, db_user, db_password, is_keystone_region, "
            "display_order, enabled FROM regions ORDER BY display_order, name"
        )
        return [dict(row) for row in cur.fetchall()]


def upsert_region(
    name: str,
    host: str,
    port: int,
    db_user: str,
    db_password: str,
    is_keystone_region: bool = False,
    display_order: int = 0,
    enabled: bool = True,
) -> None:
    with cursor() as cur:
        cur.execute(
            """
            INSERT INTO regions (name, host, port, db_user, db_password,
                                 is_keystone_region, display_order, enabled)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                host = excluded.host,
                port = excluded.port,
                db_user = excluded.db_user,
                db_password = excluded.db_password,
                is_keystone_region = excluded.is_keystone_region,
                display_order = excluded.display_order,
                enabled = excluded.enabled
            """,
            (
                name, host, int(port), db_user, db_password,
                1 if is_keystone_region else 0, int(display_order),
                1 if enabled else 0,
            ),
        )
        if is_keystone_region:
            cur.execute(
                "UPDATE regions SET is_keystone_region = 0 WHERE name <> ?",
                (name,),
            )


def delete_region(name: str) -> None:
    with cursor() as cur:
        cur.execute("DELETE FROM regions WHERE name = ?", (name,))


def get_keystone_region_name() -> Optional[str]:
    with cursor() as cur:
        cur.execute(
            "SELECT name FROM regions WHERE is_keystone_region = 1 AND enabled = 1 "
            "ORDER BY display_order, name LIMIT 1"
        )
        row = cur.fetchone()
        return row["name"] if row else None


# --- Schema name accessors ---------------------------------------------------

def get_schema_name(service: str) -> str:
    with cursor() as cur:
        cur.execute("SELECT schema_name FROM schema_names WHERE service = ?", (service,))
        row = cur.fetchone()
        return row["schema_name"] if row else service


def all_schema_names() -> Dict[str, str]:
    with cursor() as cur:
        cur.execute("SELECT service, schema_name FROM schema_names")
        return {row["service"]: row["schema_name"] for row in cur.fetchall()}


def set_schema_name(service: str, schema_name: str) -> None:
    with cursor() as cur:
        cur.execute(
            "INSERT INTO schema_names (service, schema_name) VALUES (?, ?) "
            "ON CONFLICT(service) DO UPDATE SET schema_name = excluded.schema_name",
            (service, schema_name),
        )


# --- Web settings accessors --------------------------------------------------

def web_setting(key: str, default: Optional[str] = None) -> Optional[str]:
    with cursor() as cur:
        cur.execute("SELECT value FROM web_settings WHERE key = ?", (key,))
        row = cur.fetchone()
        if row is None:
            return default
        return row["value"] if row["value"] is not None else default


def all_web_settings() -> Dict[str, str]:
    with cursor() as cur:
        cur.execute("SELECT key, value FROM web_settings")
        return {row["key"]: (row["value"] or "") for row in cur.fetchall()}


def set_web_setting(key: str, value: str) -> None:
    with cursor() as cur:
        cur.execute(
            "INSERT INTO web_settings (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )


# --- Local user accessors ----------------------------------------------------

def list_local_users() -> List[Dict[str, Any]]:
    with cursor() as cur:
        cur.execute(
            "SELECT id, username, is_admin, created_at, last_login_at "
            "FROM local_users ORDER BY username"
        )
        return [dict(row) for row in cur.fetchall()]


def get_local_user(username: str) -> Optional[Dict[str, Any]]:
    with cursor() as cur:
        cur.execute(
            "SELECT id, username, password_hash, is_admin, created_at, last_login_at "
            "FROM local_users WHERE username = ?",
            (username,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def create_local_user(username: str, password_hash: str, is_admin: bool = True) -> int:
    with cursor() as cur:
        cur.execute(
            "INSERT INTO local_users (username, password_hash, is_admin, created_at) "
            "VALUES (?, ?, ?, ?)",
            (username, password_hash, 1 if is_admin else 0, _utcnow_iso()),
        )
        return int(cur.lastrowid)


def set_local_password(username: str, password_hash: str) -> None:
    with cursor() as cur:
        cur.execute(
            "UPDATE local_users SET password_hash = ? WHERE username = ?",
            (password_hash, username),
        )


def touch_local_login(username: str) -> None:
    with cursor() as cur:
        cur.execute(
            "UPDATE local_users SET last_login_at = ? WHERE username = ?",
            (_utcnow_iso(), username),
        )


def delete_local_user(username: str) -> None:
    with cursor() as cur:
        cur.execute("DELETE FROM local_users WHERE username = ?", (username,))


def count_local_admins() -> int:
    with cursor() as cur:
        cur.execute("SELECT COUNT(*) AS n FROM local_users WHERE is_admin = 1")
        return int(cur.fetchone()["n"])


# --- Role-capability mapping accessors ---------------------------------------

def _norm_role(name: str) -> str:
    return (name or "").strip().lower()


def list_role_caps() -> List[Dict[str, str]]:
    """All (role_name, capability) rows. Ordered for stable rendering."""
    with cursor() as cur:
        cur.execute(
            "SELECT role_name, capability FROM role_capabilities "
            "ORDER BY capability, role_name"
        )
        return [dict(row) for row in cur.fetchall()]


def roles_for_capability(capability: str) -> List[str]:
    with cursor() as cur:
        cur.execute(
            "SELECT role_name FROM role_capabilities WHERE capability = ? "
            "ORDER BY role_name",
            (capability,),
        )
        return [row["role_name"] for row in cur.fetchall()]


def caps_for_roles(role_names: List[str]) -> List[str]:
    """Capabilities granted by the union of the supplied role names.

    Empty `role_names` short-circuits to an empty list to avoid the
    awkward `WHERE role_name IN ()` SQL.
    """
    normalized = [_norm_role(r) for r in role_names if _norm_role(r)]
    if not normalized:
        return []
    placeholders = ",".join("?" for _ in normalized)
    with cursor() as cur:
        cur.execute(
            f"SELECT DISTINCT capability FROM role_capabilities "
            f"WHERE role_name IN ({placeholders})",
            normalized,
        )
        return [row["capability"] for row in cur.fetchall()]


def grant_role_capability(role_name: str, capability: str) -> bool:
    """Idempotent. Returns True when a row was inserted, False when it
    already existed. Capability is **not** validated here — callers
    that want to refuse unknown capabilities should check first.
    """
    role = _norm_role(role_name)
    if not role:
        raise ValueError("role_name is required")
    with cursor() as cur:
        cur.execute(
            "INSERT OR IGNORE INTO role_capabilities (role_name, capability) "
            "VALUES (?, ?)",
            (role, capability),
        )
        return cur.rowcount > 0


def revoke_role_capability(role_name: str, capability: str) -> bool:
    role = _norm_role(role_name)
    with cursor() as cur:
        cur.execute(
            "DELETE FROM role_capabilities WHERE role_name = ? AND capability = ?",
            (role, capability),
        )
        return cur.rowcount > 0


def count_roles_for_capability(capability: str) -> int:
    with cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS n FROM role_capabilities WHERE capability = ?",
            (capability,),
        )
        return int(cur.fetchone()["n"])


# --- Audit log ---------------------------------------------------------------

def record_audit(actor_kind: str, actor_id: Optional[str], action: str, detail: str = "") -> None:
    with cursor() as cur:
        cur.execute(
            "INSERT INTO audit_log (ts, actor_kind, actor_id, action, detail) "
            "VALUES (?, ?, ?, ?, ?)",
            (_utcnow_iso(), actor_kind, actor_id, action, detail),
        )


def recent_audit(limit: int = 200) -> List[Dict[str, Any]]:
    with cursor() as cur:
        cur.execute(
            "SELECT id, ts, actor_kind, actor_id, action, detail "
            "FROM audit_log ORDER BY id DESC LIMIT ?",
            (int(limit),),
        )
        return [dict(row) for row in cur.fetchall()]


# --- Setup completeness ------------------------------------------------------

class SetupStatus:
    OK = "OK"
    NO_ADMIN = "NO_ADMIN"
    NO_REGION = "NO_REGION"
    NO_KEYSTONE_REGION = "NO_KEYSTONE_REGION"
    NO_KEYSTONE_AUTH_URL = "NO_KEYSTONE_AUTH_URL"


def setup_status() -> str:
    """Return the first missing-config reason code, or OK.

    The setup wizard and admin resume page consume this to decide what
    step to show. Keep the order aligned with the wizard step order so a
    half-completed deploy gets a guided path forward.
    """
    if count_local_admins() == 0:
        return SetupStatus.NO_ADMIN
    if not list_regions():
        return SetupStatus.NO_REGION
    if get_keystone_region_name() is None:
        return SetupStatus.NO_KEYSTONE_REGION
    if not (web_setting("keystone_auth_url") or "").strip():
        return SetupStatus.NO_KEYSTONE_AUTH_URL
    return SetupStatus.OK
