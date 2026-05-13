"""Local username/password store for administrators.

Hashes use werkzeug's PBKDF2 — no extra dependency since Flask is already
in the tree. Verification updates `last_login_at` and writes an audit
entry on success/failure.
"""

from __future__ import annotations

from typing import List, Optional

from werkzeug.security import check_password_hash, generate_password_hash

from .. import config_db


def create_admin(username: str, password: str) -> int:
    """Create a new local admin. Returns the new user's id.

    Raises ValueError if the username already exists.
    """
    username = username.strip()
    if not username:
        raise ValueError("username is required")
    if not password:
        raise ValueError("password is required")
    if config_db.get_local_user(username):
        raise ValueError(f"user already exists: {username!r}")
    pw_hash = generate_password_hash(password, method="pbkdf2:sha256")
    user_id = config_db.create_local_user(username, pw_hash, is_admin=True)
    config_db.record_audit("system", None, "local_admin_created", username)
    return user_id


def reset_password(username: str, password: str) -> None:
    if not config_db.get_local_user(username):
        raise ValueError(f"unknown user: {username!r}")
    if not password:
        raise ValueError("password is required")
    config_db.set_local_password(
        username, generate_password_hash(password, method="pbkdf2:sha256")
    )
    config_db.record_audit("system", None, "local_password_reset", username)


def verify(username: str, password: str) -> Optional[dict]:
    """Return the user row on success, or None.

    Logs both outcomes to the audit log.
    """
    user = config_db.get_local_user(username)
    if not user or not check_password_hash(user["password_hash"], password):
        config_db.record_audit("local", username, "login_failure", "")
        return None
    config_db.touch_local_login(username)
    config_db.record_audit("local", username, "login_success", "")
    return user


def list_admins() -> List[dict]:
    return [u for u in config_db.list_local_users() if u["is_admin"]]


def delete_user(username: str) -> None:
    config_db.delete_local_user(username)
    config_db.record_audit("system", None, "local_admin_deleted", username)
