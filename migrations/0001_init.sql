CREATE TABLE IF NOT EXISTS regions (
    name TEXT PRIMARY KEY,
    host TEXT NOT NULL,
    port INTEGER NOT NULL DEFAULT 3306,
    db_user TEXT NOT NULL,
    db_password TEXT NOT NULL DEFAULT '',
    is_keystone_region INTEGER NOT NULL DEFAULT 0,
    display_order INTEGER NOT NULL DEFAULT 0,
    enabled INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS schema_names (
    service TEXT PRIMARY KEY,
    schema_name TEXT NOT NULL
);

INSERT OR IGNORE INTO schema_names (service, schema_name) VALUES
    ('keystone', 'keystone'),
    ('nova_api', 'nova_api'),
    ('cinder',   'cinder'),
    ('glance',   'glance'),
    ('neutron',  'neutron');

CREATE TABLE IF NOT EXISTS web_settings (
    key TEXT PRIMARY KEY,
    value TEXT
);

INSERT OR IGNORE INTO web_settings (key, value) VALUES
    ('bind_host', '127.0.0.1'),
    ('bind_port', '8000'),
    ('keystone_auth_url', ''),
    ('keystone_default_domain', 'Default');

CREATE TABLE IF NOT EXISTS local_users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    is_admin INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    last_login_at TEXT
);

CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    actor_kind TEXT NOT NULL,
    actor_id TEXT,
    action TEXT NOT NULL,
    detail TEXT
);

CREATE INDEX IF NOT EXISTS audit_log_ts_idx ON audit_log (ts);
