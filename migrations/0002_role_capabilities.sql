CREATE TABLE IF NOT EXISTS role_capabilities (
    role_name  TEXT NOT NULL,
    capability TEXT NOT NULL,
    PRIMARY KEY (role_name, capability)
);

CREATE INDEX IF NOT EXISTS role_capabilities_capability_idx
    ON role_capabilities (capability);
