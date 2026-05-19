-- The SPLA-licensed hosts report joins Nova `compute_nodes` against the
-- Placement service's resource-provider traits. Placement runs in its own
-- schema; register its name here so it is editable under Admin -> Schemas
-- and resolvable via `placement_db()` like any other service. Existing
-- deployments pick this up on the next connect. (`get_schema_name` already
-- falls back to the service name, so the report works even without this
-- row — the migration just makes the name visible and overridable.)
INSERT OR IGNORE INTO schema_names (service, schema_name) VALUES
    ('placement', 'placement');
