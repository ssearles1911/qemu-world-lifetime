"""Authentication package.

Two authentication backends:

* `local` — username/password stored in the SQLite config DB. Reserved
  for administrators.
* `keystone` — OpenStack Keystone v3 password auth via keystoneauth1.
  Used by everyone else; report visibility is scoped to the user's
  effective project list.

`session` wires both backends into Flask sessions and provides the
`@login_required` / `@admin_required` decorators that web routes use.
"""
