# openstack-ops-bi

Multi-region OpenStack operations / BI reporting suite. Queries per-region
MariaDB replicas plus a shared Keystone directly. Ships a report plugin
architecture so new reports plug in without touching the CLI or web UI:

- **CLI** — `opsbi <report>` with a subparser per registered report, plus
  discovery commands (`list-regions`, `list-domains`, `list-cells`,
  `list-aggregates`) and configuration commands (`init`, `config`,
  `admin`, `roles`).
- **Web** — authenticated Flask app: a report catalog grouped by category,
  a form-driven page per report with one-click Excel download, a first-run
  setup wizard, and admin pages. Charts render in-browser via Chart.js and
  are embedded as PNGs in the Excel export.

## Reports

| ID | Category | Purpose |
| --- | --- | --- |
| `issues` | Findings | Cross-service health check — instances in error, stuck task_states, volumes in transient/orphaned states, long-unbound floating IPs, stale snapshots. One row per finding with severity. |
| `qemu_lifetime` | Lifecycle | Last start/stop/shelve/unshelve/shelveOffload/live-migration event per instance, grouped by project. Filter by domain, state, min-age, and region. |
| `domain_leaderboard` | Projects | Keystone domains ranked by instance count across the selected regions, broken down by vm_state. Drill into a domain's project breakdown. |
| `instance_leaderboard` | Projects | Projects ranked by instance count across the selected regions, broken down by vm_state. |
| `project_growth` | Projects | Per-project concurrent instance count over time, reconstructed from `instances.created_at` / `deleted_at`. Line chart for the top-N projects. |
| `snapshot_leaderboard` | Projects | Projects ranked by Cinder + Glance snapshot count and storage footprint; oldest-snapshot age flagged. |
| `stale_snapshots` | Capacity | Cinder volume snapshots older than N days (default 90), one row per snapshot, sorted oldest-first. |
| `fip_audit` | Capacity | Unbound floating IPs per project and region, sorted oldest-first. |
| `fip_pools` | Capacity | Per-region external-network FIP pool utilization (used / free / bound / unbound). |
| `fip_subnets` | Capacity | Per-subnet, per-allocation-pool drill-down of FIP-bearing external networks (CIDR, gateway, range bounds, used vs. free). Per-region TOTAL row. |
| `instance_history` | Lifecycle | Full Nova `instance_actions` log for one instance UUID, across every region and cell (drill-down). |
| `volume_history` | Lifecycle | Cinder metadata + attachment timeline for one volume UUID (drill-down). |
| `volume_resizes` | Lifecycle | Cinder volume extend events in the last N days (limited by `cinder.messages` retention). |
| `spla_instances` | Licensing | Active VMs whose boot volume's Glance image name matches a configurable LIKE pattern (default `%SPLA%`); per-region vCPU/memory rollups. Keystone sessions get per-row live-migrate / console actions. |

## Why query the DB instead of the API or virsh?

- **Centralized.** One MariaDB replica per region beats fanning SSH/virsh
  calls across every compute node.
- **Fast.** No per-instance round-trips; a single CTE returns the whole set
  per cell.
- **Zero control-plane impact.** Reads go to replicas; nothing touches Nova
  services or hypervisors.

The tradeoff: this is *user-visible* uptime (what Nova recorded), not the
underlying QEMU process lifetime. Live-migration is included as a lifecycle
event so operator-initiated moves show up.

## Requirements

- Python 3.8+ (the Docker image ships 3.12).
- A MariaDB replica per OpenStack region, each holding that region's
  `nova_api`, `nova_cell*`, `cinder`, `glance`, and `neutron` schemas.
  One replica (or a separate shared replica) must also host the shared
  `keystone` schema.
- A DB user with `SELECT` on those schemas. Per-region credentials are
  supported.
- MariaDB 10.2+ (the queries use CTEs and window functions). Anything
  Ussuri-era and newer is fine.
- A reachable Keystone v3 auth endpoint if you want Keystone users to log
  into the web UI. Local administrator accounts work without it, but the
  setup wizard asks for the Keystone URL.

## Install

```
git clone git@github.com:ssearles1911/openstack-ops-bi-suite.git
cd openstack-ops-bi-suite
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

`pip install -e .` puts the `opsbi` console command on your PATH and pulls
in the runtime dependencies (Flask, Flask-WTF, Werkzeug, PyMySQL,
keystoneauth1, requests, openpyxl, matplotlib, waitress). If you'd rather
not install the package itself, `pip install -r requirements.txt` installs
the same set — then invoke the CLI as `python -m openstack_bi.cli` instead
of `opsbi`.

Either way, the next step is to initialize the configuration store and run
the setup wizard — see [Configuration](#configuration). For a
container-based deploy, skip to [Docker deployment](#docker-deployment).

## Docker deployment

For a production-style deploy, run the app as a container instead of a
host virtualenv. The repo ships a production-ready image —
`python:3.12-slim`, served by `waitress` (a production WSGI server),
running as a non-root user, applying DB migrations on every start.

### docker compose (recommended)

```
docker compose up -d
```

Builds the image from this repo, starts a single `opsbi` container, and
serves the web UI on port 8000. The configuration database (regions,
schema names, Keystone URL, local administrators, role mappings, audit
log) lives in the `opsbi-config` named volume, so it survives container
restarts and image rebuilds.

### Standalone container

Compose is only a thin wrapper — the image runs on its own:

```
docker build -t openstack-ops-bi .
docker run -d --name opsbi \
  -p 8000:8000 \
  -v opsbi-config:/var/lib/opsbi \
  --restart unless-stopped \
  openstack-ops-bi
```

Either way, browse to `http://<host>:8000/` to complete the first-run
setup wizard. The container must be able to reach the per-region MariaDB
replicas and the Keystone endpoint; if those sit on private networks
unreachable from the default Docker bridge, attach an external Docker
network or run with `--network host`.

See [BOOTSTRAP.md](BOOTSTRAP.md) for host bind-mount options, running
`opsbi` CLI subcommands inside the container, TLS/reverse-proxy setup,
and how to reset the configuration database.

## Configuration

There is no `.env` file and no service-specific environment variables.
All configuration — regions and their MariaDB credentials, per-service
schema names, the Keystone auth URL, local administrator accounts, the
role-to-capability mapping, and the audit log — lives in a single SQLite
file (`opsbi.sqlite` by default). It is created and migrated by
`opsbi init` and edited three ways:

- the **first-run web setup wizard** at `/setup`,
- the **Admin** pages in the web UI, and
- the `opsbi config`, `opsbi admin`, and `opsbi roles` CLI subcommands.

### Environment variables

Only two environment variables are read, both optional:

| Variable             | Default          | Purpose |
| -------------------- | ---------------- | ------- |
| `OPSBI_CONFIG_DB`    | `./opsbi.sqlite` | Path to the configuration SQLite file. |
| `OPSBI_BIND_ADDRESS` | *(unset)*        | `host:port` for `waitress-serve` in the Docker image. Ignored by `python web.py`, which reads `bind_host` / `bind_port` from the config DB. |

### First-run setup wizard

After `opsbi init`, start the web UI and browse to `http://<host>:<port>/`.
With no administrator configured, every request is routed to the wizard,
which walks through four steps:

1. Create the first local administrator account.
2. Add at least one region (host, port, DB credentials) and mark which
   region's replica reaches the shared `keystone` schema.
3. Confirm or edit the per-service schema names (`keystone`, `nova_api`,
   `cinder`, `glance`, `neutron`).
4. Set the Keystone v3 auth URL and default user domain.

Every setting is editable afterwards under **Admin** in the top-right
navigation.

### Migrating a legacy `.env`

Earlier versions read configuration from environment variables / a `.env`
file. To carry an old `.env` into the SQLite store:

```
opsbi init
opsbi config import-env --env-file ./.env
```

This imports the regions, the Keystone region, and the schema names.
Complete the remaining wizard steps (admin account, Keystone URL)
afterwards.

### File permissions

`opsbi.sqlite` holds region MariaDB credentials and the Flask session
signing key. The app warns at startup if the file is world-readable and
refuses to start if it is world-writable (or group-writable and owned by
another user):

```
chmod 600 opsbi.sqlite
```

Under Docker the file lives on a named volume owned by UID/GID 10001 and
Docker handles the permissions for you. See [BOOTSTRAP.md](BOOTSTRAP.md)
for the full deployment and first-run guide.

## Authentication & access control

The web UI requires a login. Two kinds of accounts are supported:

- **Local administrators** — username/password stored hashed in the
  config DB. Created by the setup wizard, the **Admin → Administrators**
  page, or `opsbi admin create`. Local admins implicitly hold every
  capability.
- **Keystone users** — OpenStack users who hold the **admin role**. They
  sign in with their Keystone username, password, and (optionally)
  domain; login is rejected unless the user holds the role named under
  **Admin → Keystone** (the `keystone_admin_role` setting, default
  `admin`).

Everyone who can sign in is an administrator — they see every report and
reach the admin pages. The CLI is not gated: it runs as whoever has
shell access and is treated as root-equivalent.

A Keystone login also keeps that user's project-scoped token server-side
(in process memory, keyed by an opaque cookie value) so the app can call
the Nova API on their behalf — see [Instance actions](#instance-actions).
The token is discarded on logout and on app restart, and expires after
about an hour, after which the action prompts for a fresh sign-in.

> The role → capability mapping (**Admin → Roles**, `opsbi roles`) and
> the `view_all_projects` / `manage_*` capabilities predate the
> admin-only Keystone login gate. They remain in the code but no longer
> affect access now that every login is an administrator.

## Instance actions

When signed in via **Keystone**, the **SPLA-licensed instances** report
gains an **Actions** column with two per-row operations:

- **Live migrate** — opens an in-page dialog to pick a target host (the
  `nova-compute` hosts in that instance's region, fetched live from
  Nova) and starts a live migration — without leaving the report.
- **Console** — opens the instance's noVNC console in a new browser tab.

Both call the Nova API in the instance's region as the logged-in user,
via the scoped token kept from login, so the user needs Nova permission
for the operation (admin under default policy). They require the app's
region names to match the Keystone catalog region names. In a
local-administrator session — which has no Keystone token — the buttons
are shown disabled.

## CLI usage

### Setup & administration

```
opsbi init                       # create + migrate ./opsbi.sqlite
opsbi config show                # print the active configuration
opsbi config import-env --env-file ./.env
opsbi admin create alice         # create a local administrator (prompts for password)
opsbi admin list
opsbi admin reset-password alice
opsbi roles capabilities         # list the capability registry
opsbi roles grant reader view_all_projects
opsbi roles list
```

### Discovery

```
opsbi list-regions               # configured regions
opsbi list-domains               # enabled Keystone domains
opsbi list-cells                 # Nova cell DBs per region
opsbi list-aggregates            # Nova host aggregates per region
```

### Running reports

```
# show all registered reports
opsbi --help

# help for one report shows its parameters
opsbi qemu-lifetime --help

# run qemu-lifetime across all regions for a domain
opsbi qemu-lifetime --domain heroes

# scope to specific regions (repeat --regions); filter by state/age
opsbi qemu-lifetime --domain heroes --regions dfw1 --regions ord1 \
                    --state stopped --days 80

# use the "all states" sentinel to disable the state filter
opsbi qemu-lifetime --domain heroes --state __all__
```

Report subcommand names are the report ID with underscores replaced by
hyphens (`qemu_lifetime` → `qemu-lifetime`). Output is grouped per the
report (the qemu-lifetime report groups by project; other reports may be
flat). For each grouped section, rows come back pre-sorted by the report.

**qemu-lifetime filters:**

- **Region** — defaults to *all* configured regions. `--regions NAME` is
  repeatable.
- **State** — defaults to `vm_state=active`. Pass `--state NAME` for a
  single different state, or `--state __all__` for everything.
- **Days** — `--days N` shows instances whose most-recent lifecycle event
  is older than N days. Instances with *no* recorded lifecycle action are
  anchored to `instances.created_at` so a never-touched VM still shows up.

## Web usage

```
opsbi init
python web.py
# → serves on the bind_host/bind_port from the config DB
#   (127.0.0.1:8000 until you change it)
```

`python web.py` is the Flask development server. It reads its listen
address from the `bind_host` / `bind_port` web settings — set those in the
wizard or with `opsbi config set bind_host 0.0.0.0`.

The landing page is a login screen (or the setup wizard until it is
complete). After signing in you get the report catalog, grouped by
category. Click a report, fill in its form, click **Run report**. Results
render in-page; charts (where the report defines any) render via vendored
Chart.js. Click **Download Excel** to get an `.xlsx` with:

- Metadata header at the top (all report metadata + generated-at
  timestamp).
- Data sheet with frozen header row and auto-filter on every column.
- One sheet per chart, with a matplotlib-rendered PNG plus the raw
  series data below it for spreadsheet formulas.

For a long-running deployment, use a production WSGI server instead of the
Flask dev server (no code changes needed):

```
waitress-serve --listen=0.0.0.0:8000 web:app
```

The `OPSBI_BIND_ADDRESS` environment variable lets the Docker image pass
the listen address through to waitress. For containerized deploys see
[Docker deployment](#docker-deployment) and [BOOTSTRAP.md](BOOTSTRAP.md).

## QEMU lifetime — actions tracked

The qemu-lifetime report considers exactly these
`nova.instance_actions.action` values:

```
start, stop, shelve, unshelve, shelveOffload, live-migration
```

Deliberately excluded: `reboot`, `migrate` (cold), `resize`, `rebuild`,
`create`. These either don't match the operational signal of interest
(reboots don't correlate with maintenance windows) or duplicate
information already captured elsewhere (`create` = instance age).

To change the set, edit `LIFECYCLE_ACTIONS` in
`openstack_bi/reports/qemu_lifetime.py` — the CLI, web UI, and Excel
export all read from it.

## How it works

1. Per-region connection details come from the SQLite configuration
   store, loaded by `openstack_bi.config` into `Region` objects.
2. Each report is a `Report` subclass registered in
   `openstack_bi/reports/__init__.py`. It declares params; the CLI and
   web UI render those params into their respective input surfaces.
3. At run time, a report's `run(**kwargs)` returns a `ReportResult`
   (columns, rows, groupings, charts, metadata). The CLI prints a
   grouped/flat text table; the web UI renders Chart.js + HTML tables;
   the Excel exporter writes a workbook with a metadata block, the data
   table, and one sheet per chart (matplotlib-rendered PNG + raw series).
4. The QEMU-lifetime report specifically resolves a domain + project
   list once from the shared Keystone, then for each selected region
   discovers cell DBs and runs a CTE + window-function query per cell,
   aggregating in Python and tagging each row with its region.

## Adding a new report

1. Create `openstack_bi/reports/<slug>.py` with a subclass of
   `openstack_bi.reports.base.Report` and a module-level
   `REPORT = MyReport()`.
2. Add `from . import <slug>` and append the module to `_ORDER` in
   `openstack_bi/reports/__init__.py`.
3. That's it — the report appears in `opsbi --help` and in the web
   catalog automatically.

## Project layout

```
openstack_bi/
  config.py         Region dataclass; parse_regions(); keystone_region()
  config_db.py      SQLite configuration store + migration runner
  db.py             connect/query against one (region, database)
  openstack.py      shared Keystone + Nova cell queries
  nova.py           Nova compute REST API client (live migration, console)
  util.py           humanize, annotate_ages
  cli.py            `opsbi` entry: report subparsers + admin/config/roles
  auth/
    capabilities.py fixed capability registry
    keystone.py     Keystone v3 password auth + role/project resolution
    local.py        local administrator accounts (hashed passwords)
    session.py      Flask session helpers + capability decorators
    token_store.py  in-memory scoped Keystone token cache
  reports/
    __init__.py     registry — add new report modules here
    base.py         Report ABC + Param/ReportResult/ChartSpec
    qemu_lifetime.py  ... (one module per report)
  web/
    __init__.py     Flask app factory + auth/setup gate
    routes.py       catalog + per-report runner + Excel export
    auth_routes.py  login / logout
    setup_routes.py first-run setup wizard
    admin_routes.py admin pages (regions, schemas, keystone, users, roles, audit)
    instance_routes.py  live-migrate / console Nova actions
    forms.py        request.args → report kwargs + form-values echo
    excel.py        generic .xlsx with matplotlib chart embedding
migrations/         NNNN_*.sql config-DB schema migrations
templates/          base/catalog/report/login + admin/ + setup/ subdirs
static/
  chart.min.js      vendored Chart.js
web.py              entry shim: `waitress-serve web:app`, `python web.py`
Dockerfile          production container image
docker-compose.yml  single-container Compose deployment
BOOTSTRAP.md        deployment + first-run guide
pyproject.toml      package metadata; exposes the `opsbi` console script
requirements.txt    runtime deps (mirrors pyproject.toml)
```

## Limitations and notes

- **`instance_actions` retention.** Nova can be configured to purge old
  action rows. Instances older than the retention horizon are reported
  with `last_action = (none recorded)` and age anchored to `created_at`.
- **Live migration counts as a lifecycle event — by design.** If you want
  "user-requested" events only, remove `live-migration` from
  `LIFECYCLE_ACTIONS`.
- **One domain per run.** Cross-domain aggregation isn't built in yet.
- **`cell0` is included.** It normally holds failed-to-schedule instances
  with no lifecycle data; cost is negligible.
- **Shared Keystone assumed.** Project IDs are expected to be globally
  unique across regions. Reports resolve project names once from the
  Keystone region rather than cross-DB-joining per cell, so Keystone
  and Nova can live on different physical replicas.
- **The web UI requires login** (local administrator or a Keystone user
  with the admin role). It speaks plain HTTP and `python web.py` binds to
  `127.0.0.1` by default — terminate TLS at a reverse proxy before
  exposing it widely.
- **Read-only against the databases.** Nothing in this project writes to
  the OpenStack databases; point it at replicas. The one exception to
  "no control-plane impact" is the SPLA report's instance actions, which
  call the Nova API (live migration, console) on the user's behalf.
```
