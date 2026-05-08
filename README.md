# openstack-ops-bi

Multi-region OpenStack operations / BI reporting suite. Queries per-region
MariaDB replicas plus a shared Keystone directly. Ships a report plugin
architecture so new reports plug in without touching the CLI or web UI:

- **CLI** — `opsbi <report>` with a subparser per registered report, plus
  `opsbi list-regions`, `list-domains`, `list-cells`.
- **Web** — Flask catalog page; each report has its own form-driven page
  and one-click Excel download. Charts render in-browser via Chart.js and
  are embedded as PNGs in the Excel export.

## Reports

| ID | Purpose |
| --- | --- |
| `issues` | Cross-service health dashboard (error VMs, stuck states, orphaned volumes, old unbound FIPs, stale snapshots) grouped by severity. |
| `qemu_lifetime` | Last start/stop/shelve/unshelve/shelveOffload/live-migration per instance, grouped by project. |
| `instance_leaderboard` | Projects ranked by instance count across regions, broken down by vm_state. |
| `project_growth` | Per-project concurrent instance count over time, derived from `instances.created_at` / `deleted_at`. |
| `snapshot_leaderboard` | Projects ranked by Cinder + Glance snapshot footprint; oldest-snapshot age flagged. |
| `stale_snapshots` | Cinder snapshots older than N days, one row per snapshot, grouped by project. |
| `fip_audit` | Unbound floating IPs per project, sorted oldest-first. |
| `fip_pools` | Per-region external-network FIP pool utilization. |
| `instance_history` | Full Nova action log for one instance UUID (drill-down). |
| `volume_history` | Cinder metadata + attachment timeline for one volume UUID (drill-down). |
| `volume_resizes` | Cinder extend events in the last N days (limited by `cinder.messages` retention). |

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

- Python 3.8+.
- A MariaDB replica per OpenStack region, each holding that region's
  `nova_api` and `nova_cell*` DBs. One of them (or a separate shared
  replica) must also host the shared `keystone` DB.
- A DB user with `SELECT` on those schemas. Per-region credentials are
  supported.
- MariaDB 10.2+ (the query uses CTEs and window functions). Anything
  Ussuri-era and newer is fine.

## Install

```
git clone git@github.com:ssearles1911/openstack-ops-bi-suite.git
cd openstack-ops-bi-suite
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

`pip install -e .` puts the `opsbi` console command on your PATH and
pulls in runtime deps (PyMySQL, Flask, openpyxl, python-dotenv,
matplotlib). `pip install -r requirements.txt` still works if you prefer
not to install the package itself — in that case, invoke via
`python -m openstack_bi.cli` instead of `opsbi`.

## Configuration

All config is via environment variables. The CLI and web app auto-load a
`.env` file from the current working directory; real env vars still take
precedence, so you can override per-run.

### Multi-region (recommended)

```
OS_DB_REGIONS=dfw1,ord1
KEYSTONE_REGION=dfw1            # which region's replica reaches `keystone`

OS_DB_HOST__DFW1=replica-dfw1.internal
OS_DB_PORT__DFW1=3306
OS_DB_USER__DFW1=reporting
OS_DB_PASSWORD__DFW1=...

OS_DB_HOST__ORD1=replica-ord1.internal
OS_DB_USER__ORD1=reporting
OS_DB_PASSWORD__ORD1=...

# Optional fallbacks (used when per-region value is missing)
OS_DB_PORT=3306
OS_DB_USER=reporting
```

Per-region suffix convention: `<REGION_NAME>` uppercased, with any non-
alphanumeric character replaced by an underscore — so `dfw1` → `DFW1`,
`us-east-2` → `US_EAST_2`.

### Single-region (legacy / backwards compatible)

If `OS_DB_REGIONS` is unset but the bare `OS_DB_HOST` / `OS_DB_USER` /
`OS_DB_PASSWORD` variables are set, a single region named `default` is
synthesized. Existing deployments keep working without any `.env` changes.

### Variable reference

| Variable                 | Default     | Purpose                                                |
| ------------------------ | ----------- | ------------------------------------------------------ |
| `OS_DB_REGIONS`          | *(unset)*   | Comma-separated region names. Empty ⇒ single-region fallback. |
| `KEYSTONE_REGION`        | first listed | Region whose replica hosts shared `keystone`.         |
| `OS_DB_HOST__<REGION>`   | `127.0.0.1` | Replica host for one region.                           |
| `OS_DB_PORT__<REGION>`   | `3306`      | Replica port for one region.                           |
| `OS_DB_USER__<REGION>`   | `nova`      | DB user for one region.                                |
| `OS_DB_PASSWORD__<REGION>` | *(empty)* | DB password for one region.                            |
| `OS_DB_HOST`, etc.       | —           | Fallback values if a per-region variable is missing.   |
| `KEYSTONE_DB`            | `keystone`  | Keystone schema name.                                  |
| `NOVA_API_DB`            | `nova_api`  | Used for cell auto-discovery.                          |
| `QLR_HOST`               | `127.0.0.1` | `web.py` bind host.                                    |
| `QLR_PORT`               | `8000`      | `web.py` bind port.                                    |

### `.env` file (recommended)

```
cp .env.example .env
$EDITOR .env
```

`.env` is gitignored. Overriding for a single run:

```
OS_DB_PASSWORD__DFW1=oneoff opsbi list-domains
```

## CLI usage

```
# list what's configured / reachable
opsbi list-regions
opsbi list-domains
opsbi list-cells

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

Output is grouped per the report (the qemu-lifetime report groups by
project; other reports may be flat). For each grouped section, rows
come back pre-sorted by the report.

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
python web.py
# → http://127.0.0.1:8000/
```

Landing page is a report catalog. Click a report, fill in its form,
click **Run report**. Results render in-page; charts (where the report
defines any) render via vendored Chart.js. Click **Download Excel** to
get an `.xlsx` with:

- Metadata header at the top (all report metadata + generated-at
  timestamp).
- Data sheet with frozen header row and auto-filter on every column.
- One sheet per chart, with a matplotlib-rendered PNG plus the raw
  series data below it for spreadsheet formulas.

Bind elsewhere:

```
QLR_HOST=0.0.0.0 QLR_PORT=8000 python web.py
```

For a long-running deployment, use a production WSGI server instead of the
Flask dev server (no code changes needed):

```
pip install waitress
waitress-serve --host=0.0.0.0 --port=8000 web:app
```

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

1. Per-region connection details come from env/`.env` (parsed by
   `openstack_bi.config` into `Region` objects).
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
  db.py             connect/query against one (region, database)
  openstack.py      shared Keystone + Nova cell queries
  util.py           humanize, annotate_ages
  cli.py            `opsbi` entry: argparse subparsers per report
  reports/
    __init__.py     registry — add new report modules here
    base.py         Report ABC + Param/ReportResult/ChartSpec
    qemu_lifetime.py
  web/
    __init__.py     Flask app factory
    routes.py       catalog + per-report runner + Excel export
    forms.py        request.args → report kwargs + form-values echo
    excel.py        generic .xlsx with matplotlib chart embedding
templates/
  base.html         layout + CSS
  catalog.html      report catalog
  report.html       form + results + Chart.js canvases
static/
  chart.min.js      vendored Chart.js
web.py              entry shim: `waitress-serve web:app`, `python web.py`
pyproject.toml      exposes `opsbi` console script
requirements.txt    runtime deps
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
  unique across regions. The report resolves project names once from
  `KEYSTONE_REGION` rather than cross-DB-joining per cell, so Keystone
  and Nova can live on different physical replicas.
- **The web UI is unauthenticated** and binds to `127.0.0.1` by default.
  Put it behind auth (basic-auth reverse proxy, SSO) or keep it local
  before exposing widely.
- **Read-only replica assumption.** Nothing in this project writes; point
  it at replicas to keep the control plane out of the hot path.
