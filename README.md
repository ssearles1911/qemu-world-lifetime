# qemu-world-lifetime

QEMU instance lifetime reporting for large OpenStack deployments. Queries a
MariaDB replica (Keystone + Nova) directly to answer: **for every instance in
a given domain, when was it last started, stopped, shelved, unshelved, or
live-migrated — and how long ago?**

Ships two interfaces that share one query layer:

- **CLI** — interactive menu or flag-driven, plain-text grouped output.
- **Web** — Flask UI with browser tables and one-click Excel export.

## Why query the DB instead of the API or virsh?

- **Centralized.** One MariaDB replica beats fanning SSH/virsh calls across
  every compute node.
- **Fast.** No per-instance round-trips; a single CTE returns the whole set.
- **Zero control-plane impact.** Reads go to a replica; nothing touches Nova
  services or hypervisors.

The tradeoff: this is *user-visible* uptime (what Nova recorded), not the
underlying QEMU process lifetime. Live-migration is included as a lifecycle
event so operator-initiated moves show up.

## Requirements

- Python 3.8+
- A MariaDB replica that hosts all OpenStack DBs on the same server:
  `keystone`, `nova_api`, and every `nova_cell*` DB.
- A DB user with `SELECT` on those schemas.
- MariaDB 10.2+ (the query uses CTEs and window functions). Anything Ussuri-era
  and newer is fine.

## Install

```
git clone git@github.com:ssearles1911/qemu-world-lifetime.git
cd qemu-world-lifetime
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configuration

All config is via environment variables.

| Variable         | Default       | Purpose                                                     |
| ---------------- | ------------- | ----------------------------------------------------------- |
| `OS_DB_HOST`     | `127.0.0.1`   | Replica hostname                                            |
| `OS_DB_PORT`     | `3306`        | Replica port                                                |
| `OS_DB_USER`     | `nova`        | Any user with `SELECT` on keystone / nova_api / nova_cell*  |
| `OS_DB_PASSWORD` | *(empty)*     | DB password                                                 |
| `KEYSTONE_DB`    | `keystone`    | Keystone schema name                                        |
| `NOVA_API_DB`    | `nova_api`    | Used for cell auto-discovery                                |
| `QLR_HOST`       | `127.0.0.1`   | `web.py` bind host                                          |
| `QLR_PORT`       | `8000`        | `web.py` bind port                                          |

### `.env` file (recommended)

The CLI and web app auto-load a `.env` file from the current working
directory (via `python-dotenv`), so you don't have to export variables
each session. Copy the template and edit:

```
cp .env.example .env
$EDITOR .env
```

`.env` is gitignored. Real environment variables still take precedence
over `.env` entries, so you can override individual values for a single
run:

```
OS_DB_HOST=other-replica.internal python qemu_lifetime_report.py --list-domains
```

### Or export manually

```
export OS_DB_HOST=mariadb-replica.internal
export OS_DB_USER=reporting
export OS_DB_PASSWORD=secret
```

## CLI usage

```
# interactive — prompts for domain, then for the min-days filter
python qemu_lifetime_report.py

# fully non-interactive
python qemu_lifetime_report.py --domain heroes --days 80

# helpers
python qemu_lifetime_report.py --list-domains
python qemu_lifetime_report.py --list-cells
python qemu_lifetime_report.py --help
```

Output is grouped by project under the selected domain, sorted oldest-first
within each project so long-idle VMs surface at the top.

With `--days N`, only instances whose most-recent lifecycle action is older
than `N` days are shown. Instances with *no* recorded lifecycle action are
included and anchored to `instances.created_at` (so a never-touched VM still
shows up under any `--days` filter — which is usually what you want).

## Web usage

```
python web.py
# → http://127.0.0.1:8000/
```

Pick a domain, optionally set a minimum-days filter, click **Run report**.
The report renders grouped by project; click **Download Excel** to fetch the
same query as an `.xlsx` with:

- Metadata header (domain, filter, action set, generated-at timestamp).
- Frozen table header row and auto-filter on every column.
- A numeric `age_days` column alongside the human-readable `age`, so Excel
  can sort/filter properly.

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

## Lifecycle actions tracked

The report considers exactly these `nova.instance_actions.action` values:

```
start, stop, shelve, unshelve, shelveOffload, live-migration
```

Deliberately excluded: `reboot`, `migrate` (cold), `resize`, `rebuild`,
`create`. These either don't match the operational signal of interest
(reboots don't correlate with maintenance windows) or duplicate
information already captured elsewhere (`create` = instance age).

To change the set, edit `LIFECYCLE_ACTIONS` in `core.py` — the CLI, web UI,
and Excel export all read from it.

## How it works

1. Reads `nova_api.cell_mappings` to discover every cell DB (no hardcoding).
2. For each cell, issues one query that:
   - pre-filters `instances` to projects in the selected domain,
   - picks each instance's most recent lifecycle action via a CTE +
     `ROW_NUMBER() OVER (PARTITION BY instance_uuid ORDER BY start_time DESC)`,
   - cross-joins `keystone.project` to resolve the project name.
3. Aggregates rows from all cells in Python; computes age from
   `COALESCE(last_action_time, instances.created_at)`; renders.

The same `core.collect_report()` call powers the CLI, the web UI, and the
Excel export — the table you see in the browser and the rows in the
downloaded spreadsheet come from one query and are guaranteed to match.

## Project layout

```
core.py                  shared DB queries, action set, age annotation
qemu_lifetime_report.py  CLI entry point
web.py                   Flask app + .xlsx export
templates/index.html     single-page UI
requirements.txt         PyMySQL, Flask, openpyxl
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
- **The web UI is unauthenticated** and binds to `127.0.0.1` by default.
  Put it behind auth (basic-auth reverse proxy, SSO) or keep it local
  before exposing widely.
- **Read-only replica assumption.** Nothing in this project writes; point
  it at a replica to keep the control plane out of the hot path.
