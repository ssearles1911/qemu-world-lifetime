"""openstack_bi — multi-region OpenStack operations/BI reporting suite.

Package layout:

    config.py     per-region DB config parsed from env/`.env`
    db.py         thin MariaDB access layer keyed by `(region, database)`
    openstack.py  shared Keystone + Nova cell queries used across reports
    util.py       formatting, age annotation, time-series helpers
    cli.py        `opsbi` entry — argparse subparsers per registered report
    reports/      report plugins; see `reports/base.py` for the contract
    web/          Flask app factory + routes + generic form/Excel renderers

Entry points:

    `opsbi` console script (from pyproject.toml) → `openstack_bi.cli:main`
    `python web.py` at the repo root starts the Flask UI; the shim just
    imports `openstack_bi.web:app` so production WSGI servers can point at
    either `web:app` or `openstack_bi.web:app`.
"""
