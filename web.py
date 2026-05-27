#!/usr/bin/env python3
"""Entry shim for the web UI.

Real app lives in `openstack_bi.web`. This file exists so
`waitress-serve web:app` and `python web.py` keep working.

Starting the in-process dashboard scheduler here (instead of inside
`create_app()`) keeps `create_app()` pure for tests and the CLI — only
processes that boot through this entry shim get the daemon thread.
"""

from openstack_bi import scheduler
from openstack_bi.web import create_app

app = create_app()
scheduler.start()


if __name__ == "__main__":
    from openstack_bi import config_db

    host = config_db.web_setting("bind_host", "127.0.0.1") or "127.0.0.1"
    port_s = config_db.web_setting("bind_port", "8000") or "8000"
    try:
        port = int(port_s)
    except ValueError:
        port = 8000
    app.run(host=host, port=port, debug=False)
