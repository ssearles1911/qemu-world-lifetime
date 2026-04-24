#!/usr/bin/env python3
"""Entry shim for the web UI.

Real app lives in `openstack_bi.web`. This file exists so
`waitress-serve web:app` and `python web.py` keep working.
"""

import os

from openstack_bi.web import app

if __name__ == "__main__":
    host = os.environ.get("QLR_HOST", "127.0.0.1")
    port = int(os.environ.get("QLR_PORT", "8000"))
    app.run(host=host, port=port, debug=False)
