# syntax=docker/dockerfile:1
#
# openstack-ops-bi — production image
#
# Run with:
#
#   docker compose up -d
#
# Browse to http://<host>:8000/ for the first-run setup wizard.
# Configuration persists in the `opsbi-config` named volume mounted at
# /var/lib/opsbi inside the container.

FROM python:3.12-slim

# tini reaps zombies and forwards signals cleanly. matplotlib + openpyxl
# pull in their own wheels, so no compiler is needed here.
RUN apt-get update \
 && apt-get install -y --no-install-recommends tini \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install the package + runtime deps. Copying only the files needed for
# installation keeps the layer cacheable when other repo files change.
COPY pyproject.toml /app/
COPY openstack_bi /app/openstack_bi
COPY migrations /app/migrations
COPY templates /app/templates
COPY static /app/static
COPY web.py BOOTSTRAP.md README.md /app/
RUN pip install --no-cache-dir .

# Non-root account owns the app directory and the persistent volume.
# Fixed UID/GID makes host-side volume permissions predictable.
RUN groupadd -r --gid 10001 opsbi \
 && useradd -r -g opsbi --uid 10001 -m -d /home/opsbi -s /sbin/nologin opsbi \
 && mkdir -p /var/lib/opsbi \
 && chown -R opsbi:opsbi /var/lib/opsbi /app

USER opsbi

ENV OPSBI_CONFIG_DB=/var/lib/opsbi/opsbi.sqlite
ENV OPSBI_BIND_ADDRESS=0.0.0.0:8000

VOLUME ["/var/lib/opsbi"]
EXPOSE 8000

# Apply DB migrations idempotently on every start, then serve via
# waitress (production WSGI). Settings stored in web_settings for
# bind_host/bind_port don't apply here — the listen address is set
# explicitly so the container's port map works.
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["sh", "-c", "opsbi init >/dev/null && exec waitress-serve --listen=${OPSBI_BIND_ADDRESS} web:app"]
