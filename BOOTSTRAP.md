# Bootstrap

Configuration lives in a local SQLite file (`opsbi.sqlite` by default).
The only environment variables read at startup are `OPSBI_CONFIG_DB`
(path to the SQLite file) and, in the Docker image, `OPSBI_BIND_ADDRESS`
(`host:port` the WSGI server binds to).

## Choose a deployment path

* [Docker (Compose)](#docker-compose) — recommended for production-style
  deploys; everything is wrapped in a single container with a persistent
  named volume.
* [Manual install](#manual-install) — virtualenv on the host; useful for
  development.

After either path, complete the same web setup wizard described below.

## Docker (Compose)

```bash
docker compose up -d
```

That builds the image from this repo, starts a single `opsbi` container,
and serves the web UI on port 8000. The configuration database (regions,
schema names, Keystone URL, local administrators, role mappings, audit
log) lives in the `opsbi-config` named volume so it survives container
restarts and image rebuilds.

Browse to `http://<host>:8000/` to start the setup wizard.

To follow the logs:

```bash
docker compose logs -f opsbi
```

To rebuild after pulling new code:

```bash
git pull
docker compose build
docker compose up -d
```

To wipe the configuration database and start over:

```bash
docker compose down
docker volume rm openstack-ops-bi-suite_opsbi-config   # name may vary
docker compose up -d
```

### Customizing the Docker deploy

* **Port** — change the host side of the published port in
  `docker-compose.yml` (e.g. `"9000:8000"` to expose on 9000).
* **Bind address** — set `OPSBI_BIND_ADDRESS=0.0.0.0:8000` (default)
  via the service `environment:` block. The `bind_host` / `bind_port`
  values shown in the setup wizard are only consulted by `python
  web.py` (the dev server); production uses waitress and the env var.
* **Volume location** — replace the named volume with a host bind mount
  if you want the SQLite file on disk:
  ```yaml
  volumes:
    - ./opsbi-data:/var/lib/opsbi
  ```
  Make sure the host directory is owned by UID/GID 10001 (the container
  user) or world-writable: `chown -R 10001:10001 ./opsbi-data`.
* **Network reachability** — the container must reach the MariaDB
  replicas and the Keystone endpoint. If those are on private networks
  unreachable from the default Docker bridge, attach the container to
  an external Docker network or switch to `network_mode: host`.
* **TLS / reverse proxy** — terminate TLS in front (nginx, Caddy,
  Traefik). The container speaks plain HTTP on its listen address.

### CLI access inside the container

```bash
docker compose exec opsbi opsbi admin create alice
docker compose exec opsbi opsbi config show
docker compose exec opsbi opsbi list-aggregates
docker compose exec opsbi opsbi roles list
```

Every `opsbi …` subcommand is available the same way.

## Manual install

```bash
pip install -e .
opsbi init                 # creates ./opsbi.sqlite, applies migrations
python web.py              # serves on 127.0.0.1:8000 by default
```

For production behind a real WSGI server:

```bash
waitress-serve --listen=0.0.0.0:8000 web:app
```

## First-run setup wizard

After either deploy path, browse to `http://<host>:8000/`. The first-run
setup wizard will:

1. Create a local administrator account.
2. Add at least one region and mark which one hosts the shared Keystone schema.
3. Confirm/edit the per-service schema names.
4. Set the Keystone v3 auth URL and default user domain.

After completion, edit any of these settings under **Admin** in the
top-right navigation.

## Authentication

* **Local administrators** — username/password stored in the configuration
  database. Created via the wizard, the **Admin → Administrators** page,
  or `opsbi admin create`.
* **Keystone users** — OpenStack users who hold the **admin role**. Login
  is rejected unless the user holds the role configured under
  **Admin → Keystone** (`keystone_admin_role`, default `admin`). They see
  every report and can run the instance actions, but the **Admin** pages
  (application configuration) are reserved for local administrators — a
  Keystone session has no Admin menu.

A Keystone login keeps the user's project-scoped token in server memory
so the SPLA report's live-migration and console actions can call the Nova
API on their behalf. The token is dropped on logout and on a container
restart (re-login restores it).

## Migrating an existing `.env`

If you have a `.env` from a previous version, you can import it once:

```bash
opsbi init
opsbi config import-env --env-file ./.env
```

Then complete the remaining wizard steps (admin account + Keystone URL).

## File permissions

`opsbi.sqlite` holds region MariaDB credentials and the Flask session
signing key. The app warns at startup if the file is world-readable and
refuses to start if it is group-writable on a multi-user host.

```bash
chmod 600 opsbi.sqlite
```

Inside the Docker image, the file is owned by UID/GID 10001 and lives
on the `opsbi-config` volume — Docker handles the permissions for you
unless you're using a host bind mount (see above).
