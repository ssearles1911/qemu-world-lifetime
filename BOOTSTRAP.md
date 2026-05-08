# Bootstrap

Configuration lives in a local SQLite file (`opsbi.sqlite` by default).
The only environment variable read at startup is `OPSBI_CONFIG_DB`,
which overrides the path to that file.

## First run

```bash
pip install -e .
opsbi init                 # creates ./opsbi.sqlite, applies migrations
python web.py              # serves on 127.0.0.1:8000 by default
```

Browse to `http://127.0.0.1:8000/`. The first-run setup wizard will:

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
* **Keystone users** — anyone with credentials in the configured Keystone.
  Their report visibility is scoped to the projects they have effective
  roles on.

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
