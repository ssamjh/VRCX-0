# Headless collector and desktop history sync

The headless collector records friend activity on an always-on server. The
desktop app imports that history when it connects to the collector, including
events recorded while the desktop was closed. Imported history is available
offline.

## Docker

Docker Engine and Docker Compose are required. Download the single
`compose.headless.yaml` file from the project's GitHub repository. In its
`build.context` setting, enter the public Git URL and ref for the repository
you want to build, for example `https://github.com/OWNER/REPOSITORY.git#BRANCH`.
Alternatively, leave the required `VRCX_COLLECTOR_SOURCE` expression in place
and set that variable in the shell before running Compose. No local repository
checkout is needed; Docker fetches the selected source ref as the build
context, without fetching its Git history.

Before starting, edit `VRCX_COLLECTOR_TOKEN` in the Compose file and replace
the example with a long, random token. For example, generate one with
`openssl rand -hex 32`. Keep the edited file private and do not commit it.
The Compose project has a stable name and stores the database and saved VRChat
session in its `collector-data` volume.

Build the image, then log into VRChat interactively:

```sh
docker compose -f compose.headless.yaml build
docker compose -f compose.headless.yaml run --rm collector --login
```

Complete any two-factor challenge. After the runtime reports that it is
running, stop it with Ctrl+C and start the collector in the background:

```sh
docker compose -f compose.headless.yaml up -d
docker compose -f compose.headless.yaml logs -f collector
```

The collector listens on port 9001 on the server's loopback interface by
default. This keeps it private to the server. For desktop access from another
machine, put an HTTPS reverse proxy in front of it, or use an SSH tunnel:

```sh
ssh -N -L 9001:127.0.0.1:9001 your-server
```

Use `http://127.0.0.1:9001` in desktop settings with the tunnel. For a reverse
proxy, use its HTTPS URL and preserve the `Authorization` header. The token
grants access to private friend activity; keep it and the data volume private.

To update the collector image and restart it, run:

```sh
docker compose -f compose.headless.yaml up -d --build
```

If VRChat requires another interactive login, stop the service, run the login
command again, then restart it. Preserve the `collector-data` volume when
updating or recreating the container. Only one collector should use a data
directory at a time. The image runs as UID 10001; if you replace the named
volume with a host bind mount, give that user write access to the mounted
directory.

To expose the collector directly to other machines on your network, set
`VRCX_COLLECTOR_HOST=0.0.0.0` in the shell before running Compose. Prefer an
HTTPS reverse proxy or SSH tunnel when connecting over an untrusted network.

## Run without Docker

```sh
cargo build --locked --release -p vrcx-0-headless
VRCX_COLLECTOR_TOKEN=your-token VRCX_COLLECTOR_BIND=127.0.0.1:9001 \
  ./target/release/vrcx-0-headless --data-dir /path/to/collector-data --login
```

After the first login, run the same command without `--login`. The HTTP API is
enabled by `VRCX_COLLECTOR_TOKEN`; omitting it disables the sync API.
`VRCX_COLLECTOR_BIND` defaults to `127.0.0.1:9001`. Docker explicitly binds to
`0.0.0.0:9001` inside the container. Stop with Ctrl+C or SIGTERM.

## Desktop setup

Log into the same VRChat account on the desktop and collector. Open **Settings >
Integrations > Headless history collector**, enter the collector URL and token,
and select **Sync now** to import history. Enable automatic sync to catch up
after login and every five minutes while the desktop is running.

A connection failure leaves imported history available. The next sync resumes
from the saved checkpoint. Desktop preview builds are available from the
**Preview** GitHub Actions workflow.

Both the desktop and collector continue recording online/offline, location,
status, bio, avatar, friendship, display-name and trust-level history. Sync
reconciles the collector's observations with desktop history and makes
collector-only events available in the existing feed and friend log. A server
cannot record local VRChat game logs, so player encounters and local world
visits are collected only while the desktop is running.

## Sync behavior

The collector serves authenticated, bounded pages of history belonging to
its logged-in account. The desktop checks the account before importing.
Each collector database has a persistent source identity and each stream has
a monotonic checkpoint. The desktop commits imported rows and checkpoints
together. Repeating pages or retrying after interruption reuses the saved
collector identity and provenance mappings, so replayed rows do not create
duplicates. Both sides keep recording if the other is unavailable; desktop
history is not disabled by a collector outage. Collector events observed
during an outage are reconciled on a later sync.

Reconciliation matches observations one-to-one in order within each friend
and history stream, using the semantic transition. It does not compare
timestamps or use a time window, and ignores duration and display metadata
that can differ between independent observations. Matching consumes one local
observation for each collector observation, rather than collapsing every
occurrence of the same change. A matched entry keeps the desktop's receipt time.

The upstream service does not provide a globally unique ID for every event,
so sequence matching cannot prove identity when either recorder misses events
or identical changes recur between shared observations. It uses a bounded
recent sequence for each friend; older duplicates can remain. Unmatched events
are retained, and original observations remain in the journal when physical
copies are reconciled. Sync does not replace current friend state or propagate
deletions.

The collector keeps a separate append-only sync journal in its
database. Ordinary history deletion does not prune that journal, so other
desktops can still catch up. Account for its disk usage and keep a backup of
the collector volume if you need to preserve its recording.

If you restore an older collector database backup, rotate its sync identity
once so desktop checkpoints cannot sit ahead of the restored journal:

```sh
docker compose -f compose.headless.yaml stop collector
# Restore the backup into the collector-data volume, then:
docker compose -f compose.headless.yaml run --rm collector --reset-sync-source
```

Stop that run with Ctrl+C after startup, then use the normal `up -d` command.
For a native installation, run headless once with `--reset-sync-source` and the
usual data directory and token. The new source identity makes desktops replay
the restored journal; identical existing rows are still deduplicated.
