# Local audit-fix checks

Run `npm test -- --runInBand` for the discovered unit suites. Fixtures are synthetic
and kept inside this repository. Unit imports do not start HTTP or scheduled jobs.

Run `npm run test:management-api` for loopback-only HTTP checks. The database pool
is mocked. No database connection, scheduled maintenance or notification is used.

Run `npm run test:local-db` against the restored local `mh-17-10` database. The
command reads local credentials from `.env`, refuses non-loopback hosts, and
verifies the database/server address before work. Reporting, raw input,
configuration and external traces are tested in connection-local temporary tables.
Copied sequence defaults are redirected to temporary sequences. Restored public
business tables are not modified. The latest 21,310 previously handled raw logs
are replayed through the current mappings. No HTTP or scheduler starts.

The regression checks compare the combined-branch batch with the fixed version,
verify durable quarantine and retry behavior, reconcile outcome counters, retain
the current generic repeated-delivery policy, check missing-payload flags and URL
masking, and exercise registration against temporary configuration tables.

## Management access

All `/api` management requests now require an `Authorization: Bearer ...` header
matching `PROCESSOR_MANAGEMENT_API_KEY`. Without a configured key, management
requests return 503. Scheduled processing does not require this key. `/health`
remains available without it. Supply the actual key through the deployment's
secret configuration; do not commit it.

Registration permits only `questions`, `feedback` and `errordetails` by default.
If another reporting table is required, an operator can explicitly configure the
comma-separated `PROCESSOR_ALLOWED_EVENT_TABLES` allowlist. Identifier validation
and quoting still apply. Configuration changes commit before updating the
in-memory registry and replace an existing table's rule rather than adding a
second stale in-memory rule.

Failure records retain the original raw-delivery reference, event position and
failure code. Raw input is retained for investigation. Private event bodies and
arbitrary metadata are not duplicated into the failure store or ordinary logs.
New external records mask sensitive URL parameters and use SQL NULL for absent
payloads. These changes do not rewrite historical processed records or raw logs.

Production behavior is not checked by these commands.
