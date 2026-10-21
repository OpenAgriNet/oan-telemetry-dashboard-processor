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
business tables are not modified. Up to 500 previously handled raw logs from
14 October 2026 IST are replayed through the current mappings. No HTTP or scheduler starts.

The regression checks compare the combined-branch batch with the fixed version,
verify durable quarantine and retry behavior, reconcile outcome counters, retain
the current generic repeated-delivery policy, check missing-payload flags and URL
masking, and exercise registration against temporary configuration tables.

## Feedback comments

Likes may omit a comment. Dislikes require a nonblank text comment. This check
uses the configured feedback field mappings before inserting a reporting row.
An invalid dislike is recorded in `dead_letter_logs` with validation code
`FEEDBACK_COMMENT_REQUIRED`; its raw log is retained and valid events in the same
batch continue. The raw row is marked handled only after the batch commits.
If recording the failure fails, the existing batch rollback/retry behavior applies.

The local database check exercises mixed valid/invalid feedback using the dump's
actual mappings. No database migration is required. The separate query service
excludes historical invalid dislikes from reports without deleting them.
The chatbot must also require a comment in its own feedback submission form.

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

## UUID primary-key migration

Run `npm run test:row-ids` to verify the separate migration in
`migrations/20261018_od3109_core_row_ids/`. This is a dedicated integration
command: it creates a uniquely named loopback test database, copies only the
14 October core records from mh-17-10 through a read-only source connection,
and tests apply, complete/partial rollback, failed concurrent-index recovery,
existing-data rejection, default UUID generation and a small processor replay.
The original local dump is not modified. The copy is retained with the verified
keys applied for inspection. If the sibling service checkout is available,
12 controller/report responses are also compared before and after the migration;
standalone processor checkouts still run all database and processor cases.
The JSON report includes aggregate results only. This command does not apply
anything to production or start HTTP servers, schedulers or notifications.
