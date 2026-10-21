# Core row ID protection

This migration keeps the UUID IDs and gen_random_uuid() defaults in winston_logs, questions, feedback and errordetails. It adds primary keys to id only. It does not change MID handling, event counts, existing values, serial IDs, timestamps or relationships between events.

## Local verification

Run `npm run test:row-ids` from the processor repository. The harness requires loopback credentials and the restored source database named mh-17-10. It opens the source in a read-only transaction and creates a separate uniquely named local database. Only 14 October 2026 core rows, processing rules and supporting table definitions are copied. Business rows remain inside the local test database; the report contains aggregate results only. No production connection, scheduler or HTTP server starts. The test copy is retained for inspection.

## Applying later

This is a manual database migration, not an application startup action. No production execution is authorized by the local verification. Check the target server version, ownership, a fresh full-table ID scan, backup and deployment window before a later rollout. Local sample timings do not establish production migration timing or availability under concurrent traffic.

Use a dedicated SQL session with autocommit enabled and stop-on-error behavior. In psql use `psql -X -v ON_ERROR_STOP=1 ... -f apply.sql`. Do not put the entire file in a transaction: each concurrent index statement must run outside a transaction block. The preflight rejects unexpected ID definitions, existing primary keys, existing migration objects, and any empty or repeated IDs. It never deletes or renumbers rows.

The temporary non-null checks protect new writes and validate existing rows. Unique indexes are built concurrently, then attached as primary keys in short transactions. Final ALTER statements still require brief exclusive locks; lock_timeout is five seconds so a blocked attempt fails rather than waiting indefinitely. Index builds and validation still require time and disk space. Existing nonunique indexes are retained.

## Rollback and interruption

Stop on the first apply error and roll back any open transaction. Do not blindly rerun apply.sql: a concurrent index failure can leave an invalid index. Inspect the error, then use rollback.sql in an autocommit session with stop-on-error enabled. It handles partially added guards and indexes, drops only this migration's tagged primary keys, restores the originally nullable id columns, and retains all UUID values and unrelated indexes. It never uses CASCADE; a new dependency added later will make rollback stop for review.

Run verify.sql before/after apply and after rollback. The test harness also checks catalog equality, complete row fingerprints, duplicate/null rejection, default UUID generation, a small processor replay, error isolation and unchanged same-MID handling.

References: PostgreSQL ALTER TABLE https://www.postgresql.org/docs/18/sql-altertable.html and CREATE INDEX https://www.postgresql.org/docs/18/sql-createindex.html.
