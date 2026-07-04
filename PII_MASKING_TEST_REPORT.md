# PII Masking Test Report

## Summary

PII masking was tested on local database `bh-dev-2`, which was used as a production replica. The SQL column-level backfill completed successfully for all targeted text columns, and the final verification query reported `TOTAL_REMAINING=0`.

Result: **Pass**

## Scope

Tested tables and columns:

```text
messages.content
questions.questiontext
questions.answer
questions.answertext
questions.groupdetails
feedback.feedbacktext
feedback.questiontext
feedback.answertext
errordetails.errortext
```

`feedback.groupdetails` was skipped by the SQL text backfill because it is `jsonb`. Earlier candidate checks did not show remaining PII there.

## Method

The test used DB-side masking functions:

```text
public.contains_pii_candidate(text)
public.mask_pii_text(text)
```

Verification counted rows where:

```sql
public.contains_pii_candidate(column::text)
AND column::text IS DISTINCT FROM public.mask_pii_text(column::text)
```

This means a row was considered clean only if the DB masking function would no longer change the column value.

## Backfill Results

First pass:

```text
Updated rows: 209,488
Elapsed: 29m 37s
```

Second pass:

```text
Updated rows: 78
Elapsed: 2m 6s
Reason: adjacent values became matchable after first-pass redaction
```

Example second-pass case:

```text
[REDACTED_PHONE]/8460136593
```

## Final Verification

Final audit output:

```text
messages.content: remaining=0
questions.questiontext: remaining=0
questions.answer: remaining=0
questions.answertext: remaining=0
questions.groupdetails: remaining=0
feedback.feedbacktext: remaining=0
feedback.questiontext: remaining=0
feedback.answertext: remaining=0
errordetails.errortext: remaining=0
TOTAL_REMAINING=0
```

## Live Telemetry Processing Check

Two synthetic `winston_logs` rows with raw PII were inserted into local `bh-dev-2` and passed through the real telemetry parser plus DB-loaded event processors. The test used:

- `index.parseTelemetryMessage(...)`
- active `event_processors` mappings from the database
- the same processor `.process()` methods used for normal inserts
- `winston_logs.sync_status` update to `1`

Observed persisted output:

```text
questions.questiontext
raw:    My PM Kisan registration is UP123456789 and mobile 9876543210
stored: My PM Kisan registration is [REDACTED_FARMER_ID] and mobile [REDACTED_PHONE]

questions.answertext
raw:    Farmer email local.user@example.com and aadhaar 2345 6789 1234
stored: Farmer email [REDACTED_EMAIL] and aadhaar [REDACTED_AADHAAR]

feedback.feedbacktext
raw:    Please check mobile 9123456789 and registration RJ987654321
stored: Please check mobile [REDACTED_PHONE] and registration [REDACTED_FARMER_ID]

feedback.answertext
raw:    OTP 123456 was not accepted for feedback.user@example.com
stored: OTP [REDACTED_OTP] was not accepted for [REDACTED_EMAIL]
```

The synthetic `winston_logs` rows were marked processed:

```text
sync_status = 1
```

## Important Finding

The live telemetry insert path masked the free-text payload columns correctly, but the structured columns in those test rows were still stored as raw values:

```text
questions.mobile
questions.farmer_id
questions.email

feedback.mobile
feedback.farmer_id
feedback.email
```

So the current implementation is verified as:

- **Pass** for text payload masking on telemetry ingest
- **Pass** for historical text-column backfill
- **Not fully masked** for structured PII columns on ingest, based on the synthetic telemetry test

## Notes

- The verification confirms the targeted text columns are clean according to the deployed SQL candidate and masking rules.
- App-side JS PII masking should remain enabled for future writes and API reads.
- The synthetic telemetry test confirmed that text fields are masked during live processing, but structured columns like `mobile`, `farmer_id`, and `email` still need a separate decision or fix if production requires those to be redacted at write time.
- Production should run the same final audit after the prod backfill and attach the output as execution evidence.
