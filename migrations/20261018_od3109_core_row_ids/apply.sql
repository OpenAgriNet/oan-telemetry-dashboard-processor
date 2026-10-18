-- OD-3109: retain existing UUIDs and their defaults; enforce one nonempty id per row.
-- Run with autocommit enabled and stop on the first error. Do not wrap this file
-- in one transaction: CREATE INDEX CONCURRENTLY requires separate statements.
-- The integration harness splits only at the audit-statement markers below.
-- Nothing here rewrites existing IDs, timestamps, MID values or business data.
SET lock_timeout='5s';

-- audit-statement
SET statement_timeout='0';

-- audit-statement
DO $preflight$
DECLARE
  target text;
  missing_ids boolean;
  repeated_ids boolean;
  definition record;
BEGIN
  FOREACH target IN ARRAY ARRAY['winston_logs','questions','feedback','errordetails'] LOOP
    SELECT a.atttypid,a.attnotnull,pg_get_expr(d.adbin,d.adrelid) AS default_expr
    INTO definition FROM pg_attribute a
    LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
    WHERE a.attrelid=to_regclass(format('public.%I',target)) AND a.attname='id' AND NOT a.attisdropped;
    IF NOT FOUND THEN RAISE EXCEPTION 'Missing table or id column: %',target; END IF;
    IF definition.atttypid <> 'uuid'::regtype OR definition.attnotnull
       OR definition.default_expr IS DISTINCT FROM 'gen_random_uuid()' THEN
      RAISE EXCEPTION 'Unexpected id definition for %. Review this migration before applying.',target;
    END IF;
    IF EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid=to_regclass(format('public.%I',target))
        AND (contype='p' OR conname IN ('od3109_'||target||'_id_pk','od3109_'||target||'_id_present')))
       OR to_regclass(format('public.%I','od3109_'||target||'_id_pk')) IS NOT NULL THEN
      RAISE EXCEPTION 'Existing key or partial migration on %. Review or roll back the earlier attempt.',target;
    END IF;
    EXECUTE format('SELECT EXISTS(SELECT 1 FROM public.%I WHERE id IS NULL), EXISTS(SELECT id FROM public.%I GROUP BY id HAVING count(*)>1 LIMIT 1)',target,target)
      INTO missing_ids,repeated_ids;
    IF missing_ids OR repeated_ids THEN
      RAISE EXCEPTION 'Existing empty or repeated row IDs in %. No cleanup is performed by this migration.',target;
    END IF;
  END LOOP;
END
$preflight$;

-- audit-statement
ALTER TABLE public.winston_logs ADD CONSTRAINT od3109_winston_logs_id_present CHECK(id IS NOT NULL) NOT VALID;

-- audit-statement
ALTER TABLE public.winston_logs VALIDATE CONSTRAINT od3109_winston_logs_id_present;

-- audit-statement
CREATE UNIQUE INDEX CONCURRENTLY od3109_winston_logs_id_pk ON public.winston_logs(id);

-- audit-statement
BEGIN;
ALTER TABLE public.winston_logs ALTER COLUMN id SET NOT NULL;
ALTER TABLE public.winston_logs ADD CONSTRAINT od3109_winston_logs_id_pk PRIMARY KEY USING INDEX od3109_winston_logs_id_pk;
COMMENT ON CONSTRAINT od3109_winston_logs_id_pk ON public.winston_logs IS 'OD-3109 UUID row identity primary key v1';
ALTER TABLE public.winston_logs DROP CONSTRAINT od3109_winston_logs_id_present;
COMMIT;

-- audit-statement
ALTER TABLE public.questions ADD CONSTRAINT od3109_questions_id_present CHECK(id IS NOT NULL) NOT VALID;

-- audit-statement
ALTER TABLE public.questions VALIDATE CONSTRAINT od3109_questions_id_present;

-- audit-statement
CREATE UNIQUE INDEX CONCURRENTLY od3109_questions_id_pk ON public.questions(id);

-- audit-statement
BEGIN;
ALTER TABLE public.questions ALTER COLUMN id SET NOT NULL;
ALTER TABLE public.questions ADD CONSTRAINT od3109_questions_id_pk PRIMARY KEY USING INDEX od3109_questions_id_pk;
COMMENT ON CONSTRAINT od3109_questions_id_pk ON public.questions IS 'OD-3109 UUID row identity primary key v1';
ALTER TABLE public.questions DROP CONSTRAINT od3109_questions_id_present;
COMMIT;

-- audit-statement
ALTER TABLE public.feedback ADD CONSTRAINT od3109_feedback_id_present CHECK(id IS NOT NULL) NOT VALID;

-- audit-statement
ALTER TABLE public.feedback VALIDATE CONSTRAINT od3109_feedback_id_present;

-- audit-statement
CREATE UNIQUE INDEX CONCURRENTLY od3109_feedback_id_pk ON public.feedback(id);

-- audit-statement
BEGIN;
ALTER TABLE public.feedback ALTER COLUMN id SET NOT NULL;
ALTER TABLE public.feedback ADD CONSTRAINT od3109_feedback_id_pk PRIMARY KEY USING INDEX od3109_feedback_id_pk;
COMMENT ON CONSTRAINT od3109_feedback_id_pk ON public.feedback IS 'OD-3109 UUID row identity primary key v1';
ALTER TABLE public.feedback DROP CONSTRAINT od3109_feedback_id_present;
COMMIT;

-- audit-statement
ALTER TABLE public.errordetails ADD CONSTRAINT od3109_errordetails_id_present CHECK(id IS NOT NULL) NOT VALID;

-- audit-statement
ALTER TABLE public.errordetails VALIDATE CONSTRAINT od3109_errordetails_id_present;

-- audit-statement
CREATE UNIQUE INDEX CONCURRENTLY od3109_errordetails_id_pk ON public.errordetails(id);

-- audit-statement
BEGIN;
ALTER TABLE public.errordetails ALTER COLUMN id SET NOT NULL;
ALTER TABLE public.errordetails ADD CONSTRAINT od3109_errordetails_id_pk PRIMARY KEY USING INDEX od3109_errordetails_id_pk;
COMMENT ON CONSTRAINT od3109_errordetails_id_pk ON public.errordetails IS 'OD-3109 UUID row identity primary key v1';
ALTER TABLE public.errordetails DROP CONSTRAINT od3109_errordetails_id_present;
COMMIT;

-- audit-statement
RESET statement_timeout;

-- audit-statement
RESET lock_timeout;
