-- OD-3109 rollback. Removes only this migration's guards and keys.
-- No rows, IDs, timestamps or unrelated indexes are removed.
-- Run with autocommit enabled; stop on error. No CASCADE is used.
SET lock_timeout='5s';

-- audit-statement
DO $rollback$
DECLARE key_oid oid;
BEGIN
  SELECT oid INTO key_oid FROM pg_constraint
    WHERE conrelid='public.winston_logs'::regclass AND conname='od3109_winston_logs_id_pk';
  IF key_oid IS NOT NULL AND obj_description(key_oid,'pg_constraint') IS DISTINCT FROM 'OD-3109 UUID row identity primary key v1' THEN
    RAISE EXCEPTION 'The key on winston_logs is not tagged as this migration. Stop for review.';
  END IF;
  IF key_oid IS NOT NULL THEN
    ALTER TABLE public.winston_logs DROP CONSTRAINT od3109_winston_logs_id_pk;
    ALTER TABLE public.winston_logs ALTER COLUMN id DROP NOT NULL;
  END IF;
  ALTER TABLE public.winston_logs DROP CONSTRAINT IF EXISTS od3109_winston_logs_id_present;
END
$rollback$;

-- audit-statement
DROP INDEX CONCURRENTLY IF EXISTS public.od3109_winston_logs_id_pk;

-- audit-statement
DO $rollback$
DECLARE key_oid oid;
BEGIN
  SELECT oid INTO key_oid FROM pg_constraint
    WHERE conrelid='public.questions'::regclass AND conname='od3109_questions_id_pk';
  IF key_oid IS NOT NULL AND obj_description(key_oid,'pg_constraint') IS DISTINCT FROM 'OD-3109 UUID row identity primary key v1' THEN
    RAISE EXCEPTION 'The key on questions is not tagged as this migration. Stop for review.';
  END IF;
  IF key_oid IS NOT NULL THEN
    ALTER TABLE public.questions DROP CONSTRAINT od3109_questions_id_pk;
    ALTER TABLE public.questions ALTER COLUMN id DROP NOT NULL;
  END IF;
  ALTER TABLE public.questions DROP CONSTRAINT IF EXISTS od3109_questions_id_present;
END
$rollback$;

-- audit-statement
DROP INDEX CONCURRENTLY IF EXISTS public.od3109_questions_id_pk;

-- audit-statement
DO $rollback$
DECLARE key_oid oid;
BEGIN
  SELECT oid INTO key_oid FROM pg_constraint
    WHERE conrelid='public.feedback'::regclass AND conname='od3109_feedback_id_pk';
  IF key_oid IS NOT NULL AND obj_description(key_oid,'pg_constraint') IS DISTINCT FROM 'OD-3109 UUID row identity primary key v1' THEN
    RAISE EXCEPTION 'The key on feedback is not tagged as this migration. Stop for review.';
  END IF;
  IF key_oid IS NOT NULL THEN
    ALTER TABLE public.feedback DROP CONSTRAINT od3109_feedback_id_pk;
    ALTER TABLE public.feedback ALTER COLUMN id DROP NOT NULL;
  END IF;
  ALTER TABLE public.feedback DROP CONSTRAINT IF EXISTS od3109_feedback_id_present;
END
$rollback$;

-- audit-statement
DROP INDEX CONCURRENTLY IF EXISTS public.od3109_feedback_id_pk;

-- audit-statement
DO $rollback$
DECLARE key_oid oid;
BEGIN
  SELECT oid INTO key_oid FROM pg_constraint
    WHERE conrelid='public.errordetails'::regclass AND conname='od3109_errordetails_id_pk';
  IF key_oid IS NOT NULL AND obj_description(key_oid,'pg_constraint') IS DISTINCT FROM 'OD-3109 UUID row identity primary key v1' THEN
    RAISE EXCEPTION 'The key on errordetails is not tagged as this migration. Stop for review.';
  END IF;
  IF key_oid IS NOT NULL THEN
    ALTER TABLE public.errordetails DROP CONSTRAINT od3109_errordetails_id_pk;
    ALTER TABLE public.errordetails ALTER COLUMN id DROP NOT NULL;
  END IF;
  ALTER TABLE public.errordetails DROP CONSTRAINT IF EXISTS od3109_errordetails_id_present;
END
$rollback$;

-- audit-statement
DROP INDEX CONCURRENTLY IF EXISTS public.od3109_errordetails_id_pk;

-- audit-statement
RESET lock_timeout;
