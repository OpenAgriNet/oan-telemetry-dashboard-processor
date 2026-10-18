-- Read-only verification. ID uniqueness is per table.
SELECT 'winston_logs' AS table_name,count(*) AS rows,count(*) FILTER(WHERE id IS NULL) AS missing_ids,
 count(id)-count(DISTINCT id) AS extra_rows_with_repeated_ids FROM public.winston_logs;

SELECT 'questions' AS table_name,count(*) AS rows,count(*) FILTER(WHERE id IS NULL) AS missing_ids,
 count(id)-count(DISTINCT id) AS extra_rows_with_repeated_ids FROM public.questions;

SELECT 'feedback' AS table_name,count(*) AS rows,count(*) FILTER(WHERE id IS NULL) AS missing_ids,
 count(id)-count(DISTINCT id) AS extra_rows_with_repeated_ids FROM public.feedback;

SELECT 'errordetails' AS table_name,count(*) AS rows,count(*) FILTER(WHERE id IS NULL) AS missing_ids,
 count(id)-count(DISTINCT id) AS extra_rows_with_repeated_ids FROM public.errordetails;

SELECT c.relname AS table_name,co.conname,pg_get_constraintdef(co.oid) AS definition FROM pg_constraint co JOIN pg_class c ON c.oid=co.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname IN('winston_logs','questions','feedback','errordetails') AND co.contype='p' ORDER BY c.relname;
