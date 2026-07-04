CREATE OR REPLACE FUNCTION public.contains_pii_candidate(value text)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
  SELECT COALESCE($1, '') ~* ANY(ARRAY[
    $rx$[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$rx$,
    $rx$(^|[^0-9])([+]?[0-9]{1,3}[[:space:]-]?)?[6-9][0-9]{9}([^0-9]|$)$rx$,
    $rx$(^|[^0-9])[0-9]{3}[-.[:space:]][0-9]{3}[-.[:space:]][0-9]{4}([^0-9]|$)$rx$,
    $rx$(^|[^0-9])[2-9][0-9]{3}[[:space:]-]?[0-9]{4}[[:space:]-]?[0-9]{4}([^0-9]|$)$rx$,
    $rx$([0-9][ -]*){13,19}$rx$,
    $rx$eyJ[A-Za-z0-9_-]{6,}\.[A-Za-z0-9._-]{6,}\.[A-Za-z0-9._-]{6,}$rx$,
    $rx$Bearer[[:space:]]+[A-Za-z0-9\-._~+/]+=*$rx$,
    $rx$(api[_-]?key|access[_-]?token|refresh[_-]?token|authorization|secret|password)[[:space:]]*[:=][[:space:]]*[^[:space:],;"']+$rx$,
    $rx$(farmer[[:space:]_-]*id|farmer[[:space:]_-]*identifier|fid|pm[[:space:]]*[-_]?[[:space:]]*kisan|registration[[:space:]]*(number|no|id)|beneficiary[[:space:]]*(id|number))[[:space:]]*(:|=|is|as)?[[:space:]]*[A-Za-z0-9-]*[0-9][A-Za-z0-9-]{2,}$rx$,
    $rx$(^|[^A-Za-z0-9])[A-Za-z]{2}[-[:space:]]?[0-9]{9}([^A-Za-z0-9]|$)$rx$,
    $rx$(^|[^0-9])[0-9]{6}([^0-9]|$)$rx$,
    $rx$(^|[^0-9])[0-9]{11}([^0-9]|$)$rx$
  ]);
$$;

CREATE OR REPLACE FUNCTION public.mask_pii_text(value text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
  SELECT regexp_replace(
    regexp_replace(
      regexp_replace(
        regexp_replace(
          regexp_replace(
            regexp_replace(
              regexp_replace(
                regexp_replace(
                  regexp_replace(
                    regexp_replace(
                      regexp_replace(
                        regexp_replace(
                          COALESCE($1, ''),
                          $rx$((?:farmer[[:space:]_-]*id(?:entifier)?|fid|pm[[:space:]]*[-_]?[[:space:]]*kisan[[:space:]]*(?:registration[[:space:]]*(?:number|no|id)|beneficiary[[:space:]]*(?:id|number)|id|number|no)|registration[[:space:]]*(?:number|no|id)|beneficiary[[:space:]]*(?:id|number))[[:space:]]*(?::|=|is|as)?[[:space:]]*)([A-Za-z0-9-]*[0-9][A-Za-z0-9-]{2,})$rx$,
                          '\1[REDACTED_FARMER_ID]',
                          'gi'
                        ),
                        $rx$"farmer[_-]?id"[[:space:]]*:[[:space:]]*"[^"]+"$rx$,
                        '"farmer_id":"[REDACTED_FARMER_ID]"',
                        'gi'
                      ),
                      $rx$(^|[^A-Za-z0-9])([A-Za-z]{2}[-[:space:]]?[0-9]{9})([^A-Za-z0-9]|$)$rx$,
                      '\1[REDACTED_FARMER_ID]\3',
                      'g'
                    ),
                    $rx$((?:otp|one[[:space:]-]*time[[:space:]-]*password|verification[[:space:]]*code|auth(?:entication)?[[:space:]]*code)[[:space:]]*(?::|=|is|as)?[[:space:]]*)([0-9]{4,8})$rx$,
                    '\1[REDACTED_OTP]',
                    'gi'
                  ),
                  $rx$([0-9][ -]*){13,19}$rx$,
                  '[REDACTED_CARD]',
                  'g'
                ),
                $rx$(^|[^0-9])((?:[+]?[0-9]{1,3}[[:space:]-]?)?[6-9][0-9]{9})([^0-9]|$)$rx$,
                '\1[REDACTED_PHONE]\3',
                'g'
              ),
              $rx$(^|[^0-9])([0-9]{3}[-.[:space:]][0-9]{3}[-.[:space:]][0-9]{4})([^0-9]|$)$rx$,
              '\1[REDACTED_PHONE]\3',
              'g'
            ),
            $rx$(^|[^0-9])([2-9][0-9]{3}[[:space:]-]?[0-9]{4}[[:space:]-]?[0-9]{4})([^0-9]|$)$rx$,
            '\1[REDACTED_AADHAAR]\3',
            'g'
          ),
          $rx$[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$rx$,
          '[REDACTED_EMAIL]',
          'g'
        ),
        $rx$eyJ[A-Za-z0-9_-]{6,}\.[A-Za-z0-9._-]{6,}\.[A-Za-z0-9._-]{6,}$rx$,
        '[REDACTED_TOKEN]',
        'g'
      ),
      $rx$(Bearer[[:space:]]+)[A-Za-z0-9\-._~+/]+=*$rx$,
      '\1[REDACTED_TOKEN]',
      'gi'
    ),
    $rx$(api[_-]?key|access[_-]?token|refresh[_-]?token|authorization|secret|password)([[:space:]]*[:=][[:space:]]*)[^[:space:],;"']+$rx$,
    '\1\2[REDACTED_TOKEN]',
    'gi'
  );
$$;
