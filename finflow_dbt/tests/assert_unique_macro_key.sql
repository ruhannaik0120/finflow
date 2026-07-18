SELECT indicator_name, indicator_date, COUNT(*) AS row_count
FROM {{ ref('stg_macro_indicators') }}
GROUP BY indicator_name, indicator_date
HAVING COUNT(*) > 1
