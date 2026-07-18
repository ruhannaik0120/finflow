SELECT run_id
FROM {{ source('raw', 'pipeline_logs') }}
WHERE status NOT IN ('SUCCESS', 'PARTIAL_SUCCESS', 'NO_CHANGES', 'NO_VALID_ROWS', 'FAILED')
   OR pipeline_name NOT IN ('STOCK_INGESTION', 'MACRO_INGESTION')
