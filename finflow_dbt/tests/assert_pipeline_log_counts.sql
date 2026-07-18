SELECT run_id
FROM {{ source('raw', 'pipeline_logs') }}
WHERE rows_fetched < 0
   OR rows_valid < 0
   OR rows_inserted < 0
   OR rows_updated < 0
   OR rows_unchanged < 0
   OR rows_dropped < 0
   OR duration_ms < 0
   OR (status <> 'FAILED' AND rows_valid + rows_dropped <> rows_fetched)
   OR (
       status IN ('SUCCESS', 'PARTIAL_SUCCESS', 'NO_CHANGES')
       AND rows_inserted + rows_updated + rows_unchanged <> rows_valid
   )
