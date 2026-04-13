WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_macro_indicators') }}
),

cleaned AS (
    SELECT
        UPPER(indicator_name)        AS indicator_name,
        indicator_date::DATE         AS indicator_date,
        value::FLOAT                 AS value,
        loaded_at                    AS loaded_at,

        -- Remove duplicates, keep latest loaded row
        ROW_NUMBER() OVER (
            PARTITION BY indicator_name, indicator_date
            ORDER BY loaded_at DESC
        ) AS row_num

    FROM source
    WHERE value IS NOT NULL
)

SELECT
    indicator_name,
    indicator_date,
    value,
    loaded_at
FROM cleaned
WHERE row_num = 1