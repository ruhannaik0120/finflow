WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_stock_prices') }}
),

cleaned AS (
    SELECT
        UPPER(ticker)                    AS ticker,
        trade_date::DATE                 AS trade_date,
        open_price::FLOAT                AS open_price,
        high_price::FLOAT                AS high_price,
        low_price::FLOAT                 AS low_price,
        close_price::FLOAT               AS close_price,
        volume::BIGINT                   AS volume,
        loaded_at                        AS loaded_at,

        -- Remove duplicates, keep the latest loaded row
        ROW_NUMBER() OVER (
            PARTITION BY ticker, trade_date
            ORDER BY loaded_at DESC
        ) AS row_num

    FROM source
    WHERE close_price IS NOT NULL
      AND volume > 0
)

SELECT
    ticker,
    trade_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    loaded_at
FROM cleaned
WHERE row_num = 1