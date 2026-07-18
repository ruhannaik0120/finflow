SELECT ticker, trade_date, COUNT(*) AS row_count
FROM {{ ref('stg_stock_prices') }}
GROUP BY ticker, trade_date
HAVING COUNT(*) > 1
