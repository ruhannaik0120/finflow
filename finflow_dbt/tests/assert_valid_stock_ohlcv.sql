SELECT ticker, trade_date
FROM {{ ref('stg_stock_prices') }}
WHERE open_price <= 0
   OR high_price <= 0
   OR low_price <= 0
   OR close_price <= 0
   OR volume <= 0
   OR high_price < open_price
   OR high_price < low_price
   OR high_price < close_price
   OR low_price > open_price
   OR low_price > high_price
   OR low_price > close_price
