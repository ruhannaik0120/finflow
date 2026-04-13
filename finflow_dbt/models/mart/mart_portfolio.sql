WITH stock_data AS (
    SELECT * FROM {{ ref('stg_stock_prices') }}
),

-- Step 1: calculate daily return first on its own
with_returns AS (
    SELECT
        ticker,
        trade_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,

        LAG(close_price) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
        ) AS prev_close_price,

        ROUND(
            (close_price - LAG(close_price) OVER (
                PARTITION BY ticker ORDER BY trade_date
            )) / NULLIF(LAG(close_price) OVER (
                PARTITION BY ticker ORDER BY trade_date
            ), 0) * 100,
        4) AS daily_return_pct

    FROM stock_data
),

-- Step 2: now use daily_return_pct in further window functions
with_metrics AS (
    SELECT
        ticker,
        trade_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        prev_close_price,
        daily_return_pct,

        -- 20 day moving average
        ROUND(AVG(close_price) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ), 4) AS ma_20day,

        -- 50 day moving average
        ROUND(AVG(close_price) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ), 4) AS ma_50day,

        -- 20 day rolling volatility
        ROUND(STDDEV(daily_return_pct) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ), 4) AS rolling_volatility_20d,

        -- Cumulative return from first date
        ROUND(
            (close_price - FIRST_VALUE(close_price) OVER (
                PARTITION BY ticker ORDER BY trade_date
            )) / NULLIF(FIRST_VALUE(close_price) OVER (
                PARTITION BY ticker ORDER BY trade_date
            ), 0) * 100,
        4) AS cumulative_return_pct,

        -- Running peak for drawdown
        MAX(close_price) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_peak

    FROM with_returns
),

-- Step 3: calculate drawdown using running peak
with_drawdown AS (
    SELECT
        *,
        ROUND(
            (close_price - running_peak) / NULLIF(running_peak, 0) * 100,
        4) AS drawdown_pct
    FROM with_metrics
)

SELECT
    ticker,
    trade_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    prev_close_price,
    daily_return_pct,
    ma_20day,
    ma_50day,
    rolling_volatility_20d,
    cumulative_return_pct,
    running_peak,
    drawdown_pct,
    CURRENT_TIMESTAMP() AS updated_at
FROM with_drawdown
ORDER BY ticker, trade_date