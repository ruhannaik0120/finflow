WITH stock_returns AS (
    SELECT
        ticker,
        trade_date,
        daily_return_pct
    FROM {{ ref('mart_portfolio') }}
    WHERE daily_return_pct IS NOT NULL
),

macro AS (
    SELECT
        indicator_name,
        indicator_date,
        value,
        ROUND(
            (value - LAG(value) OVER (
                PARTITION BY indicator_name
                ORDER BY indicator_date
            )) / NULLIF(LAG(value) OVER (
                PARTITION BY indicator_name
                ORDER BY indicator_date
            ), 0) * 100,
        4) AS indicator_change_pct
    FROM {{ ref('stg_macro_indicators') }}
),

joined AS (
    SELECT
        s.ticker,
        s.trade_date,
        s.daily_return_pct,
        m.indicator_name,
        m.value                AS indicator_value,
        m.indicator_change_pct
    FROM stock_returns s
    LEFT JOIN macro m
        ON s.trade_date = m.indicator_date
    WHERE m.indicator_name IS NOT NULL
),

-- Build rolling 20 day windows manually
windowed AS (
    SELECT
        ticker,
        trade_date,
        indicator_name,
        indicator_value,
        indicator_change_pct,
        daily_return_pct,

        -- Components needed for manual correlation formula
        AVG(daily_return_pct) OVER (
            PARTITION BY ticker, indicator_name
            ORDER BY trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS avg_return,

        AVG(indicator_change_pct) OVER (
            PARTITION BY ticker, indicator_name
            ORDER BY trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS avg_indicator,

        STDDEV(daily_return_pct) OVER (
            PARTITION BY ticker, indicator_name
            ORDER BY trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS std_return,

        STDDEV(indicator_change_pct) OVER (
            PARTITION BY ticker, indicator_name
            ORDER BY trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS std_indicator,

        AVG(daily_return_pct * indicator_change_pct) OVER (
            PARTITION BY ticker, indicator_name
            ORDER BY trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS avg_product

    FROM joined
),

-- Apply Pearson correlation formula manually
-- r = (E[XY] - E[X]*E[Y]) / (std_x * std_y)
with_correlation AS (
    SELECT
        ticker,
        trade_date,
        indicator_name,
        indicator_value,
        indicator_change_pct,
        daily_return_pct,

        ROUND(
            (avg_product - avg_return * avg_indicator)
            / NULLIF(std_return * std_indicator, 0),
        4) AS rolling_correlation_20d

    FROM windowed
)

SELECT
    ticker,
    trade_date,
    indicator_name,
    indicator_value,
    indicator_change_pct,
    daily_return_pct,
    rolling_correlation_20d,
    CURRENT_TIMESTAMP() AS updated_at
FROM with_correlation
ORDER BY ticker, indicator_name, trade_date