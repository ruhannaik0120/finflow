WITH portfolio AS (
    SELECT * FROM {{ ref('mart_portfolio') }}
),

-- Sharpe ratio needs average return and volatility
sharpe_calc AS (
    SELECT
        ticker,
        trade_date,
        daily_return_pct,
        rolling_volatility_20d,
        drawdown_pct,
        cumulative_return_pct,

        -- Annualised Sharpe Ratio (risk free rate assumed 5% annually = 0.0198% daily)
        ROUND(
            (AVG(daily_return_pct) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) - 0.0198)
            / NULLIF(rolling_volatility_20d, 0)
            * SQRT(252),
        4) AS sharpe_ratio_20d,

        -- Max drawdown over entire history up to this date
        MIN(drawdown_pct) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS max_drawdown_pct,

        -- Value at Risk 95% confidence (1.645 standard deviations)
        ROUND(
            AVG(daily_return_pct) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) - 1.645 * NULLIF(rolling_volatility_20d, 0),
        4) AS var_95_pct,

        -- 20 day average volume for liquidity measure
        ROUND(AVG(volume) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ), 0) AS avg_volume_20d

    FROM portfolio
    WHERE daily_return_pct IS NOT NULL
)

SELECT
    ticker,
    trade_date,
    daily_return_pct,
    rolling_volatility_20d,
    sharpe_ratio_20d,
    max_drawdown_pct,
    var_95_pct,
    avg_volume_20d,
    cumulative_return_pct,
    CURRENT_TIMESTAMP() AS updated_at
FROM sharpe_calc
ORDER BY ticker, trade_date