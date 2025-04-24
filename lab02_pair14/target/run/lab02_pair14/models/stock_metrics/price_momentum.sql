
  
    

        create or replace transient table USER_DB_BOA.analytics.price_momentum
         as
        (WITH base AS (
    SELECT 
        symbol,
        date,
        close,
        LAG(close, 5) OVER (PARTITION BY symbol ORDER BY date) AS close_5_days_ago
    FROM USER_DB_BOA.analytics.raw_stocks_data
)

SELECT
    symbol,
    date,
    round(close, 2) as close,
    round(close_5_days_ago, 2) as close_5_days_ago,
    round(close - close_5_days_ago, 2) AS price_momentum
FROM base
WHERE close_5_days_ago IS NOT NULL
        );
      
  