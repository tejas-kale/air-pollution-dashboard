WITH rolling_means AS (
    SELECT 
        city,
        timestamp,
        -- Calculate rolling means
        AVG(pm2_5) OVER (
            PARTITION BY city 
            ORDER BY timestamp 
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) as pm2_5_24h_mean,
        AVG(pm10) OVER (
            PARTITION BY city 
            ORDER BY timestamp 
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) as pm10_24h_mean,
        AVG(no2) OVER (
            PARTITION BY city 
            ORDER BY timestamp 
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) as no2_24h_mean,
        AVG(so2) OVER (
            PARTITION BY city 
            ORDER BY timestamp 
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) as so2_24h_mean,
        AVG(co) OVER (
            PARTITION BY city 
            ORDER BY timestamp 
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) as co_24h_mean,
        -- Count non-null measurements in window
        COUNT(CASE WHEN pm2_5 IS NOT NULL THEN 1 END) OVER (
            PARTITION BY city 
            ORDER BY timestamp 
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) as valid_measurements,
        -- Count total periods in window
        COUNT(*) OVER (
            PARTITION BY city 
            ORDER BY timestamp 
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) as total_periods
    FROM {{ source('air_pollution', 'air_pollution') }}
)

SELECT 
    city,
    timestamp,
    ROUND(GREATEST(pm2_5_24h_mean, 0), 2) as pm2_5_24h_mean,
    ROUND(GREATEST(pm10_24h_mean, 0), 2) as pm10_24h_mean,
    ROUND(GREATEST(no2_24h_mean, 0), 2) as no2_24h_mean,
    ROUND(GREATEST(so2_24h_mean, 0), 2) as so2_24h_mean,
    ROUND(GREATEST(co_24h_mean, 0), 2) as co_24h_mean,
    valid_measurements,
    total_periods,
    ROUND(CAST(valid_measurements AS FLOAT64) / total_periods * 100, 1) as data_completeness_pct
FROM rolling_means
WHERE total_periods = 24  -- Ensure we have a full 24-hour window
ORDER BY city, timestamp DESC 