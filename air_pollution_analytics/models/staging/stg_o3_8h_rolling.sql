WITH hourly_data AS (
    -- Get base hourly data
    SELECT 
        city,
        timestamp,
        o3
    FROM {{ source('air_pollution', 'air_pollution') }}
),

rolling_8h AS (
    -- Calculate 8-hour rolling average
    SELECT 
        city,
        timestamp,
        o3,
        AVG(o3) OVER (
            PARTITION BY city 
            ORDER BY timestamp 
            ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
        ) as o3_8h_mean,
        -- Count measurements in 8-hour window
        COUNT(*) OVER (
            PARTITION BY city 
            ORDER BY timestamp 
            ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
        ) as measurements_in_window
    FROM hourly_data
),

daily_max AS (
    -- Get daily maximum of 8-hour rolling averages
    SELECT
        city,
        DATE(timestamp) as date,
        MAX(CASE 
            WHEN measurements_in_window = 8  -- Only consider complete 8-hour windows
            THEN o3_8h_mean 
            ELSE NULL 
        END) as daily_max_o3_8h
    FROM rolling_8h
    GROUP BY city, DATE(timestamp)
)

SELECT 
    city,
    date,
    ROUND(daily_max_o3_8h, 2) as daily_max_o3_8h
FROM daily_max
ORDER BY city, date DESC 