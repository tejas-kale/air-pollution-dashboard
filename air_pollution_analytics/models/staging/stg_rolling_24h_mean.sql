WITH all_timestamps AS (
    -- Get min and max timestamps across all cities
    SELECT 
        MIN(timestamp) as min_timestamp,
        MAX(timestamp) as max_timestamp
    FROM {{ source('air_pollution', 'air_pollution') }}
),

time_sequence AS (
    -- Generate hourly timestamps
    SELECT timestamp
    FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(
        (SELECT min_timestamp FROM all_timestamps),
        (SELECT max_timestamp FROM all_timestamps),
        INTERVAL 1 HOUR
    )) as timestamp
),

city_list AS (
    -- Get distinct cities
    SELECT DISTINCT city 
    FROM {{ source('air_pollution', 'air_pollution') }}
),

time_city_matrix AS (
    -- Create all possible combinations of cities and timestamps
    SELECT 
        c.city,
        t.timestamp
    FROM city_list c
    CROSS JOIN time_sequence t
),

complete_hourly_data AS (
    -- Join with actual measurements to identify gaps
    SELECT 
        tcm.city,
        tcm.timestamp as expected_timestamp,
        ap.timestamp as actual_timestamp,
        ap.pm2_5,
        ap.pm10,
        ap.no2,
        ap.so2,
        ap.co,
        -- Flag to identify if this is a real measurement or gap
        CASE WHEN ap.timestamp IS NOT NULL THEN 1 ELSE 0 END as is_actual_measurement
    FROM time_city_matrix tcm
    LEFT JOIN {{ source('air_pollution', 'air_pollution') }} ap
        ON tcm.city = ap.city 
        AND ap.timestamp = tcm.timestamp
),

rolling_means AS (
    SELECT 
        city,
        expected_timestamp as timestamp,
        -- Calculate rolling means
        AVG(pm2_5) OVER (
            PARTITION BY city 
            ORDER BY expected_timestamp 
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) as pm2_5_24h_mean,
        AVG(pm10) OVER (
            PARTITION BY city 
            ORDER BY expected_timestamp 
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) as pm10_24h_mean,
        AVG(no2) OVER (
            PARTITION BY city 
            ORDER BY expected_timestamp 
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) as no2_24h_mean,
        AVG(so2) OVER (
            PARTITION BY city 
            ORDER BY expected_timestamp 
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) as so2_24h_mean,
        AVG(co) OVER (
            PARTITION BY city 
            ORDER BY expected_timestamp 
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) as co_24h_mean,
        -- Count measurements in window
        COUNT(*) OVER (
            PARTITION BY city 
            ORDER BY expected_timestamp 
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) as total_periods,
        -- Count actual measurements in window
        SUM(is_actual_measurement) OVER (
            PARTITION BY city 
            ORDER BY expected_timestamp 
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) as valid_measurements,
        -- Flag current record
        is_actual_measurement as is_actual_reading
    FROM complete_hourly_data
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
    ROUND(CAST(valid_measurements AS FLOAT64) / total_periods * 100, 1) as data_completeness_pct,
    is_actual_reading
FROM rolling_means
WHERE 
    -- Only show periods with at least 75% data completeness (18 out of 24 hours)
    valid_measurements >= 18
    AND total_periods = 24  -- Ensure we have a full 24-hour window
    AND is_actual_reading = 1  -- Only show points where we actually have a reading
ORDER BY city, timestamp DESC 