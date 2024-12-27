SELECT 
    city,
    EXTRACT(YEAR FROM timestamp) as year,
    ROUND(AVG(pm2_5), 2) as pm2_5_annual_mean,
    ROUND(AVG(pm10), 2) as pm10_annual_mean,
    ROUND(AVG(no2), 2) as no2_annual_mean,
    COUNT(*) as measurement_count
FROM {{ source('air_pollution', 'air_pollution') }}
GROUP BY city, EXTRACT(YEAR FROM timestamp)
ORDER BY city, year DESC 