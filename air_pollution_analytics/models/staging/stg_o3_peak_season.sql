WITH daily_max_base AS (
    -- Get daily maximum 8-hour averages
    SELECT *
    FROM {{ ref('stg_o3_8h_rolling') }}
),

monthly_stats AS (
    -- Calculate monthly statistics
    SELECT
        city,
        EXTRACT(YEAR FROM date) as year,
        EXTRACT(MONTH FROM date) as month,
        AVG(daily_max_o3_8h) as monthly_mean_o3,
        COUNT(*) as days_in_month
    FROM daily_max_base
    GROUP BY city, EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date)
),

ranked_months AS (
    -- Rank months by ozone levels within each year and city
    SELECT
        city,
        year,
        month,
        monthly_mean_o3,
        days_in_month,
        RANK() OVER (
            PARTITION BY city, year
            ORDER BY monthly_mean_o3 DESC
        ) as month_rank
    FROM monthly_stats
    WHERE days_in_month >= 14  -- Require at least 14 days of data in a month
),

peak_season_months AS (
    -- Identify peak season (top 3 months)
    SELECT *
    FROM ranked_months
    WHERE month_rank <= 3
),

peak_season_data AS (
    -- Get daily data for peak season months
    SELECT
        d.city,
        d.date,
        d.daily_max_o3_8h,
        p.year,
        p.month,
        p.monthly_mean_o3,
        p.month_rank
    FROM daily_max_base d
    INNER JOIN peak_season_months p
        ON d.city = p.city
        AND EXTRACT(YEAR FROM d.date) = p.year
        AND EXTRACT(MONTH FROM d.date) = p.month
)

SELECT 
    city,
    date,
    ROUND(daily_max_o3_8h, 2) as daily_max_o3_8h,
    year,
    month,
    ROUND(monthly_mean_o3, 2) as monthly_mean_o3,
    month_rank
FROM peak_season_data
ORDER BY city, date DESC 