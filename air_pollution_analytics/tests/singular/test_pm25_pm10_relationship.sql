-- Test that PM2.5 is always less than or equal to PM10
-- except when PM10 is 0, which happens when the actual
-- PM10 value is negative (which is not possible).
select *
from {{ ref('stg_rolling_24h_mean') }}
where pm10_24h_mean > 0 and pm2_5_24h_mean > pm10_24h_mean 