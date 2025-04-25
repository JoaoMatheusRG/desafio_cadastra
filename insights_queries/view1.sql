-- Tendência de temperatura média diária dos últimos 7 dias por cidade. 
WITH daily_avg_temp AS (
  SELECT 
    city_id,
    city_name,
    DATE(forecast_timestamp) AS forecast_date,
    AVG(temperature_celsius) AS avg_temp_celsius
  FROM 
    `${project_id}.${dataset_id}.forecasts`
  WHERE 
    forecast_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  GROUP BY 
    city_id, city_name, forecast_date
)

SELECT 
  city_name,
  forecast_date,
  avg_temp_celsius,
  AVG(avg_temp_celsius) OVER (PARTITION BY city_name ORDER BY forecast_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS trend_last_7_days
FROM 
  daily_avg_temp
ORDER BY 
  city_name, forecast_date DESC;
