-- Top 3 cidades com maior variação de temperatura em 24h. 
WITH temperature_variation AS (
  SELECT 
    city_id,
    city_name,
    MAX(temperature_celsius) - MIN(temperature_celsius) AS temp_variation_24h
  FROM 
    `${project_id}.${dataset_id}.forecasts`
  WHERE 
    forecast_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
  GROUP BY 
    city_id, city_name
)

SELECT 
  city_name,
  temp_variation_24h
FROM 
  temperature_variation
ORDER BY 
  temp_variation_24h DESC
LIMIT 3;
