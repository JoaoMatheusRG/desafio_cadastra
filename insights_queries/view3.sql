-- Identificação de dias/horários com umidade acima de 90%
WITH high_humidity AS (
  SELECT 
    city_id,
    city_name,
    forecast_timestamp,
    humidity
  FROM 
    `${project_id}.${dataset_id}.forecasts`
  WHERE 
    humidity > 90
)

SELECT 
  city_name,
  forecast_timestamp,
  humidity
FROM 
  high_humidity
ORDER BY 
  forecast_timestamp;