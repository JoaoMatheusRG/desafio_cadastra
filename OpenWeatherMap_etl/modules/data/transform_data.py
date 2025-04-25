import os
import logging
import pandas as pd
from datetime import datetime
import pytz

from modules.data.utils import kelvin_to_celsius, kelvin_to_fahrenheit

def transform_data(data: list) -> pd.DataFrame | None:
    """
        Transforma os dados brutos da API em um DataFrame limpo e estruturado.
        
        Input:
            data: dados diretos da api

        Output:
            df_final: dados transformados 
    """
    if not data:
        logging.warning("Nenhum dado para transformar.")
        return None

    try:
        df = pd.json_normalize(data)

        required_cols = {
            'dt': 'forecast_unix_ts',
            'main.temp': 'temp_kelvin',
            'main.feels_like': 'feels_like_kelvin',
            'main.humidity': 'humidity',
            'main.temp_min': 'temp_min_kelvin',
            'main.temp_max': 'temp_max_kelvin',
            'weather': 'weather_info',
            'wind.speed': 'wind_speed',
            'wind.deg': 'wind_deg',
            'pop': 'probability_precipitation',
            'city_id': 'city_id',
            'city_name': 'city_name'
        }

        cols_to_select = {api_col: new_name for api_col, new_name in required_cols.items() if api_col in df.columns}
        df = df[list(cols_to_select.keys())]
        df.rename(columns=cols_to_select, inplace=True)

        df['forecast_timestamp'] = pd.to_datetime(df['forecast_unix_ts'], unit='s', utc=True)
        df['temperature_celsius'] = df['temp_kelvin'].apply(kelvin_to_celsius)
        df['temperature_fahrenheit'] = df['temp_kelvin'].apply(kelvin_to_fahrenheit)
        df['feels_like_celsius'] = df['feels_like_kelvin'].apply(kelvin_to_celsius)

        df['weather_description'] = df['weather_info'].apply(lambda x: x[0]['description'] if isinstance(x, list) and len(x) > 0 and 'description' in x[0] else None)
        df['weather_main'] = df['weather_info'].apply(lambda x: x[0]['main'] if isinstance(x, list) and len(x) > 0 and 'main' in x[0] else None)
        df['weather_icon'] = df['weather_info'].apply(lambda x: x[0]['icon'] if isinstance(x, list) and len(x) > 0 and 'icon' in x[0] else None)

        # Remove valores ausentes
        df.dropna(subset=['temperature_celsius'], inplace=True)

        # Remove outliers (>3σ por cidade)
        def remove_outliers(group, col):
            mean = group[col].mean()
            std = group[col].std()
            return group[(group[col] >= mean - 3 * std) & (group[col] <= mean + 3 * std)]

        df = df.groupby('city_name', group_keys=False).apply(remove_outliers, col='temperature_celsius')

        df['load_timestamp'] = datetime.now(pytz.utc)

        # Derivadas por cidade e data
        df['forecast_date'] = df['forecast_timestamp'].dt.date
        df['forecast_hour'] = df['forecast_timestamp'].dt.hour

        # Média diária de temperatura
        daily_avg_temp = df.groupby(['city_name', 'forecast_date'])['temperature_celsius'].mean().reset_index(name='daily_avg_temp_celsius')

        # Média diária de feels_like
        daily_avg_feels = df.groupby(['city_name', 'forecast_date'])['feels_like_celsius'].mean().reset_index(name='daily_avg_feels_like_celsius')

        # Tendência de umidade: diferença entre 12h e 00h
        humidity_trend = df[df['forecast_hour'].isin([0, 12])][['city_name', 'forecast_date', 'forecast_hour', 'humidity']]
        humidity_pivot = humidity_trend.pivot_table(index=['city_name', 'forecast_date'], columns='forecast_hour', values='humidity')
        humidity_pivot['humidity_trend'] = humidity_pivot[12] - humidity_pivot[0]
        humidity_pivot.reset_index(inplace=True)

        # Merge das métricas derivadas
        df = df.merge(daily_avg_temp, on=['city_name', 'forecast_date'], how='left')
        df = df.merge(daily_avg_feels, on=['city_name', 'forecast_date'], how='left')
        df = df.merge(humidity_pivot[['city_name', 'forecast_date', 'humidity_trend']], on=['city_name', 'forecast_date'], how='left')

        final_cols = [
            'city_id', 'city_name', 'forecast_timestamp', 'temperature_celsius',
            'temperature_fahrenheit', 'feels_like_celsius', 'humidity',
            'temp_min_kelvin', 'temp_max_kelvin', 'wind_speed', 'wind_deg',
            'probability_precipitation', 'weather_main', 'weather_description', 'weather_icon',
            'load_timestamp', 'daily_avg_temp_celsius', 'daily_avg_feels_like_celsius', 'humidity_trend'
        ]

        df_final = df[[col for col in final_cols if col in df.columns]].copy()
        df_final['city_id'] = df_final['city_id'].astype(int)

        logging.info(f"Transformação concluída. {len(df_final)} registros prontos para carga.")
        return df_final

    except Exception as e:
        logging.error(f"Erro durante a transformação dos dados: {e}", exc_info=True)
        return None
