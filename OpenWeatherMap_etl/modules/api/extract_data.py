import os
import time
import logging
import requests
from datetime import datetime, timezone
from dateutil import parser

# Configurações
API_ENDPOINT = "https://api.openweathermap.org/data/2.5/forecast"
MAX_RETRIES = 3
INITIAL_BACKOFF = 2

def extract_data(api_key: str, CITY_IDS: dict[str, int]) -> list:
    """
    Extrai dados da API OpenWeatherMap para o horário atual.
    
    Inputs:
        api_key: chave da API do OpenWeatherMap
        CITY_IDS: dicionário nome -> id de cidade

    Output:
        all_forecast_data: lista com os dados da API
    """
    all_forecast_data = []
    session = requests.Session()
    now = datetime.now(timezone.utc)


    for city_name, city_id in CITY_IDS.items():
        params = {
            'id': city_id,
            'appid': api_key,
            'lang': 'pt_br'
        }
        retries = 0

        while retries < MAX_RETRIES:
            try:
                response = session.get(API_ENDPOINT, params=params, timeout=15)
                logging.info(f"url = {response.request.url}")
                response.raise_for_status()
                data = response.json()

                if 'list' in data and 'city' in data:
                    city_data = []

                    for item in data['list']:
                        item['city_id'] = data['city']['id']
                        item['city_name'] = data['city']['name']
                        city_data.append(item)

                    all_forecast_data.extend(city_data)
                    logging.info(f"Dados extraídos para {city_name}: {len(city_data)} registros filtrados.")
                else:
                    logging.warning(f"Resposta inesperada para {city_name}: 'list' ou 'city' ausente.")
                break  # Sucesso

            except requests.exceptions.Timeout:
                retries += 1
                logging.warning(f"Timeout para {city_name}. Tentativa {retries}/{MAX_RETRIES}.")
                time.sleep(INITIAL_BACKOFF * (2 ** (retries - 1)))

            except requests.exceptions.HTTPError as e:
                logging.error(f"Erro HTTP {e.response.status_code} para {city_name}: {e.response.text}")
                if 500 <= e.response.status_code < 600:
                    retries += 1
                    time.sleep(INITIAL_BACKOFF * (2 ** (retries - 1)))
                else:
                    break  # Erro fatal

            except requests.exceptions.RequestException as e:
                retries += 1
                logging.warning(f"Erro de requisição para {city_name}: {e}")
                time.sleep(INITIAL_BACKOFF * (2 ** (retries - 1)))

    logging.info(f"Extração concluída. Total de {len(all_forecast_data)} registros obtidos.")
    return all_forecast_data
