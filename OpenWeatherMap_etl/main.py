import os
import time
import logging

from modules.authentication.get_secret import get_secret
from modules.api.extract_data import extract_data
from modules.data.transform_data import transform_data
from modules.load.load_data_to_bigquery import load_data_to_bigquery

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# IDs das cidades
CITY_IDS = {
    "Sao Paulo": 3448439,
    "Rio de Janeiro": 3451190,
    "Salvador": 3450554,
    "Curitiba": 3464975,
    "Porto Alegre": 3452925
}



PROJECT_ID = os.environ.get("PROJECT_ID")
SECRET_ID = os.environ.get("SECRET_NAME")
BQ_DATASET = "weather_insights"
BQ_TABLE = "forecasts"
BQ_STAGING_TABLE = "forecasts_staging"

def main(request=None):
    """Função principal que orquestra o pipeline ETL."""
    logging.info("--- Iniciando Pipeline ETL de Previsão do Tempo ---")

    start_time = time.time()

    # 1. Obter API Key
    api_key = get_secret(PROJECT_ID, SECRET_ID)
    if not api_key:
        logging.error("Não foi possível obter a API Key. Abortando.")
        return "Erro: API Key não encontrada.", 500

    # 2. Extração (passa flag para a função de extração se necessário)
    raw_data = extract_data(api_key, CITY_IDS)
    if not raw_data:
        logging.warning("Nenhum dado foi extraído. Finalizando.")
        return "Aviso: Nenhum dado extraído.", 200

    # 3. Transformação
    transformed_df = transform_data(raw_data)
    if transformed_df is None:
        logging.error("Falha na transformação dos dados. Abortando carregamento.")
        return "Erro: Falha na transformação.", 500

    # 4. Carregamento
    load_data_to_bigquery(
        transformed_df,
        PROJECT_ID,
        BQ_DATASET,
        BQ_TABLE,
        BQ_STAGING_TABLE
    )

    end_time = time.time()
    logging.info(f"--- Pipeline ETL concluído em {end_time - start_time:.2f} segundos ---")

    return "Pipeline executado com sucesso!", 200
