import logging
import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

def load_data_to_bigquery(df: pd.DataFrame, 
                          PROJECT_ID: str,
                          BQ_DATASET: str,
                          BQ_TABLE: str,
                          BQ_STAGING_TABLE: str) -> None:
    """
      Carrega o DataFrame transformado no BigQuery usando MERGE, com particionamento e clusterização.

      Inputs:
          df: dados transformados
          PROJECT_ID: ID do projeto no Google Cloud
          BQ_DATASET: nome do dataset no BigQuery
          BQ_TABLE: nome da tabela final particionada
          BQ_STAGING_TABLE: nome da staging table temporária
    """
    if df is None or df.empty:
        logging.warning("DataFrame vazio ou nulo. Nenhum dado para carregar no BigQuery.")
        return

    try:
        client = bigquery.Client(project=PROJECT_ID)
        dataset_ref = client.dataset(BQ_DATASET)
        staging_table_ref = dataset_ref.table(BQ_STAGING_TABLE)
        final_table_ref = dataset_ref.table(BQ_TABLE)

        schema = [
            bigquery.SchemaField("city_id", "INTEGER", description="ID da cidade"),
            bigquery.SchemaField("city_name", "STRING", description="Nome da cidade"),
            bigquery.SchemaField("forecast_timestamp", "TIMESTAMP", description="Data e hora da previsão (UTC)"),
            bigquery.SchemaField("temperature_celsius", "FLOAT", description="Temperatura em graus Celsius"),
            bigquery.SchemaField("temperature_fahrenheit", "FLOAT", description="Temperatura em Fahrenheit"),
            bigquery.SchemaField("feels_like_celsius", "FLOAT", description="Sensação térmica em Celsius"),
            bigquery.SchemaField("humidity", "FLOAT", description="Umidade relativa (%)"),
            bigquery.SchemaField("temp_min_kelvin", "FLOAT", description="Temperatura mínima (Kelvin)"),
            bigquery.SchemaField("temp_max_kelvin", "FLOAT", description="Temperatura máxima (Kelvin)"),
            bigquery.SchemaField("wind_speed", "FLOAT", description="Velocidade do vento (m/s)"),
            bigquery.SchemaField("wind_deg", "FLOAT", description="Direção do vento (graus)"),
            bigquery.SchemaField("probability_precipitation", "FLOAT", description="Probabilidade de precipitação"),
            bigquery.SchemaField("weather_main", "STRING", description="Condição principal do tempo"),
            bigquery.SchemaField("weather_description", "STRING", description="Descrição textual do tempo"),
            bigquery.SchemaField("weather_icon", "STRING", description="Ícone do tempo"),
            bigquery.SchemaField("load_timestamp", "TIMESTAMP", description="Timestamp de carga"),
            bigquery.SchemaField("daily_avg_temp_celsius", "FLOAT", description="Temperatura média diária (Celsius)"),
            bigquery.SchemaField("humidity_trend", "FLOAT", description="Diferença de umidade entre 12h e 00h"),
            bigquery.SchemaField("daily_avg_feels_like_celsius", "FLOAT", description="Sensação térmica média diária (Celsius)")
        ]

        try:
            client.get_table(final_table_ref)
            logging.info("Tabela final já existe.")
        except NotFound:
            logging.info("Tabela final não existe. Criando com particionamento e clusterização.")
            table = bigquery.Table(final_table_ref, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="forecast_timestamp"
            )
            table.clustering_fields = ["city_id"]
            table = client.create_table(table)
            logging.info(f"Tabela {BQ_TABLE} criada com particionamento e clusterização.")

        # === 1. Carregar na Staging (sobrescreve) ===
        logging.info(f"Carregando {len(df)} registros para a staging: {BQ_DATASET}.{BQ_STAGING_TABLE}")
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            create_disposition='CREATE_IF_NEEDED',
            schema=schema
        )
        load_job = client.load_table_from_dataframe(df, staging_table_ref, job_config=job_config)
        load_job.result()  # Aguarda conclusão da carga
        logging.info(f"Staging carregada com sucesso: {load_job.output_rows} linhas.")

        # === 2. Verificar se a staging tem dados antes do MERGE ===
        staging_table = client.get_table(staging_table_ref)
        staging_row_count = staging_table.num_rows

        if staging_row_count > 0:
            logging.info(f"Staging contém {staging_row_count} registros. Prosseguindo com o MERGE...")

            cols_to_update = [col for col in df.columns if col not in ['city_id', 'forecast_timestamp']]
            update_set_clause = ",\n".join([f"target.{col} = source.{col}" for col in cols_to_update])
            insert_cols = ",\n".join(df.columns)
            source_cols = ",\n".join([f"source.{col}" for col in df.columns])

            merge_sql = f"""
            MERGE `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}` AS target
            USING `{PROJECT_ID}.{BQ_DATASET}.{BQ_STAGING_TABLE}` AS source
            ON target.city_id = source.city_id AND target.forecast_timestamp = source.forecast_timestamp
            WHEN MATCHED THEN
              UPDATE SET
                {update_set_clause}
            WHEN NOT MATCHED THEN
              INSERT ({insert_cols})
              VALUES ({source_cols});
            """
            merge_job = client.query(merge_sql)
            merge_job.result()

            if merge_job.errors:
                logging.error(f"Erro no MERGE: {merge_job.errors}")
            else:
                logging.info(f"MERGE completo. Linhas afetadas: {merge_job.num_dml_affected_rows or 'desconhecido'}")
        else:
            logging.warning("A tabela staging está vazia após a carga. MERGE abortado.")

    except Exception as e:
        logging.error(f"Erro durante o carregamento para o BigQuery: {e}", exc_info=True)
