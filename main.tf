terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}


provider "archive" {}
data "google_project" "current" {}

# --- Variáveis ---
variable "project_id" {
  description = "O ID do seu projeto GCP."
  type        = string
}

variable "region" {
  description = "A região GCP onde os recursos serão criados."
  type        = string
  default     = "us-central1"
}

variable "secret_name" {
  description = "O nome do segredo no Secret Manager para a API Key."
  type        = string
  default     = "openweathermap-api-key"
}

variable "secret_value" {
  description = "O valor da API Key para ser armazenado no Secret Manager."
  type        = string
  sensitive   = true 
}

variable "function_name" {
  description = "O nome da Cloud Function."
  type        = string
  default     = "openweathermap-etl-function"
}


variable "function_service_account_id" {
  type        = string
  description = "Service account usada pela Cloud Function"
}

variable "scheduler_service_account_id" {
  type        = string
  description = "Service account usada pelo Cloud Scheduler"
}



# --- Recursos ---
resource "google_secret_manager_secret" "api_key_secret" {
  secret_id = var.secret_name

  replication {
      user_managed {
        replicas {
          location = var.region
        }
      }
  }
}

resource "google_secret_manager_secret_version" "api_key_secret_version" {
  secret      = google_secret_manager_secret.api_key_secret.id
  secret_data = var.secret_value
}


resource "google_storage_bucket" "function_source_bucket" {
  name          = "${var.project_id}-cf-source-${var.region}"
  location      = var.region
  force_destroy = true
  uniform_bucket_level_access = true 
}

data "archive_file" "function_source_zip" {
  type        = "zip"
  source_dir  = "OpenWeatherMap_etl"
  output_path = "${path.module}/function_source.zip"
}

resource "google_storage_bucket_object" "function_source_object" {
  name   = "${var.function_name}-${data.archive_file.function_source_zip.output_md5}.zip"
  bucket = google_storage_bucket.function_source_bucket.name
  source = data.archive_file.function_source_zip.output_path
}

data "google_service_account" "cloud_function_service_account" {
  account_id = var.function_service_account_id
}

resource "google_cloudfunctions_function" "openweathermap_etl" {
  name                  = var.function_name
  runtime               = "python311"
  region                = var.region
  entry_point           = "main"
  source_archive_bucket = google_storage_bucket.function_source_bucket.name
  source_archive_object = google_storage_bucket_object.function_source_object.name
  timeout               = 300
  available_memory_mb   = 2048
  trigger_http          = true
  service_account_email = data.google_service_account.cloud_function_service_account.email

  environment_variables = {
    PROJECT_ID  = var.project_id
    SECRET_NAME = var.secret_name 
  }
}

resource "google_secret_manager_secret_iam_member" "api_key_secret_accessor" {
  secret_id = google_secret_manager_secret.api_key_secret.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_cloudfunctions_function.openweathermap_etl.service_account_email}"
}

data "google_service_account" "cloud_scheduler_service_account" {
  account_id = var.scheduler_service_account_id
}

resource "google_cloud_scheduler_job" "openweathermap_scheduler" {
  name     = "openweathermap-etl-scheduler"
  region   = var.region
  schedule = "0 */3 * * *"
  time_zone = "Etc/UTC"

  http_target {
    uri = google_cloudfunctions_function.openweathermap_etl.https_trigger_url
    http_method = "POST"

    oidc_token {
      service_account_email = data.google_service_account.cloud_scheduler_service_account.email
      audience              = google_cloudfunctions_function.openweathermap_etl.https_trigger_url
    }
  }
}

resource "google_cloudfunctions_function_iam_member" "scheduler_invoker" {
  project        = google_cloudfunctions_function.openweathermap_etl.project
  region         = google_cloudfunctions_function.openweathermap_etl.region
  cloud_function = google_cloudfunctions_function.openweathermap_etl.name
  role           = "roles/cloudfunctions.invoker"
  member         = "serviceAccount:${data.google_service_account.cloud_scheduler_service_account.email}"
}

resource "google_bigquery_dataset" "weather_dataset" {
  dataset_id    = "weather_insights"
  location      = var.region
  friendly_name = "Dados e Insights de Tempo"
  description   = "Dataset para dados de tempo e views."
}

resource "google_bigquery_table" "forecasts" {
  dataset_id = google_bigquery_dataset.weather_dataset.dataset_id
  table_id   = "forecasts"
  project    = var.project_id

  schema = jsonencode([
    { name = "city_id", type = "INTEGER", mode = "NULLABLE", description = "ID da cidade" },
    { name = "city_name", type = "STRING", mode = "NULLABLE", description = "Nome da cidade" },
    { name = "forecast_timestamp", type = "TIMESTAMP", mode = "NULLABLE", description = "Data e hora da previsão (UTC)" },
    { name = "temperature_celsius", type = "FLOAT", mode = "NULLABLE", description = "Temperatura em graus Celsius" },
    { name = "temperature_fahrenheit", type = "FLOAT", mode = "NULLABLE", description = "Temperatura em Fahrenheit" },
    { name = "feels_like_celsius", type = "FLOAT", mode = "NULLABLE", description = "Sensação térmica em Celsius" },
    { name = "humidity", type = "FLOAT", mode = "NULLABLE", description = "Umidade relativa (%)" },
    { name = "temp_min_kelvin", type = "FLOAT", mode = "NULLABLE", description = "Temperatura mínima (Kelvin)" },
    { name = "temp_max_kelvin", type = "FLOAT", mode = "NULLABLE", description = "Temperatura máxima (Kelvin)" },
    { name = "wind_speed", type = "FLOAT", mode = "NULLABLE", description = "Velocidade do vento (m/s)" },
    { name = "wind_deg", type = "FLOAT", mode = "NULLABLE", description = "Direção do vento (graus)" },
    { name = "probability_precipitation", type = "FLOAT", mode = "NULLABLE", description = "Probabilidade de precipitação" },
    { name = "weather_main", type = "STRING", mode = "NULLABLE", description = "Condição principal do tempo" },
    { name = "weather_description", type = "STRING", mode = "NULLABLE", description = "Descrição textual do tempo" },
    { name = "weather_icon", type = "STRING", mode = "NULLABLE", description = "Ícone do tempo" },
    { name = "load_timestamp", type = "TIMESTAMP", mode = "NULLABLE", description = "Timestamp de carga" },
    { name = "daily_avg_temp_celsius", type = "FLOAT", mode = "NULLABLE", description = "Temperatura média diária (Celsius)" },
    { name = "humidity_trend", type = "FLOAT", mode = "NULLABLE", description = "Diferença de umidade entre 12h e 00h" },
    { name = "daily_avg_feels_like_celsius", type = "FLOAT", mode = "NULLABLE", description = "Sensação térmica média diária (Celsius)" },
  ])

  time_partitioning {
    type  = "DAY"
    field = "forecast_timestamp"
  }

  clustering = ["city_id"]

  depends_on = [
    google_bigquery_dataset.weather_dataset
  ]
}



resource "google_bigquery_table" "view1" {
  dataset_id = google_bigquery_dataset.weather_dataset.dataset_id
  project    = var.project_id
  table_id   = "daily_avg_temperature_view"

  view {
    query = templatefile("insights_queries/view1.sql", {
                project_id = var.project_id
                dataset_id = google_bigquery_dataset.weather_dataset.dataset_id
              })  
    use_legacy_sql = false
  }

  depends_on = [google_bigquery_table.forecasts]  
}

resource "google_bigquery_table" "view2" {
  dataset_id = google_bigquery_dataset.weather_dataset.dataset_id
  project    = var.project_id
  table_id   = "temp_variation_24h_view"

  view {
    query = templatefile("insights_queries/view2.sql", {
                project_id = var.project_id
                dataset_id = google_bigquery_dataset.weather_dataset.dataset_id
              })  
    use_legacy_sql = false
  }

  depends_on = [google_bigquery_table.forecasts]  
}

resource "google_bigquery_table" "view3" {
  dataset_id = google_bigquery_dataset.weather_dataset.dataset_id
  project    = var.project_id
  table_id   = "high_humidity_view"

  view {
    query = templatefile("insights_queries/view3.sql", {
                project_id = var.project_id
                dataset_id = google_bigquery_dataset.weather_dataset.dataset_id
              }) 
    use_legacy_sql = false
  }

  depends_on = [google_bigquery_table.forecasts]  
}
