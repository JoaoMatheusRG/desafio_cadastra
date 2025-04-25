#!/bin/bash
TFVARS_FILE="terraform.tfvars"

echo "Autenticando com gcloud..."
gcloud auth application-default login

echo "Inicializando Terraform..."
terraform init --upgrade

echo "Validando configuração Terraform..."
terraform validate

read -p "Digite o valor da variável secret_value (API key do OpenWeatherMap): " secret_value
read -p "Digite o valor da variável function_service_account_id: " function_service_account_id
read -p "Digite o valor da variável scheduler_service_account_id: " scheduler_service_account_id

echo "Executando terraform plan..."
terraform plan \
  -var-file="$TFVARS_FILE" \
  -var="secret_value=$secret_value" \
  -var="function_service_account_id=$function_service_account_id" \
  -var="scheduler_service_account_id=$scheduler_service_account_id"

echo "Executando terraform apply..."
terraform apply \
  -var-file="$TFVARS_FILE" \
  -var="secret_value=$secret_value" \
  -var="function_service_account_id=$function_service_account_id" \
  -var="scheduler_service_account_id=$scheduler_service_account_id" \
  -auto-approve
