import logging
from google.cloud import secretmanager

def get_secret(project_id: str, secret_id: str) -> str | None:
    """
        Busca um secret do Google Cloud Secret Manager.

        Inputs:
            project_id: nome do projeto no google cloud
            secret_id: nome da api key do openwearthermap salva no secret manager

        Output:
            payload: valor da api key
    """
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        payload = response.payload.data.decode("UTF-8")
        logging.info(f"Secret '{secret_id}' acessado com sucesso.")
        return payload
    except Exception as e:
        logging.error(f"Falha ao acessar secret '{secret_id}': {e}", exc_info=True)
        return None

