#set the entry point to 'trigger_dag'
import google.auth
from typing import Any
from google.auth.transport.requests import AuthorizedSession
import requests
import logging
import re
import google.cloud.logging

client = google.cloud.logging.Client()
client.setup_logging()

AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
CREDENTIALS, _ = google.auth.default(scopes=[AUTH_SCOPE])

dag_trigger_rules = [
    { 'dag': 'elt_financial_data_pipeline', 'regex': 'financial_data' }
]

def trigger_dag(request):
    """
    Cloud Function to trigger a Composer DAG based on a file name.
    Can be triggered by an HTTP request or a Cloud Storage event.
    """
    # The event payload is the JSON body of the HTTP request
    # For GCS events, the payload is passed directly as the first argument.
    data = request.get_json(silent=True) if hasattr(request, 'get_json') else request

    web_server_url = "https://composer-airflow-web-ui-dot-europe-west1.composer.googleusercontent.com"

    object_name = data['name']
    logging.info(f"Processing object: {object_name}")

    if 'proceed/' in object_name:
        logging.info(f"Skipping file in 'proceed' directory: {object_name}")
        return "Skipped: File is in proceed directory.", 200

    for rule in dag_trigger_rules:
        regex = rule['regex']
        if re.search(regex, object_name):
            dag_name = rule['dag']
            logging.info('Successfully triggered DAG: {}'.format(dag_name))
            endpoint = f"api/v1/dags/{dag_name}/dagRuns"
            url = f"{web_server_url}/{endpoint}"
            composer_response = make_composer3_web_server_request(url, method='POST', json={"conf": data})
            return composer_response.text, composer_response.status_code

    logging.warning(f"No matching DAG trigger rule found for object: {object_name}")
    return f"No matching DAG trigger rule found for object: {object_name}", 404

def make_composer3_web_server_request(
    url: str, method: str = "GET", **kwargs: Any
) -> google.auth.transport.Response:

    authed_session = AuthorizedSession(CREDENTIALS)

    #Set the default timeout, if missing
    if "timeout" not in kwargs:
        kwargs["timeout"] = 90

    return authed_session.request(method, url, **kwargs)
