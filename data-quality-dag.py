from airflow import DAG
from airflow.providers.google.cloud.operators.dataplex import (
    DataplexCreateOrUpdateDataQualityScanOperator,
    DataplexCreateOrUpdateDataProfileScanOperator,
    DataplexRunDataQualityScanOperator,
    DataplexRunDataProfileScanOperator,
    DataplexGetDataQualityScanResultOperator,
    DataplexGetDataProfileScanResultOperator,
)
from airflow.operators.email import EmailOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

PROJECT_ID = 'your-project-id'
REGION = 'europe-west1' # Assuming the same region as other DAGs
DATASET_ID = 'bank_data_dev'
TABLE_ID = 'transformed_data'
 
# Unique IDs for the scans
DATA_QUALITY_SCAN_ID = 'financial-data-quality-scan'
DATA_PROFILE_SCAN_ID = 'financial-data-profile-scan'

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dataplex_etl_with_quality_checks_and_profile_scan',
    default_args=default_args,
    description='A DAG to run Dataplex data quality and profile scans on a BigQuery table. Triggered by another DAG.',
    schedule_interval=None,
    catchup=False,
    tags=['dataplex', 'data-quality']
) as dag:

    # Task Group for Data Quality Scan
    with TaskGroup(group_id='data_quality_scan_group') as data_quality_scan_group:
        create_or_update_quality_scan = DataplexCreateOrUpdateDataQualityScanOperator(
            task_id='create_or_update_quality_scan',
            project_id=PROJECT_ID,
            region=REGION,
            data_scan_id=DATA_QUALITY_SCAN_ID,
            body={
                'data': {
                    'resource': f'//bigquery.googleapis.com/projects/{PROJECT_ID}/datasets/{DATASET_ID}/tables/{TABLE_ID}'
                },
                'data_quality_spec': {
                    'rules': [
                        {
                            'dimension': 'COMPLETENESS',
                            'name': 'transaction-id-not-null',
                            'description': 'transaction_id must not be null',
                            'column': 'transaction_id',
                            'threshold': 1.0,
                            'non_null_expectation': {}
                        },
                        {
                            'dimension': 'UNIQUENESS',
                            'name': 'unique-transaction-id',
                            'description': 'transaction_id must be unique',
                            'column': 'transaction_id',
                            'threshold': 1.0,
                            'uniqueness_expectation': {}
                        },
                        {
                            'dimension': 'COMPLETENESS',
                            'name': 'reference-token-not-null',
                            'description': 'reference_token must not be null',
                            'column': 'reference_token',
                            'threshold': 1.0,
                            'non_null_expectation': {}
                        },
                        {
                            'dimension': 'UNIQUENESS',
                            'name': 'unique-reference-token',
                            'description': 'reference_token must be unique',
                            'column': 'reference_token',
                            'threshold': 1.0,
                            'uniqueness_expectation': {}
                        },
                        {
                            'dimension': 'COMPLETENESS',
                            'name': 'customer-id-not-null',
                            'description': 'customer_id must not be null',
                            'column': 'customer_id',
                            'threshold': 1.0,
                            'non_null_expectation': {}
                        },
                        {
                            'dimension': 'VALIDITY',
                            'name': 'fee-percentage-in-range',
                            'description': 'Fee percentage should be between 0 and 100',
                            'column': 'fee_percentage',
                            'threshold': 1.0,
                            'range_expectation': {
                                'min_value': '0',
                                'max_value': '100'
                            }
                        },
                    ],
                    'post_scan_actions': {
                        'bigquery_export': {
                            'results_table': f'//bigquery.googleapis.com/projects/{PROJECT_ID}/datasets/{DATASET_ID}/tables/dq_results'
                        }
                    }
                },
                'execution_spec': {
                    'trigger': {
                        'on_demand': {}
                    }
                }
            }
        )

        run_quality_scan = DataplexRunDataQualityScanOperator(
            task_id='run_quality_scan',
            project_id=PROJECT_ID,
            region=REGION,
            data_scan_id=DATA_QUALITY_SCAN_ID,
        )

        get_quality_scan_results = DataplexGetDataQualityScanResultOperator(
            task_id='get_quality_scan_results',
            project_id=PROJECT_ID,
            region=REGION,
            data_scan_id=DATA_QUALITY_SCAN_ID,
            job_id="{{ task_instance.xcom_pull(task_ids='data_quality_scan_group.run_quality_scan').split('/')[-1] }}",
        )

        send_quality_results_email = EmailOperator(
            task_id='send_quality_scan_results_email',
            to='your-email',
            subject='Dataplex Data Quality Scan Results for {{ ds }}',
            html_content="""
            <h3>Dataplex Data Quality Scan Results</h3>
            <p>DAG: {{ dag.dag_id }}</p>
            <p>Execution Date: {{ ds }}</p>
            <pre>{{ task_instance.xcom_pull(task_ids='data_quality_scan_group.get_quality_scan_results') | tojson(indent=4) }}</pre>
            """,
        )

        create_or_update_quality_scan >> run_quality_scan >> get_quality_scan_results >> send_quality_results_email

    # Task Group for Data Profile Scan
    with TaskGroup(group_id='data_profile_scan_group') as data_profile_scan_group:
        create_or_update_profile_scan = DataplexCreateOrUpdateDataProfileScanOperator(
            task_id='create_or_update_profile_scan',
            project_id=PROJECT_ID,
            region=REGION,
            data_scan_id=DATA_PROFILE_SCAN_ID,
            body={
                'data': {
                    'resource': f'//bigquery.googleapis.com/projects/{PROJECT_ID}/datasets/{DATASET_ID}/tables/{TABLE_ID}'
                },
                'data_profile_spec': {},  # An empty spec triggers a default profile scan
                'execution_spec': {
                    'trigger': {
                        'on_demand': {}
                    }
                }
            }
        )

        run_profile_scan = DataplexRunDataProfileScanOperator(
            task_id='run_profile_scan',
            project_id=PROJECT_ID,
            region=REGION,
            data_scan_id=DATA_PROFILE_SCAN_ID,
        )

        get_profile_scan_results = DataplexGetDataProfileScanResultOperator(
            task_id='get_profile_scan_results',
            project_id=PROJECT_ID,
            region=REGION,
            data_scan_id=DATA_PROFILE_SCAN_ID,
        )

        create_or_update_profile_scan >> run_profile_scan >> get_profile_scan_results
