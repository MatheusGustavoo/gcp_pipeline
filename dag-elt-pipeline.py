from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import logging

# Default args
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=5),
}

def log_job_status(status, job_name, **context):
    """Log Cloud Run job status"""
    ti = context['ti']
    execution_date = context['ds']
    duration = ti.duration if ti.duration else 0
    
    log_message = f"""
    ==========================================
    Cloud Run Job Status: {status}
    ==========================================
    Job Name: {job_name}
    Task ID: {ti.task_id}
    Execution Date: {execution_date}
    Duration: {duration}s
    Status: {status}
    ==========================================
    """
    
    logging.info(log_message)
    print(log_message)

def move_files_to_proceed_folder(ti, **kwargs):
    """
    Moves files from the source GCS path to the proceed folder.
    """
    source_bucket = 'elt-dev'
    destination_bucket = 'elt-dev'
    destination_folder = 'proceed/'
    
    # Pull the list of objects from the list_gcs_objects task
    source_objects = ti.xcom_pull(task_ids='list_gcs_objects', key='return_value')
    
    if not source_objects:
        logging.info("No files found to move.")
        return

    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    for obj in source_objects:
        destination_object = f"{destination_folder}{obj}"
        
        # First, copy the object to the new location
        gcs_hook.rewrite(
            source_bucket=source_bucket,
            source_object=obj,
            destination_bucket=destination_bucket,
            destination_object=destination_object,
        )

        # Then, delete the original object
        gcs_hook.delete(bucket_name=source_bucket, object_name=obj)

        logging.info(f"Moved {obj} to {destination_object}")

with DAG(
    'elt_bank_data_pipeline',
    default_args=default_args,
    description='DBT pipeline with task groups and Cloud Run job status',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['dbt', 'data-pipeline'],
) as dag:

    # Task to list objects in the GCS bucket with a specific prefix
    list_gcs_objects = GCSListObjectsOperator(
        task_id='list_gcs_objects',
        bucket='elt-dev',
        prefix='bank_data',
        delimiter='/',
        gcp_conn_id='google_cloud_default',
    )
    # Task to load all CSV files from GCS bucket to BigQuery
    load_to_bigquery = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket='elt-dev',
        source_objects=list_gcs_objects.output,
        destination_project_dataset_table='your-project-id.staging.raw_data',
        source_format='CSV',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        gcp_conn_id='google_cloud_default',
    )

    move_gcs_files = PythonOperator(
        task_id='move_gcs_files_to_proceed',
        python_callable=move_files_to_proceed_folder,
    )    
    
    # Task Group 1: Test Raw Data
    with TaskGroup("test_raw_data_group", tooltip="Test raw data in BigQuery") as test_raw_data_group:
        test_raw = CloudRunExecuteJobOperator(
            task_id='execute_test_raw_data_job',
            project_id='your-project-id',
            region='europe-west1',
            job_name='dbt-test-job',
        )

        # This task runs only if test_raw succeeds
        log_test_raw_success = PythonOperator(
            task_id='log_test_raw_success',
            python_callable=log_job_status,
            op_kwargs={
                'status': 'SUCCESS ✅',
                'job_name': 'dbt-test-job'
            },
            trigger_rule='all_success',
        )
        
        # This task runs only if test_raw fails
        log_test_raw_failure = PythonOperator(
            task_id='log_test_raw_failure',
            python_callable=log_job_status,
            op_kwargs={
                'status': 'FAILED ❌',
                'job_name': 'dbt-test-job'
            },
            trigger_rule='one_failed',
        )

        # A dummy task to join the success/failure branches
        join_raw = DummyOperator(
            task_id='join_raw',
            trigger_rule='all_done',
        )

        test_raw >> [log_test_raw_success, log_test_raw_failure] >> join_raw
    
    # Task Group 2: Transform Data
    with TaskGroup("transform_data_group", tooltip="Transform data with dbt") as transform_data_group:
        transform = CloudRunExecuteJobOperator(
            task_id='execute_transform_job',
            project_id='your-project-id',
            region='europe-west1',
            job_name='dbt-transform-job',
        )

        log_transform_success = PythonOperator(
            task_id='log_transform_success',
            python_callable=log_job_status,
            op_kwargs={
                'status': 'SUCCESS ✅',
                'job_name': 'dbt-transform-job'
            },
            trigger_rule='all_success',
        )
        
        log_transform_failure = PythonOperator(
            task_id='log_transform_failure',
            python_callable=log_job_status,
            op_kwargs={
                'status': 'FAILED ❌',
                'job_name': 'dbt-transform-job'
            },
            trigger_rule='one_failed',
        )

        join_transform = DummyOperator(
            task_id='join_transform',
            trigger_rule='none_failed_or_skipped',
        )

        transform >> [log_transform_success, log_transform_failure] >> join_transform
    
    # Task Group 3: Test Transformed Data
    with TaskGroup("test_transformed_data_group", tooltip="Run transformed data quality tests") as test_transformed_data_group:
        test_transformed = CloudRunExecuteJobOperator(
            task_id='execute_transformed_data_test_job',
            project_id='your-project-id',
            region='europe-west1',
            job_name='dbt-test-transformed-job',
        )

        log_test_success = PythonOperator(
            task_id='log_test_success',
            python_callable=log_job_status,
            op_kwargs={
                'status': 'ALL TESTS PASSED ✅',
                'job_name': 'dbt-test-transformed-job'
            },
            trigger_rule='all_success',
        )
        
        log_test_failure = PythonOperator(
            task_id='log_test_failure',
            python_callable=log_job_status,
            op_kwargs={
                'status': 'TESTS FAILED ❌',
                'job_name': 'dbt-test-transformed-job'
            },
            trigger_rule='one_failed',
        )

        join_test = DummyOperator(
            task_id='join_test',
            trigger_rule='none_failed_or_skipped',
        )

        test_transformed >> [log_test_success, log_test_failure] >> join_test
    
    # Final Pipeline Success
    log_pipeline_success = PythonOperator(
        task_id='log_pipeline_success',
        python_callable=lambda **context: logging.info(
            f"""
            ==========================================
            🎉 DBT PIPELINE COMPLETED SUCCESSFULLY 🎉
            ==========================================
            DAG: {context['dag'].dag_id}
            Execution Date: {context['ds']}
            Run ID: {context['run_id']}
            Summary:
              ✅ Raw data tested
              ✅ Data transformed
              ✅ All tests passed
            Status: PIPELINE COMPLETE
            Data is ready for downstream consumption.
            ==========================================
            """
        ),
        trigger_rule='all_success',
    )
    
    # Final failure task
    send_failure_email = EmailOperator(
        task_id='send_failure_email',
        to='your-email',
        conn_id='smtp_default',
        subject='[FAILED] DBT Pipeline: {{ dag.dag_id }}',
        html_content="""
        <h3>DBT Pipeline Failed</h3>
        <p>DAG: {{ dag.dag_id }}</p>
        <p>Execution Date: {{ ds }}</p>
        <p>Run ID: {{ run_id }}</p>
        <p>Check the Airflow logs for more details.</p>
        """,
        trigger_rule='one_failed',
    )

    # Final success task
    send_success_email = EmailOperator(
        task_id='send_success_email',
        to='your-email',
        conn_id='smtp_default',
        subject='[SUCCESS] DBT Pipeline: {{ dag.dag_id }}',
        html_content="""
        <h3>DBT Pipeline Completed Successfully</h3>
        <p>DAG: {{ dag.dag_id }}</p>
        <p>Execution Date: {{ ds }}</p>
        <p>Run ID: {{ run_id }}</p>
        <p>Data is ready for downstream consumption.</p>
        """,
        trigger_rule='all_success',
    )

    # Task to trigger the Dataplex data quality scan DAG
    trigger_data_quality_dag = TriggerDagRunOperator(
        task_id='trigger_data_quality_dag',
        trigger_dag_id='dataplex_etl_with_quality_checks_and_profile_scan',  # This must match the dag_id of your quality check DAG
        wait_for_completion=True,  # Wait for the quality scan DAG to finish
        deferrable=True, # Use deferrable mode to free up worker slots while waiting
        failed_states=['failed'], # Consider the run failed if the downstream DAG fails
        trigger_rule='all_success',
    )

    # Task Group Dependencies
    list_gcs_objects >> load_to_bigquery >> move_gcs_files

    move_gcs_files >> test_raw_data_group
    test_raw_data_group >> transform_data_group 
    transform_data_group >> test_transformed_data_group
    
    # Set dependencies for final status tasks
    test_transformed_data_group >> log_pipeline_success
    log_pipeline_success >> send_success_email >> trigger_data_quality_dag

    # The failure email should be triggered if any of the main tasks fail
    test_transformed_data_group >> send_failure_email
