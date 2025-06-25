from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# config for Dataproc
GCP_PROJECT = "e-object-459802-s8"
GCP_REGION = "us-central1" 
DATAPROC_CLUSTER = "retail-cluster"
GCS_PYSPARK_SCRIPT = "gs://retail-pipeline-etl/code/Retail_data_cleansing.py"
GCS_INPUT_FILE = "gs://retail-pipeline-etl/raw_data/online_Retail.csv"
BQ_OUTPUT_TABLE = "e-object-459802-s8.staging.retail_cleaned"
GCS_TEMP_BUCKET = "retail-pipeline-etl-temp"

with DAG(
    dag_id="retail-dag",
    schedule=None, 
    start_date=datetime.now() - timedelta(days=1), 
    catchup=False
) as dag:
    
    local_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id = "upload_csv_to_gcs",
        src = "/usr/local/airflow/include/Online_Retail.csv",
        dst = "raw_data/online_retail.csv",
        bucket="retail-pipeline-etl",
        gcp_conn_id = 'gcp',
        mime_type = 'text/csv'
    )
    
    # 1. Run PySpark Job on Dataproc
    spark_job = {
        "reference": {"project_id": GCP_PROJECT},
        "placement": {"cluster_name": DATAPROC_CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": GCS_PYSPARK_SCRIPT,
            "args": [
                "--input", GCS_INPUT_FILE,
                "--output_table", BQ_OUTPUT_TABLE,
                "--temp_bucket", GCS_TEMP_BUCKET
            ]
        },
    }
    run_pyspark = DataprocSubmitJobOperator(
        task_id="run_pyspark",
        job=spark_job,
        region=GCP_REGION,
        project_id=GCP_PROJECT,
        gcp_conn_id = 'gcp'
    )

    # 2. DBT steps
    run_dbt_excluding_dim_cutomers = BashOperator(
        task_id="run_dbt_models_except_dim_customers",
        bash_command = "/home/astro/dbt_venv/bin/dbt run "
                       "--exclude dim_customers "
                       "--project-dir /usr/local/airflow/include/dbt "
                       "--profiles-dir /usr/local/airflow/include/dbt"
                       
    )
    
    run_dbt_snapshot = BashOperator(
        task_id = "run_dbt_snapshot",
        bash_command = "/home/astro/dbt_venv/bin/dbt snapshot "
                       "--project-dir /usr/local/airflow/include/dbt "
                       "--profiles-dir /usr/local/airflow/include/dbt"
    )
    
    run_dbt_dim_customers = BashOperator(
        task_id = "run_dbt_model_dim_customers",
        bash_command = "/home/astro/dbt_venv/bin/dbt run "
                       "--select dim_customers "
                       "--project-dir /usr/local/airflow/include/dbt "
                       "--profiles-dir /usr/local/airflow/include/dbt"
                       
    )   
    
    
    run_dbt_test =  BashOperator(
        task_id = "run_dbt_test",
        bash_command = "/home/astro/dbt_venv/bin/dbt test "
                       "--project-dir /usr/local/airflow/include/dbt "
                       "--profiles-dir /usr/local/airflow/include/dbt"
    )
    
    
    local_csv_to_gcs >> run_pyspark >> run_dbt_excluding_dim_cutomers >> run_dbt_snapshot >> run_dbt_dim_customers >> run_dbt_test
    
