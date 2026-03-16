from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator
)

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


PROJECT_ID = "kishan-learning-project-1"
REGION = "us-east1"
CLUSTER_NAME = "sales-cluster"


# Function to determine process date
def get_process_date(**context):

    conf = context["dag_run"].conf

    if conf and "process_date" in conf:
        return conf["process_date"].replace("-", "")

    return context["ds"].replace("-", "")


with DAG(
    dag_id="sales_processing_pipeline",
    start_date=datetime(2026,1,1),
    schedule=None,
    catchup=False
) as dag:


    # Step 1 — Determine process date
    get_date = PythonOperator(
        task_id="get_process_date",
        python_callable=get_process_date
    )


    # Step 2 — Create Dataproc cluster
    create_cluster = DataprocCreateClusterOperator(
    task_id="create_cluster",
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    cluster_config={
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n1-standard-2",
                "disk_config": {
                    "boot_disk_type": "pd-standard",
                    "boot_disk_size_gb": 32
                },
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": "n1-standard-2",
                "disk_config": {
                    "boot_disk_type": "pd-standard",
                    "boot_disk_size_gb": 32
                },
            },
        }
    )


    # Step 3 — PySpark Job Definition
    pyspark_job = {
        "reference": {"project_id": PROJECT_ID},

        "placement": {"cluster_name": CLUSTER_NAME},

        "pyspark_job": {

            "main_python_file_uri":
            "gs://airflow-pysaprk-dags/pyspark_jobs/sales_etl_job.py",

            "args": [
                "--process_date",
                "{{ ti.xcom_pull(task_ids='get_process_date') }}"
            ]
        }
    }


    # Step 4 — Run Spark Job
    run_spark_job = DataprocSubmitJobOperator(
        task_id="run_spark_job",
        job=pyspark_job,
        region=REGION,
        project_id=PROJECT_ID
    )

    # Step 5 — Delete cluster
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",

        project_id=PROJECT_ID,

        cluster_name=CLUSTER_NAME,

        region=REGION,

        trigger_rule="all_done"
    )


    # Step 6 — Load output to BigQuery
    load_to_bq = GCSToBigQueryOperator(
        task_id="load_bq",

        bucket="sales-data-bucket",

        source_objects=[
            "output/sales_count_{{ ti.xcom_pull(task_ids='get_process_date') }}/*.csv"
        ],

        destination_project_dataset_table="dataproc_gds.sales_counts_gcsToBQload",

        source_format="CSV",

        skip_leading_rows=1,

        write_disposition="WRITE_APPEND",

        create_disposition="CREATE_IF_NEEDED"
    )


    


    # DAG dependencies
    get_date >> create_cluster >> run_spark_job >> delete_cluster >> load_to_bq