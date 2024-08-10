from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator


PROJECT_ID = 'dataproc-testing-410711'
REGION = 'europe-west1'
SERVICE_ACCOUNT = "dataproc-editor@dataproc-testing-410711.iam.gserviceaccount.com"
DATA_URI = 'gs://medallion-files/raw_data_download.py'
BRONZE_URI = 'gs://medallion-files/bronze_table_creation.py'
SILVER_URI = 'gs://medallion-files/silver_table_creation.py'
GOLD_URI = 'gs://medallion-files/gold_table_creation.py'

CLUSTER_NAME = 'medallion-cluster'

default_args = {
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': 0,
    'email_on_failure': False,
    'email_on_retry': False,
}

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n2-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    },
    "worker_config": {
        "num_instances": 4,
        "machine_type_uri": "n2-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    },
    "secondary_worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n2-standard-4",
        "disk_config": {
            "boot_disk_type": "pd-standard",
            "boot_disk_size_gb": 32,
        },
        "is_preemptible": True,
        "preemptibility": "PREEMPTIBLE",
    },
    "initialization_actions": [{
        "executable_file": "gs://medallion-files/medallion_init.sh"
    }],

    "gce_cluster_config": {
        "service_account": SERVICE_ACCOUNT,
        "metadata": {
            "PIP_PACKAGES": "delta-spark google-cloud-secretmanager kaggle"
        },
        "service_account_scopes": [
            "https://www.googleapis.com/auth/cloud-platform"
        ],
        
    },

    "software_config": {"image_version":"2.1.61-debian11"
    },
}

DATA_DOWNLOAD = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": DATA_URI},
}

BRONZE_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": BRONZE_URI},
}

SILVER_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": SILVER_URI},
}

GOLD_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": GOLD_URI},
}


with DAG(
'medallion-architecture',
default_args=default_args,
description='Creates medallion architecture using delta tables with upserts using GCP Dataproc',
schedule_interval=None,
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        gcp_conn_id='dataproc_connection', 
        use_if_exists=True,
    )

    download_data = DataprocSubmitJobOperator(
    task_id="download_data",
    job=DATA_DOWNLOAD,
    region=REGION,
    project_id=PROJECT_ID,
    gcp_conn_id='dataproc_connection',
    )

    bronze_creation = DataprocSubmitJobOperator(
    task_id="bronze_creation",
    job=BRONZE_JOB, region=REGION,
    project_id=PROJECT_ID,
    gcp_conn_id='dataproc_connection'
    )

    silver_creation = DataprocSubmitJobOperator(
    task_id="silver_creation",
    job=SILVER_JOB,
    region=REGION,
    project_id=PROJECT_ID,
    gcp_conn_id='dataproc_connection'
    )

    gold_creation = DataprocSubmitJobOperator(
    task_id="gold_creation",
    job=GOLD_JOB, region=REGION,
    project_id=PROJECT_ID,
    gcp_conn_id='dataproc_connection'
    )

    delete_cluster = DataprocDeleteClusterOperator(
    task_id="delete_cluster",
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    gcp_conn_id='dataproc_connection'
    )

    create_cluster >> download_data >> bronze_creation >> silver_creation >> gold_creation >> delete_cluster