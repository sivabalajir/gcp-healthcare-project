import airflow
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Define constants
PROJECT_ID = "long-nation-476207-n7"
LOCATION = "US"

# Your bucket updated (if needed later for gs:// paths)
COMPOSER_BUCKET = "healthcare-bucket021125"

# SQL file paths inside Cloud Composer environment
SQL_FILE_PATH_1 = "/home/airflow/gcs/data/BQ/bronze.sql"
SQL_FILE_PATH_2 = "/home/airflow/gcs/data/BQ/silver.sql"
SQL_FILE_PATH_3 = "/home/airflow/gcs/data/BQ/gold.sql"

# Read SQL query from file
def read_sql_file(file_path):
    with open(file_path, "r") as file:
        return file.read()

BRONZE_QUERY = read_sql_file(SQL_FILE_PATH_1)
SILVER_QUERY = read_sql_file(SQL_FILE_PATH_2)
GOLD_QUERY = read_sql_file(SQL_FILE_PATH_3)

# Define default arguments
ARGS = {
    "owner": "Siva Balaji",
    "start_date": None,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["***@gmail.com"],
    "email_on_success": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# Define the DAG
with DAG(
    dag_id="bigquery_dag",
    schedule_interval=None,
    description="DAG to run the BigQuery jobs",
    default_args=ARGS,
    tags=["gcs", "bq", "etl", "marvel"]
) as dag:

    # Task to create bronze tables
    bronze_tables = BigQueryInsertJobOperator(
        task_id="bronze_tables",
        configuration={
            "query": {
                "query": BRONZE_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        project_id=PROJECT_ID,
        location=LOCATION
    )

    # Task to create silver tables
    silver_tables = BigQueryInsertJobOperator(
        task_id="silver_tables",
        configuration={
            "query": {
                "query": SILVER_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        project_id=PROJECT_ID,
        location=LOCATION
    )

    # Task to create gold tables
    gold_tables = BigQueryInsertJobOperator(
        task_id="gold_tables",
        configuration={
            "query": {
                "query": GOLD_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        project_id=PROJECT_ID,
        location=LOCATION
    )

# Define dependencies
bronze_tables >> silver_tables >> gold_tables
