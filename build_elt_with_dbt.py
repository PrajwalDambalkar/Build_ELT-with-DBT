"""
A basic dbt DAG that shows how to run dbt commands via the BashOperator
Follows the standard dbt seed, run, and test pattern.
"""

from pendulum import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import snowflake.connector


DBT_PROJECT_DIR = "/opt/airflow/lab02_pair14"

def return_snowflake_conn():
    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_connect')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

# conn = return_snowflake_conn()

conn = BaseHook.get_connection('snowflake_connect')
with DAG(
    "Lab02_ELT_with_DBT",
    start_date=datetime(2024, 10, 14),
    description="A sample Airflow DAG to invoke dbt runs using a BashOperator",
    schedule=None,
    catchup=False,
    default_args={
        "env": {
            "DBT_USER": conn.login,
            "DBT_PASSWORD": conn.password,
            "DBT_ACCOUNT": conn.extra_dejson.get("account"),
            # "DBT_SCHEMA": conn.schema,
            "DBT_DATABASE": conn.extra_dejson.get("database"),
            # "DBT_ROLE": conn.extra_dejson.get("role"),
            "DBT_WAREHOUSE": conn.extra_dejson.get("warehouse"),
            "DBT_TYPE": "snowflake"
        }
    },
) as dag:
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run --profiles-dir /opt/airflow/lab02_pair14 --project-dir /opt/airflow/lab02_pair14"
,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test --profiles-dir /opt/airflow/lab02_pair14 --project-dir /opt/airflow/lab02_pair14"
,
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command="dbt snapshot --profiles-dir /opt/airflow/lab02_pair14 --project-dir /opt/airflow/lab02_pair14"
,
    )

    dbt_run >> dbt_test >> dbt_snapshot
