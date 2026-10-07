import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="seoul_subway_pipeline",
    start_date=pendulum.datetime(2026, 9, 30, tz="Asia/Seoul"),
    schedule="*/30 * * * *",
    catchup=False,
    tags=["seoul", "subway", "etl"],
) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command="python /opt/airflow/src/extract.py",
        cwd="/opt/airflow",
    )

    transform = BashOperator(
        task_id="transform",
        bash_command="python /opt/airflow/src/transform.py",
        cwd="/opt/airflow",
    )

    load = BashOperator(
        task_id="load",
        bash_command="python /opt/airflow/src/load.py",
        cwd="/opt/airflow",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "dbt run "
            "--project-dir /opt/airflow/metro_dbt "
            "--profiles-dir /opt/airflow/metro_dbt"
        ),
        cwd="/opt/airflow/metro_dbt",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "dbt test "
            "--project-dir /opt/airflow/metro_dbt "
            "--profiles-dir /opt/airflow/metro_dbt"
        ),
        cwd="/opt/airflow/metro_dbt",
    )

    extract >> transform >> load >> dbt_run >> dbt_test