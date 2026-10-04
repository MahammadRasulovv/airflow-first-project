from datetime import datetime, timedelta

from airflow.sdk import DAG, TaskGroup
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.smtp.operators.smtp import EmailOperator

MAIL_TO = "mehemmed.rasulov05@gmail.com"

default_args = {
    "owner": "mahammad",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="write_csv_to_postgres",
    start_date=datetime(2026, 10, 1),
    schedule="0 */4 * * *",
    catchup=False,
    default_args=default_args,
) as dag:

    check_file = FileSensor(
        task_id="check_products_file",
        filepath="/opt/airflow/files/products.csv",
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=60 * 5,
        mode="reschedule",
    )

    load_csv = SQLExecuteQueryOperator(
        task_id="load_csv_to_postgres",
        conn_id="pg_code",
        sql="""
            CREATE TABLE IF NOT EXISTS products (
                id INT,
                name TEXT,
                price NUMERIC
            );
            COPY products FROM '/files/products.csv' DELIMITER ',' CSV HEADER;
        """,
    )

    with TaskGroup(group_id="send_result_mail") as send_result_mail:

        mail_success = EmailOperator(
            task_id="mail_success",
            conn_id="smtp_default",
            to=MAIL_TO,
            subject="write_csv_to_postgres: UĞURLU",
            html_content="DAG write_csv_to_postgres uğurla bitdi.",
            trigger_rule="all_success",
            retries=0,
        )

        mail_failed = EmailOperator(
            task_id="mail_failed",
            conn_id="smtp_default",
            to=MAIL_TO,
            subject="write_csv_to_postgres: ERROR",
            html_content="DAG write_csv_to_postgres error ilə bitdi (3 retry bitdi). Loglara bax.",
            trigger_rule="one_failed",
            retries=0,
        )

    check_file >> load_csv >> send_result_mail