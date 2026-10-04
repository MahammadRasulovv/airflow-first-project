from datetime import datetime, timedelta

from airflow.sdk import DAG, TaskGroup
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.smtp.operators.smtp import EmailOperator

MAIL_TO = "mehemmed.rasulov05@gmail.com"
FILE_PATH = "/opt/airflow/files/products.csv"

default_args = {
    "owner": "mahammad",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def load_csv_to_postgres():
    hook = PostgresHook(postgres_conn_id="pg_code")

    hook.run("""
        DROP TABLE IF EXISTS products;
        CREATE TABLE products (
            product_id   TEXT,
            product_name TEXT,
            category     TEXT,
            unit_price   NUMERIC,
            supplier     TEXT
        );
    """)

    hook.copy_expert("COPY products FROM STDIN WITH CSV HEADER", FILE_PATH)


with DAG(
    dag_id="write_csv_to_postgres_success",
    start_date=datetime(2026, 10, 1),
    schedule="0 */4 * * *",
    catchup=False,
    default_args=default_args,
) as dag:

    check_file = FileSensor(
        task_id="check_products_file",
        filepath=FILE_PATH,
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=60 * 5,
        mode="reschedule",
    )

    load_csv = PythonOperator(
        task_id="load_csv_to_postgres",
        python_callable=load_csv_to_postgres,
    )

    with TaskGroup(group_id="send_result_mail") as send_result_mail:

        mail_success = EmailOperator(
            task_id="mail_success",
            conn_id="smtp_default",
            to=MAIL_TO,
            subject="write_csv_to_postgres_success: UĞURLU",
            html_content="DAG write_csv_to_postgres_success uğurla bitdi, CSV Postgres-ə yazıldı.",
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