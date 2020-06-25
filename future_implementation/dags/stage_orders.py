import datetime
import logging

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Felix',
    'depends_on_past': False,
    'start_date': days_ago(2),
}

dag = DAG(
    'stage_orders',
    default_args=default_args,
    description='Stage Rebuy Orders to S3'
)

CREATE_ORDER_OVERVIEWS_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS order_overviews (
        packages INTEGER NOT NULL,
        order_id TIMESTAMP NOT NULL,
        created TIMESTAMP NOT NULL,
        voucher_code VARCHAR NOT NULL,
        voucher_price VARCHAR,
        total_order_price VARCHAR,
        shipping_fee VARCHAR,
        email VARCHAR,
        PRIMARY KEY(order_id)
    );
"""

create_order_overviews = PostgresOperator(
    task_id="create_order_overviews",
    dag=dag,
    postgres_conn_id="arbitrage-cluster",
    sql=CREATE_ORDER_OVERVIEWS_TABLE_SQL
)

create_order_overviews


# def load_data_to_redshift(*args, **kwargs):
#     aws_hook = AwsHook("aws_credentials")
#     credentials = aws_hook.get_credentials()
#     redshift_hook = PostgresHook("redshift")
#     redshift_hook.run(sql_statements.COPY_ALL_TRIPS_SQL.format(
#         credentials.access_key, credentials.secret_key))
