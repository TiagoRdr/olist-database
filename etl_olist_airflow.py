from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

from utils.transformations import transform_orders, transform_reviews_postgres
from utils.loaders import load_orders_postgres, load_order_items_postgres, load_products_postgres, load_reviews_postgres
from utils.create_tables import create_tables  # Importando a função de criação de tabelas

with DAG('load_data_postgres', start_date=datetime(2024, 12, 18), schedule_interval=None, catchup=False) as dag:

    # Task para criar tabelas
    create_all_tables_task = PythonOperator(
        task_id="create_all_tables",
        python_callable=create_tables
                )

    transform_orders_task = PythonOperator(
        task_id='transform_orders',
        python_callable=transform_orders,
        provide_context=True
    )

    load_orders_postgres_task = PythonOperator(
        task_id='load_orders_postgres',
        python_callable=load_orders_postgres,
        provide_context=True
    )

    load_order_items_postgres_task = PythonOperator(
        task_id='load_order_items_postgres',
        python_callable=load_order_items_postgres
    )

    load_products_postgres_task = PythonOperator(
        task_id='load_products_postgres',
        python_callable=load_products_postgres
    )

    transform_reviews_postgres_task = PythonOperator(
        task_id='transform_reviews_postgres',
        python_callable=transform_reviews_postgres,
        provide_context=True
    )

    load_reviews_postgres_task = PythonOperator(
        task_id='load_reviews_postgres',
        python_callable=load_reviews_postgres,
        provide_context=True
    )

    # Ordem de execução
    create_all_tables_task >> [transform_orders_task, transform_reviews_postgres_task, load_order_items_postgres_task, load_products_postgres_task]
    transform_orders_task >> load_orders_postgres_task
    transform_reviews_postgres_task >> load_reviews_postgres_task
