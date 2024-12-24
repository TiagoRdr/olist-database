import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

def load_orders_postgres(**kwargs):
    ti = kwargs['ti']
    df_orders_customers_json = ti.xcom_pull(task_ids='transform_orders')
    df_orders_customers = pd.read_json(df_orders_customers_json, orient='split')

    postgres_hook = PostgresHook(postgres_conn_id='postgres-airflow')
    engine = postgres_hook.get_sqlalchemy_engine()
    df_orders_customers.to_sql('olist_orders_dataset', con=engine, if_exists='append', index=False)

def load_order_items_postgres():
    postgres_hook = PostgresHook(postgres_conn_id='postgres-airflow')
    df_orders_items = pd.read_csv("https://raw.githubusercontent.com/TiagoRdr/olist-database/refs/heads/main/olist_order_items_dataset.csv")
    engine = postgres_hook.get_sqlalchemy_engine()
    df_orders_items.to_sql('olist_order_items_dataset', con=engine, if_exists='append', index=False)

def load_products_postgres():
    postgres_hook = PostgresHook(postgres_conn_id='postgres-airflow')
    df_products = pd.read_csv("https://raw.githubusercontent.com/TiagoRdr/olist-database/refs/heads/main/olist_products_dataset.csv")
    engine = postgres_hook.get_sqlalchemy_engine()
    df_products.to_sql('olist_products_dataset', con=engine, if_exists='append', index=False)

def load_reviews_postgres(**kwargs):
    ti = kwargs['ti']
    df_reviews_json = ti.xcom_pull(task_ids='transform_reviews_postgres')
    df_reviews = pd.read_json(df_reviews_json, orient='split')

    postgres_hook = PostgresHook(postgres_conn_id='postgres-airflow')
    engine = postgres_hook.get_sqlalchemy_engine()
    df_reviews.to_sql('olist_order_reviews_dataset', con=engine, if_exists='append', index=False)
