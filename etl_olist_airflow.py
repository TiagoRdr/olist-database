from airflow import DAG
from datetime import datetime
import pandas as pd
import numpy as np
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook



def transform_orders():
    df_orders = pd.read_csv("https://raw.githubusercontent.com/TiagoRdr/olist-database/refs/heads/main/olist_orders_dataset.csv", sep=',')
    df_customers = pd.read_csv("https://raw.githubusercontent.com/TiagoRdr/olist-database/refs/heads/main/olist_customers_dataset.csv", sep=',')

    df_orders_customers = df_orders.merge(df_customers, on='customer_id', how='left')

    regioes = {
    'AC': 'Norte', 'AL': 'Nordeste', 'AP': 'Norte', 'AM': 'Norte', 'BA': 'Nordeste', 'CE': 'Nordeste',
    'DF': 'Centro-Oeste', 'ES': 'Sudeste', 'GO': 'Centro-Oeste', 'MA': 'Nordeste', 'MT': 'Centro-Oeste',
    'MS': 'Centro-Oeste', 'MG': 'Sudeste', 'PR': 'Sul', 'PB': 'Nordeste', 'PA': 'Norte', 'PE': 'Nordeste',
    'PI': 'Nordeste', 'RJ': 'Sudeste', 'RN': 'Nordeste', 'RS': 'Sul', 'RO': 'Norte', 'RR': 'Norte',
    'SC': 'Sul', 'SP': 'Sudeste', 'SE': 'Nordeste', 'TO': 'Norte'
    }

    df_orders_customers['customer_region'] = df_orders_customers['customer_state'].map(regioes)
    df_orders_customers['order_delivery_on_time_days'] = (df_orders_customers['order_estimated_delivery_date'] - df_orders_customers['order_delivered_customer_date']).dt.days
    df_orders_customers['order_delivery_on_time'] = np.where(df_orders_customers['order_delivery_on_time_days'] > 1, 
                                                        'Entregue fora do prazo', 
                                                        'Entregue no prazo')
    
    return df_orders_customers


# Defina a função para inserir dados
def insert_orders():
    postgres_hook = PostgresHook(postgres_conn_id='postgres-airflow')

    df_orders = pd.read_csv("https://raw.githubusercontent.com/TiagoRdr/olist-database/refs/heads/main/olist_orders_dataset.csv", sep=',')

    # Usando SQLAlchemy para inserir os dados no banco
    engine = postgres_hook.get_sqlalchemy_engine()

    # Inserir dados no PostgreSQL usando o SQLAlchemy engine
    df_orders.to_sql('olist_orders_dataset', con=engine, if_exists='append', index=False)



# Defina a função para inserir dados
def insert_order_items():
    postgres_hook = PostgresHook(postgres_conn_id='postgres-airflow')

    df_orders_items = pd.read_csv("https://raw.githubusercontent.com/TiagoRdr/olist-database/refs/heads/main/olist_order_items_dataset.csv", sep=',')

    # Usando SQLAlchemy para inserir os dados no banco
    engine = postgres_hook.get_sqlalchemy_engine()

    # Inserir dados no PostgreSQL usando o SQLAlchemy engine
    df_orders_items.to_sql('olist_order_items_dataset', con=engine, if_exists='append', index=False)


# Defina a função para inserir dados
def insert_products():
    postgres_hook = PostgresHook(postgres_conn_id='postgres-airflow')

    df_products = pd.read_csv("https://raw.githubusercontent.com/TiagoRdr/olist-database/refs/heads/main/olist_products_dataset.csv", sep=',')

    # Usando SQLAlchemy para inserir os dados no banco
    engine = postgres_hook.get_sqlalchemy_engine()

    # Inserir dados no PostgreSQL usando o SQLAlchemy engine
    df_products.to_sql('olist_products_dataset', con=engine, if_exists='append', index=False)


# Defina a função para inserir dados
def insert_reviews():
    postgres_hook = PostgresHook(postgres_conn_id='postgres-airflow')

    df_reviews = pd.read_csv("https://raw.githubusercontent.com/TiagoRdr/olist-database/refs/heads/main/olist_order_reviews_dataset.csv", sep=',')

    # Usando SQLAlchemy para inserir os dados no banco
    engine = postgres_hook.get_sqlalchemy_engine()

    # Inserir dados no PostgreSQL usando o SQLAlchemy engine
    df_reviews.to_sql('olist_order_reviews_dataset', con=engine, if_exists='append', index=False)



with DAG('load_data_postgres', start_date=datetime(2024, 12, 18), schedule_interval=None, catchup=False) as dag:

    create_table_orders = PostgresOperator(
        task_id = 'create_table_orders',
        postgres_conn_id = 'postgres-airflow',
        sql = """
            CREATE TABLE IF NOT EXISTS olist_orders_dataset (
            order_id VARCHAR(255) NOT NULL,
            customer_id VARCHAR(255) NOT NULL,
            order_status VARCHAR(50) NOT NULL,
            order_purchase_timestamp TIMESTAMP NOT NULL,
            order_approved_at TIMESTAMP DEFAULT NULL,
            order_delivered_carrier_date TIMESTAMP DEFAULT NULL,
            order_delivered_customer_date TIMESTAMP DEFAULT NULL,
            order_estimated_delivery_date TIMESTAMP NOT NULL,
            PRIMARY KEY (order_id)
            );""")
    
    create_table_orders_items = PostgresOperator(
        task_id = 'create_table_orders_items',
        postgres_conn_id = 'postgres-airflow',
        sql = """
            CREATE TABLE IF NOT EXISTS olist_order_items_dataset (
            order_id VARCHAR(255) NOT NULL,
            order_item_id INTEGER NOT NULL,
            product_id VARCHAR(255) NOT NULL,
            seller_id VARCHAR(255) NOT NULL,
            shipping_limit_date TIMESTAMP NOT NULL,
            price NUMERIC(10, 2) NOT NULL,
            freight_value NUMERIC(10, 2) NOT NULL,
            PRIMARY KEY (order_id, order_item_id)
            );""")
    

    create_table_products = PostgresOperator(
        task_id = 'create_table_products',
        postgres_conn_id = 'postgres-airflow',
        sql = """
            CREATE TABLE IF NOT EXISTS olist_products_dataset (
            product_id VARCHAR(255) NOT NULL,
            product_category_name VARCHAR(255) DEFAULT NULL,
            product_name_lenght INTEGER DEFAULT NULL,
            product_description_lenght INTEGER DEFAULT NULL,
            product_photos_qty INTEGER DEFAULT NULL,
            product_weight_g INTEGER DEFAULT NULL,
            product_length_cm INTEGER DEFAULT NULL,
            product_height_cm INTEGER DEFAULT NULL,
            product_width_cm INTEGER DEFAULT NULL,
            PRIMARY KEY (product_id)
            );
            """)
    

    create_table_reviews = PostgresOperator(
        task_id = 'create_table_reviews',
        postgres_conn_id = 'postgres-airflow',
        sql = """
            CREATE TABLE IF NOT EXISTS olist_order_reviews_dataset (
            review_id VARCHAR(255) DEFAULT NULL,
            order_id VARCHAR(255) DEFAULT NULL,
            review_score INTEGER DEFAULT NULL,
            review_comment_title VARCHAR(255) DEFAULT NULL,
            review_comment_message TEXT,
            review_creation_date DATE DEFAULT NULL,
            review_answer_timestamp TIMESTAMP DEFAULT NULL,
            column1 INTEGER DEFAULT NULL
            );
            """)
        

    # Task para inserir dados
    insert_orders = PythonOperator(
        task_id='insert_orders',
        python_callable=insert_orders
    )

    # Task para inserir dados
    insert_order_items = PythonOperator(
        task_id='insert_order_items',
        python_callable=insert_order_items
    )

    # Task para inserir dados
    insert_products = PythonOperator(
        task_id='insert_products',
        python_callable=insert_products
    )

    # Task para inserir dados
    insert_reviews = PythonOperator(
        task_id='insert_reviews',
        python_callable=insert_reviews
    )

    # Ordem de execução
    create_table_orders >> insert_orders
    create_table_orders_items >> insert_order_items 
    create_table_products >> insert_products 
    create_table_reviews >> insert_reviews 
