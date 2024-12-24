from airflow.providers.postgres.hooks.postgres import PostgresHook

def create_tables(postgres_conn_id='postgres-airflow'):
    sql_create_tables = """
        CREATE TABLE IF NOT EXISTS olist_orders_dataset (
            order_id VARCHAR(255) NOT NULL,
            customer_id VARCHAR(255) NOT NULL,
            customer_city VARCHAR(255) NOT NULL,
            customer_state VARCHAR(255) NOT NULL,
            order_status VARCHAR(50) NOT NULL,
            order_purchase_timestamp TIMESTAMP NOT NULL,
            order_approved_at TIMESTAMP DEFAULT NULL,
            order_delivered_carrier_date TIMESTAMP DEFAULT NULL,
            order_delivered_customer_date TIMESTAMP DEFAULT NULL,
            order_estimated_delivery_date TIMESTAMP DEFAULT NULL,
            customer_region VARCHAR(50) DEFAULT NULL,
            order_delivery_on_time_days INTEGER DEFAULT NULL,
            order_delivery_on_time VARCHAR(50) DEFAULT NULL,
            PRIMARY KEY (order_id)
        );
        CREATE TABLE IF NOT EXISTS olist_order_items_dataset (
            order_id VARCHAR(255) NOT NULL,
            order_item_id INTEGER NOT NULL,
            product_id VARCHAR(255) NOT NULL,
            seller_id VARCHAR(255) NOT NULL,
            shipping_limit_date TIMESTAMP NOT NULL,
            price NUMERIC(10, 2) NOT NULL,
            freight_value NUMERIC(10, 2) NOT NULL,
            PRIMARY KEY (order_id, order_item_id)
        );
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
        CREATE TABLE IF NOT EXISTS olist_order_reviews_dataset (
            review_id VARCHAR(255) DEFAULT NULL,
            order_id VARCHAR(255) DEFAULT NULL,
            review_score INTEGER DEFAULT NULL,
            review_comment_title VARCHAR(255) DEFAULT NULL,
            review_comment_message TEXT,
            review_creation_date DATE DEFAULT NULL,
            review_answer_timestamp TIMESTAMP DEFAULT NULL,
            positive_nagative_review VARCHAR(30) DEFAULT NULL,
            review_time_answer_days INTEGER DEFAULT NULL
        );
    """
    
    # Conectando ao banco de dados PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    
    # Executando o SQL para criar as tabelas
    cursor.execute(sql_create_tables)
    connection.commit()
    cursor.close()
    connection.close()
