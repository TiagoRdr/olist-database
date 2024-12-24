import pandas as pd
import numpy as np

def transform_orders(**kwargs):
    df_orders = pd.read_csv("https://raw.githubusercontent.com/TiagoRdr/olist-database/refs/heads/main/olist_orders_dataset.csv")
    df_customers = pd.read_csv("https://raw.githubusercontent.com/TiagoRdr/olist-database/refs/heads/main/olist_customers_dataset.csv")

    df_orders_customers = df_orders.merge(df_customers, on='customer_id', how='left')
    df_orders_customers.drop(columns=["customer_unique_id", "customer_zip_code_prefix"], inplace=True)

    regioes = {
        'AC': 'Norte', 'AL': 'Nordeste', 'AP': 'Norte', 'AM': 'Norte', 'BA': 'Nordeste', 'CE': 'Nordeste',
        'DF': 'Centro-Oeste', 'ES': 'Sudeste', 'GO': 'Centro-Oeste', 'MA': 'Nordeste', 'MT': 'Centro-Oeste',
        'MS': 'Centro-Oeste', 'MG': 'Sudeste', 'PR': 'Sul', 'PB': 'Nordeste', 'PA': 'Norte', 'PE': 'Nordeste',
        'PI': 'Nordeste', 'RJ': 'Sudeste', 'RN': 'Nordeste', 'RS': 'Sul', 'RO': 'Norte', 'RR': 'Norte',
        'SC': 'Sul', 'SP': 'Sudeste', 'SE': 'Nordeste', 'TO': 'Norte'
    }

    df_orders_customers['customer_region'] = df_orders_customers['customer_state'].map(regioes)
    df_orders_customers['order_delivery_on_time_days'] = (
        pd.to_datetime(df_orders_customers['order_estimated_delivery_date']) - 
        pd.to_datetime(df_orders_customers['order_delivered_customer_date'])
    ).dt.days
    df_orders_customers['order_delivery_on_time'] = np.where(
        df_orders_customers['order_delivery_on_time_days'] > 1, 'Entregue no prazo', 'Entregue fora do prazo'
    )

    return df_orders_customers.to_json(orient='split')

def transform_reviews_postgres(**kwargs):    
    df_reviews = pd.read_csv("https://raw.githubusercontent.com/TiagoRdr/olist-database/refs/heads/main/olist_order_reviews_dataset.csv")
    df_reviews['positive_nagative_review'] = np.where(df_reviews['review_score'] > 3, 'Positivo', 'Negativo')
    df_reviews['review_time_answer_days'] = (
        pd.to_datetime(df_reviews['review_answer_timestamp']) - 
        pd.to_datetime(df_reviews['review_creation_date'])
    ).dt.days

    return df_reviews.to_json(orient='split')
