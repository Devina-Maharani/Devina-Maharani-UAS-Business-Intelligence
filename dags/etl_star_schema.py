from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

# Path setup
dag_path = os.path.dirname(__file__)
output_path = os.path.join(dag_path, 'star_output')
os.makedirs(output_path, exist_ok=True)


def extract():
    df = pd.read_csv(os.path.join(dag_path, 'pricerunner_aggregate.csv'))
    df.columns = df.columns.str.strip()
    df.to_csv(os.path.join(output_path, 'raw_data.csv'), index=False)


def transform():
    df = pd.read_csv(os.path.join(output_path, 'raw_data.csv'))
    df.columns = df.columns.str.strip()

    # Tambahkan ID dimensi dan dummy kolom
    df['product_id'] = df['Product ID']
    df['category_id'] = df['Category ID']
    df['merchant_id'] = df['Merchant ID']
    df['cluster_id'] = df['Cluster ID']
    df['date_id'] = 1
    df['status_id'] = 1
    df['offer_quality_id'] = 1

    # --- DIMENSI --- (dim_product, dim_category, dim_merchant, dim_cluster, dim_date, dim_offer_quality, dim_distribution_status)
    dim_product = df[['product_id', 'Product Title']].drop_duplicates()
    dim_product['product_description'] = 'N/A'
    dim_product['product_url'] = 'https://example.com/product'
    dim_product['image_url'] = 'https://example.com/image.jpg'
    dim_product['price'] = 100000.0
    dim_product['currency'] = 'IDR'
    dim_product['rating'] = 4.0
    dim_product['is_active'] = True
    dim_product['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    dim_product['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    dim_product.columns = [
        'product_id', 'product_title', 'product_description', 'product_url',
        'image_url', 'price', 'currency', 'rating', 'is_active',
        'created_at', 'updated_at'
    ]
    dim_product.to_csv(os.path.join(output_path, 'dim_product.csv'), index=False)

    dim_category = df[['category_id', 'Category Label']].drop_duplicates()
    dim_category.columns = ['category_id', 'category_name']
    dim_category['parent_category_id'] = None
    dim_category['category_level'] = 1
    dim_category['category_path'] = dim_category['category_name']
    dim_category['is_standardized'] = True
    dim_category['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    dim_category['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    dim_category.to_csv(os.path.join(output_path, 'dim_category.csv'), index=False)

    dim_merchant = df[['merchant_id']].drop_duplicates()
    dim_merchant['merchant_name'] = 'Merchant ' + dim_merchant['merchant_id'].astype(str)
    dim_merchant['merchant_rating'] = 4.5
    dim_merchant['merchant_type'] = 'resmi'
    dim_merchant['merchant_url'] = 'https://example.com/store'
    dim_merchant['merchant_location'] = 'Indonesia'
    dim_merchant['joined_date'] = datetime.now().strftime('%Y-%m-%d')
    dim_merchant['is_active'] = True
    dim_merchant.to_csv(os.path.join(output_path, 'dim_merchant.csv'), index=False)

# --- DIM CLUSTER (Pakai data mentah, jaminan benar) ---
    dim_cluster = df[['Cluster ID', 'Cluster Label']].drop_duplicates()
    dim_cluster.columns = ['cluster_id', 'cluster_name']

# Hindari pakai groupby-transform, langsung hitung size pakai dataframe terpisah
    cluster_sizes = df.groupby('cluster_id')['product_id'].count().reset_index()
    cluster_sizes.columns = ['cluster_id', 'cluster_size']

# Gabungkan ukuran cluster
    dim_cluster = dim_cluster.merge(cluster_sizes, on='cluster_id', how='left')

# Tambahkan kolom lain
    dim_cluster['cluster_description'] = 'Produk: ' + dim_cluster['cluster_name']
    dim_cluster['cluster_keywords'] = 'otomatis'
    dim_cluster['cluster_confidence'] = 95.0
    dim_cluster['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    dim_cluster['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Simpan hasil
    dim_cluster.to_csv(os.path.join(output_path, 'dim_cluster.csv'), index=False)

    dim_date = pd.DataFrame([{
        'date_id': 1,
        'full_date': datetime.today().strftime('%Y-%m-%d'),
        'day_of_week': datetime.today().isoweekday(),
        'day_name': datetime.today().strftime('%A'),
        'month': datetime.today().month,
        'month_name': datetime.today().strftime('%B'),
        'quarter': (datetime.today().month - 1) // 3 + 1,
        'year': datetime.today().year,
        'is_weekend': datetime.today().isoweekday() in [6, 7]
    }])
    dim_date.to_csv(os.path.join(output_path, 'dim_date.csv'), index=False)

    dim_offer_quality = pd.DataFrame([{
        'offer_quality_id': 1,
        'quality_level': 'Baik',
        'price_to_rating_ratio': 25000.0,
        'price_competitiveness': 'Kompetitif',
        'merchant_reliability_score': 90.0,
        'description': 'Penawaran baik dengan merchant terpercaya'
    }])
    dim_offer_quality.to_csv(os.path.join(output_path, 'dim_offer_quality.csv'), index=False)

    dim_distribution_status = pd.DataFrame([{
        'status_id': 1,
        'status_name': 'Normal',
        'status_description': 'Distribusi produk stabil',
        'recommended_action': 'Tidak ada tindakan',
        'color_code': '#00FF00'
    }])
    dim_distribution_status.to_csv(os.path.join(output_path, 'dim_distribution_status.csv'), index=False)

    # --- FAKTA 1: fact_product_classification ---
    fact1 = df[['product_id', 'category_id', 'merchant_id', 'cluster_id', 'date_id']].copy()
    fact1['product_classification_id'] = fact1.index + 1
    fact1['classification_confidence'] = 100.0
    fact1['is_validated'] = True
    fact1['last_update_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    fact1 = fact1[['product_classification_id', 'product_id', 'category_id', 'merchant_id', 'cluster_id', 'date_id',
                   'classification_confidence', 'is_validated', 'last_update_timestamp']]
    fact1.to_csv(os.path.join(output_path, 'fact_product_classification.csv'), index=False)

    # --- FAKTA 2: fact_price_comparison ---
    fact2 = df[['product_id', 'merchant_id', 'cluster_id', 'category_id', 'date_id', 'offer_quality_id']].copy()
    fact2['price_comparison_id'] = fact2.index + 1
    fact2['price'] = 100000.0
    fact2['price_difference_from_avg'] = 0.0
    fact2['price_difference_percentage'] = 0.0
    fact2['is_best_offer'] = True
    fact2['price_rank_in_cluster'] = 1
    fact2['price_rank_in_category'] = 1
    fact2['price_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    fact2 = fact2[['price_comparison_id', 'product_id', 'merchant_id', 'cluster_id', 'category_id', 'date_id', 'offer_quality_id',
                   'price', 'price_difference_from_avg', 'price_difference_percentage', 'is_best_offer',
                   'price_rank_in_cluster', 'price_rank_in_category', 'price_timestamp']]
    fact2.to_csv(os.path.join(output_path, 'fact_price_comparison.csv'), index=False)

    # --- FAKTA 3: fact_product_distribution ---
    grouped = df.groupby(['category_id', 'cluster_id', 'date_id', 'status_id']).agg(
        total_products=('Product ID', 'count'),
        total_merchants=('Merchant ID', pd.Series.nunique),
        avg_price=('Product ID', lambda x: 100000.0),
        min_price=('Product ID', lambda x: 95000.0),
        max_price=('Product ID', lambda x: 105000.0),
        price_std_deviation=('Product ID', lambda x: 2000.0),
        avg_rating=('Product ID', lambda x: 4.2),
        duplicates_detected=('Product ID', lambda x: 0),
        miscategorized_count=('Product ID', lambda x: 0)
    ).reset_index()
    grouped['distribution_id'] = grouped.index + 1
    grouped['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    grouped = grouped[['distribution_id', 'category_id', 'cluster_id', 'date_id', 'status_id',
                       'total_products', 'total_merchants', 'avg_price', 'min_price', 'max_price',
                       'price_std_deviation', 'avg_rating', 'duplicates_detected',
                       'miscategorized_count', 'last_updated']]
    grouped.to_csv(os.path.join(output_path, 'fact_product_distribution.csv'), index=False)


def load():
    print("Star Schema ETL complete. Files saved in:", output_path)


with DAG(
    dag_id='etl_star_schema',
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['star_schema', 'etl', 'ecommerce']
) as dag:

    t1 = PythonOperator(
        task_id='extract',
        python_callable=extract
    )

    t2 = PythonOperator(
        task_id='transform',
        python_callable=transform
    )

    t3 = PythonOperator(
        task_id='load',
        python_callable=load
    )

    t1 >> t2 >> t3
