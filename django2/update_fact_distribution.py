import pandas as pd
from datetime import datetime
import os

# Path ke folder output ETL
base_path = os.path.join('star_output')

# Baca data dim_product & raw_data
dim_product = pd.read_csv(os.path.join(base_path, 'dim_product.csv'))
raw_data = pd.read_csv(os.path.join(base_path, 'raw_data.csv'))

# Gabungkan data dari raw_data dan dim_product
df = raw_data.copy()
df.columns = df.columns.str.strip()
dim_product['product_id'] = dim_product['product_id'].astype(str)
df['Product ID'] = df['Product ID'].astype(str)

# Gabung untuk ambil price dan rating
merged = df.merge(dim_product[['product_id', 'price', 'rating']], left_on='Product ID', right_on='product_id', how='left')

# Tambahkan kolom ID lainnya (dummy, tetap)
merged['category_id'] = merged['Category ID']
merged['cluster_id'] = merged['Cluster ID']
merged['date_id'] = 1
merged['status_id'] = 1

# Kelompokkan untuk fakta distribusi
fact = merged.groupby(['category_id', 'cluster_id', 'date_id', 'status_id']).agg(
    total_products=('Product ID', 'count'),
    total_merchants=('Merchant ID', pd.Series.nunique),
    avg_price=('price', 'mean'),
    min_price=('price', 'min'),
    max_price=('price', 'max'),
    price_std_deviation=('price', 'std'),
    avg_rating=('rating', 'mean'),
    duplicates_detected=('Product ID', lambda x: 0),
    miscategorized_count=('Product ID', lambda x: 0)
).reset_index()

# Kolom tambahan
fact['distribution_id'] = fact.index + 1
fact['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
# Ganti NaN jadi 0 (terutama untuk std deviation)
fact = fact.fillna({
    'price_std_deviation': 0,
    'avg_rating': 0,
    'avg_price': 0,
    'min_price': 0,
    'max_price': 0
})

# Urutkan kolom
fact = fact[[
    'distribution_id', 'category_id', 'cluster_id', 'date_id', 'status_id',
    'total_products', 'total_merchants', 'avg_price', 'min_price', 'max_price',
    'price_std_deviation', 'avg_rating', 'duplicates_detected',
    'miscategorized_count', 'last_updated'
]]

# Simpan ke CSV
fact.to_csv(os.path.join(base_path, 'fact_product_distribution.csv'), index=False)
print("✅ fact_product_distribution.csv berhasil diperbarui.")