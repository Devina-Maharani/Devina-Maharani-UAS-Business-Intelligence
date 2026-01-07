import pandas as pd
import random
from datetime import datetime, timedelta

# --- Fungsi harga berdasarkan kategori ---
def assign_price_by_category(cat: str):
    cat = cat.lower()
    if 'mobile' in cat:
        return random.randint(1500000, 15000000)
    elif 'fridge freezer' in cat:
        return random.randint(5000000, 20000000)
    elif 'fridge' in cat:
        return random.randint(3000000, 15000000)
    elif 'washing machine' in cat:
        return random.randint(2000000, 10000000)
    elif 'dishwasher' in cat:
        return random.randint(3000000, 12000000)
    elif 'microwave' in cat:
        return random.randint(500000, 3000000)
    elif 'tv' in cat:
        return random.randint(1500000, 20000000)
    elif 'digital camera' in cat:
        return random.randint(1000000, 10000000)
    elif 'cpu' in cat:
        return random.randint(1000000, 8000000)
    else:
        return random.randint(500000, 5000000)

# --- Fungsi rating berdasarkan kategori ---
def assign_rating_by_category(cat: str):
    cat = cat.lower()
    if 'mobile' in cat:
        return round(random.uniform(4.4, 4.8), 1)
    elif 'fridge freezer' in cat or 'fridge' in cat:
        return round(random.uniform(4.0, 4.5), 1)
    elif 'washing machine' in cat or 'dishwasher' in cat:
        return round(random.uniform(3.8, 4.4), 1)
    elif 'microwave' in cat:
        return round(random.uniform(3.7, 4.3), 1)
    elif 'tv' in cat:
        return round(random.uniform(4.2, 4.7), 1)
    elif 'digital camera' in cat:
        return round(random.uniform(4.0, 4.6), 1)
    elif 'cpu' in cat:
        return round(random.uniform(4.1, 4.6), 1)
    else:
        return round(random.uniform(3.5, 4.5), 1)

# --- Fungsi tanggal acak ---
def generate_random_date(start, end):
    delta = end - start
    random_days = random.randint(0, delta.days)
    return (start + timedelta(days=random_days)).strftime('%Y-%m-%d %H:%M:%S')

# --- Baca file utama ---
df_product = pd.read_csv('star_output/dim_product.csv')

if 'category_name' not in df_product.columns:
    raise RuntimeError("❌ Kolom 'category_name' tidak ditemukan di dim_product.csv")

# --- Generate kolom harga dan rating ---
df_product['price'] = df_product['category_name'].fillna('').apply(assign_price_by_category)
df_product['rating'] = df_product['category_name'].fillna('').apply(assign_rating_by_category)

# --- Generate tanggal created_at dan updated_at ---
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 12, 31)
df_product['created_at'] = df_product.apply(
    lambda row: generate_random_date(start_date, end_date), axis=1
)
df_product['updated_at'] = df_product['created_at']

# --- Simpan ulang dim_product.csv ---
df_product.to_csv('star_output/dim_product.csv', index=False)
print("✅ dim_product.csv berhasil diperbarui dengan harga, rating, dan tanggal acak.")

# --- Update harga juga di fact_price_comparison.csv ---
df_fact = pd.read_csv('star_output/fact_price_comparison.csv')
df_fact = df_fact.drop('price', axis=1, errors='ignore')

# Normalisasi ID sebelum merge
df_fact['product_id'] = df_fact['product_id'].astype(str).str.strip()
df_product['product_id'] = df_product['product_id'].astype(str).str.strip()

df_fact = df_fact.merge(df_product[['product_id', 'price']], on='product_id', how='left')
df_fact.to_csv('star_output/fact_price_comparison.csv', index=False)
print("✅ fact_price_comparison.csv berhasil diperbarui dengan harga yang sama.")