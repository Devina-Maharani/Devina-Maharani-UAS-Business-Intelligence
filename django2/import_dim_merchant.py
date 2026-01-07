import os, django, pandas as pd
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'mysite.settings'); django.setup()
from bi.models import Merchant

df = pd.read_csv('./star_output/dim_merchant.csv')
for _, row in df.iterrows():
    Merchant.objects.update_or_create(
        merchant_id=row['merchant_id'],
        defaults={
            'merchant_name': row['merchant_name'],
            'merchant_rating': row['merchant_rating'],
            'merchant_type': row['merchant_type'],
            'merchant_url': row['merchant_url'],
            'merchant_location': row['merchant_location'],
            'joined_date': row['joined_date'],
            'is_active': row['is_active']
        })
print("✅ dim_merchant.csv selesai")