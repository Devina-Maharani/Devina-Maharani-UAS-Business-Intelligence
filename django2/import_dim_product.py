import os, django, pandas as pd
from django.utils import timezone
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'mysite.settings'); django.setup()
from bi.models import Product

df = pd.read_csv('./star_output/dim_product.csv')
for _, row in df.iterrows():
    Product.objects.update_or_create(
        product_id=row['product_id'],
        defaults={
            'product_title': row['product_title'],
            'product_description': row['product_description'],
            'product_url': row['product_url'],
            'image_url': row['image_url'],
            'price': row['price'],
            'currency': row['currency'],
            'rating': row['rating'],
            'is_active': row['is_active'],
            'created_at': timezone.now(),
            'updated_at': timezone.now()
        })
print("✅ dim_product.csv selesai")