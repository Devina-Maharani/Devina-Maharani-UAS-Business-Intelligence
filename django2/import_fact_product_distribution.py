import os, django, pandas as pd
from django.utils import timezone
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'mysite.settings'); django.setup()
from bi.models import ProductDistribution

df = pd.read_csv('./star_output/fact_product_distribution.csv')
for _, row in df.iterrows():
    ProductDistribution.objects.update_or_create(
        distribution_id=row['distribution_id'],
        defaults={
            'category_id': row['category_id'],
            'cluster_id': row['cluster_id'],
            'date_id': row['date_id'],
            'status_id': row['status_id'],
            'total_products': row['total_products'],
            'total_merchants': row['total_merchants'],
            'avg_price': row['avg_price'],
            'min_price': row['min_price'],
            'max_price': row['max_price'],
            'price_std_deviation': row['price_std_deviation'],
            'avg_rating': row['avg_rating'],
            'duplicates_detected': row['duplicates_detected'],
            'miscategorized_count': row['miscategorized_count'],
            'last_updated': timezone.now()
        })
print("✅ fact_product_distribution.csv selesai")