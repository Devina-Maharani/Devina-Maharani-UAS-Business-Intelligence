import os, django, pandas as pd
from django.utils import timezone
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'mysite.settings'); django.setup()
from bi.models import PriceComparison

df = pd.read_csv('./star_output/fact_price_comparison.csv')
for _, row in df.iterrows():
    PriceComparison.objects.update_or_create(
        price_comparison_id=row['price_comparison_id'],
        defaults={
            'product_id': row['product_id'],
            'merchant_id': row['merchant_id'],
            'cluster_id': row['cluster_id'],
            'category_id': row['category_id'],
            'date_id': row['date_id'],
            'offer_quality_id': row['offer_quality_id'],
            'price': row['price'],
            'price_difference_from_avg': row['price_difference_from_avg'],
            'price_difference_percentage': row['price_difference_percentage'],
            'is_best_offer': row['is_best_offer'],
            'price_rank_in_cluster': row['price_rank_in_cluster'],
            'price_rank_in_category': row['price_rank_in_category'],
            'price_timestamp': timezone.now()
        })
print("✅ fact_price_comparison.csv selesai")