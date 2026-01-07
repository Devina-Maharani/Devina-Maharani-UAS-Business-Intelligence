import os, django, pandas as pd
from django.utils import timezone
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'mysite.settings'); django.setup()
from bi.models import ProductClassification

df = pd.read_csv('./star_output/fact_product_classification.csv')
for _, row in df.iterrows():
    ProductClassification.objects.update_or_create(
        product_classification_id=row['product_classification_id'],
        defaults={
            'product_id': row['product_id'],
            'category_id': row['category_id'],
            'merchant_id': row['merchant_id'],
            'cluster_id': row['cluster_id'],
            'date_id': row['date_id'],
            'classification_confidence': row['classification_confidence'],
            'is_validated': row['is_validated'],
            'last_update_timestamp': timezone.now()
        })
print("✅ fact_product_classification.csv selesai")