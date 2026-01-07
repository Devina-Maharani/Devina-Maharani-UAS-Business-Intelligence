import os, django, pandas as pd
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'mysite.settings'); django.setup()
from bi.models import OfferQuality

df = pd.read_csv('./star_output/dim_offer_quality.csv')
for _, row in df.iterrows():
    OfferQuality.objects.update_or_create(
        offer_quality_id=row['offer_quality_id'],
        defaults={
            'quality_level': row['quality_level'],
            'price_to_rating_ratio': row['price_to_rating_ratio'],
            'price_competitiveness': row['price_competitiveness'],
            'merchant_reliability_score': row['merchant_reliability_score'],
            'description': row['description']
        })
print("✅ dim_offer_quality.csv selesai")
