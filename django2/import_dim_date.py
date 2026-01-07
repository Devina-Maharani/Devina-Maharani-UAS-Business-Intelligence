import os, django, pandas as pd
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'mysite.settings'); django.setup()
from bi.models import Date

df = pd.read_csv('./star_output/dim_date.csv')
for _, row in df.iterrows():
    Date.objects.update_or_create(
        date_id=row['date_id'],
        defaults={
            'full_date': row['full_date'],
            'day_of_week': row['day_of_week'],
            'day_name': row['day_name'],
            'month': row['month'],
            'month_name': row['month_name'],
            'quarter': row['quarter'],
            'year': row['year'],
            'is_weekend': row['is_weekend']
        })
print("✅ dim_date.csv selesai")