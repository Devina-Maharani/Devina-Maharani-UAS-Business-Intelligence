import os, django, pandas as pd
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'mysite.settings'); django.setup()
from bi.models import DistributionStatus

df = pd.read_csv('./star_output/dim_distribution_status.csv')
for _, row in df.iterrows():
    DistributionStatus.objects.update_or_create(
        status_id=row['status_id'],
        defaults={
            'status_name': row['status_name'],
            'status_description': row['status_description'],
            'recommended_action': row['recommended_action'],
            'color_code': row['color_code']
        })
print("✅ dim_distribution_status.csv selesai")