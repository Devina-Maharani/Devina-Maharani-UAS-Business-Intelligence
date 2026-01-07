import os, django, pandas as pd
from django.utils import timezone
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'mysite.settings'); django.setup()
from bi.models import Cluster

df = pd.read_csv('./star_output/dim_cluster.csv')
for _, row in df.iterrows():
    Cluster.objects.update_or_create(
        cluster_id=row['cluster_id'],
        defaults={
            'cluster_name': row['cluster_name'],
            'cluster_description': row['cluster_description'],
            'cluster_keywords': row['cluster_keywords'],
            'cluster_size': row['cluster_size'],
            'cluster_confidence': row['cluster_confidence'],
            'created_at': timezone.now(),
            'updated_at': timezone.now()
        })
print("✅ dim_cluster.csv selesai")