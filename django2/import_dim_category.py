import os, django, pandas as pd
from django.utils import timezone
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'mysite.settings'); django.setup()
from bi.models import Category

df = pd.read_csv('./star_output/dim_category.csv')
for _, row in df.iterrows():
    Category.objects.update_or_create(
        category_id=row['category_id'],
        defaults={
            'category_name': row['category_name'],
            'parent_category_id': row['parent_category_id'],
            'category_level': row['category_level'],
            'category_path': row['category_path'],
            'is_standardized': row['is_standardized'],
            'created_at': timezone.now(),
            'updated_at': timezone.now()
        })
print("✅ dim_category.csv selesai")