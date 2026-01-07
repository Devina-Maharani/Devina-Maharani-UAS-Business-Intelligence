from django.db import models

# --- DIMENSI ---

class Product(models.Model):
    product_id = models.CharField(max_length=100, primary_key=True)
    product_title = models.CharField(max_length=255)
    product_description = models.TextField()
    product_url = models.URLField()
    image_url = models.URLField()
    price = models.FloatField()
    currency = models.CharField(max_length=10)
    rating = models.FloatField()
    is_active = models.BooleanField()
    created_at = models.DateTimeField()
    updated_at = models.DateTimeField()

    def __str__(self):
        return self.product_title

class Category(models.Model):
    category_id = models.CharField(max_length=100, primary_key=True)
    category_name = models.CharField(max_length=255)
    parent_category_id = models.CharField(max_length=100, null=True, blank=True)
    category_level = models.IntegerField()
    category_path = models.TextField()
    is_standardized = models.BooleanField()
    created_at = models.DateTimeField()
    updated_at = models.DateTimeField()

    def __str__(self):
        return self.category_name

class Merchant(models.Model):
    merchant_id = models.CharField(max_length=100, primary_key=True)
    merchant_name = models.CharField(max_length=255)
    merchant_rating = models.FloatField()
    merchant_type = models.CharField(max_length=50)
    merchant_url = models.URLField()
    merchant_location = models.CharField(max_length=255)
    joined_date = models.DateField()
    is_active = models.BooleanField()

    def __str__(self):
        return self.merchant_name

class Cluster(models.Model):
    cluster_id = models.CharField(max_length=100, primary_key=True)
    cluster_name = models.CharField(max_length=255)
    cluster_description = models.TextField()
    cluster_keywords = models.TextField()
    cluster_size = models.IntegerField()
    cluster_confidence = models.FloatField()
    created_at = models.DateTimeField()
    updated_at = models.DateTimeField()

    def __str__(self):
        return self.cluster_name

class Date(models.Model):
    date_id = models.IntegerField(primary_key=True)
    full_date = models.DateField()
    day_of_week = models.IntegerField()
    day_name = models.CharField(max_length=20)
    month = models.IntegerField()
    month_name = models.CharField(max_length=20)
    quarter = models.IntegerField()
    year = models.IntegerField()
    is_weekend = models.BooleanField()

    def __str__(self):
        return str(self.full_date)

class OfferQuality(models.Model):
    offer_quality_id = models.IntegerField(primary_key=True)
    quality_level = models.CharField(max_length=50)
    price_to_rating_ratio = models.FloatField()
    price_competitiveness = models.CharField(max_length=50)
    merchant_reliability_score = models.FloatField()
    description = models.TextField()

    def __str__(self):
        return self.quality_level

class DistributionStatus(models.Model):
    status_id = models.IntegerField(primary_key=True)
    status_name = models.CharField(max_length=50)
    status_description = models.TextField()
    recommended_action = models.TextField()
    color_code = models.CharField(max_length=10)

    def __str__(self):
        return self.status_name

# --- FAKTA ---

class ProductClassification(models.Model):
    product_classification_id = models.AutoField(primary_key=True)
    product = models.ForeignKey(Product, on_delete=models.CASCADE)
    category = models.ForeignKey(Category, on_delete=models.CASCADE)
    merchant = models.ForeignKey(Merchant, on_delete=models.CASCADE)
    cluster = models.ForeignKey(Cluster, on_delete=models.CASCADE)
    date = models.ForeignKey(Date, on_delete=models.CASCADE)
    classification_confidence = models.FloatField()
    is_validated = models.BooleanField()
    last_update_timestamp = models.DateTimeField()

class PriceComparison(models.Model):
    price_comparison_id = models.AutoField(primary_key=True)
    product = models.ForeignKey(Product, on_delete=models.CASCADE)
    merchant = models.ForeignKey(Merchant, on_delete=models.CASCADE)
    cluster = models.ForeignKey(Cluster, on_delete=models.CASCADE)
    category = models.ForeignKey(Category, on_delete=models.CASCADE)
    date = models.ForeignKey(Date, on_delete=models.CASCADE)
    offer_quality = models.ForeignKey(OfferQuality, on_delete=models.CASCADE)
    price = models.FloatField()
    price_difference_from_avg = models.FloatField()
    price_difference_percentage = models.FloatField()
    is_best_offer = models.BooleanField()
    price_rank_in_cluster = models.IntegerField()
    price_rank_in_category = models.IntegerField()
    price_timestamp = models.DateTimeField()

class ProductDistribution(models.Model):
    distribution_id = models.AutoField(primary_key=True)
    category = models.ForeignKey(Category, on_delete=models.CASCADE)
    cluster = models.ForeignKey(Cluster, on_delete=models.CASCADE)
    date = models.ForeignKey(Date, on_delete=models.CASCADE)
    status = models.ForeignKey(DistributionStatus, on_delete=models.CASCADE)
    total_products = models.IntegerField()
    total_merchants = models.IntegerField()
    avg_price = models.FloatField()
    min_price = models.FloatField()
    max_price = models.FloatField()
    price_std_deviation = models.FloatField()
    avg_rating = models.FloatField()
    duplicates_detected = models.IntegerField()
    miscategorized_count = models.IntegerField()
    last_updated = models.DateTimeField()
