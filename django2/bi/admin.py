from django.contrib import admin

from django.contrib import admin
from .models import *

admin.site.register(Product)
admin.site.register(Category)
admin.site.register(Merchant)
admin.site.register(Cluster)
admin.site.register(Date)
admin.site.register(OfferQuality)
admin.site.register(DistributionStatus)

admin.site.register(ProductClassification)
admin.site.register(PriceComparison)
admin.site.register(ProductDistribution)
