from django.urls import path
from .views import dashboard
from .views import analysis_price
from .views import chart_product_category_actual_predicted
from .views import chart_rating_actual_predicted
from .views import chart_avg_price_actual_predicted

urlpatterns = [
    path('dashboard/', dashboard, name='dashboard'),
    path('analysis/price/', analysis_price, name='analysis_price'),
    path('chart/category-actual-predicted/', chart_product_category_actual_predicted, name='category_actual_predicted'),
    path('chart/rating-actual-predicted/', chart_rating_actual_predicted, name='rating_actual_predicted'),
    path('chart/avg-price-actual-predicted/', chart_avg_price_actual_predicted, name='avg_price_actual_predicted'),
]