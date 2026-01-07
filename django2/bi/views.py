from django.shortcuts import render
import pandas as pd
from django.conf import settings
import os
import numpy as np
from sklearn.linear_model import LinearRegression

def analysis_price(request):
    path = os.path.join(settings.BASE_DIR, 'star_output', 'dim_product.csv')
    df = pd.read_csv(path)

    # Pastikan kolom waktu dan harga valid
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df = df.dropna(subset=['created_at', 'price'])

    # Buat kolom bulanan
    df['year_month'] = df['created_at'].dt.to_period('M').astype(str)

    # Hitung rata-rata harga per bulan
    df_summary = df.groupby('year_month')['price'].mean().reset_index()

    # Konversi index jadi fitur numerik untuk regresi
    df_summary['time_index'] = np.arange(len(df_summary)).reshape(-1, 1)

    # Siapkan data X dan y
    X = df_summary['time_index'].values.reshape(-1, 1)
    y = df_summary['price'].values

    # Model linear regression
    model = LinearRegression()
    model.fit(X, y)

    # Prediksi
    df_summary['predicted_price'] = model.predict(X)

    context = {
        'labels': df_summary['year_month'].tolist(),
        'actual': df_summary['price'].round().astype(int).tolist(),
        'predicted': df_summary['predicted_price'].round().astype(int).tolist()
    }

    return render(request, 'average_price_chart.html', context)

def chart_product_category_actual_predicted(request):
    path = os.path.join(settings.BASE_DIR, 'star_output', 'dim_product.csv')
    df = pd.read_csv(path)

    # Pastikan kolom ada
    if 'category_name' not in df.columns:
        return render(request, 'bi/error.html', {'message': 'Kolom category_name tidak ditemukan'})

    # Hitung jumlah produk aktual per kategori
    actual_df = df.groupby('category_name')['product_id'].count().reset_index()
    actual_df.columns = ['category_name', 'actual_total']

    # Urutkan berdasarkan jumlah produk terbanyak
    actual_df = actual_df.sort_values(by='actual_total', ascending=False).reset_index(drop=True)

    # Ambil top 10 kategori
    top_n = actual_df.head(10).copy()

    # Buat time_index (0 - 9) untuk prediksi
    top_n['index'] = np.arange(len(top_n)).reshape(-1, 1)

    # Siapkan model regresi
    model = LinearRegression()
    model.fit(top_n['index'].values.reshape(-1, 1), top_n['actual_total'].values)

    # Prediksi jumlah produk
    top_n['predicted_total'] = model.predict(top_n['index'].values.reshape(-1, 1)).round().astype(int)

    context = {
        'labels': top_n['category_name'].tolist(),
        'actual': top_n['actual_total'].tolist(),
        'predicted': top_n['predicted_total'].tolist()
    }

    return render(request, 'chart_product_category_actual_predicted.html', context)

def chart_rating_actual_predicted(request):
    path = os.path.join(settings.BASE_DIR, 'star_output', 'dim_product.csv')
    df = pd.read_csv(path)

    # Pastikan kolom waktu dan rating valid
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df = df.dropna(subset=['created_at', 'rating'])

    # Ambil bulan dari created_at
    df['year_month'] = df['created_at'].dt.to_period('M').astype(str)

    # Hitung rata-rata rating per bulan
    df_summary = df.groupby('year_month')['rating'].mean().reset_index()
    df_summary.columns = ['month', 'avg_rating']

    # Ubah 'month' menjadi fitur numerik (jumlah bulan sejak bulan pertama)
    df_summary['month_num'] = pd.to_datetime(df_summary['month']).dt.month + 12 * (pd.to_datetime(df_summary['month']).dt.year - pd.to_datetime(df_summary['month']).dt.year.min())

    # Model regresi linear untuk prediksi
    X = df_summary['month_num'].values.reshape(-1, 1)  # Menggunakan bulan sebagai fitur numerik
    y = df_summary['avg_rating'].values

    model = LinearRegression()
    model.fit(X, y)

    # Prediksi
    df_summary['predicted_rating'] = model.predict(X).round(2)

    context = {
        'labels': df_summary['month'].tolist(),
        'actual': df_summary['avg_rating'].round(2).tolist(),
        'predicted': df_summary['predicted_rating'].tolist(),
    }

    return render(request, 'chart_rating_actual_predicted.html', context)

def chart_avg_price_actual_predicted(request):
    path = os.path.join(settings.BASE_DIR, 'star_output', 'dim_product.csv')
    df = pd.read_csv(path)

    # Validasi data
    if 'category_name' not in df.columns or 'price' not in df.columns:
        return render(request, 'bi/error.html', {'message': 'Kolom category_name atau price tidak ditemukan'})

    # Grouping harga rata-rata aktual per kategori
    df_actual = df.groupby('category_name')['price'].mean().reset_index()
    df_actual.columns = ['category', 'actual_avg_price']

    # Sort dan buat indeks untuk regresi
    df_actual = df_actual.sort_values(by='actual_avg_price', ascending=False).reset_index(drop=True)
    df_actual['index'] = np.arange(len(df_actual))

    # Linear Regression untuk prediksi harga
    model = LinearRegression()
    X = df_actual['index'].values.reshape(-1, 1)
    y = df_actual['actual_avg_price'].values
    model.fit(X, y)
    df_actual['predicted_avg_price'] = model.predict(X).round()

    context = {
        'labels': df_actual['category'].tolist(),
        'actual': df_actual['actual_avg_price'].astype(int).tolist(),
        'predicted': df_actual['predicted_avg_price'].astype(int).tolist(),
    }

    return render(request, 'chart_avg_price_actual_predicted.html', context)

def dashboard(request):
    return render(request, 'dashboard.html')
