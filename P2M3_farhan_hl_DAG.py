'''
=================================================
Milestone 3

Nama  : Farhan Hamid Lubis
Batch : FTDS-052-RMT 

Program ini dibuat untuk melakukan automatisasi ekstrasi, transformasi dan loading data dari PostgreSQL ke ElasticSearch menggunakan Apache Airflow. Dataset yang dipakai adalah dataset mengenai penjualan e-commerce yang mencakup informasi order, produk, status pengiriman, channel penjualan, serta lokasi pelanggan.

Fitur-fitur:
- Fetch data dari PostgreSQL (table_m3)
- Data Cleaning: hapus duplikat, normalisasi kolom, handling missing values
- Simpan data clean ke CSV
- Load data clean ke Elasticsearch menggunakan bulk helper
=================================================
'''

# ================================================================
# IMPORT LIBRARY
# ================================================================
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


# ================================================================
# SETUP CONFIGURATION DAN DEFAULT ARGUMENTS
# ================================================================
# Default Arguments
default_args = {
    'owner'           : 'farhan_hamid_lubis',
    'depends_on_past' : False,
    'retries'         : 1,
    'retry_delay'     : timedelta(minutes=0.2),
}

# Koneksi - Konstanta
POSTGRES_CONFIG = {
    'host'    : 'postgres',
    'database': 'airflow',
    'user'    : 'airflow',
    'password': 'airflow',
    'port'    : 5432
}

ELASTICSEARCH_HOST = 'elasticsearch'
ELASTICSEARCH_PORT = 9200
INDEX_NAME         = 'ecommerce_sales_farhan'

RAW_CSV_PATH   = '/opt/airflow/dags/P2M3_farhan_hl_data_stages.csv'
CLEAN_CSV_PATH = '/opt/airflow/dags/P2M3_farhan_hl_data_clean.csv'


# ================================================================
# TASK 1: FETCH DATA DARI POSTGRES
# ================================================================
def fetch_from_postgresql(columns=None):
    '''
    Fungsi ini ditujukan untuk mengambil data dari PostgreSQL (table_m3) berdasarkan kolom-kolom yang ditentukan, lalu menyimpannya ke file CSV sebagai raw data.

    Parameters:
        columns : list of str (opsional)
                  - Daftar nama kolom yang ingin diambil dari table_m3.
                  - Jika tidak diisi (None), semua kolom akan diambil (SELECT *).
        Contoh: ['Order ID', 'Date', 'Status', 'Amount']

    Return:
        None, data disimpan langsung ke RAW_CSV_PATH sebagai file CSV.

    Contoh penggunaan:
        - Ambil semua kolom
          contoh: fetch_from_postgresql()

        - Ambil kolom tertentu saja
          contoh: fetch_from_postgresql(columns=['Order ID', 'Date', 'Status', 'Amount'])
    '''

    # Membuat koneksi ke PostgreSQL
    conn = psycopg2.connect(
        host     = POSTGRES_CONFIG['host'],
        database = POSTGRES_CONFIG['database'],
        user     = POSTGRES_CONFIG['user'],
        password = POSTGRES_CONFIG['password'],
        port     = POSTGRES_CONFIG['port']
    )

    # Menentukan kolom yang akan di-fetch
    if columns is None:
        select_clause = '*'
    else:
        # Wrap setiap nama kolom dengan double quote untuk handle spasi dan karakter khusus
        select_clause = ',\n'.join([f'"{col}"' for col in columns])
        print(select_clause)

    # Query dinamis berdasarkan kolom yang dipilih
    query = f'''
        SELECT 
            {select_clause}
        FROM table_m3;
    '''

    # Membaca data ke dalam DataFrame
    df = pd.read_sql(query, conn)

    # Menutup koneksi setelah data berhasil diambil
    conn.close()

    # Menyimpan data mentah ke file CSV tanpa modifikasi apapun
    df.to_csv(RAW_CSV_PATH, index=False)

    print(f'Berhasil mengambil {len(df)} baris data dari PostgreSQL.')
    print(f'Kolom yang diambil: {list(df.columns)}')
    print(f'Data raw disimpan ke: {RAW_CSV_PATH}')


# ================================================================
# Task 2: Data Cleaning
# ================================================================
def data_cleaning():
    '''
    Fungsi ini ditujukan untuk melakukan serangkaian proses Data Cleaning pada data yang telah diambil dari PostgreSQL, kemudian menyimpan hasilnya ke file CSV baru sebagai data yang telah bersih.

    Proses Data Cleaning yang dilakukan:
        1. Menghapus baris yang duplikat.
        2. Normalisasi nama kolom:
           - Semua nama kolom diubah menjadi lowercase.
           - Spasi di tengah nama kolom diubah menjadi underscore (_).
           - Menghapus spasi, tab, atau simbol yang tidak diperlukan.
        3. Handling Missing Values:
           - Kolom numerikal diisi dengan nilai median.
           - Kolom kategorikal diisi dengan nilai modus (nilai terbanyak).

    Parameters:
        Tidak ada parameter eksternal, fungsi ini menggunakan konstanta RAW_CSV_PATH dan CLEAN_CSV_PATH yang telah didefinisikan di bagian setup configuration.

    Return:
        None, data clean disimpan langsung ke CLEAN_CSV_PATH sebagai file CSV.

    Contoh penggunaan:
        data_cleaning()
    '''

    # Membaca data raw dari file CSV
    df = pd.read_csv(RAW_CSV_PATH)
    print(f'Data raw dimuat: {df.shape[0]} baris, {df.shape[1]} kolom.')

    # ----------------------------------------------------------
    # Step 1: Hapus Data Duplikat
    # ----------------------------------------------------------
    jumlah_sebelum = len(df)
    df.drop_duplicates(inplace=True)
    jumlah_sesudah = len(df)
    print(f'Duplikat dihapus: {jumlah_sebelum - jumlah_sesudah} baris.')

    # ----------------------------------------------------------
    # Step 2: Normalisasi Nama Kolom
    # ----------------------------------------------------------
    # Mengubah semua nama kolom menjadi lowercase
    df.columns = df.columns.str.lower()

    # Mengganti spasi di tengah nama kolom dengan underscore
    df.columns = df.columns.str.replace(' ', '_', regex=False)

    # Menghapus simbol, spasi/tab di awal-akhir, dan karakter tidak perlu
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace(r'[^\w]', '_', regex=True)
    df.columns = df.columns.str.replace(r'_+', '_', regex=True)
    df.columns = df.columns.str.strip('_')

    print(f'Nama kolom setelah normalisasi: {list(df.columns)}')

    # ----------------------------------------------------------
    # Step 3: Handling Missing Values
    # ----------------------------------------------------------
    # Kolom numerikal: isi dengan median
    num_cols = df.select_dtypes(include='number').columns
    for col in num_cols:
        missing = df[col].isnull().sum()
        if missing > 0:
            df[col].fillna(df[col].median(), inplace=True)
            print(f'Kolom numerikal "{col}": {missing} missing values diisi dengan median.')

    # Kolom kategorikal: isi dengan modus
    cat_cols = df.select_dtypes(include='object').columns
    for col in cat_cols:
        missing = df[col].isnull().sum()
        if missing > 0:
            df[col].fillna(df[col].mode()[0], inplace=True)
            print(f'Kolom kategorikal "{col}": {missing} missing values diisi dengan modus.')

    print('Handling Missing Values selesai.')

    # ----------------------------------------------------------
    # Simpan data clean ke CSV
    # ----------------------------------------------------------
    df.to_csv(CLEAN_CSV_PATH, index=False)
    print(f'Data clean disimpan ke: {CLEAN_CSV_PATH}')
    print(f'Jumlah data final: {df.shape[0]} baris, {df.shape[1]} kolom.')


# ================================================================
# Task 3: Post to Elasticsearch
# ================================================================
def generate_bulk_actions(df, index_name):
    '''
    Generator function yang menghasilkan action dict untuk setiap baris dataFrame agar dapat digunakan oleh elasticsearch bulk helper.

    Parameters:
        df         : pandas dataframe, data yang akan dimasukkan ke Elasticsearch
        index_name : string, nama index Elasticsearch yang menjadi tujuan

    Return:
        Generator of dict, setiap dict merepresentasikan satu dokumen dalam format yang dibutuhkan oleh bulk helper.

    Contoh penggunaan:
        actions = generate_bulk_actions(df, 'ecommerce_sales_farhan')
    '''
    for i, row in df.iterrows():
        yield {
            '_index' : index_name,
            '_id'    : i,
            '_source': row.to_dict()
        }


def post_to_elasticsearch():
    '''
    Fungsi ini ditujukan untuk membaca data clean dari file CSV dan memasukkannya ke dalam Elasticsearch secara bulk menggunakan elasticsearch bulk helper agar proses insert lebih cepat dan efisien dibandingkan insert satu per satu.

    Parameters:
        Tidak ada parameter eksternal, fungsi ini menggunakan konstanta CLEAN_CSV_PATH, ELASTICSEARCH_HOST, ELASTICSEARCH_PORT, dan INDEX_NAME yang telah didefinisikan sebelumnya.

    Return:
        None, data dimasukkan langsung ke Elasticsearch index.

    Contoh penggunaan:
        post_to_elasticsearch()
    '''

    # Membaca data clean
    df = pd.read_csv(CLEAN_CSV_PATH)
    print(f'Data clean dimuat: {len(df)} baris.')

    # Membuat koneksi ke Elasticsearch
    es = Elasticsearch(
        [{'host': ELASTICSEARCH_HOST, 'port': ELASTICSEARCH_PORT, 'scheme': 'http'}]
    )

    # Memastikan koneksi berhasil
    if not es.ping():
        raise ConnectionError('Tidak dapat terhubung ke Elasticsearch!')

    print('Berhasil terhubung ke Elasticsearch.')

    # Memasukkan data ke Elasticsearch secara bulk
    sukses, gagal = bulk(
        client  = es,
        actions = generate_bulk_actions(df, INDEX_NAME),
        stats_only = True
    )

    print(f'Berhasil memasukkan {sukses} dokumen ke index "{INDEX_NAME}".')
    if gagal:
        print(f'Gagal memasukkan {gagal} dokumen.')


# ================================================================
# Definisi DAG
# ================================================================
with DAG(
    dag_id            = 'P2M3_farhan_hl_DAG_final',
    description       = 'DAG untuk pipeline ETL data e-commerce: PostgreSQL → Cleaning → Elasticsearch',
    default_args      = default_args,
    schedule_interval = '10,20,30 9 * * 6',   # Setiap Sabtu jam 09:10, 09:20, 09:30
    start_date        = datetime(2024, 11, 1),  # Mulai 01 November 2024
    catchup           = False,
    tags              = ['milestone3', 'farhan', 'ecommerce', 'etl']
) as dag:

    # ----------------------------------------------------------
    # Task 1: Fetch from PostgreSQL
    # ----------------------------------------------------------
    
    # Fitur yang di-fetch beserta alasannya:
    # - Order ID           : Identifier unik setiap transaksi, digunakan sebagai primary key analisis
    # - Date               : Tanggal transaksi, digunakan untuk analisis tren penjualan dari waktu ke waktu
    # - Status             : Status order (Shipped, Cancelled, dll), penting untuk analisis tingkat keberhasilan order
    # - Fulfilment         : Metode fulfillment (Amazon/Merchant), untuk membandingkan performa antar metode pengiriman
    # - Sales Channel      : Channel penjualan (Amazon.in/Non-Amazon), untuk analisis efektivitas channel
    # - ship-service-level : Tingkat layanan pengiriman (Standard/Expedited), untuk analisis preferensi pengiriman pelanggan
    # - Category           : Kategori produk, kolom kunci untuk analisis performa penjualan per kategori
    # - Size               : Ukuran produk, untuk analisis distribusi permintaan berdasarkan ukuran
    # - Courier Status     : Status kurir pengiriman, untuk analisis performa logistik
    # - Qty                : Jumlah item per transaksi, untuk analisis volume penjualan
    # - Amount             : Nilai transaksi dalam INR, kolom utama untuk analisis revenue
    # - ship-city          : Kota tujuan pengiriman, untuk analisis distribusi geografis tingkat kota
    # - ship-state         : Provinsi/state tujuan pengiriman, untuk analisis distribusi geografis tingkat regional
    # - B2B                : Indikator transaksi Business-to-Business, untuk segmentasi pelanggan retail dan bisnis
    
    # Fitur yang tidak di-fetch beserta alasannya:
    # - currency           : tidak ada variasi nilai (semua INR), tidak memberikan insight analisis
    # - ship-country       : tidak ada variasi nilai (semua IN), tidak memberikan insight analisis
    # - fulfilled-by       : tidak ada variasi nilai, informasi sudah terwakili oleh kolom Fulfilment
    # - Unnamed: 22        : merupakan kolom kosong tanpa nilai yang berarti
    # - promotion-ids      : formatnya berisi multiple value yang dipisah koma sehingga menyebabkan inkonsistensi parsing CSV
    # - index              : merupakan artefak index pandas, bukan data bisnis
    # - ship-postal-code   : informasi geografisnya sudah terwakili oleh ship-city dan ship-state
    # - SKU                : terlalu granular (7195 unique values), tidak efektif untuk analisis agregat
    # - ASIN               : terlalu granular (7190 unique values), tidak efektif untuk analisis agregat
    # - Style              : terlalu granular (1377 unique values), sudah terwakili oleh kolom Category

    COLUMNS_TO_FETCH = [
        'Order ID',
        'Date',
        'Status',
        'Fulfilment',
        'Sales Channel ',
        'ship-service-level',
        'Category',
        'Size',
        'Courier Status',
        'Qty',
        'Amount',
        'ship-city',
        'ship-state',
        'B2B'
    ]
    task_fetch = PythonOperator(
        task_id         = 'Fetch_from_Postgresql',
        python_callable = fetch_from_postgresql,
        op_kwargs       = {'columns': COLUMNS_TO_FETCH},
    )

    # ----------------------------------------------------------
    # Task 2: Data Cleaning
    # ----------------------------------------------------------
    task_clean = PythonOperator(
        task_id         = 'Data_Cleaning',
        python_callable = data_cleaning,
    )

    # ----------------------------------------------------------
    # Task 3: Post to Elasticsearch
    # ----------------------------------------------------------
    task_post = PythonOperator(
        task_id         = 'Post_to_Elasticsearch',
        python_callable = post_to_elasticsearch,
    )

    # ----------------------------------------------------------
    # Urutan eksekusi task
    # ----------------------------------------------------------
    task_fetch >> task_clean >> task_post