import os
import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# MinIO elérési adatok (Docker hálózaton belül)
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "gas-data"

STORAGE_OPTIONS = {
    "key": MINIO_ACCESS_KEY,
    "secret": MINIO_ACCESS_KEY,
    "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}
}

# --- 1. Adatgeneráló függvény (Landing Zone: MinIO) ---
def generate_fake_data():
    import pandas as pd
    import numpy as np
    from faker import Faker
    import uuid

    fake = Faker()
    
    # Eszközök generálása
    devices = []
    for _ in range(100):
        devices.append({
            "device_id": str(uuid.uuid4()),
            "model_name": fake.random_element(["Itron-G4", "Itron-G6", "SmartGas-2000"]),
            "location_id": fake.zipcode(),
            "city": fake.city()
        })
    devices_df = pd.DataFrame(devices)
    
    # Fogyasztási adatok
    consumption_data = []
    for _ in range(1000):
        dev = devices_df.sample(1).iloc[0]
        consumption_data.append({
            "reading_id": str(uuid.uuid4()),
            "device_id": dev["device_id"],
            "timestamp": fake.date_time_this_month(),
            "consumption": round(np.random.uniform(0.1, 5.0), 4),
            "status": fake.random_element(["OK", "LOW_BATTERY", "SIGNAL_LOSS"]),
            "metadata": {"temp": 20, "pressure": 1.013} # Semi-structured field
        })
    consumption_df = pd.DataFrame(consumption_data)

    # Mentés a MinIO-ba (S3-ra)
    # A Pandas az s3:// protokollon keresztül közvetlenül a MinIO-ba ír
    devices_df.to_csv(f"s3://{BUCKET_NAME}/raw/devices.csv", storage_options=STORAGE_OPTIONS, index=False)
    consumption_df.to_parquet(f"s3://{BUCKET_NAME}/raw/readings.parquet", storage_options=STORAGE_OPTIONS, index=False)
    
    print("Siker: Adatok feltöltve a MinIO Landing Zone-ba.")

# --- 2. Transzformációs függvény (Warehouse: DuckDB) ---
def run_transformations(db_path="/opt/airflow/data/gas_warehouse.duckdb"):
    import pandas as pd
    import duckdb
    import os

    # Adatok beolvasása közvetlenül a MinIO-ból
    df_readings = pd.read_parquet(f"s3://{BUCKET_NAME}/raw/readings.parquet", storage_options=STORAGE_OPTIONS)
    df_devices = pd.read_csv(f"s3://{BUCKET_NAME}/raw/devices.csv", storage_options=STORAGE_OPTIONS)

    # 1. Adattisztítás
    df_readings['timestamp'] = pd.to_datetime(df_readings['timestamp'])
    df_readings = df_readings.dropna(subset=['consumption'])

    # 2. Dimenzió táblák létrehozása (Csillag séma)
    df_time = pd.DataFrame()
    df_time['timestamp'] = df_readings['timestamp'].unique()
    df_time['hour'] = df_time['timestamp'].dt.hour
    df_time['day'] = df_time['timestamp'].dt.day
    df_time['is_weekend'] = df_time['timestamp'].dt.dayofweek > 4

    # 3. Aggregáció (Napi fogyasztás eszközönként)
    df_daily = df_readings.copy()
    df_daily['date'] = df_daily['timestamp'].dt.date
    df_daily_agg = df_daily.groupby(['date', 'device_id'])['consumption'].sum().reset_index()

    # 4. Betöltés DuckDB-be
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con = duckdb.connect(db_path)
    
    con.execute("CREATE OR REPLACE TABLE dim_device AS SELECT * FROM df_devices")
    con.execute("CREATE OR REPLACE TABLE dim_time AS SELECT * FROM df_time")
    con.execute("CREATE OR REPLACE TABLE fact_gas_usage AS SELECT * FROM df_readings")
    con.execute("CREATE OR REPLACE TABLE agg_daily_usage AS SELECT * FROM df_daily_agg")
    
    con.close()
    print(f"Siker: Adatmodell (Csillag séma) elkészült a DuckDB-ben.")

# --- 3. Airflow DAG ---
with DAG(
    'gas_iot_pipeline_v2',
    default_args={'owner': 'airflow', 'start_date': datetime(2024, 1, 1)},
    schedule_interval='@daily',
    catchup=False
) as dag:

    t1 = PythonOperator(task_id='ingest_to_minio', python_callable=generate_fake_data)
    t2 = PythonOperator(task_id='transform_to_duckdb', python_callable=run_transformations)

    t1 >> t2