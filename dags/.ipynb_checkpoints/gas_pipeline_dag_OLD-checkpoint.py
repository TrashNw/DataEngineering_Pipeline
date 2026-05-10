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
    
    devices = []
    for _ in range(100):
        devices.append({
            "device_id": str(uuid.uuid4()),
            "model_name": fake.random_element(["Itron-G4", "Itron-G6", "SmartGas-2000"]),
            "location_id": fake.zipcode(),
            "city": fake.city()
        })
    devices_df = pd.DataFrame(devices)
    
    consumption_data = []
    for _ in range(1000):
        dev = devices_df.sample(1).iloc[0]
        consumption_data.append({
            "reading_id": str(uuid.uuid4()),
            "device_id": dev["device_id"],
            "timestamp": fake.date_time_this_month(),
            "consumption": round(np.random.uniform(0.1, 5.0), 4),
            "status": fake.random_element(["OK", "LOW_BATTERY", "SIGNAL_LOSS"]),
            # metadata dict-et JSON stringgé alakítjuk, SQLite nem tud dict-et tárolni
            "metadata": '{"temp": 20, "pressure": 1.013}'
        })
    consumption_df = pd.DataFrame(consumption_data)

    devices_df.to_csv(f"s3://{BUCKET_NAME}/raw/devices.csv", storage_options=STORAGE_OPTIONS, index=False)
    consumption_df.to_parquet(f"s3://{BUCKET_NAME}/raw/readings.parquet", storage_options=STORAGE_OPTIONS, index=False)
    
    print("Siker: Adatok feltöltve a MinIO Landing Zone-ba.")

# --- 2. Transzformációs függvény (Warehouse: SQLite) ---
def run_transformations(db_path="/opt/airflow/data/gas_warehouse.sqlite"):
    import pandas as pd
    import sqlite3
    import os

    df_readings = pd.read_parquet(f"s3://{BUCKET_NAME}/raw/readings.parquet", storage_options=STORAGE_OPTIONS)
    df_devices = pd.read_csv(f"s3://{BUCKET_NAME}/raw/devices.csv", storage_options=STORAGE_OPTIONS)

    # 1. Adattisztítás
    df_readings['timestamp'] = pd.to_datetime(df_readings['timestamp'])
    df_readings = df_readings.dropna(subset=['consumption'])
    # SQLite nem kezeli a dict típust - JSON stringgé alakítás ha szükséges
    if 'metadata' in df_readings.columns:
        df_readings['metadata'] = df_readings['metadata'].astype(str)

    # 2. Dimenzió táblák
    df_time = pd.DataFrame()
    df_time['timestamp'] = df_readings['timestamp'].unique()
    df_time['timestamp'] = df_time['timestamp'].astype(str)  # SQLite kompatibilis formátum
    df_time['hour'] = pd.to_datetime(df_time['timestamp']).dt.hour
    df_time['day'] = pd.to_datetime(df_time['timestamp']).dt.day
    df_time['is_weekend'] = (pd.to_datetime(df_time['timestamp']).dt.dayofweek > 4).astype(int)  # SQLite bool = int

    # 3. Aggregáció
    df_daily = df_readings.copy()
    df_daily['date'] = df_daily['timestamp'].dt.date.astype(str)
    df_daily_agg = df_daily.groupby(['date', 'device_id'])['consumption'].sum().reset_index()

    # timestamp-ot stringgé az SQLite kompatibilitás miatt
    df_readings['timestamp'] = df_readings['timestamp'].astype(str)

    # 4. Betöltés SQLite-ba
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con = sqlite3.connect(db_path)
    
    df_devices.to_sql('dim_device', con, if_exists='replace', index=False)
    df_time.to_sql('dim_time', con, if_exists='replace', index=False)
    df_readings.to_sql('fact_gas_usage', con, if_exists='replace', index=False)
    df_daily_agg.to_sql('agg_daily_usage', con, if_exists='replace', index=False)
    
    con.close()
    print(f"Siker: Adatmodell (Csillag séma) elkészült a SQLite-ban: {db_path}")

# --- 3. Analitikai lekérdezések ---
def run_analytics(db_path="/opt/airflow/data/gas_warehouse.sqlite"):
    import sqlite3
    import pandas as pd
    import os

    con = sqlite3.connect(db_path)
    output_dir = "/opt/airflow/data/analytics"
    os.makedirs(output_dir, exist_ok=True)

    # --- 1. Lekérdezés: Top 10 legnagyobb fogyasztású eszköz ---
    q1 = """
        SELECT 
            f.device_id,
            d.model_name,
            d.city,
            ROUND(SUM(f.consumption), 4) AS total_consumption
        FROM fact_gas_usage f
        JOIN dim_device d ON f.device_id = d.device_id
        GROUP BY f.device_id, d.model_name, d.city
        ORDER BY total_consumption DESC
        LIMIT 10
    """
    df1 = pd.read_sql_query(q1, con)
    df1.to_csv(f"{output_dir}/top10_consumers.csv", index=False)
    print("1. lekérdezés kész: top10_consumers.csv")

    # --- 2. Lekérdezés: Napi átlagfogyasztás trendje ---
    q2 = """
        SELECT 
            date,
            ROUND(AVG(consumption), 4) AS avg_daily_consumption,
            COUNT(DISTINCT device_id)  AS active_devices
        FROM agg_daily_usage
        GROUP BY date
        ORDER BY date ASC
    """
    df2 = pd.read_sql_query(q2, con)
    df2.to_csv(f"{output_dir}/daily_trend.csv", index=False)
    print("2. lekérdezés kész: daily_trend.csv")

    # --- 3. Lekérdezés: Eszközállapot megoszlása (OK / HIBA) ---
    q3 = """
        SELECT 
            status,
            COUNT(*) AS event_count,
            ROUND(COUNT(*) * 100.0 / 
                (SELECT COUNT(*) FROM fact_gas_usage), 2) AS percentage
        FROM fact_gas_usage
        GROUP BY status
        ORDER BY event_count DESC
    """
    df3 = pd.read_sql_query(q3, con)
    df3.to_csv(f"{output_dir}/status_distribution.csv", index=False)
    print("3. lekérdezés kész: status_distribution.csv")

    con.close()
    print(f"Siker: Analytics CSV-k elmentve: {output_dir}")

# --- 4. Airflow DAG ---
with DAG(
    'gas_iot_pipeline_v2',
    default_args={'owner': 'airflow', 'start_date': datetime(2024, 1, 1)},
    schedule_interval='@daily',
    catchup=False
) as dag:

    t1 = PythonOperator(task_id='ingest_to_minio',       python_callable=generate_fake_data)
    t2 = PythonOperator(task_id='transform_to_duckdb',   python_callable=run_transformations)
    t3 = PythonOperator(task_id='run_analytics',         python_callable=run_analytics)

    t1 >> t2 >> t3
