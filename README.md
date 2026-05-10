# IoT Gázóra Adatfeldolgozó Pipeline

>  A projekt célja szimulált Itron smart meter adatok begyűjtése, transzformációja és vizualizációja egy teljes Data Engineering pipeline segítségével.
## Architektúra áttekintés

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DOCKER COMPOSE STACK                         │
│                                                                     │
│  ┌──────────────┐    ┌──────────────┐    ┌───────────────────────┐  │
│  │   Airflow    │    │    MinIO     │    │       SQLite          │  │
│  │  (Orchestr.) │───▶│  Landing     │───▶│  gas_warehouse.sqlite│  │
│  │  :8080       │    │  Zone :9000  │    │  (Warehouse)          │  │
│  └──────────────┘    └──────────────┘    └───────────────────────┘  │
│         │                                          │                │
│         │ DAG futás                                │ SQL lekérdezés │
│         ▼                                          ▼                │
│  ┌──────────────┐                        ┌──────────────────────┐   │
│  │   Jupyter    │                        │      Metabase        │   │
│  │  :8888       │                        │      :3000           │   │
│  └──────────────┘                        └──────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘

Pipeline lépései:
  [1] generate_fake_data  ->  MinIO (raw/devices.csv, raw/readings.parquet) "Bronz"
  [2] transform_to_sqlite ->  SQLite (dim_device, dim_time, fact_gas_usage, agg_daily_usage) "Silver" 
  [3] run_analytics       ->  CSV riportok (data/analytics/*.csv) "Gold"
  [4] metabase            ->  Kiszolgálás
```

##  Pipeline elemei

| Komponens | Eszköz | Port |
|---|---|---|
| Orchestráció | Apache Airflow 2.9.2 | 8080 |
| Landing Zone | MinIO (S3-kompatibilis) | 9000 / 9001 |
| Adatbázis (Airflow) | PostgreSQL 16 | - |
| Adattárház | SQLite | - |
| Vizualizáció | Metabase | 3000 |
| Fejlesztés / exploráció | Jupyter Notebook | 8888 |

## Futtatás

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- Docker Compose v2
- Windows / macOS / Linux

## Telepítés és indítás

### 1. Repository klónozása

```bash
git clone https://github.com/TrashNw/DataEngineering_Pipeline
cd DataEngineering_Pipeline
```

### 2. Stack indítása

```bash
docker compose up -d
```

### 3. Szolgáltatások elérhetősége

| Szolgáltatás | URL | Bejelentkezés |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Metabase | http://localhost:3000 | (első indításkor kell bejelentkezni) |
| Jupyter | http://localhost:8888 | Token: `homework` |

## A pipeline futtatása

### Airflow DAG manuális indítása

1. Nyisd meg az Airflow UI-t: http://localhost:8080
2. Keresd meg a `gas_iot_pipeline_v2` DAG-ot
3. Kattints a **Trigger DAG** gombra
4. Kövesd a futást a **Graph** nézetben

### A pipeline 3 lépése

```
ingest_to_minio  →  transform_to_sqlite  →  run_analytics
```

| Task | Leírás | Kimenet |
|---|---|---|
| `ingest_to_minio` | 100 eszköz + 1000 mérés generálása Faker-rel | MinIO: `raw/devices.csv`, `raw/readings.parquet` |
| `transform_to_sqlite` | Adat tisztítás, csillag séma felépítése | `data/gas_warehouse.sqlite` |
| `run_analytics` | 3 analitikai lekérdezés futtatása | `data/analytics/*.csv` |

## Metabase beállítása
1. Nyisd meg: http://localhost:3000
2. **Add Database** → **SQLite**
3. Database file: `/home/metabase/data/gas_warehouse.sqlite`

## Analitikai kimenetek

A pipeline futása után a `./data/analytics/` mappában találhatók:

| Fájl | Tartalom |
|---|---|
| `top10_consumers.csv` | Top 10 legnagyobb fogyasztású eszköz városonként |
| `daily_trend.csv` | Napi átlagfogyasztás és aktív eszközök száma |
| `status_distribution.csv` | OK / LOW_BATTERY / SIGNAL_LOSS megoszlás (%) |

---

## Stack leállítása

```bash
docker compose down
```

## Projektstruktúra

```
DataEngineering_Pipeline/
├── dags/
│   ├── scripts/
│   │    └(Beta pipelines in notebooks)
│   └── gas_pipeline.py        # Airflow DAG (3 task)
├── data/
│   ├── gas_warehouse.sqlite   # Adattárház (generálódik)
│   ├── analytics/             # CSV riportok (generálódnak)
│   └── diagrams/              # Metabase diagrammok
├── logs/                      # Airflow logok
├── minio_data/                # MinIO tároló
├── metabase_plugins/          # DuckDB driver JAR (nem releváns)
├── docker-compose.yml
├── Technikai Dokumentáció.md
└── README.md
```